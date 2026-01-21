#include <cassert>
#include <spdlog/spdlog.h>
#include "exception.hpp"
#include "messaging.hpp"
#include "utils.hpp"
#include "user.hpp"
#include "context.hpp"
#include "session.hpp"
#include "tasks.hpp"

// maximum message size (in bytes) for a non logged user
#define MAX_MSG_BYTES_FROM_NO_USER 1024
// maximum time (in millis) between login and load messages reception
#define TIMEOUT_NO_USER 5000

#define MAX_REVS_IN_CHANGES_PUSH 1000
#define MAX_BYTES_IN_CHANGES_PUSH (10 * 1024 * 1024)

// ==========================================================
// session_t methods
// ==========================================================

nplex::session_t::session_t(const context_ptr &context, uv_stream_t *stream) : 
    m_context{context}, 
    m_con{this, stream}
{
    assert(stream);
    assert(stream->loop);
    assert(stream->loop->data);

    m_con.config(MAX_MSG_BYTES_FROM_NO_USER, 0, 0);
    m_con.set_connection_lost(TIMEOUT_NO_USER);
    m_state = state_e::CONNECTED;
    m_id = m_con.addr().str();
}

void nplex::session_t::disconnect(int rc)
{
    if (m_state == state_e::CLOSED)
        return;

    m_state = state_e::CLOSED;
    m_con.disconnect(rc);
}

void nplex::session_t::shutdown(int rc)
{
    if (m_state == state_e::CLOSED)
        return;

    m_state = state_e::CLOSED;
    m_con.shutdown(rc);
}

void nplex::session_t::send(flatbuffers::DetachedBuffer &&buf)
{
    if (m_state == state_e::CLOSED)
        return;

    auto msg = flatbuffers::GetRoot<nplex::msgs::Message>(buf.data());
    auto type = msg->content_type();

    // update lrev based on message content
    switch (type)
    {
        case msgs::MsgContent::CHANGES_PUSH: {
            auto changes = msg->content_as_CHANGES_PUSH();
            if (changes && changes->updates() && changes->updates()->size() > 0) {
                auto len = changes->updates()->size();
                m_lrev = changes->updates()->Get(len - 1)->rev();
            }
            break;
        }
        case msgs::MsgContent::LOAD_RESPONSE: {
            auto resp = msg->content_as_LOAD_RESPONSE();
            if (resp && resp->snapshot())
                m_lrev = resp->snapshot()->rev();
            break;
        }

        default:
            break;
    }

    // update the message with the current revision
    update_crev(buf, m_context->last_persisted_rev());

    if (m_state == state_e::SYNCED && m_con.is_blocked())
    {
        SPDLOG_DEBUG("Message {} to {} discarded (queue is full)", msgs::EnumNameMsgContent(type), m_id);
        m_con.shutdown(ERR_QUEUE_LENGTH);
        return;
    }

    size_t bytes = buf.size();
    m_con.send(std::move(buf));

    if (type == msgs::MsgContent::KEEPALIVE_PUSH)
        SPDLOG_TRACE("Sent {} to {} ({})", msgs::EnumNameMsgContent(type), m_id, bytes_to_string(bytes));
    else
        SPDLOG_DEBUG("Sent {} to {} ({})", msgs::EnumNameMsgContent(type), m_id, bytes_to_string(bytes));
}

void nplex::session_t::process_delivery(const msgs::Message *msg)
{
    assert(msg);
    assert(msg->content());

    if (m_state != state_e::SYNCING)
        return;

    if (m_ongoing_sync_task)
        return;

    // Get the available output queue
    auto stats = m_con.queue_stats();
    stats.max_msgs = (stats.max_msgs == 0 ? UINT32_MAX : stats.max_msgs);
    stats.max_bytes = (stats.max_bytes == 0 ? UINT32_MAX : stats.max_bytes);

    // Check if there is enough space in the output queue
    if (static_cast<double>(stats.num_msgs) / static_cast<double>(stats.max_msgs) > 0.66 || 
        static_cast<double>(stats.num_bytes) / static_cast<double>(stats.max_bytes) > 0.66) {
        return;
    }

    do_sync();
}

void nplex::session_t::process_request(const msgs::Message *msg)
{
    if (m_state == state_e::CLOSED)
        return;

    if (!msg || !msg->content()) {
        disconnect(ERR_MSG_ERROR);
        return;
    }

    SPDLOG_DEBUG("Received {} from {}", msgs::EnumNameMsgContent(msg->content_type()), m_id);

    switch (msg->content_type())
    {
        case msgs::MsgContent::LOGIN_REQUEST:
            process_login_request(msg->content_as_LOGIN_REQUEST());
            break;

        case msgs::MsgContent::LOAD_REQUEST:
            process_load_request(msg->content_as_LOAD_REQUEST());
            break;

        case msgs::MsgContent::SUBMIT_REQUEST:
            process_submit_request(msg->content_as_SUBMIT_REQUEST());
            break;

        case msgs::MsgContent::PING_REQUEST:
            process_ping_request(msg->content_as_PING_REQUEST());
            break;

        default:
            disconnect(ERR_MSG_ERROR);
    }
}

void nplex::session_t::process_login_request(const msgs::LoginRequest *req)
{
    if (m_state != state_e::CONNECTED) {
        disconnect(ERR_MSG_UNEXPECTED);
        return;
    }

    if (!req) {
        disconnect(ERR_MSG_ERROR);
        return;
    }

    if (req->api_version() != API_VERSION) {
        send(
            create_login_msg(
                req->cid(), 
                msgs::LoginCode::UNSUPPORTED_API_VERSION
            ) 
        );
        shutdown(ERR_API_VERSION);
        return;
    }

    if (!req->user() || !req->password()) {
        send(
            create_login_msg(
                req->cid(), 
                msgs::LoginCode::INVALID_CREDENTIALS
            ) 
        );
        shutdown(ERR_MSG_ERROR);
        return;
    }

    auto it = m_context->users.find(req->user()->str());
    if (it == m_context->users.end()) {
        send(
            create_login_msg(
                req->cid(), 
                msgs::LoginCode::INVALID_CREDENTIALS
            ) 
        );
        shutdown(ERR_USR_NOT_FOUND);
        return;
    }

    auto &user = it->second;

    if (user->password != req->password()->str()) {
        send(
            create_login_msg(
                req->cid(), 
                msgs::LoginCode::INVALID_CREDENTIALS
            ) 
        );
        shutdown(ERR_USR_INVL_PWD);
        return;
    }

    if (user->max_connections && user->num_connections >= user->max_connections) {
        send(
            create_login_msg(
                req->cid(), 
                msgs::LoginCode::TOO_MANY_CONNECTIONS
            ) 
        );
        shutdown(ERR_USR_MAX_CONN);
        return;
    }

    m_user = user;
    m_id = fmt::format("{}@{}", user->name, m_con.addr().str());
    m_con.config(user->max_msg_bytes, user->max_queue_length, user->max_queue_bytes);
    m_state = state_e::LOGGED;

    // enable keepalive
    m_con.set_keepalive(user->keepalive_millis);

    // enable connection-lost
    auto keepalive_millis = static_cast<double>(m_user->keepalive_millis);
    auto millis = static_cast<std::uint32_t>(keepalive_millis * m_user->timeout_factor);
    m_con.set_connection_lost(millis);

    user->num_connections++;

    SPDLOG_INFO("New session: {}", m_id);

    send(
        create_login_msg(
            req->cid(), 
            msgs::LoginCode::AUTHORIZED,
            m_context->minimum_rev(),
            m_context->last_persisted_rev(),
            *user
        ) 
    );
}

void nplex::session_t::process_load_request(const msgs::LoadRequest *req)
{
    if (m_state != state_e::LOGGED || m_load_cid != 0) {
        disconnect(ERR_MSG_UNEXPECTED);
        return;
    }

    switch (req->mode())
    {
        case msgs::LoadMode::SNAPSHOT_AT_FIXED_REV:
            SPDLOG_INFO("{} requested snapshot r{}", m_id, req->rev());
            if (req->rev() == m_context->repo.rev())
                send_repo_snapshot(req->cid());
            else
                send_fixed_snapshot(req->cid(), req->rev());
            break;

        case msgs::LoadMode::SNAPSHOT_AT_LAST_REV:
            SPDLOG_INFO("{} requested current snapshot", m_id);
            if (m_context->last_persisted_rev() == m_context->repo.rev())
                send_repo_snapshot(req->cid());
            else
                send_fixed_snapshot(req->cid(), m_context->last_persisted_rev());
            break;

        case msgs::LoadMode::ONLY_UPDATES_FROM_REV:
            SPDLOG_INFO("{} requested only updates from current revision", m_id);
            send_only_updates(req->cid(), req->rev());
            break;

        default:
            disconnect(ERR_MSG_ERROR);
    }
}

void nplex::session_t::process_submit_request(const nplex::msgs::SubmitRequest *req)
{
    if (m_state == state_e::CONNECTED) {
        disconnect(ERR_MSG_UNEXPECTED);
        return;
    }

    update_t update;

    auto rc = m_context->repo.try_commit(*m_user, req, update);

    send(
        create_submit_msg(
            req->cid(), 
            m_context->last_persisted_rev(),
            rc,
            (rc == msgs::SubmitCode::ACCEPTED ? m_context->repo.rev() : 0)
        )
    );

    if (rc != msgs::SubmitCode::ACCEPTED)
        return;

    // TODO: persist update
    // TODO: repo cleanup (purge)

    push_changes({&update, 1});
}

void nplex::session_t::process_ping_request(const nplex::msgs::PingRequest *req)
{
    if (m_state != state_e::CONNECTED) {
        disconnect(ERR_MSG_UNEXPECTED);
        return;
    }

    send(
        create_ping_msg(
            req->cid(), 
            m_context->last_persisted_rev(),
            (req->payload() ? req->payload()->str() : "")
        )
    );
}

void nplex::session_t::send_repo_snapshot(std::size_t cid)
{
    load_builder_t builder(cid);

    builder.set_snapshot(m_context->repo, m_user);

    auto buf = builder.finish(m_context->last_persisted_rev(), true);
    
    send(std::move(buf));
    
    assert(m_context->last_persisted_rev() == m_context->repo.rev());

    m_state = state_e::SYNCING;
    m_lrev = m_context->repo.rev();
    m_load_cid = cid;

    do_sync();
}

void nplex::session_t::send_fixed_snapshot(std::size_t cid, rev_t rev)
{
    auto min_rev = m_context->minimum_rev();
    auto max_rev = m_context->last_persisted_rev();

    if (rev < min_rev || rev > max_rev) {
        send(load_builder_t{cid}.finish(max_rev, false));
        return;
    }

    m_state = state_e::SYNCING;
    m_load_cid = cid;

    m_context->submit_task(new repo_task_t(m_context->storage, shared_from_this(), rev, cid));
}

void nplex::session_t::send_only_updates(std::size_t cid, rev_t rev)
{
    auto min_rev = m_context->minimum_rev();
    auto max_rev = m_context->last_persisted_rev();

    if (rev < min_rev || rev > max_rev) {
        send(load_builder_t{m_load_cid}.finish(max_rev, false));
        return;
    }

    m_state = state_e::SYNCING;
    m_load_cid = cid;
    m_lrev = rev;

    do_sync();
}

void nplex::session_t::do_sync()
{
    if (m_lrev == m_context->last_persisted_rev()) {
        m_state = state_e::SYNCED;
        return;
    }

    m_state = state_e::SYNCING;

    if (m_ongoing_sync_task)
        return;

    // Get the available output queue
    const auto &stats = m_con.queue_stats();
    std::uint32_t max_msgs = static_cast<std::uint32_t>(stats.max_msgs - stats.num_msgs);
    std::uint32_t max_bytes = static_cast<std::uint32_t>(stats.max_bytes - stats.num_bytes);

    // TODO: check if info in cache

    sync_task_t *task = new sync_task_t(m_context->storage, shared_from_this(), m_lrev, max_msgs, max_bytes);

    task->config_builder(m_load_cid, MAX_REVS_IN_CHANGES_PUSH, MAX_BYTES_IN_CHANGES_PUSH);

    m_ongoing_sync_task = true;
    m_context->submit_task(task);
}

void nplex::session_t::push_changes(const std::span<update_t> &updates)
{
    if (updates.empty())
        return;

    changes_builder_t builder{m_load_cid, m_user, UINT32_MAX, UINT32_MAX};

    builder.append_updates(updates);

    if (!builder.empty())
        send(builder.finish(m_context->last_persisted_rev(), false));

    m_lrev = updates.back().meta->rev;
}

const char * nplex::session_t::strerror() const
{
    auto error = m_con.error();

    if (error < 0)
        return uv_strerror(error);

    switch (error)
    {
        case ERR_CLOSED_BY_LOCAL: return "closed by server";
        case ERR_CLOSED_BY_PEER: return "closed by peer";
        case ERR_MSG_ERROR: return "invalid message";
        case ERR_MSG_UNEXPECTED: return "unexpected message";
        case ERR_MSG_SIZE: return "message too large";
        case ERR_USR_NOT_FOUND: return "user not found";
        case ERR_USR_INVL_PWD: return "invalid password";
        case ERR_USR_MAX_CONN: return "maximum usr connections reached";
        case ERR_MAX_CONN: return "maximum total connections reached";
        case ERR_API_VERSION: return "unsupported API version";
        case ERR_CONNECTION_LOST: return "connection lost";
        case ERR_QUEUE_LENGTH: return "max queue length exceeded";
        case ERR_QUEUE_BYTES: return "max queue bytes exceeded";
        default: return "unknow error";
    }
}
