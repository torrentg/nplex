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
// maximum time (in millis) for a non logged user
#define TIMEOUT_NO_USER 5000
// maximum (aprox) bytes in an UpdatesPush message
#define MAX_BYTES_IN_PUSH (1 * 1024 * 1024)
// maximum numbers of items in a sync batch
#define MAX_ITEMS_IN_SYNC 10000
// maximum % ocuppancy in output queue to start a sync
#define MAX_PCT_OCUPPANCY_OUTPUT_QUEUE 66

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
    m_id = m_con.addr().str();
}

void nplex::session_t::disconnect(int rc)
{
    if (is_closed())
        return;

    m_con.disconnect(rc);
}

void nplex::session_t::shutdown(int rc)
{
    if (is_closed())
        return;

    m_con.shutdown(rc);
}

void nplex::session_t::send(flatbuffers::DetachedBuffer &&buf)
{
    if (is_closed())
        return;

    auto msg = flatbuffers::GetRoot<nplex::msgs::Message>(buf.data());
    auto type = msg->content_type();
    auto bytes = buf.size();

    if (m_con.is_blocked())
    {
        SPDLOG_INFO("{} exceeded output queue limits (shutdown)", m_id);
        m_con.shutdown(ERR_QUEUE_LENGTH);
        return;
    }

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

    if (m_updates_cid != 0 && m_lrev < m_context->last_persisted_rev())
        do_sync();
}

void nplex::session_t::process_request(const msgs::Message *msg)
{
    if (is_closed())
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

        case msgs::MsgContent::SNAPSHOT_REQUEST:
            process_snapshot_request(msg->content_as_SNAPSHOT_REQUEST());
            break;

        case msgs::MsgContent::UPDATES_REQUEST:
            process_updates_request(msg->content_as_UPDATES_REQUEST());
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
    if (is_logged()) {
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
    m_user->num_connections++;
    m_id = fmt::format("{}@{}", m_user->name, m_con.addr().str());
    m_con.config(m_user->max_msg_bytes, m_user->max_queue_length, m_user->max_queue_bytes);

    // enable keepalive
    m_con.set_keepalive(m_user->keepalive_millis);

    // enable connection-lost
    auto keepalive_millis = static_cast<double>(m_user->keepalive_millis);
    auto millis = static_cast<std::uint32_t>(keepalive_millis * m_user->timeout_factor);
    m_con.set_connection_lost(millis);

    SPDLOG_INFO("New session: {}", m_id);

    send(
        create_login_msg(
            req->cid(), 
            msgs::LoginCode::AUTHORIZED,
            m_context->minimum_rev(),
            m_context->last_persisted_rev(),
            m_user
        ) 
    );
}

void nplex::session_t::process_snapshot_request(const msgs::SnapshotRequest *req)
{
    if (!is_logged()) {
        disconnect(ERR_MSG_UNEXPECTED);
        return;
    }

    std::uint64_t cid = req->cid();
    rev_t rev = req->rev();

    if (rev == 0)
        rev = m_context->last_persisted_rev();

    SPDLOG_INFO("{} requested snapshot r{}", m_id, rev);

    // case: requested snapshot out of range
    if (rev < m_context->minimum_rev() || rev > m_context->last_persisted_rev())
    {
        send(
            create_snapshot_msg(
                cid,
                m_context->last_persisted_rev(),
                m_context->minimum_rev(),
                false,
                repo_t{},
                nullptr
            )
        );

        return;
    }

    // case: requested current snapshot
    if (rev == m_context->last_persisted_rev() && rev == m_context->repo.rev())
    {
        send_snapshot(cid, m_context->repo, m_user);
        return;
    }

    // case: requested snapshot need to be retrieved from storage
    auto task = new snapshot_task_t(shared_from_this(), rev, cid);
    m_context->submit_task(task);
}

void nplex::session_t::process_updates_request(const msgs::UpdatesRequest *req)
{
    if (!is_logged()) {
        disconnect(ERR_MSG_UNEXPECTED);
        return;
    }

    std::uint64_t cid = req->cid();
    rev_t rev = req->rev();

    if (rev == 0)
        rev = m_context->last_persisted_rev();

    SPDLOG_INFO("{} requested updates r{}", m_id, rev);

    // case: requested updates out of range (reject)
    if (rev < m_context->minimum_rev())
    {
        send(
            create_updates_msg(
                cid,
                m_context->last_persisted_rev(),
                m_context->minimum_rev(),
                false
            )
        );

        return;
    }

    m_updates_cid = cid;

    // updates request acceptance
    send(
        create_updates_msg(
            cid,
            m_context->last_persisted_rev(),
            m_context->minimum_rev(),
            true
        )
    );

    // case: requested updates from current revision
    if (rev == m_context->last_persisted_rev()) {
        m_lrev = m_context->last_persisted_rev();
        return;
    }

    // case: requested updates from a past revision
    do_sync();
}

void nplex::session_t::process_submit_request(const nplex::msgs::SubmitRequest *req)
{
    if (!is_logged()) {
        disconnect(ERR_MSG_UNEXPECTED);
        return;
    }

    update_t update;

    // try to commit the update
    auto rc = m_context->repo.try_commit(*m_user, req, update);

    // case: commit rejected
    if (rc != msgs::SubmitCode::ACCEPTED) {
        send(
            create_submit_msg(
                req->cid(), 
                m_context->last_persisted_rev(),
                rc,
                0
            )
        );

        return;
    }

    // case: commit accepted
    send(
        create_submit_msg(
            req->cid(), 
            m_context->last_persisted_rev(),
            rc,
            m_context->repo.rev()
        )
    );

    // persist the update
    assert(m_context->repo.rev() == update.meta->rev);
    m_context->persist(std::move(update));

    // TODO: repo cleanup (purge)
}

void nplex::session_t::process_ping_request(const nplex::msgs::PingRequest *req)
{
    send(
        create_ping_msg(
            req->cid(), 
            m_context->last_persisted_rev(),
            (req->payload() ? req->payload()->str() : "")
        )
    );
}

void nplex::session_t::do_sync()
{
    if (is_closed() || !m_updates_cid || m_lrev == m_context->last_persisted_rev())
        return;

    const auto &stats = m_con.queue_stats();

    // Check output queue availability
    if ((stats.num_msgs  * 100) / stats.max_msgs  > MAX_PCT_OCUPPANCY_OUTPUT_QUEUE || 
        (stats.num_bytes * 100) / stats.max_bytes > MAX_PCT_OCUPPANCY_OUTPUT_QUEUE) {
        return;
    }

    std::uint32_t max_bytes = static_cast<std::uint32_t>(stats.max_bytes - stats.num_bytes);

    // TODO: check if info in cache

    sync_task_t *task = new sync_task_t(shared_from_this(), m_lrev, m_updates_cid, MAX_ITEMS_IN_SYNC, max_bytes);
    m_context->submit_task(task);
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

void nplex::session_t::send_snapshot(std::size_t cid, const repo_t &repo, const user_ptr &user)
{
    if (is_closed())
        return;

    send(
        create_snapshot_msg(
            cid,
            m_context->last_persisted_rev(),
            m_context->minimum_rev(),
            true,
            repo,
            user
        )
    );
}

void nplex::session_t::send_updates(std::size_t cid, rev_t from_rev, rev_t to_rev, const std::span<update_dto_t> &updates)
{
    if (is_closed() || cid != m_updates_cid || updates.empty())
        return;

    assert(m_lrev == 0 || m_lrev + 1 == from_rev);

    updates_builder_t builder;
    auto it = updates.begin();

    while (it != updates.end())
    {
        for (; it != updates.end(); ++it)
        {
            builder.append(*it);

            if (builder.bytes() >= MAX_BYTES_IN_PUSH)
                break;
        }

        if (!builder.empty())
            send(builder.finish(m_updates_cid, m_context->last_persisted_rev()));
    }

    m_lrev = to_rev;
}

void nplex::session_t::push_changes(const std::span<update_t> &updates)
{
    if (is_closed() || m_updates_cid == 0 || updates.empty())
        return;

    if (m_lrev + 1 != updates.front().meta->rev)
        return;

    updates_builder_t builder;
    auto it = updates.begin();

    while (it != updates.end())
    {
        for (; it != updates.end(); ++it)
        {
            builder.append(*it, m_user);

            if (builder.bytes() >= MAX_BYTES_IN_PUSH)
                break;

            m_lrev = it->meta->rev;
        }

        if (!builder.empty())
            send(builder.finish(m_updates_cid, m_context->last_persisted_rev()));
    }
}
