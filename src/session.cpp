#include <cassert>
#include <spdlog/spdlog.h>
#include "exception.hpp"
#include "messaging.hpp"
#include "utils.hpp"
#include "user.hpp"
#include "context.hpp"
#include "session.hpp"
#include "tasks.hpp"

// maximum time (in millis) for a not logged user
#define TIMEOUT_NO_USER 5000
// maximum (approx) bytes in an UpdatesPush message
#define MAX_BYTES_IN_PUSH (1 * 1024 * 1024)
// maximum number of items in a sync batch
#define MAX_ITEMS_IN_SYNC 10000
// maximum % occupancy in output queue to start a sync
#define MAX_PCT_OCCUPANCY_OUTPUT_QUEUE 66

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

    m_id = m_con.addr().str();

    connection_params_t default_params = {
        .max_unack_msgs = 3,
        .max_unack_bytes = (3 * 1024),
        .keepalive_millis = TIMEOUT_NO_USER,
        .timeout_factor = 1.0f
    };

    m_con.config(default_params);
    m_con.enable_connection_lost();
}

void nplex::session_t::release()
{
    assert(is_closed());
    m_context->release_session(this);
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
        m_con.shutdown(ERR_UNACK);
        return;
    }

    m_con.send(std::move(buf));

    if (type == msgs::MsgContent::KEEPALIVE_PUSH)
        SPDLOG_TRACE("Sent {} to {} ({})", msgs::EnumNameMsgContent(type), m_id, bytes_to_string(bytes));
    else
        SPDLOG_DEBUG("Sent {} to {} ({})", msgs::EnumNameMsgContent(type), m_id, bytes_to_string(bytes));
}

void nplex::session_t::process_delivery([[maybe_unused]] const msgs::Message *msg)
{
    assert(msg);
    assert(msg->content());

    if (m_updates_cid != 0 && m_lrev < m_context->last_persisted_rev() && !m_sync_in_progress)
        do_sync(m_updates_cid);
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

    auto user = m_context->get_user(req->user()->str());
    if (!user) {
        send(
            create_login_msg(
                req->cid(), 
                msgs::LoginCode::INVALID_CREDENTIALS
            ) 
        );
        shutdown(ERR_USR_NOT_FOUND);
        return;
    }

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

    if (user->params.max_connections && user->num_connections >= user->params.max_connections) {
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
    m_con.config(m_user->params.connection);

    m_con.enable_keepalive();
    m_con.enable_connection_lost();

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

    SPDLOG_INFO("{} requested snapshot at r{}", m_id, rev);

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
    if (rev == m_context->last_persisted_rev() && rev == m_context->repo().rev())
    {
        send_snapshot(cid, m_context->repo(), m_user);
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

    SPDLOG_INFO("{} requested updates from r{}", m_id, rev);

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

    m_lrev = rev;
    do_sync(m_updates_cid);
}

void nplex::session_t::process_submit_request(const nplex::msgs::SubmitRequest *req)
{
    if (!is_logged()) {
        disconnect(ERR_MSG_UNEXPECTED);
        return;
    }

    auto [rc, erev] = m_context->try_commit(req, *m_user);

    send(
        create_submit_msg(
            req->cid(), 
            m_context->last_persisted_rev(),
            rc,
            erev
        )
    );
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

void nplex::session_t::do_sync(std::size_t cid)
{
    if (is_closed() || !m_updates_cid || cid != m_updates_cid)
        return;

    if (m_lrev == m_context->last_persisted_rev() || m_sync_in_progress)
        return;

    const auto &stats = m_con.stats();
    const auto &params = m_con.params();

    // Check output queue availability
    if ((stats.unack_msgs  * 100) / params.max_unack_msgs  > MAX_PCT_OCCUPANCY_OUTPUT_QUEUE || 
        (stats.unack_bytes * 100) / params.max_unack_bytes > MAX_PCT_OCCUPANCY_OUTPUT_QUEUE) {
        return;
    }

    std::uint32_t max_bytes = static_cast<std::uint32_t>(params.max_unack_bytes - stats.unack_bytes);
    rev_t from_rev = m_lrev + 1;

    // First, try to serve updates from the in-memory cache.
    auto updates = m_context->get_cached_updates(from_rev, max_bytes);

    if (!updates.empty()) {
        push_changes(updates, true);
        return;
    }

    // Fallback: launch async sync task which will read from storage.
    sync_task_t *task = new sync_task_t(shared_from_this(), m_lrev, m_updates_cid, MAX_ITEMS_IN_SYNC, max_bytes);
    m_context->submit_task(task);
    m_sync_in_progress = true;
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
        case ERR_UNACK: return "max unack limits exceeded";
        default: return "unknown error";
    }
}

void nplex::session_t::send_keepalive()
{
    if (is_closed())
        return;

    rev_t crev = m_context->last_persisted_rev();
    send(create_keepalive_msg(crev));
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

void nplex::session_t::send_updates(std::size_t cid, [[maybe_unused]] rev_t from_rev, rev_t to_rev, const std::span<const update_dto_t> &updates)
{
    if (is_closed() || cid != m_updates_cid || updates.empty()) {
        m_sync_in_progress = false;
        return;
    }

    assert(m_lrev == 0 || m_lrev + 1 == from_rev);

    rev_t crev = m_context->last_persisted_rev();
    updates_builder_t builder;
    auto it = updates.begin();

    while (it != updates.end())
    {
        for (; it != updates.end(); ++it)
        {
            if (builder.bytes() >= MAX_BYTES_IN_PUSH)
                break;

            assert(m_lrev < it->rev);
            assert(from_rev <= it->rev && it->rev <= to_rev);
            m_lrev = it->rev;

            builder.append(*it);
        }

        if (!builder.empty())
            send(builder.finish(m_updates_cid, crev));
    }

    m_lrev = to_rev;

    // Case: synced and last update was not sent
    if (m_lrev == crev && updates.back().rev != to_rev) {
        update_dto_t dto = {
            .rev = crev,
            .user = "admin"
        };
        builder.append(dto, nullptr, true);
        send(builder.finish(m_updates_cid, crev));
    }

    m_sync_in_progress = false;
}

void nplex::session_t::push_changes(const std::span<const update_t> &updates, bool force)
{
    if (is_closed() || m_updates_cid == 0 || updates.empty())
        return;

    if (m_lrev + 1 != updates.front().meta->rev)
        return;

    rev_t crev = m_context->last_persisted_rev();
    updates_builder_t builder;
    auto it = updates.begin();

    while (it != updates.end())
    {
        for (; it != updates.end(); ++it)
        {
            if (builder.bytes() >= m_context->params().max_msg_bytes)
                break;

            m_lrev = it->meta->rev;

            builder.append(*it, m_user, (force && m_lrev == crev));
        }

        if (!builder.empty())
            send(builder.finish(m_updates_cid, crev));
    }
}
