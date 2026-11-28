#include "session.hpp"
#include "exception.hpp"
#include "messaging.hpp"
#include "utils.hpp"
#include "user.hpp"
#include "context.hpp"
#include "tasks.hpp"
#include "logger.hpp"
#include <algorithm>
#include <cassert>

// Maximum time (in millis) for a not logged user
static constexpr std::uint32_t TIMEOUT_NO_USER = 5000;
// Maximum number of bytes in a sync batch
static constexpr std::uint32_t MAX_BYTES_IN_SYNC = 50 * 1024 * 1024;
// Maximum % occupancy in output queue to restart a sync
static constexpr std::uint32_t MAX_PCT_OCCUPANCY_OUTPUT_QUEUE = 50;

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

    flush_buffered_msgs();

    m_con.shutdown(rc);
}

bool nplex::session_t::should_throttle() const
{
    const auto &stats = m_con.stats();
    const auto &params = m_con.params();

    if (params.max_unack_bytes && stats.unack_bytes + m_buffered_bytes >= params.max_unack_bytes)
        return true;

    if (params.max_unack_msgs && stats.unack_msgs + m_buffered_msgs.size() >= params.max_unack_msgs)
        return true;

    return false;
}

void nplex::session_t::send(DetachedBuffer &&buf)
{
    if (is_closed())
        return;

    flush_buffered_msgs();

    if (m_con.is_blocked())
    {
        LOG_INFO("{} exceeded output queue limits (shutdown)", m_id);
        m_con.shutdown(ERR_UNACK);
        return;
    }

    trace_output_msg(buf);

    m_con.send(std::move(buf));
}

void nplex::session_t::send_buffered_msg(DetachedBuffer &&buf)
{
    if (is_closed())
        return;

    m_buffered_bytes += buf.size();
    m_buffered_msgs.emplace_back(std::move(buf));

    if (m_buffered_msgs.size() >= outgoing_network_frames_t::max_num_msgs())
        flush_buffered_msgs();
}

void nplex::session_t::flush_buffered_msgs()
{
    if (is_closed() || m_buffered_msgs.empty())
        return;

    // Queue limits are intentionally not checked here. Reads may stop only
    // after a limit is exceeded, which could otherwise lead to disconnect.

    for (const auto &buf : m_buffered_msgs)
        trace_output_msg(buf);

    m_con.send(m_buffered_msgs);

    m_buffered_msgs.clear();
    m_buffered_bytes = 0;
}

void nplex::session_t::process_delivery([[maybe_unused]] const msgs::Message *msg)
{
    assert(msg);
    assert(msg->content());

    if (m_updates_cid != 0 && m_lrev < m_context->last_persisted_rev() && !m_sync_in_progress)
        do_sync(m_updates_cid);
}

void nplex::session_t::process_requests(std::span<const msgs::Message *> msgs)
{
    for (const auto *msg : msgs)
        process_request(msg);

    flush_buffered_msgs();
}

void nplex::session_t::process_request(const msgs::Message *msg)
{
    if (is_closed())
        return;

    if (!msg || !msg->content()) {
        disconnect(ERR_MSG_ERROR);
        return;
    }

    LOG_DEBUG("Received {} from {}", msgs::EnumNameMsgContent(msg->content_type()), m_id);

    if (should_throttle()) {
        disconnect(ERR_EXCD_LIMITS);
        return;
    }

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

        case msgs::MsgContent::SESSIONS_REQUEST:
            process_sessions_request(msg->content_as_SESSIONS_REQUEST());
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

    if (req->schema() != SCHEMA3_HASH) {
        send(
            create_login_response(
                req->cid(), 
                msgs::LoginCode::UNSUPPORTED_SCHEMA
            ) 
        );
        shutdown(ERR_SCHEMA);
        return;
    }

    if (!req->user() || !req->password()) {
        send(
            create_login_response(
                req->cid(), 
                msgs::LoginCode::INVALID_CREDENTIALS
            ) 
        );
        shutdown(ERR_MSG_ERROR);
        return;
    }

    auto usr = m_context->get_user(req->user()->str());
    if (!usr) {
        send(
            create_login_response(
                req->cid(), 
                msgs::LoginCode::INVALID_CREDENTIALS
            ) 
        );
        shutdown(ERR_USR_NOT_FOUND);
        return;
    }

    if (usr->password != req->password()->str()) {
        send(
            create_login_response(
                req->cid(), 
                msgs::LoginCode::INVALID_CREDENTIALS
            ) 
        );
        shutdown(ERR_USR_INVL_PWD);
        return;
    }

    if (usr->params.max_connections && usr->num_connections >= usr->params.max_connections) {
        send(
            create_login_response(
                req->cid(), 
                msgs::LoginCode::TOO_MANY_CONNECTIONS
            ) 
        );
        shutdown(ERR_USR_MAX_CONN);
        return;
    }

    m_user = usr;
    m_user->num_connections++;
    m_id = fmt::format("{}@{}", m_user->name, m_con.addr().str());
    m_con.config(m_user->params.connection);

    m_con.enable_keepalive();
    m_con.enable_connection_lost();

    LOG_INFO("New session: {}", m_id);

    send(
        create_login_response(
            req->cid(), 
            msgs::LoginCode::AUTHORIZED,
            m_context->minimum_rev(),
            m_context->last_persisted_rev(),
            m_user
        ) 
    );

    m_context->publish(shared_from_this());
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

    LOG_INFO("{} requested snapshot at r{}", m_id, rev);

    // Case: requested snapshot out of range
    if (rev < m_context->minimum_rev() || rev > m_context->last_persisted_rev())
    {
        send_buffered_msg(
            create_snapshot_response_reject(
                cid,
                m_context->last_persisted_rev(),
                m_context->minimum_rev()
            )
        );

        return;
    }

    // Case: requested current snapshot
    if (rev == m_context->last_persisted_rev() && rev == m_context->store().rev())
    {
        auto snapshot = m_context->store().snapshot(m_user);
        snapshot_response_builder_t builder;
        builder.set_snapshot(snapshot);
        auto buf = builder.finish(cid, m_context->last_persisted_rev(), m_context->minimum_rev());
        send(std::move(buf));
        return;
    }

    // Case: requested snapshot need to be retrieved from storage
    m_context->submit_task(
        new build_snapshot_task_t(
            rev,
            m_user,
            m_context->storage()
        ),
        [self = shared_from_this(), cid](task_t *task) {
            auto result = dynamic_cast<build_snapshot_task_t *>(task);
            self->process_build_snapshot_task_result(cid, result);
        }
    );
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

    LOG_DEBUG("{} requested updates from r{}", m_id, rev);

    // Case: requested updates out of range (reject)
    if (rev < m_context->minimum_rev())
    {
        send_buffered_msg(
            create_updates_response(
                cid,
                m_context->last_persisted_rev(),
                m_context->minimum_rev(),
                false
            )
        );

        return;
    }

    // Case: request accepted
    m_updates_cid = cid;

    send_buffered_msg(
        create_updates_response(
            cid,
            m_context->last_persisted_rev(),
            m_context->minimum_rev(),
            true
        )
    );

    m_lrev = rev;
    do_sync(m_updates_cid);
}

void nplex::session_t::process_sessions_request(const msgs::SessionsRequest *req)
{
    if (!is_logged()) {
        disconnect(ERR_MSG_UNEXPECTED);
        return;
    }

    rev_t crev = m_context->last_persisted_rev();
    std::uint64_t cid = req->cid();
    sessions_response_builder_t builder;

    if (m_user->params.can_monitor)
    {
        for (const auto &session : m_context->sessions())
            builder.append(session);
    }

    send_buffered_msg(builder.finish(cid, crev));

    m_sessions_cid = ((m_user->params.can_monitor && req->stream()) ? cid : 0);
}

void nplex::session_t::process_submit_request(const nplex::msgs::SubmitRequest *req)
{
    if (!is_logged()) {
        disconnect(ERR_MSG_UNEXPECTED);
        return;
    }

    auto [rc, erev] = m_context->try_commit(req, m_user);

    send_buffered_msg(
        create_submit_response(
            req->cid(), 
            m_context->last_persisted_rev(),
            rc,
            erev
        )
    );
}

void nplex::session_t::process_ping_request(const nplex::msgs::PingRequest *req)
{
    send_buffered_msg(
        create_ping_response(
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

    // Check output queue availability.
    // Keep a headroom below hard unack limits to avoid triggering shutdown on send().
    if ((stats.unack_msgs  * 100) / params.max_unack_msgs  >= MAX_PCT_OCCUPANCY_OUTPUT_QUEUE || 
        (stats.unack_bytes * 100) / params.max_unack_bytes >= MAX_PCT_OCCUPANCY_OUTPUT_QUEUE) {
        return;
    }

    std::uint32_t max_total_bytes = MAX_BYTES_IN_SYNC;
    std::uint32_t unack_budget = static_cast<std::uint32_t>(params.max_unack_bytes - stats.unack_bytes);

    max_total_bytes = std::min(max_total_bytes, unack_budget);

    if (max_total_bytes == 0)
        return;

    rev_t from_rev = m_lrev + 1;

    // First, try to serve updates from the in-memory cache.
    auto updates = m_context->get_cached_updates(from_rev, max_total_bytes);

    if (!updates.empty()) {
        push_updates(updates, true);
        return;
    }

    // Fallback: launch async task to read updates from storage
    m_sync_in_progress = true;

    m_context->submit_task(
        new read_updates_task_t(
            m_lrev + 1,
            m_user,
            m_context->storage(),
            m_context->params().msg_max_bytes,
            max_total_bytes
        ),
        [self = shared_from_this(), cid = m_updates_cid](task_t *task) {
            auto result = dynamic_cast<read_updates_task_t *>(task);
            self->process_read_updates_task_result(cid, result);
        }
    );
}

const char * nplex::session_t::strerror() const
{
    auto errnum = m_con.error();

    if (errnum < 0)
        return uv_strerror(errnum);

    switch (errnum)
    {
        case ERR_CLOSED_BY_LOCAL: return "closed by server";
        case ERR_CLOSED_BY_PEER: return "closed by peer";
        case ERR_EXCD_LIMITS: return "exceeded limits";
        case ERR_MSG_ERROR: return "invalid message";
        case ERR_MSG_UNEXPECTED: return "unexpected message";
        case ERR_MSG_SIZE: return "message too large";
        case ERR_USR_NOT_FOUND: return "user not found";
        case ERR_USR_INVL_PWD: return "invalid password";
        case ERR_USR_MAX_CONN: return "maximum usr connections reached";
        case ERR_MAX_CONN: return "maximum total connections reached";
        case ERR_SCHEMA: return "unsupported API version";
        case ERR_CONNECTION_LOST: return "connection lost";
        case ERR_UNACK: return "max unack limits exceeded";
        case ERR_DATA_RETRIEVAL: return "data retrieval error";
        default: return "unknown error";
    }
}

void nplex::session_t::send_keepalive()
{
    if (is_closed())
        return;

    rev_t crev = m_context->last_persisted_rev();
    send(create_keepalive_push(crev));
}

void nplex::session_t::process_build_snapshot_task_result(std::size_t cid, build_snapshot_task_t *task)
{
    assert(task);

    if (is_closed())
        return;

    if (task->excpt) {
        disconnect(ERR_DATA_RETRIEVAL);
        return;
    }

    auto buf = task->builder.finish(cid, m_context->last_persisted_rev(), m_context->minimum_rev());

    send_buffered_msg(std::move(buf));
    flush_buffered_msgs();
}

void nplex::session_t::process_read_updates_task_result(std::size_t cid, read_updates_task_t *task)
{
    assert(task);

    if (is_closed() || cid != m_updates_cid)
        return;

    if (task->excpt) {
        disconnect(ERR_DATA_RETRIEVAL);
        return;
    }

    assert(m_lrev == 0 || m_lrev + 1 == task->rev);

    rev_t crev = m_context->last_persisted_rev();

    for (auto &msg : task->msgs)
    {
        if (msg.empty())
            continue;

        auto buf = msg.finish(m_updates_cid, crev);
        send_buffered_msg(std::move(buf));
    }

    m_lrev = task->last_rev;
    m_sync_in_progress = false;

    flush_buffered_msgs();
    do_sync(m_updates_cid);
}

void nplex::session_t::push_updates(std::span<const update_t> updates, bool syncing)
{
    if (is_closed() || m_updates_cid == 0 || updates.empty())
        return;

    if (m_lrev + 1 != updates.front().meta->rev)
        return;

    auto msg_max_bytes = m_context->params().msg_max_bytes;
    rev_t crev = m_context->last_persisted_rev();
    updates_push_builder_t builder(m_user);
    auto it = updates.begin();

    // TODO: manage the output queue overflow case

    while (it != updates.end())
    {
        for (; it != updates.end(); ++it)
        {
            if (msg_max_bytes && builder.bytes() >= msg_max_bytes)
                break;

            m_lrev = it->meta->rev;

            builder.append(*it);
        }

        bool force_ending_update = (syncing && m_lrev == crev);

        if (!builder.empty() || force_ending_update)
        {
            auto buf = builder.finish(m_updates_cid, crev, force_ending_update);
            send_buffered_msg(std::move(buf));
        }
    }

    flush_buffered_msgs();
}

void nplex::session_t::push_session(const session_ptr &session)
{
    if (is_closed() || m_sessions_cid == 0)
        return;

    send(
        create_sessions_push(
            m_sessions_cid,
            m_context->last_persisted_rev(),
            session
        )
    );
}

void nplex::session_t::trace_output_msg(const DetachedBuffer &buf)
{
    auto bytes = buf.size();

    if (bytes == 0)
        return;

    auto msg = fb::GetRoot<msgs::Message>(buf.data());
    auto type = msg->content_type();

    if (type == nplex::msgs::MsgContent::KEEPALIVE_PUSH)
        LOG_TRACE("Sent {} to {} ({})", msgs::EnumNameMsgContent(type), m_id, bytes_to_string(bytes));
    else
        LOG_DEBUG("Sent {} to {} ({})", msgs::EnumNameMsgContent(type), m_id, bytes_to_string(bytes));
}
