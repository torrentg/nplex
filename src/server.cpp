#include <cstdlib>
#include <cstring>
#include <cassert>
#include <stdexcept>
#include <filesystem>
#include <fmt/core.h>
#include <fmt/ranges.h>
#include <sys/socket.h>
#include <spdlog/spdlog.h>
#include "messaging.hpp"
#include "exception.hpp"
#include "tasks.hpp"
#include "server.hpp"

/**
 * Notes on this compilation unit:
 * 
 * When we use the libuv library we apply C conventions (instead of C++ ones):
 *   - When in Rome, do as the Romans do.
 *   - calloc/free are used instead of new/delete.
 *   - pointers to static functions.
 *   - C-style pointer casting.
 */

#define MAX_QUEUED_CONNECTIONS 128

// ==========================================================
// Internal (static) functions
// ==========================================================

// TODO: remove this test function
static void cb_timer_test(uv_timer_t *timer)
{
    auto *server = (nplex::server_t *) timer->loop->data;
    server->simule_submit();
}

/**
 * Convert an string address to a sockaddr.
 * 
 * This is a blocking function.
 * 
 * @param loop Event loop.
 * @param addr Address to convert.
 * 
 * @return The sockaddr.
 * 
 * @exception nplex_exception If the address is invalid. 
 */
static struct sockaddr_storage get_sockaddr(uv_loop_t *loop, const nplex::addr_t &addr)
{
    using namespace nplex;

    int rc = 0;
    struct sockaddr_storage ret;

    std::memset(&ret, 0, sizeof(ret));

    switch(addr.family())
    {
        case AF_INET:
            if ((rc = uv_ip4_addr(addr.host().c_str(), addr.port(), (struct sockaddr_in*) &ret)) != 0)
                throw std::runtime_error(uv_strerror(rc));
            break;

        case AF_INET6:
            if ((rc = uv_ip6_addr(addr.host().c_str(), addr.port(), (struct sockaddr_in6*) &ret)) != 0)
                throw std::runtime_error(uv_strerror(rc));
            break;

        case AF_UNSPEC:
        {
            uv_getaddrinfo_t req;
            struct addrinfo hints;

            std::memset(&req, 0, sizeof(req));
            std::memset(&hints, 0, sizeof(hints));

            hints.ai_socktype = SOCK_STREAM;
            hints.ai_protocol = IPPROTO_TCP;

            // request address info made synchronously (at this point no other tasks are done)
            std::string port = std::to_string(addr.port());
            if ((rc = uv_getaddrinfo(loop, &req, NULL, addr.host().c_str(), port.c_str(), &hints)) != 0)
                throw std::runtime_error(uv_strerror(rc));

            if (req.addrinfo->ai_family != AF_INET && req.addrinfo->ai_family != AF_INET6) {
                uv_freeaddrinfo(req.addrinfo);
                throw std::runtime_error(fmt::format("Invalid address family: {}", addr.str()));
            }

            memmove(&ret, req.addrinfo->ai_addr, req.addrinfo->ai_addrlen);
            uv_freeaddrinfo(req.addrinfo);
            break;
        }

        default:
            throw std::runtime_error(fmt::format("Unrecognized address family: {}", addr.str()));
    }

    return ret;
}

static void cb_signal_handler(uv_signal_t *handle, int signum)
{
    SPDLOG_WARN("Signal received: {}({})", ::strsignal(signum), signum);
    uv_stop(handle->loop);
    uv_signal_stop(handle);
    uv_close((uv_handle_t*) handle, nullptr);
}

static void cb_process_async(uv_async_t *handle)
{
    UNUSED(handle);
    //TODO: use async signal to stop the thread (ex. SIGINT signal) or notify a write
}

static void cb_close_handle(uv_handle_t *handle, void *arg)
{
    UNUSED(arg);

    if (uv_is_closing(handle))
        return;

    switch(handle->type)
    {
        case UV_SIGNAL:
            SPDLOG_DEBUG("Closing UV_SIGNAL");
            uv_close(handle, nullptr);
            break;
        case UV_ASYNC:
            SPDLOG_DEBUG("Closing UV_ASYNC");
            uv_close(handle, nullptr);
            break;
        case UV_TCP:
            SPDLOG_DEBUG("Closing UV_TCP");
            if (handle->data)   // connection
                uv_close(handle, nplex::cb_close_session);
            else                // listener
                uv_close(handle, nullptr);
            break;
        case UV_TIMER:
            SPDLOG_DEBUG("Closing UV_TIMER");
            uv_close(handle, (uv_close_cb) free);
            break;
        case UV_WORK:
            assert(false);
            break;
        default:
            SPDLOG_DEBUG("Closing OTHER");
            uv_close(handle, (uv_close_cb) free);
    }
}

static void cb_tcp_connection(uv_stream_t *stream, int status)
{
    if (status < 0) {
        SPDLOG_WARN(uv_strerror(status));
        return;
    }

    auto *server = (nplex::server_t *) stream->loop->data;
    assert(server);
    server->append_session(stream);
}

static bool is_valid_user(const nplex::user_t &user)
{
    if (!user.active)
        return false;

    if (user.password.empty())
        return false;

    if (user.max_connections == 0)
        return false;

    if (user.timeout_factor <= 1.0)
        return false;

    int sum = 0;

    for (const auto &perm : user.permissions)
        sum += perm.mode;

    if (sum == 0)
        return false;

    return true;
}

// ==========================================================
// server_t methods
// ==========================================================

void nplex::server_t::init(const params_t &params)
{
    init_users(params);
    init_data(params);
    init_event_loop(params);
    init_network(params);
}

void nplex::server_t::init_users(const params_t &params)
{
    std::vector<std::string> valid_users;

    for (const auto &user : params.users)
    {
        if (!is_valid_user(user))
            continue;

        if (m_users.contains(user.name))
            throw nplex_exception(fmt::format("Duplicated user ({})", user.name));

        m_users[user.name] = std::make_shared<user_t>(user);
        valid_users.push_back(user.name);
    }

    if (m_users.empty())
        throw nplex_exception("No valid users found");

    SPDLOG_INFO("Users: [{}]", fmt::join(valid_users, ", "));
}

void nplex::server_t::init_data(const params_t &params)
{
    m_storage = std::make_shared<storage_t>(params);

    auto [min_rev, max_rev] = m_storage->get_range();

    m_repo = m_storage->get_repo(max_rev);
}

void nplex::server_t::init_event_loop(const params_t &params)
{
    UNUSED(params);

    int rc = 0;

    m_loop = std::make_unique<uv_loop_t>();

    if (uv_loop_init(m_loop.get()) != 0)
        throw nplex_exception("Error initializing the event loop (uv_loop_init)");

    m_loop->data = this;

    m_async = std::make_unique<uv_async_t>();

    if (uv_async_init(m_loop.get(), m_async.get(), ::cb_process_async) != 0)
        throw nplex_exception(uv_strerror(rc));

    m_signal = std::make_unique<uv_signal_t>();

    if ((rc = uv_signal_init(m_loop.get(), m_signal.get())) != 0)
        throw nplex_exception(uv_strerror(rc));

    if (uv_signal_start(m_signal.get(), ::cb_signal_handler, SIGINT) != 0)
        throw nplex_exception("Error setting signal handler");

    // TODO: Remove this testing code
    uv_timer_t *timer = (uv_timer_t *) malloc(sizeof(uv_timer_t));
    uv_timer_init(m_loop.get(), timer);
    uv_timer_start(timer, ::cb_timer_test, 4000, 4000);
    uv_timer_start(timer, [](uv_timer_t *x) {
        auto *server = (nplex::server_t *) x->loop->data;
        server->simule_submit();
    }, 1000, 1000);
}

void nplex::server_t::init_network(const params_t &params)
{
    int rc = 0;
    struct sockaddr_storage addr_in = ::get_sockaddr(m_loop.get(), params.addr);

    m_max_connections = params.max_connections;

    m_tcp = std::make_unique<uv_tcp_t>();

    if ((rc = uv_tcp_init(m_loop.get(), m_tcp.get())) != 0)
        throw nplex_exception(uv_strerror(rc));

    if ((rc = uv_tcp_bind(m_tcp.get(), reinterpret_cast<struct sockaddr *>(&addr_in), 0)) != 0)
        throw nplex_exception(uv_strerror(rc));

    if ((rc = uv_listen((uv_stream_t*) m_tcp.get(), MAX_QUEUED_CONNECTIONS, ::cb_tcp_connection)) != 0)
        throw nplex_exception(uv_strerror(rc));

    SPDLOG_INFO("Nplex listening on {}", params.addr.str());
}

void nplex::server_t::run()
{
    try {
        m_running = true;
        SPDLOG_DEBUG("Event loop started");
        uv_run(m_loop.get(), UV_RUN_DEFAULT);
    }
    catch (const std::exception &e) {
        SPDLOG_ERROR("{}", e.what());
    }

    m_running = false;

    // stoping the thread writing to journal
    m_storage->close();

    // stoping tasks in the thread-pool
    if (m_num_running_tasks != 0)
        uv_run(m_loop.get(), UV_RUN_DEFAULT);

    uv_walk(m_loop.get(), ::cb_close_handle, NULL);
    while (uv_run(m_loop.get(), UV_RUN_NOWAIT));
    uv_loop_close(m_loop.get());
    SPDLOG_DEBUG("Event loop terminated");
}

void nplex::server_t::stop()
{
    // TODO: check if running
    // TODO: send signal to stop the event loop
}

void nplex::server_t::append_session(uv_stream_t *stream)
{
    assert(stream);

    if (!m_running)
        return;

    if (m_sessions.size() >= m_max_connections) {
        SPDLOG_WARN("Connection rejected (max clients reached)");
        return;
    }

    auto session = std::make_shared<session_t>(stream);
    SPDLOG_DEBUG("New connection: {}", session->m_addr.str());

    m_sessions.insert(session);
}

void nplex::server_t::release_session(session_t *session)
{
    if (!session)
        return;

    auto it = m_sessions.find(session);

    if (it == m_sessions.end())
        return;

    SPDLOG_INFO("Connection {} closed ({})", session->m_addr.str(), session->strerror());

    if (session->m_user && session->m_user->num_connections > 0)
        session->m_user->num_connections--;

    m_sessions.erase(it);
}

void nplex::server_t::on_msg_received(session_t *session, const msgs::Message *msg)
{
    if (!msg || !msg->content()) {
        session->disconnect(ERR_MSG_ERROR);
        return;
    }

    SPDLOG_DEBUG("Received {} from {}", msgs::EnumNameMsgContent(msg->content_type()), session->m_addr.str());

    switch (msg->content_type())
    {
        case msgs::MsgContent::LOGIN_REQUEST:
            process_login_request(session, msg->content_as_LOGIN_REQUEST());
            break;

        case msgs::MsgContent::LOAD_REQUEST:
            process_load_request(session, msg->content_as_LOAD_REQUEST());
            break;

        case msgs::MsgContent::SUBMIT_REQUEST:
            process_submit_request(session, msg->content_as_SUBMIT_REQUEST());
            break;

        case msgs::MsgContent::PING_REQUEST:
            process_ping_request(session, msg->content_as_PING_REQUEST());
            break;

        default:
            session->disconnect(ERR_MSG_ERROR);
    }
}

void nplex::server_t::process_login_request(session_t *session, const nplex::msgs::LoginRequest *req)
{
    if (session->m_state != session_t::state_e::CONNECTED) {
        session->disconnect(ERR_MSG_UNEXPECTED);
        return;
    }

    if (!req || !req->user() || !req->password()) {
        session->disconnect(ERR_MSG_ERROR);
        return;
    }

    if (req->api_version() != API_VERSION) {
        session->send(
            create_login_msg(
                req->cid(), 
                msgs::LoginCode::UNSUPPORTED_API_VERSION) 
        );
        session->disconnect(ERR_API_VERSION);
        return;
    }

    auto it = m_users.find(req->user()->str());
    if (it == m_users.end()) {
        session->send(
            create_login_msg(
                req->cid(), 
                msgs::LoginCode::INVALID_CREDENTIALS) 
        );
        session->disconnect(ERR_USR_NOT_FOUND);
        return;
    }

    auto &user = it->second;

    if (user->password != req->password()->str()) {
        session->send(
            create_login_msg(
                req->cid(), 
                msgs::LoginCode::INVALID_CREDENTIALS) 
        );
        session->disconnect(ERR_USR_INVL_PWD);
        return;
    }

    if (user->num_connections >= user->max_connections) {
        session->send(
            create_login_msg(
                req->cid(), 
                msgs::LoginCode::TOO_MANY_CONNECTIONS) 
        );
        session->disconnect(ERR_USR_MAX_CONN);
        return;
    }

    session->do_step1(user);
    user->num_connections++;

    SPDLOG_INFO("User {} logged from {}", user->name, session->m_addr.str());

    session->send(
        create_login_msg(
            req->cid(), 
            msgs::LoginCode::AUTHORIZED,
            0, //rev0,
            m_repo.rev(),
            *user
        ) 
    );
}

void nplex::server_t::process_load_request(session_t *session, const nplex::msgs::LoadRequest *req)
{
    if (session->m_state != session_t::state_e::LOGGED) {
        session->disconnect(ERR_MSG_UNEXPECTED);
        return;
    }

    switch (req->mode())
    {
        case msgs::LoadMode::SNAPSHOT_AT_FIXED_REV:
            if (req->rev() == m_repo.rev())
                send_last_snapshot(session, req->cid());
            else
                send_fixed_snapshot(session, req->rev(), req->cid());
            break;

        case msgs::LoadMode::SNAPSHOT_AT_LAST_REV:
            send_last_snapshot(session, req->cid());
            break;

        case msgs::LoadMode::ONLY_UPDATES_FROM_REV:
            //TODO: pending
            break;

        default:
            session->disconnect(ERR_MSG_ERROR);
    }
}

void nplex::server_t::process_submit_request(session_t *session, const nplex::msgs::SubmitRequest *req)
{
    if (session->m_state == session_t::state_e::CONNECTED) {
        session->disconnect(ERR_MSG_UNEXPECTED);
        return;
    }

    update_t update;

    auto rc = m_repo.try_commit(*session->m_user, req, update);

    session->send(
        create_submit_msg(
            req->cid(), 
            m_repo.rev(),
            rc,
            (rc == msgs::SubmitCode::ACCEPTED ? m_repo.rev() : 0)
        )
    );

    if (rc != msgs::SubmitCode::ACCEPTED)
        return;

    // TODO: repo cleanup (purge)

    push_update(update);
}

void nplex::server_t::process_ping_request(session_t *session, const nplex::msgs::PingRequest *req)
{
    if (session->m_state == session_t::state_e::CONNECTED) {
        session->disconnect(ERR_MSG_UNEXPECTED);
        return;
    }

    session->send(
        create_ping_msg(
            req->cid(), 
            m_repo.rev(),
            (req->payload() ? req->payload()->str() : "")
        )
    );
}

void nplex::server_t::send_last_snapshot(session_t *session, std::size_t cid)
{
    using namespace msgs;
    using namespace flatbuffers;

    FlatBufferBuilder builder;

    auto msg = CreateMessage(builder, 
        MsgContent::LOAD_RESPONSE,
        CreateLoadResponse(builder, 
            cid, 
            m_repo.rev(),
            true,
            m_repo.serialize(builder, session->m_user)
        ).Union() 
    );

    builder.Finish(msg);

    session->send(builder.Release());
    session->do_step2();
    session->m_state = session_t::state_e::SYNCED;
}

void nplex::server_t::send_fixed_snapshot(session_t *session, rev_t rev, std::size_t cid)
{
    auto [min_rev, max_rev] = m_storage->get_range();
    if (rev < min_rev || rev > max_rev) {
        session->send(
            create_load_err_msg(cid, m_repo.rev())
        );
        return;
    }

    submit_task(new repo_task_t(m_storage, session, rev, cid));
}

void nplex::server_t::submit_task(task_t *task)
{
    assert(task);

    if (!m_running) {
        delete task;
        return;
    }

    int rc = 0;
    if ((rc = uv_queue_work(m_loop.get(), &task->work, cb_task_run, cb_task_after)) != 0)
        throw std::runtime_error(uv_strerror(rc));

    m_num_running_tasks++;
}

void nplex::server_t::release_task(task_t *)
{
    assert(m_num_running_tasks);
    m_num_running_tasks--;

    if (!m_running)
        uv_stop(m_loop.get());
}

void nplex::server_t::push_update(const update_t &update)
{
    for (auto &session : m_sessions)
    {
        if (session->m_state != session_t::state_e::SYNCED)
            continue;

        flatbuffers::FlatBufferBuilder builder;

        auto off = serialize_update(builder, update, session->m_user.get());

        if (off.IsNull())
            continue;

        session->send(
            create_update_msg(builder, 0, m_repo.rev(), off)
        );
    }
}

// TODO: remove this test function
void nplex::server_t::simule_submit()
{
    auto it = m_users.find("admin");
    if (it == m_users.end()) {
        SPDLOG_WARN("Admin user not found");
        return;
    }

    auto &user = it->second;

    // Create a valid SubmitRequest message
    using namespace msgs;
    flatbuffers::FlatBufferBuilder builder;

    std::vector<flatbuffers::Offset<msgs::KeyValue>> upserts;
    std::vector<flatbuffers::Offset<flatbuffers::String>> deletes;
    std::vector<flatbuffers::Offset<flatbuffers::String>> ensures;

    upserts.push_back(
        CreateKeyValue(
            builder, 
            builder.CreateString("key1"), 
            builder.CreateVector((uint8_t *) "value1", 7)
        )
    );
    upserts.push_back(
        CreateKeyValue(
            builder, 
            builder.CreateString("key2"), 
            builder.CreateVector((uint8_t *) "value2", 7)
        )
    );

    auto msg = CreateMessage(builder, 
        MsgContent::SUBMIT_REQUEST, 
        CreateSubmitRequest(builder, 
            4,              // cid
            m_repo.rev(),   // crev
            1,              // type
            builder.CreateVector(upserts),
            builder.CreateVector(deletes),
            builder.CreateVector(ensures),
            true
        ).Union()
    );

    builder.Finish(msg);

    auto submit_req = flatbuffers::GetRoot<msgs::Message>(builder.GetBufferPointer())->content_as_SUBMIT_REQUEST();

    update_t update;
    auto rc = m_repo.try_commit(*user, submit_req, update);

    if (rc == msgs::SubmitCode::ACCEPTED) {
        SPDLOG_DEBUG("Update rev = {}", m_repo.rev());
        push_update(update);
    }
    else {
        SPDLOG_ERROR("Error try_commit = {}", static_cast<int>(rc));
    }

    m_storage->write_entry(std::move(update));
}
