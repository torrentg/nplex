#include <cstdlib>
#include <cstring>
#include <cassert>
#include <stdexcept>
#include <fmt/core.h>
#include <sys/socket.h>
#include <spdlog/spdlog.h>
#include "messaging.hpp"
#include "exception.hpp"
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
    // auto *impl = (nplex::client_t::impl_t *) handle->loop->data;
    // impl->process_commands();
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
                uv_close(handle, nplex::cb_close_client);
            else                // listener
                uv_close(handle, nullptr);
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

    using namespace nplex;
    auto *server = (server_t *) stream->loop->data;
    assert(server);

    auto *client = new client_t(stream);
    server->append_client(client);
}

static bool is_valid_user(const nplex::user_params_t &user)
{
    return (user.active && !user.password.empty() && user.max_connections > 0 && !user.permissions.empty());
}

// ==========================================================
// server_t methods
// ==========================================================

nplex::server_t::server_t(const server_params_t &params) : m_params(params)
{
    int rc = 0;

    for (const auto &user_params : params.users)
    {
        if (!is_valid_user(user_params))
            continue;

        if (m_users.contains(user_params.name))
            throw nplex_exception(fmt::format("Duplicated user ({})", user_params.name));

        m_users[user_params.name] = std::make_shared<user_t>(user_params);
    }

    if (m_users.empty())
        throw nplex_exception("No valid users found");

    m_loop = std::make_unique<uv_loop_t>();

    if (uv_loop_init(m_loop.get()) != 0)
        throw nplex_exception("Error initializing the event loop (uv_loop_init)");

    m_loop->data = this;

    struct sockaddr_storage addr_in = ::get_sockaddr(m_loop.get(), params.addr);

    m_tcp = std::make_unique<uv_tcp_t>();

    if ((rc = uv_tcp_init(m_loop.get(), m_tcp.get())) != 0)
        throw nplex_exception(uv_strerror(rc));

    if ((rc = uv_tcp_bind(m_tcp.get(), reinterpret_cast<struct sockaddr *>(&addr_in), 0)) != 0)
        throw nplex_exception(uv_strerror(rc));

    if ((rc = uv_listen((uv_stream_t*) m_tcp.get(), MAX_QUEUED_CONNECTIONS, ::cb_tcp_connection)) != 0)
        throw nplex_exception(uv_strerror(rc));

    SPDLOG_INFO("Nplex listening on {}", params.addr.str());

    m_async = std::make_unique<uv_async_t>();

    if (uv_async_init(m_loop.get(), m_async.get(), ::cb_process_async) != 0)
        throw nplex_exception(uv_strerror(rc));

    m_signal = std::make_unique<uv_signal_t>();

    if ((rc = uv_signal_init(m_loop.get(), m_signal.get())) != 0)
        throw nplex_exception(uv_strerror(rc));

    if (uv_signal_start(m_signal.get(), ::cb_signal_handler, SIGINT) != 0)
        throw nplex_exception("Error starting signal handler (uv_signal_start)");

    // TODO: start write thread
}

void nplex::server_t::run()
{
    try {
        SPDLOG_DEBUG("Event loop started");
        uv_run(m_loop.get(), UV_RUN_DEFAULT);
    }
    catch (const std::exception &e) {
        SPDLOG_ERROR("{}", e.what());
    }
SPDLOG_ERROR("Event loop terminated (PRE)");
    uv_walk(m_loop.get(), ::cb_close_handle, NULL);
    while (uv_run(m_loop.get(), UV_RUN_NOWAIT));
    uv_loop_close(m_loop.get());
    SPDLOG_DEBUG("Event loop terminated");
}

void nplex::server_t::append_client(client_t *client)
{
    assert(client);

    SPDLOG_DEBUG("New connection: {}", client->m_addr.str());

    if (m_clients.size() >= m_params.max_connections) {
        SPDLOG_WARN("Max clients reached: {}", m_params.max_connections);
        client->disconnect(ERR_MAX_CONN);
        return;
    }

    m_clients.insert(client);

}

void nplex::server_t::release_client(client_t *client)
{
    if (!client)
        return;

    m_clients.erase(client);

    if (client->m_user && client->m_user->num_connections > 0)
        client->m_user->num_connections--;

    SPDLOG_INFO("Connection {} closed ({})", client->m_addr.str(), client->strerror());
}

void nplex::server_t::on_msg_delivered(client_t *client, const msgs::Message *msg)
{
    UNUSED(client);
    UNUSED(msg);
    // TODO: report client activity
}

void nplex::server_t::on_msg_received(client_t *client, const msgs::Message *msg)
{
    if (!msg || !msg->content()) {
        client->disconnect(ERR_MSG_ERROR);
        return;
    }

    // TODO: report client activity

    switch (msg->content_type())
    {
        case msgs::MsgContent::LOGIN_REQUEST:
            process_login_request(client, msg->content_as_LOGIN_REQUEST());
            break;

        case msgs::MsgContent::LOAD_REQUEST:
            process_load_request(client, msg->content_as_LOAD_REQUEST());
            break;

        case msgs::MsgContent::SUBMIT_REQUEST:
            process_submit_request(client, msg->content_as_SUBMIT_REQUEST());
            break;

        case msgs::MsgContent::PING_REQUEST:
            process_ping_request(client, msg->content_as_PING_REQUEST());
            break;

        default:
            client->disconnect(ERR_MSG_ERROR);
    }
}

void nplex::server_t::process_login_request(client_t *client, const nplex::msgs::LoginRequest *req)
{
    if (client->m_state != client_t::state_e::CONNECTED) {
        client->disconnect(ERR_MSG_UNEXPECTED);
        return;
    }

    if (!req || !req->user() || !req->password()) {
        client->disconnect(ERR_MSG_ERROR);
        return;
    }

    if (req->api_version() != API_VERSION) {
        client->send(
            create_login_msg(
                req->cid(), 
                msgs::LoginCode::UNSUPPORTED_API_VERSION) 
        );
        client->disconnect(ERR_API_VERSION);
        return;
    }

    auto it = m_users.find(req->user()->str());
    if (it == m_users.end()) {
        client->send(
            create_login_msg(
                req->cid(), 
                msgs::LoginCode::INVALID_CREDENTIALS) 
        );
        client->disconnect(ERR_USR_NOT_FOUND);
        return;
    }

    auto &user = it->second;

    if (user->params.password != req->password()->str()) {
        client->send(
            create_login_msg(
                req->cid(), 
                msgs::LoginCode::INVALID_CREDENTIALS) 
        );
        client->disconnect(ERR_USR_INVL_PWD);
        return;
    }

    if (user->num_connections + 1 >= user->params.max_connections) {
        client->send(
            create_login_msg(
                req->cid(), 
                msgs::LoginCode::TOO_MANY_CONNECTIONS) 
        );
        client->disconnect(ERR_USR_MAX_CONN);
        return;
    }

    client->m_user = user;
    client->m_state = client_t::state_e::LOGGED;
    client->params.max_msg_bytes = user->params.max_msg_bytes;
    client->params.max_unack_bytes = user->params.max_unack_bytes;
    client->params.max_unack_msgs = user->params.max_unack_msgs;
    user->num_connections++;

    SPDLOG_INFO("User {} logged from {}", user->params.name, client->m_addr.str());

    client->send(
        create_login_msg(
            req->cid(), 
            msgs::LoginCode::AUTHORIZED,
            0, //rev0,
            0, //crev,
            user->params.can_force,
            user->params.keepalive_millis
        ) 
    );
}

void nplex::server_t::process_load_request(client_t *client, const nplex::msgs::LoadRequest *req)
{
    UNUSED(client);
    UNUSED(req);
    // TODO: implement
}

void nplex::server_t::process_submit_request(client_t *client, const nplex::msgs::SubmitRequest *req)
{
    UNUSED(client);
    UNUSED(req);
    // TODO: implement
}

void nplex::server_t::process_ping_request(client_t *client, const nplex::msgs::PingRequest *req)
{
    UNUSED(client);
    UNUSED(req);
    // TODO: implement
}
