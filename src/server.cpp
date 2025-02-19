#include <cstdlib>
#include <cstring>
#include <cassert>
#include <stdexcept>
#include <fmt/core.h>
#include <sys/socket.h>
#include <spdlog/spdlog.h>
#include "exception.hpp"
#include "server.hpp"

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
            if ((rc = uv_getaddrinfo(loop, &req, NULL, addr.host().c_str(), NULL, &hints)) != 0)
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
    spdlog::warn("Signal received: {}({})", ::strsignal(signum), signum);
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

static void cb_close_connection(uv_handle_t *handle)
{
    if (uv_is_closing(handle))
        return;

    using namespace nplex;
    auto *server = (server_t *) handle->loop->data;
    assert(server);
    server->release_client((client_t *) handle);
}

static void cb_close_handle(uv_handle_t *handle, void *arg)
{
    UNUSED(arg);

    if (uv_is_closing(handle))
        return;

    switch(handle->type)
    {
        case UV_SIGNAL:
        case UV_ASYNC:
            uv_close(handle, NULL);
            break;
        case UV_TCP:
            cb_close_connection(handle);
            break;
        default:
            uv_close(handle, (uv_close_cb) free);
    }
}

static void cb_tcp_connection(uv_stream_t *stream, int status)
{
    if (status < 0) {
        spdlog::warn(uv_strerror(status));
        return;
    }

    using namespace nplex;
    auto *server = (server_t *) stream->loop->data;
    assert(server);
    server->append_client(stream);
}

// ==========================================================
// server_t methods
// ==========================================================

nplex::server_t::server_t(const server_params_t &params) : m_params(params)
{
    int rc = 0;

    for (const auto &user_params : params.users)
    {
        if (!user_params.is_valid())
            continue;

        if (m_users.contains(user_params.name))
            throw nplex_exception(fmt::format("Duplicated user ({})", user_params.name));

        m_users[user_params.name] = std::make_shared<user_t>(user_params);
    }

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

    spdlog::info("Nplex listening on {}", params.addr.str());

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
        spdlog::debug("Event loop started");
        uv_run(m_loop.get(), UV_RUN_DEFAULT);
    }
    catch (const std::exception &e) {
        spdlog::error("{}", e.what());
    }

    uv_walk(m_loop.get(), ::cb_close_handle, NULL);
    while (uv_run(m_loop.get(), UV_RUN_NOWAIT));
    uv_loop_close(m_loop.get());
    spdlog::debug("Event loop terminated");
}

void nplex::server_t::append_client(uv_stream_t *stream)
{
    m_clients.emplace_back(std::make_unique<client_t>(stream));
}

void nplex::server_t::release_client(client_t *con)
{
    if (!con)
        return;

    auto it = std::find_if(m_clients.begin(), m_clients.end(),
                           [con](const std::unique_ptr<client_t> &con_) {
                               return con_.get() == con;
                           });

    if (it != m_clients.end()) {
        spdlog::info("Releasing connection: {}", con->m_addr.str());
        m_clients.erase(it);
    }
}
