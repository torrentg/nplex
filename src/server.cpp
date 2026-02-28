#include <cstdlib>
#include <cstring>
#include <cassert>
#include <stdexcept>
#include <filesystem>
#include <sys/socket.h>
#include <spdlog/spdlog.h>
#include "exception.hpp"
#include "messaging.hpp"
#include "context.hpp"
#include "config.hpp"
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
 * @param[in] loop Event loop.
 * @param[in] addr Address to convert.
 * 
 * @return The sockaddr.
 * 
 * @exception nplex_exception If the address is invalid. 
 */
static struct sockaddr_storage get_sockaddr(uv_loop_t *loop, const nplex::addr_t &addr)
{
    using namespace nplex;

    int rc = 0;
    sockaddr_storage ret;

    std::memset(&ret, 0, sizeof(ret));

    switch(addr.family())
    {
        case AF_INET:
            if ((rc = uv_ip4_addr(addr.host().c_str(), addr.port(), reinterpret_cast<sockaddr_in *>(&ret))) != 0)
                throw std::runtime_error(uv_strerror(rc));
            break;

        case AF_INET6:
            if ((rc = uv_ip6_addr(addr.host().c_str(), addr.port(), reinterpret_cast<sockaddr_in6 *>(&ret))) != 0)
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

            std::string port = std::to_string(addr.port());
            if ((rc = uv_getaddrinfo(loop, &req, nullptr, addr.host().c_str(), port.c_str(), &hints)) != 0)
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
    uv_signal_stop(handle);
    uv_stop(handle->loop);
}

static void cb_close_handle(uv_handle_t *handle, void *arg)
{
    UNUSED(arg);

    if (uv_is_closing(handle))
        return;

    switch(handle->type)
    {
        case UV_SIGNAL:
            SPDLOG_TRACE("Closing UV_SIGNAL");
            uv_close(handle, nullptr);
            break;
        case UV_TCP:
            SPDLOG_TRACE("Closing UV_TCP");
            uv_close(handle, nullptr);
            break;
        case UV_ASYNC:
            SPDLOG_TRACE("Closing UV_ASYNC");
            uv_close(handle, nullptr);
            break;
        case UV_TIMER:
            SPDLOG_TRACE("Closing UV_TIMER");
            uv_close(handle, (uv_close_cb) free);
            break;
        default:
            SPDLOG_TRACE("Closing UV_{}", uv_handle_type_name(handle->type));
            uv_close(handle, (uv_close_cb) free);
    }
}

static void cb_tcp_connection(uv_stream_t *stream, int status)
{
    if (status < 0) {
        SPDLOG_WARN(uv_strerror(status));
        return;
    }

    auto context = static_cast<nplex::context_t *>(stream->loop->data);
    assert(context);

    context->append_session(stream);
}

// ==========================================================
// server_t methods
// ==========================================================

void nplex::server_t::init(const config_t &config)
{
    SPDLOG_INFO("Initializing Nplex server ...");

    try {
        init_event_loop(config);
        init_context(config);
        init_signals(config);
        init_network(config);
        init_test(config);
    } catch (...) {
        m_loop.reset();
        m_context.reset();
        m_sigint.reset();
        m_sigterm.reset();
        m_tcp.reset();
        throw;
    }
}

void nplex::server_t::init_event_loop(const config_t &)
{
    int rc = 0;

    m_loop = std::make_unique<uv_loop_t>();

    if ((rc = uv_loop_init(m_loop.get())) != 0)
        throw nplex_exception(uv_strerror(rc));
}

void nplex::server_t::init_context(const config_t &config)
{
    m_context = std::make_shared<context_t>(m_loop.get(), config);
    m_loop->data = m_context.get();
}

void nplex::server_t::init_signals(const config_t &)
{
    int rc = 0;

    m_sigint = std::make_unique<uv_signal_t>();

    if ((rc = uv_signal_init(m_loop.get(), m_sigint.get())) != 0)
        throw nplex_exception(uv_strerror(rc));

    if ((rc = uv_signal_start(m_sigint.get(), ::cb_signal_handler, SIGINT)) != 0)
        throw nplex_exception(uv_strerror(rc));

    m_sigterm = std::make_unique<uv_signal_t>();

    if ((rc = uv_signal_init(m_loop.get(), m_sigterm.get())) != 0)
        throw nplex_exception(uv_strerror(rc));

    if ((rc = uv_signal_start(m_sigterm.get(), ::cb_signal_handler, SIGTERM)) != 0)
        throw nplex_exception(uv_strerror(rc));
}

// TODO: Remove this testing code
#define TEST_DELAY_BETWEEN_UPDATES_MS 0
void nplex::server_t::init_test(const config_t &)
{
    if (TEST_DELAY_BETWEEN_UPDATES_MS > 0)
    {
        uv_timer_t *timer = (uv_timer_t *) malloc(sizeof(uv_timer_t));
        timer->data = this;
        uv_timer_init(m_loop.get(), timer);
        uv_timer_start(timer, [](uv_timer_t *x) {
            auto server = (nplex::server_t *) x->data;
            server->simule_submit();
        }, TEST_DELAY_BETWEEN_UPDATES_MS, TEST_DELAY_BETWEEN_UPDATES_MS);
    }
}

void nplex::server_t::init_network(const config_t &config)
{
    int rc = 0;
    struct sockaddr_storage addr_in = ::get_sockaddr(m_loop.get(), config.context.addr);

    m_tcp = std::make_unique<uv_tcp_t>();
    m_tcp->data = this;

    if ((rc = uv_tcp_init(m_loop.get(), m_tcp.get())) != 0)
        throw nplex_exception(uv_strerror(rc));

    if ((rc = uv_tcp_bind(m_tcp.get(), reinterpret_cast<struct sockaddr *>(&addr_in), 0)) != 0)
        throw nplex_exception(uv_strerror(rc));

    if ((rc = uv_listen(reinterpret_cast<uv_stream_t *>(m_tcp.get()), MAX_QUEUED_CONNECTIONS, ::cb_tcp_connection)) != 0)
        throw nplex_exception(uv_strerror(rc));

    SPDLOG_INFO("Nplex listening on {}", config.context.addr.str());
}

void nplex::server_t::run() noexcept
{
    std::exception_ptr excp;

    if (!m_loop || !m_context)
        return;

    try {
        m_context->open();
        SPDLOG_INFO("Nplex server started");
        uv_run(m_loop.get(), UV_RUN_DEFAULT);
    }
    catch (const std::exception &e) {
        excp = std::current_exception();
        SPDLOG_ERROR("{}", e.what());
    }

    // stoping context (sessions, writer thread, etc)
    m_context->close();

    // awaiting until all tasks terminated and all sessions closed
    while (m_context->has_active_tasks_or_sessions())
        uv_run(m_loop.get(), UV_RUN_DEFAULT);

    // closing remaining objects
    uv_walk(m_loop.get(), ::cb_close_handle, nullptr);
    while (uv_run(m_loop.get(), UV_RUN_NOWAIT));
    uv_loop_close(m_loop.get());

    // free resources
    m_context.reset();
    m_tcp.reset();
    m_sigint.reset();
    m_sigterm.reset();
    m_loop.reset();

    SPDLOG_INFO("Nplex server terminated");

    if (excp)
        std::rethrow_exception(excp);
}

// TODO: remove this test function
void nplex::server_t::simule_submit()
{
    auto user = m_context->get_user("admin");
    if (!user) {
        SPDLOG_WARN("Admin user not found");
        return;
    }

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
            builder.CreateVector(reinterpret_cast<const uint8_t *>("value1"), 7)
        )
    );
    upserts.push_back(
        CreateKeyValue(
            builder, 
            builder.CreateString("key2"), 
            builder.CreateVector(reinterpret_cast<const uint8_t *>("value2"), 7)
        )
    );

    auto msg = CreateMessage(builder, 
        MsgContent::SUBMIT_REQUEST, 
        CreateSubmitRequest(builder, 
            4,              // cid
            m_context->last_persisted_rev(), // crev
            1,              // type
            builder.CreateVector(upserts),
            builder.CreateVector(deletes),
            builder.CreateVector(ensures),
            true
        ).Union()
    );

    builder.Finish(msg);

    auto submit_req = flatbuffers::GetRoot<msgs::Message>(builder.GetBufferPointer())->content_as_SUBMIT_REQUEST();

    m_context->try_commit(submit_req, *user);
}
