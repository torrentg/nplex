#include <cassert>
#include <arpa/inet.h>
#include <spdlog/spdlog.h>
#include "exception.hpp"
#include "messaging.hpp"
#include "server.hpp"
#include "utils.hpp"
#include "session.hpp"

/**
 * Notes on this compilation unit:
 * 
 * When we use the libuv library we apply C conventions (instead of C++ ones):
 *   - When in Rome, do as the Romans do.
 *   - calloc/free are used instead of new/delete.
 *   - pointers to static functions.
 *   - C-style pointer casting.
 */

// maximum time between connection established and the login message received
#define TIMEOUT_STEP_1 5000
// maximum time between login and load messages reception
#define TIMEOUT_STEP_2 5000

// ==========================================================
// Internal (static) functions
// ==========================================================

template <typename T>
static uv_handle_t * get_handle(T *obj) {
    return reinterpret_cast<uv_handle_t *>(obj);
}

template <typename T>
static uv_stream_t * get_stream(T *obj) {
    return reinterpret_cast<uv_stream_t *>(obj);
}

static int get_peeraddr(const uv_tcp_t *con, char *str, size_t len)
{
    int rc = 0;
    sockaddr_storage sa;
    int sa_len = sizeof(sa);
    sockaddr_in *addr_in = reinterpret_cast<sockaddr_in *>(&sa);
    uint16_t port = 0;
    char ip[128] = {0};

    if ((rc = uv_tcp_getpeername(con, reinterpret_cast<sockaddr *>(&sa), &sa_len)) != 0)
        return rc;

    if ((rc = uv_ip_name(reinterpret_cast<sockaddr *>(&sa), ip, sizeof(ip))) != 0)
        return rc;

    port = ntohs(addr_in->sin_port);

    if (addr_in->sin_family == AF_INET6)
        snprintf(str, len, "[%s]:%hu", ip, port);
    else
        snprintf(str, len, "%s:%hu", ip, port);

    return 0;
}

static void cb_tcp_alloc(uv_handle_t *handle, size_t suggested_size, uv_buf_t *buf)
{
    UNUSED(suggested_size);

    auto *obj = reinterpret_cast<nplex::session_t *>(handle);

    buf->base = obj->input_buffer;
    buf->len = sizeof(obj->input_buffer);
}

static void cb_tcp_read(uv_stream_t *stream, ssize_t nread, const uv_buf_t *buf)
{
    using namespace nplex;

    auto *obj = reinterpret_cast<nplex::session_t *>(stream);
    auto *server = reinterpret_cast<nplex::server_t *>(stream->loop->data);

    if (nread == UV_EOF || buf->base == NULL) {
        obj->disconnect(ERR_CLOSED_BY_PEER);
        return;
    }

    if (nread < 0) {
        obj->disconnect((int) nread);
        return;
    }

    obj->report_peer_activity();

    obj->input_msg.append(buf->base, static_cast<std::size_t>(nread));

    while (obj->input_msg.size() >= sizeof(output_msg_t::len))
    {
        const char *ptr = obj->input_msg.c_str();
        std::uint32_t len = ntohl_ptr(ptr);

        if (len > obj->params.max_msg_bytes) {
            obj->disconnect(ERR_MSG_SIZE);
            return;
        }

        if (obj->input_msg.size() < len)
            break;

        auto msg = parse_network_msg(ptr, len);

        if (!msg) {
            obj->disconnect(ERR_MSG_ERROR);
            return;
        }

        server->on_msg_received(obj, msg);

        obj->input_msg.erase(0, len);
    }
}

static void cb_tcp_write(uv_write_t *req, int status)
{
    using namespace nplex;

    auto msg = std::unique_ptr<output_msg_t>(reinterpret_cast<output_msg_t *>(req));
    auto *obj = reinterpret_cast<session_t *>(req->handle);

    assert(obj->stats.unack_msgs > 0);
    assert(obj->stats.unack_bytes >= msg->length());

    obj->stats.unack_msgs--;
    obj->stats.unack_bytes -= msg->length();
    obj->stats.sent_msgs++;
    obj->stats.sent_bytes += msg->length();

    if (status < 0) {
        obj->disconnect(status);
        return;
    }

    obj->report_peer_activity();

    auto *ptr = flatbuffers::GetRoot<nplex::msgs::Message>(msg->content.data());
    assert(ptr);

    // TODO: notify server delivered message
    //obj->server()->on_msg_delivered(obj, ptr);
}

static void cb_timer_disconnect(uv_timer_t *timer)
{
    assert(timer->data);
    auto *session = static_cast<nplex::session_t *>(timer->data);

    switch (session->m_state)
    {
        case nplex::session_t::state_e::CLOSED:
            uv_timer_stop(timer);
            assert(false);
            break;
        case nplex::session_t::state_e::CONNECTED:
            session->disconnect(ERR_TIMEOUT_STEP_1);
            break;
        case nplex::session_t::state_e::LOGGED:
            session->disconnect(ERR_TIMEOUT_STEP_2);
            break;
        default:
            session->disconnect(ERR_CONNECTION_LOST);
            break;
    }
}

static void cb_timer_keepalive(uv_timer_t *timer)
{
    assert(timer->data);
    auto *session = static_cast<nplex::session_t *>(timer->data);

    switch (session->m_state)
    {
        case nplex::session_t::state_e::LOGGED:
        case nplex::session_t::state_e::SYNCING:
        case nplex::session_t::state_e::SYNCED:
            session->send_keepalive();
            break;
        default:
            uv_timer_stop(timer);
            assert(false);
            break;
    }
}

static void cb_close_timer(uv_handle_t *handle)
{
    SPDLOG_TRACE("Closing timer");
    delete reinterpret_cast<uv_timer_t *>(handle);
}

static void cb_close_session(uv_handle_t *handle)
{
    SPDLOG_TRACE("Closing session");

    using namespace nplex;

    assert(handle->data);
    auto *session = static_cast<session_t *>(handle->data);

    assert(handle->loop->data);
    auto *server = static_cast<server_t *>(handle->loop->data);

    // There is no guarantee on timer destruction order
    // In fact, they are destroyed after the session removal

    session->m_state = session_t::state_e::CLOSED;
    session->m_timer_disconnect = nullptr;
    session->m_timer_keepalive = nullptr;

    server->release_session(session);
}

// ==========================================================
// server_t methods
// ==========================================================

nplex::session_t::session_t(uv_stream_t *stream) : m_state{state_e::CLOSED}
{
    assert(stream);
    assert(stream->loop);
    assert(stream->loop->data);

    // We cannot throw an exception in this constructor because we need 
    // to close properly (in the event loop) the uv_tcp_t object.

    char addr_str[256] = {0};
    int rc = 0;

    if ((rc = uv_tcp_init(stream->loop, &m_tcp)) != 0)
        goto CTOR_ERR;

    if ((rc = uv_accept(stream, get_stream(&m_tcp))) != 0)
        goto CTOR_ERR;

    if ((rc = uv_read_start(get_stream(&m_tcp), ::cb_tcp_alloc, ::cb_tcp_read)) != 0)
        goto CTOR_ERR;

    if ((rc = get_peeraddr(&m_tcp, addr_str, sizeof(addr_str))) != 0)
        goto CTOR_ERR;

    try {
        m_timer_disconnect = new uv_timer_t{};
    }
    catch (const std::bad_alloc &) {
        rc = ENOMEM;
        goto CTOR_ERR;
    }

    if ((rc = uv_timer_init(stream->loop, m_timer_disconnect)) != 0) {
        delete m_timer_disconnect;
        m_timer_disconnect = nullptr;
        goto CTOR_ERR;
    }

    m_timer_disconnect->data = this;

    if ((rc = uv_timer_start(m_timer_disconnect, ::cb_timer_disconnect, TIMEOUT_STEP_1, 0)) != 0) {
        uv_close(get_handle(m_timer_disconnect), ::cb_close_timer);
        m_timer_disconnect = nullptr;
        goto CTOR_ERR;
    }

    m_tcp.data = this;
    m_addr = addr_t(addr_str);
    m_id = m_addr.str();
    m_state = session_t::state_e::CONNECTED;
    return;

CTOR_ERR:
    m_error = rc;
    m_tcp.data = this;
    uv_close(get_handle(this), ::cb_close_session);
}

nplex::session_t::~session_t()
{
    assert(m_state == state_e::CLOSED);
    assert(m_timer_keepalive == nullptr);
    assert(m_timer_disconnect == nullptr);
}

void nplex::session_t::disconnect(int rc)
{
    if (m_state == state_e::CLOSED)
        return;

    if (!m_error)
        m_error = rc;

    if (m_timer_disconnect && !uv_is_closing(get_handle(m_timer_disconnect))) {
        uv_timer_stop(m_timer_disconnect);
        uv_close(get_handle(m_timer_disconnect), ::cb_close_timer);
        m_timer_disconnect = nullptr;
    }

    if (m_timer_keepalive && !uv_is_closing(get_handle(m_timer_keepalive))) {
        uv_timer_stop(m_timer_keepalive);
        uv_close(get_handle(m_timer_keepalive), ::cb_close_timer);
        m_timer_keepalive = nullptr;
    }

    uv_close(get_handle(&m_tcp), ::cb_close_session);
}

void nplex::session_t::send(flatbuffers::DetachedBuffer &&buf)
{
    auto len = get_msg_length(buf);

    if (stats.unack_msgs >= params.max_unack_msgs)
        throw nplex_exception("Output message queue is full");

    if (stats.unack_bytes + len >= params.max_unack_bytes)
        throw nplex_exception("Too many output unacked bytes");

    auto *msg = new output_msg_t(std::move(buf));

    assert(len == msg->length());

    uv_write(&msg->req, get_stream(&m_tcp), msg->buf.data(), static_cast<unsigned int>(msg->buf.size()), ::cb_tcp_write);

    stats.unack_msgs++;
    stats.unack_bytes += static_cast<std::uint32_t>(len);

    auto aux = flatbuffers::GetRoot<nplex::msgs::Message>(msg->content.data());

    if (aux->content_type() == msgs::MsgContent::KEEPALIVE_PUSH)
        SPDLOG_TRACE("Sent {} to {}", msgs::EnumNameMsgContent(aux->content_type()), m_id);
    else
        SPDLOG_DEBUG("Sent {} to {}", msgs::EnumNameMsgContent(aux->content_type()), m_id);

    if (m_timer_keepalive && uv_is_active(get_handle(m_timer_keepalive)))
        uv_timer_again(m_timer_keepalive);
}

void nplex::session_t::send_keepalive()
{
    assert(m_tcp.loop->data);
    const auto *server = reinterpret_cast<nplex::server_t *>(m_tcp.loop->data);
    rev_t crev = server->rev();

    send(create_keepalive_msg(crev));

    if (m_timer_keepalive && uv_is_active(get_handle(m_timer_keepalive)))
        uv_timer_again(m_timer_keepalive);
}

void nplex::session_t::do_step1(const user_ptr &user)
{
    int rc = 0;

    m_user = user;
    m_state = session_t::state_e::LOGGED;
    m_id = fmt::format("{}@{}", user->name, m_addr.str());
    params.max_msg_bytes = user->max_msg_bytes;
    params.max_unack_bytes = user->max_unack_bytes;
    params.max_unack_msgs = user->max_unack_msgs;

    assert(m_timer_disconnect);
    uv_timer_stop(m_timer_disconnect);
    uv_timer_start(m_timer_disconnect, ::cb_timer_disconnect, TIMEOUT_STEP_2, 0);

    auto keepalive_millis = user->keepalive_millis;
    if (keepalive_millis == 0)
        return;

    assert(!m_timer_keepalive);

    m_timer_keepalive = new uv_timer_t{};

    if ((rc = uv_timer_init(m_tcp.loop, m_timer_keepalive)) != 0) {
        delete m_timer_keepalive;
        m_timer_keepalive = nullptr;
        disconnect(rc);
        return;
    }

    m_timer_keepalive->data = this;

    if ((rc = uv_timer_start(m_timer_keepalive, ::cb_timer_keepalive, keepalive_millis, keepalive_millis)) != 0)
        disconnect(rc);
}

void nplex::session_t::do_step2()
{
    m_state = session_t::state_e::SYNCING;

    assert(m_timer_disconnect);
    uv_timer_stop(m_timer_disconnect);

    uint64_t keepalive_millis = m_user->keepalive_millis;
    if (keepalive_millis == 0) {
        uv_close(get_handle(m_timer_disconnect), ::cb_close_timer);
        m_timer_disconnect = nullptr;
    }
    else {
        assert(m_user->timeout_factor > 1.0);
        uint64_t millis = static_cast<uint64_t>(static_cast<double>(keepalive_millis) * m_user->timeout_factor);
        uv_timer_start(m_timer_disconnect, ::cb_timer_disconnect, millis, millis);
    }
}

void nplex::session_t::report_peer_activity()
{
    auto handle = get_handle(m_timer_disconnect);

    if (handle && uv_is_active(handle) && !uv_is_closing(handle) && uv_timer_get_repeat(m_timer_disconnect))
        uv_timer_again(m_timer_disconnect);
}

std::string nplex::session_t::strerror() const
{
    if (m_error < 0)
        return uv_strerror(m_error);

    switch (m_error)
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
        case ERR_TIMEOUT_STEP_1: return "login request timeout";
        case ERR_TIMEOUT_STEP_2: return "load request timeout";
        case ERR_CONNECTION_LOST: return "connection lost";
        default: return fmt::format("unknow error -{}-", m_error);
    }
}
