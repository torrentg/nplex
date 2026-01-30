#include <cassert>
#include <arpa/inet.h>
#include <spdlog/spdlog.h>
#include <flatbuffers/flatbuffers.h>
#include "messaging.hpp"
#include "context.hpp"
#include "session.hpp"
#include "connection.hpp"

// ==========================================================
// Internal (static) functions
// ==========================================================

template <typename T>
static auto get_handle(T* obj) -> decltype(reinterpret_cast<std::conditional_t<std::is_const<T>::value, const uv_handle_t*, uv_handle_t*>>(obj)) {
    return reinterpret_cast<std::conditional_t<std::is_const<T>::value, const uv_handle_t*, uv_handle_t*>>(obj);
}

template <typename T>
static auto get_stream(T* obj) -> decltype(reinterpret_cast<std::conditional_t<std::is_const<T>::value, const uv_stream_t*, uv_stream_t*>>(obj)) {
    return reinterpret_cast<std::conditional_t<std::is_const<T>::value, const uv_stream_t*, uv_stream_t*>>(obj);
}

static int get_peeraddr(const uv_tcp_t *con, char *str, size_t len)
{
    int rc = 0;
    sockaddr_storage sa = {};
    int sa_len = sizeof(sa);

    if ((rc = uv_tcp_getpeername(con, reinterpret_cast<sockaddr *>(&sa), &sa_len)) != 0)
        return rc;

    char ip[INET6_ADDRSTRLEN] = {0};
    if ((rc = uv_ip_name(reinterpret_cast<sockaddr *>(&sa), ip, sizeof(ip))) != 0)
        return rc;

    const int family = sa.ss_family;

    if (family == AF_INET) {
        auto *addr_in = reinterpret_cast<const sockaddr_in *>(&sa);
        const uint16_t port = ntohs(addr_in->sin_port);
        std::snprintf(str, len, "%s:%hu", ip, port);
        return 0;
    }

    if (family == AF_INET6) {
        auto *addr_in6 = reinterpret_cast<const sockaddr_in6 *>(&sa);
        const uint16_t port = ntohs(addr_in6->sin6_port);
        std::snprintf(str, len, "[%s]:%hu", ip, port);
        return 0;
    }

    return UV_EAI_FAMILY;
}

static void cb_tcp_alloc(uv_handle_t *handle, size_t suggested_size, uv_buf_t *buf)
{
    UNUSED(suggested_size);

    auto obj = reinterpret_cast<nplex::connection_s *>(handle);

    buf->base = obj->m_input_buffer;
    buf->len = sizeof(obj->m_input_buffer);
}

static void cb_tcp_read(uv_stream_t *stream, ssize_t nread, const uv_buf_t *buf)
{
    using namespace nplex;

    auto obj = reinterpret_cast<nplex::connection_s *>(stream);

    if (nread == 0)
        return;

    if (nread == UV_EOF || buf->base == nullptr) {
        obj->disconnect(ERR_CLOSED_BY_PEER);
        return;
    }

    if (nread < 0) {
        obj->disconnect((int) nread);
        return;
    }

    obj->report_peer_activity();

    obj->m_input_msg.append(buf->base, static_cast<std::size_t>(nread));

    while (obj->m_input_msg.size() >= sizeof(output_msg_t::len))
    {
        const char *ptr = obj->m_input_msg.data();
        std::uint32_t len = ntohl_ptr(ptr);

        if (len > obj->m_params.max_msg_bytes) {
            obj->disconnect(ERR_MSG_SIZE);
            return;
        }

        if (obj->m_input_msg.size() < len)
            break;

        auto msg = parse_network_msg(ptr, len);

        if (!msg) {
            obj->disconnect(ERR_MSG_ERROR);
            return;
        }

        obj->m_stats.recv_msgs++;
        obj->m_stats.recv_bytes += len;
        obj->session()->process_request(msg);

        obj->m_input_msg.erase(0, len);
    }
}

static void cb_tcp_write(uv_write_t *req, int status)
{
    using namespace nplex;

    auto msg = std::unique_ptr<output_msg_t>(reinterpret_cast<output_msg_t *>(req));
    auto obj = reinterpret_cast<connection_s *>(req->handle);

    assert(msg);
    assert(obj->m_stats.unack_msgs > 0);
    assert(obj->m_stats.unack_bytes >= msg->length());

    obj->m_stats.unack_msgs--;
    obj->m_stats.unack_bytes -= msg->length();
    obj->m_stats.sent_msgs++;
    obj->m_stats.sent_bytes += msg->length();

    if (status < 0) {
        obj->disconnect(status);
        return;
    }

    obj->report_peer_activity();

    auto ptr = flatbuffers::GetRoot<nplex::msgs::Message>(msg->content.data());
    assert(ptr);

    obj->session()->process_delivery(ptr);
}

static void cb_close_timer(uv_handle_t *handle)
{
    SPDLOG_TRACE("Closing timer");
    delete reinterpret_cast<uv_timer_t *>(handle);
}

static void cb_close_connection(uv_handle_t *handle)
{
    SPDLOG_TRACE("Closing connection");

    using namespace nplex;

    auto obj = reinterpret_cast<connection_s *>(handle);

    // There is no guarantee on timer destruction order
    // In fact, they are destroyed after the session removal

    obj->m_timer_disconnect = nullptr;
    obj->m_timer_keepalive = nullptr;

    auto session = obj->session();
    obj->context()->release_session(session);
}

static void cb_tcp_shutdown(uv_shutdown_t *req, int status)
{
    SPDLOG_TRACE("Shutdown connection");

    using namespace nplex;

    auto obj = reinterpret_cast<connection_s *>(req->data);
    assert(obj);

    delete req;

    obj->disconnect(status);
}

// ==========================================================
// connection_s methods
// ==========================================================

nplex::connection_s::connection_s(session_t *session, uv_stream_t *stream)
{
    assert(stream);
    assert(stream->loop);
    assert(stream->loop->data);

    // We cannot throw an exception in this constructor because we need 
    // to close it properly (in the event loop) due to the uv_tcp_t object.

    int rc = 0;
    char addr_str[256] = {0};

    if ((rc = uv_tcp_init(stream->loop, &m_tcp)) != 0)
        goto CTOR_ERR;

    m_tcp.data = session;

    if ((rc = uv_accept(stream, get_stream(&m_tcp))) != 0)
        goto CTOR_ERR;

    if ((rc = uv_read_start(get_stream(&m_tcp), ::cb_tcp_alloc, ::cb_tcp_read)) != 0)
        goto CTOR_ERR;

    if ((rc = get_peeraddr(&m_tcp, addr_str, sizeof(addr_str))) != 0)
        goto CTOR_ERR;

    m_addr = addr_t(addr_str);

    return;

CTOR_ERR:
    m_error = rc;
    uv_close(get_handle(this), ::cb_close_connection);
}

nplex::connection_s::~connection_s()
{
    assert(is_closed());
    assert(m_timer_keepalive == nullptr);
    assert(m_timer_disconnect == nullptr);
}

void nplex::connection_s::disconnect(int rc)
{
    if (!m_error)
        m_error = rc;

    if (m_timer_disconnect && !uv_is_closing(get_handle(m_timer_disconnect))) {
        uv_timer_stop(m_timer_disconnect);
        uv_close(get_handle(m_timer_disconnect), ::cb_close_timer);
    }

    if (m_timer_keepalive && !uv_is_closing(get_handle(m_timer_keepalive))) {
        uv_timer_stop(m_timer_keepalive);
        uv_close(get_handle(m_timer_keepalive), ::cb_close_timer);
    }

    if (!uv_is_closing(get_handle(&m_tcp)))
        uv_close(get_handle(&m_tcp), ::cb_close_connection);

    m_timer_disconnect = nullptr;
    m_timer_keepalive = nullptr;
}

void nplex::connection_s::shutdown(int rc)
{
    if (!m_error)
        m_error = rc;

    if (uv_is_closing(get_handle(&m_tcp)))
        return;

    uv_read_stop(get_stream(&m_tcp));

    auto req = new uv_shutdown_t{};
    req->data = this;

    int uvrc = uv_shutdown(req, get_stream(&m_tcp), ::cb_tcp_shutdown);
    if (uvrc != 0) {
        delete req;
        if (uvrc == UV_EALREADY)
            return;
        disconnect(uvrc);
    }
}

bool nplex::connection_s::is_blocked() const
{
    if (m_stats.unack_msgs == 0) // always there is room for the first message
        return false;

    if (m_stats.unack_msgs >= m_params.max_unack_msgs)
        return true;

    if (m_stats.unack_bytes >= m_params.max_unack_bytes)
        return true;

    return false;
}

bool nplex::connection_s::is_closed() const
{
    return uv_is_closing(get_handle(&m_tcp));
}

void nplex::connection_s::send(flatbuffers::DetachedBuffer &&buf)
{
    int rc = 0;

    auto msg = new output_msg_t(std::move(buf));

    if ((rc = uv_write(&msg->req, get_stream(&m_tcp), msg->buf.data(), static_cast<unsigned int>(msg->buf.size()), ::cb_tcp_write)) != 0) {
        delete msg;
        disconnect(rc);
        return;
    }

    m_stats.unack_msgs++;
    m_stats.unack_bytes += msg->length();

    if (m_timer_keepalive && uv_is_active(get_handle(m_timer_keepalive)))
        uv_timer_again(m_timer_keepalive);
}

void nplex::connection_s::send_keepalive()
{
    if (m_stats.unack_msgs > 0)
        return;

    rev_t crev = context()->last_persisted_rev();
    session()->send(create_keepalive_msg(crev));
}

void nplex::connection_s::report_peer_activity()
{
    auto handle = get_handle(m_timer_disconnect);

    if (handle && uv_is_active(handle) && !uv_is_closing(handle) && uv_timer_get_repeat(m_timer_disconnect))
        uv_timer_again(m_timer_disconnect);
}

// ==========================================================
// connection_t methods
// ==========================================================

void nplex::connection_t::config(std::uint32_t max_msg_bytes, std::uint32_t max_queue_length, std::uint32_t max_queue_bytes)
{
    m_params.max_msg_bytes = (max_msg_bytes == 0 ? UINT32_MAX : max_msg_bytes);
    m_params.max_unack_msgs = (max_queue_length == 0 ? UINT32_MAX : max_queue_length);
    m_params.max_unack_bytes = (max_queue_bytes == 0 ? UINT32_MAX : max_queue_bytes);
}

void nplex::connection_t::set_timer(uv_timer_t *&timer, std::uint32_t millis, uv_timer_cb timer_cb)
{
    int rc = 0;

    if (millis == 0 && timer == nullptr)
        return;

    if (timer == nullptr)
    {
        timer = new uv_timer_t{};

        if ((rc = uv_timer_init(m_tcp.loop, timer)) != 0) {
            delete timer;
            timer = nullptr;
            disconnect(rc);
            return;
        }

        timer->data = static_cast<connection_s *>(this);
    }

    uv_timer_stop(timer);

    if (millis == 0) {
        uv_close(get_handle(timer), ::cb_close_timer);
        timer = nullptr;
        return;
    }

    if ((rc = uv_timer_start(timer, timer_cb, millis, millis)) != 0)
        disconnect(rc);
}

void nplex::connection_t::set_keepalive(std::uint32_t millis)
{
    set_timer(m_timer_keepalive, millis, [](auto timer) {
        static_cast<connection_s *>(timer->data)->send_keepalive();
        uv_timer_again(timer);
    });
}

void nplex::connection_t::set_connection_lost(std::uint32_t millis)
{
    set_timer(m_timer_disconnect, millis, [](auto timer) {
        static_cast<connection_s *>(timer->data)->disconnect(ERR_CONNECTION_LOST);
    });
}
