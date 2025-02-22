#include <cassert>
#include <arpa/inet.h>
#include <spdlog/spdlog.h>
#include "exception.hpp"
#include "messaging.hpp"
#include "server.hpp"
#include "client.hpp"

/**
 * Notes on this compilation unit:
 * 
 * When we use the libuv library we apply C conventions (instead of C++ ones):
 *   - When in Rome, do as the Romans do.
 *   - calloc/free are used instead of new/delete.
 *   - pointers to static functions.
 *   - C-style pointer casting.
 */

// ==========================================================
// Internal (static) functions
// ==========================================================

static int get_peeraddr(const uv_tcp_t *con, char *str, size_t len)
{
    int rc = 0;
    struct sockaddr_storage sa;
    int sa_len = sizeof(sa);
    struct sockaddr_in *addr_in = (struct sockaddr_in *) &sa;
    uint16_t port = 0;
    char ip[128] = {0};

    if ((rc = uv_tcp_getpeername((const uv_tcp_t *) con, (struct sockaddr *) &sa, &sa_len)) != 0)
        return rc;

    if ((rc = uv_ip_name((struct sockaddr *) &sa, ip, sizeof(ip))) != 0)
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
    using namespace nplex;
    UNUSED(suggested_size);

    auto *obj = (client_t *) handle;

    buf->base = obj->input_buffer;
    buf->len = sizeof(obj->input_buffer);
}

static void cb_tcp_read(uv_stream_t *stream, ssize_t nread, const uv_buf_t *buf)
{
    using namespace nplex;

    auto *obj = (client_t *) stream;
    auto *server = (server_t *) stream->loop->data;

    if (nread == UV_EOF || buf->base == NULL) {
        obj->disconnect(ERR_CLOSED_BY_PEER);
        return;
    }

    if (nread < 0) {
        obj->disconnect((int) nread);
        return;
    }

    obj->input_msg.append(buf->base, static_cast<std::size_t>(nread));

    while (obj->input_msg.size() >= sizeof(output_msg_t::len))
    {
        const char *ptr = obj->input_msg.c_str();
        std::uint32_t len = ntohl(*((const std::uint32_t *) ptr));

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

    auto msg = std::unique_ptr<output_msg_t>((output_msg_t *) req);
    auto *obj = (client_t *) req->handle;

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

    auto *ptr = flatbuffers::GetRoot<nplex::msgs::Message>(msg->content.data());
    assert(ptr);

    // TODO: notify server delivered message
    //obj->client()->on_msg_delivered(obj, ptr);
}

// ==========================================================
// nplex functions
// ==========================================================

void nplex::cb_close_client(uv_handle_t *handle)
{
    using namespace nplex;
    auto *client = (client_t *) handle->data;
    assert(client);
    auto *server = (server_t *) handle->loop->data;
    assert(server);
    client->m_state = client_t::state_e::CLOSED;
    server->release_client(client);
    delete client;
}

// ==========================================================
// server_t methods
// ==========================================================

nplex::client_t::client_t(uv_stream_t *stream) : m_state{state_e::CLOSED}
{
    assert(stream);
    assert(stream->loop);
    assert(stream->loop->data);

    // We cannot throw an exception in this constructor because we need 
    // to close properly (in the event loop) the uv_tcp_t object.

    char addr_str[256] = {0};
    int rc = 0;

    if ((rc = uv_tcp_init(stream->loop, &m_tcp)) != 0)
        goto CLIENT_ERR;

    if ((rc = uv_accept(stream, (uv_stream_t *) &m_tcp)) != 0)
        goto CLIENT_ERR;

    if ((rc = uv_read_start((uv_stream_t*) &m_tcp, ::cb_tcp_alloc, ::cb_tcp_read)) != 0)
        goto CLIENT_ERR;

    if ((rc = get_peeraddr(&m_tcp, addr_str, sizeof(addr_str))) != 0)
        goto CLIENT_ERR;

    m_tcp.data = this;
    m_addr = addr_t(addr_str);
    m_state = client_t::state_e::CONNECTED;
    return;

CLIENT_ERR:
    m_error = rc;
    m_tcp.data = this;
    uv_close((uv_handle_t*) this, cb_close_client);
    SPDLOG_WARN(uv_strerror(rc));
}

nplex::client_t::~client_t()
{
    //assert(m_state == state_e::CLOSED);
}

void nplex::client_t::disconnect(int rc)
{
    if (m_state == state_e::CLOSED)
        return;

    if (!m_error)
        m_error = rc;

    uv_close((uv_handle_t *) &m_tcp, cb_close_client);
}

void nplex::client_t::send(flatbuffers::DetachedBuffer &&buf)
{
    auto len = get_msg_length(buf);

    if (stats.unack_msgs >= params.max_unack_msgs)
        throw nplex_exception("Output message queue is full");

    if (len > params.max_msg_bytes)
        throw nplex_exception("Message too large");

    if (stats.unack_bytes + len >= params.max_unack_bytes)
        throw nplex_exception("Too many output unacked bytes");

    auto *msg = new output_msg_t(std::move(buf));

    assert(len == msg->length());

    uv_write(&msg->req, (uv_stream_t *) &m_tcp, msg->buf.data(), (unsigned int) msg->buf.size(), ::cb_tcp_write);

    stats.unack_msgs++;
    stats.unack_bytes += static_cast<std::uint32_t>(len);
}

std::string nplex::client_t::strerror() const
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
        default: return fmt::format("unknow error -{}-", m_error);
    }
}
