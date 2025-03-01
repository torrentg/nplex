#pragma once

#include <uv.h>
#include <flatbuffers/flatbuffers.h>
#include "addr.hpp"
#include "user.hpp"
#include "params.hpp"

namespace nplex {

/**
 * Internal class representing a client session.
 * 
 * server_t is accessed via tcp.loop->data.
 */
struct session_t
{
    enum class state_e : std::uint8_t {
        CONNECTED,
        LOGGED,
        SYNCING,
        SYNCED,
        CLOSED
    };

    uv_tcp_t m_tcp;
    uv_timer_t *m_timer_disconnect = nullptr;
    uv_timer_t *m_timer_keepalive = nullptr;
    addr_t m_addr;
    state_e m_state;
    int m_error = 0;
    user_ptr m_user;

    char input_buffer[UINT16_MAX] = {0};
    std::string input_msg;

    struct {
        std::uint32_t max_unack_msgs = 1;       // enough to send login response
        std::uint32_t max_unack_bytes = 1024;   // enough to send login response
        std::uint32_t max_msg_bytes = 1024;     // enough to receive login message
    } params;

    struct {
        std::uint32_t unack_msgs = 0;
        std::uint32_t unack_bytes = 0;
        std::size_t recv_msgs = 0;
        std::size_t recv_bytes = 0;
        std::size_t sent_msgs = 0;
        std::size_t sent_bytes = 0;
    } stats;

    session_t(uv_stream_t *stream);
    ~session_t();

    void disconnect(int rc = 0);
    void send(flatbuffers::DetachedBuffer &&buf);
    void send_keepalive();
    void do_step1(const user_ptr &user);
    void do_step2();
    void report_peer_activity();
    std::string strerror() const;
};

void cb_close_session(uv_handle_t *handle);

} // namespace nplex
