#pragma once

#include <uv.h>
#include <flatbuffers/flatbuffers.h>
#include "addr.hpp"
#include "user.hpp"
#include "params.hpp"

namespace nplex {

/**
 * Class representing a client session.
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

    uv_tcp_t m_tcp = {};
    uv_timer_t *m_timer_disconnect = nullptr;
    uv_timer_t *m_timer_keepalive = nullptr;
    addr_t m_addr;                              // peer address
    state_e m_state = state_e::CLOSED;          // session state
    int m_error = 0;                            // disconnection cause
    user_ptr m_user;                            // session user
    std::string m_id;                           // session identifier (user@addr)
    rev_t m_lrev = 0;                           // last revision (maybe unacked, maybe not sent because no data)
    std::size_t m_load_cid = 0;                 // load correlation id
    bool m_ongoing_sync_task = false;           // true if a sync task is running

    char input_buffer[UINT16_MAX] = {0};        // input buffer used by read()
    std::string input_msg;                      // current incoming message

    struct {
        std::uint32_t queue_msgs = 0;           // number of messages in output queue
        std::uint32_t queue_bytes = 0;          // number of bytes in output queue
        std::size_t recv_msgs = 0;              // number of received messages
        std::size_t recv_bytes = 0;             // number of received bytes
        std::size_t sent_msgs = 0;              // number of sent messages (acknowledged)
        std::size_t sent_bytes = 0;             // number of sent bytes (acknowledged)
    } stats;

    explicit session_t(uv_stream_t *stream);
    ~session_t();
    session_t(const session_t&) = delete;
    session_t& operator=(const session_t&) = delete;

    void disconnect(int rc = 0);
    void send(flatbuffers::DetachedBuffer &&buf);
    void send_keepalive();
    void do_step1(const user_ptr &user);
    void do_step2();
    void report_peer_activity();
    std::string strerror() const;
};

using session_ptr = std::shared_ptr<session_t>;

} // namespace nplex
