#pragma once

#include <uv.h>
#include <flatbuffers/flatbuffers.h>
#include "addr.hpp"

namespace nplex {

// Forward declarations
class session_t;
class server_t;

/**
 * Connection states.
 */
enum class conn_state_e : std::uint8_t {
    CONNECTED,
    LOGGED,
    SYNCING,
    SYNCED,
    CLOSED
};

/**
 * Connection memory layout.
 * 
 * These are the connection_t private members of connection_t that are 
 * accessed by public libuv callbacks.
 * 
 * m_tcp.data points to the session.
 * m_tcp.loop->data points to the server.
 */
struct connection_s
{
    uv_tcp_t m_tcp = {};                            // libuv tcp handle (must be first)
    uv_timer_t *m_timer_disconnect = nullptr;       // connection-lost timer
    uv_timer_t *m_timer_keepalive = nullptr;        // keepalive timer
    addr_t m_addr;                                  // peer address
    int m_error = 0;                                // disconnection cause
    conn_state_e m_state = conn_state_e::CLOSED;    // session state

    char input_buffer[UINT16_MAX] = {0};            // input buffer used by read()
    std::string input_msg;                          // current incoming message

    struct {
        std::uint32_t max_msg_bytes = 0;            // maximum message size
        std::uint32_t max_queue_length = 0;         // maximum number of messages in output queue
        std::uint32_t max_queue_bytes = 0;          // maximum number of bytes in output queue
    } settings;

    struct {
        std::uint32_t queue_msgs = 0;               // number of messages in output queue
        std::uint32_t queue_bytes = 0;              // number of bytes in output queue
        std::size_t recv_msgs = 0;                  // number of received messages
        std::size_t recv_bytes = 0;                 // number of received bytes
        std::size_t sent_msgs = 0;                  // number of sent messages (acknowledged)
        std::size_t sent_bytes = 0;                 // number of sent bytes (acknowledged)
    } stats;

    connection_s(session_t *session, uv_stream_t *stream);
    ~connection_s();
    connection_s(const connection_s &) = delete;
    connection_s & operator=(const connection_s &) = delete;

    void disconnect(int rc = 0);
    void send(flatbuffers::DetachedBuffer &&buf);
    void report_peer_activity();
    void send_keepalive();

    server_t * server() const { return reinterpret_cast<server_t *>(m_tcp.loop->data); }
    session_t * session() const { return reinterpret_cast<session_t *>(m_tcp.data); }
};

/**
 * Class representing a network connection.
 * 
 * This class knows nothing about bussiness logic.
 * Deals with the event loop, messages, timers, etc.
 * 
 * Main dutties are:
 * - Send messages
 * - Receive messages
 * - Keep the connection alive (keepalive)
 * - Disconnect on peer inactivity (connection lost)
 * - Handle disconnections
 * - Manage libuv resources
 */
class connection_t : private connection_s
{
  public:

    connection_t(session_t *session, uv_stream_t *stream) : connection_s(session, stream) {}
    ~connection_t() = default;
    connection_t(const connection_t&) = delete;
    connection_t& operator=(const connection_t&) = delete;

    const addr_t & addr() const { return m_addr; }
    int error() const { return m_error; }
    conn_state_e state() const { return m_state; }

    void state(conn_state_e val) { m_state = val; }

    void config(std::uint32_t max_msg_bytes, std::uint32_t max_queue_length, std::uint32_t max_queue_bytes) {
        settings.max_msg_bytes = max_msg_bytes;
        settings.max_queue_length = max_queue_length;
        settings.max_queue_bytes = max_queue_bytes;
    }

    /**
     * Enable or disable the keepalive feature.
     * 
     * If no messages was sent in the last `millis` milliseconds, then a keepalive message is sent.
     * 
     * @param millis Max gap between messages (0 = disable keepalive).
     */
    void set_keepalive(std::uint32_t millis);

    /**
     * Enable or disable the connection-lost feature.
     * 
     * Disconnects the connection if no peer activity in the last `millis` milliseconds.
     * Use this functionality with the keepalive feature.
     * 
     * Peer activity is:
     *   - Peer ackowledges a message from local.
     *   - Local receives a message from peer.
     * 
     * @param millis Maximum millis without peer activity (0 = disable connection-lost).
     */
    void set_connection_lost(std::uint32_t millis);

    using connection_s::disconnect;
    using connection_s::send;

  private:

    void set_timer(uv_timer_t *&timer, std::uint32_t millis, uv_timer_cb timer_cb);
};

} // namespace nplex
