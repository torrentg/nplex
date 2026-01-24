#pragma once

#include <uv.h>
#include "addr.hpp"

// Forward declaration
namespace flatbuffers {
    class DetachedBuffer;
}

namespace nplex {

// Forward declarations
class session_t;
struct context_t;

struct queue_stats_t
{
    std::size_t max_msgs = UINT32_MAX;              // maximum number of messages in the queue
    std::size_t max_bytes = UINT32_MAX;             // maximum number of bytes in the queue
    std::size_t num_msgs = 0;                       // current number of messages in the queue
    std::size_t num_bytes = 0;                      // current number of bytes in the queue
};

/**
 * Connection memory layout.
 * 
 * These are the connection_t private members of connection_t that are 
 * accessed by public libuv callbacks.
 * 
 * m_tcp.data points to the session.
 * m_tcp.loop->data points to the context.
 */
struct connection_s
{
    uv_tcp_t m_tcp = {};                            // libuv tcp handle (must be first)
    uv_timer_t *m_timer_disconnect = nullptr;       // connection-lost timer
    uv_timer_t *m_timer_keepalive = nullptr;        // keepalive timer
    addr_t m_addr;                                  // peer address
    int m_error = 0;                                // disconnection cause

    std::uint32_t input_max_msg_bytes = 0;          // maximum incomming message size (0 = unlimited)
    char input_buffer[UINT16_MAX] = {0};            // input buffer used by read()
    std::string input_msg;                          // current incoming message

    queue_stats_t m_queue_stats;                    // output queue stats

    connection_s(session_t *session, uv_stream_t *stream);
    ~connection_s();
    connection_s(const connection_s &) = delete;
    connection_s & operator=(const connection_s &) = delete;

    bool is_blocked() const;
    bool is_closed() const;

    void shutdown(int rc);
    void disconnect(int rc);
    void send(flatbuffers::DetachedBuffer &&buf);
    void report_peer_activity();
    void send_keepalive();

    context_t * context() const { return reinterpret_cast<context_t *>(m_tcp.loop->data); }
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
    const queue_stats_t & queue_stats() const { return m_queue_stats; }

    /**
     * Configure the connection parameters.
     * 
     * @param[in] max_msg_bytes Maximum incoming message size (0 = unlimited).
     * @param[in] max_queue_length Maximum number of messages in the output queue (0 = unlimited).
     * @param[in] max_queue_bytes Maximum number of bytes in the output queue (0 = unlimited).
     */
    void config(std::uint32_t max_msg_bytes, std::uint32_t max_queue_length, std::uint32_t max_queue_bytes);

    /**
     * Enable or disable the keepalive feature.
     * 
     * If no messages was sent in the last `millis` milliseconds, then a keepalive message is sent.
     * 
     * @param[in] millis Max gap between messages (0 = disable keepalive).
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
     * @param[in] millis Maximum millis without peer activity (0 = disable connection-lost).
     */
    void set_connection_lost(std::uint32_t millis);

    using connection_s::is_blocked;
    using connection_s::is_closed;
    using connection_s::disconnect;
    using connection_s::shutdown;
    using connection_s::send;

  private:

    void set_timer(uv_timer_t *&timer, std::uint32_t millis, uv_timer_cb timer_cb);
};

} // namespace nplex
