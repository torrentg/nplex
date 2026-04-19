#pragma once

#include <chrono>
#include <uv.h>
#include "addr.hpp"
#include "params.hpp"

// Forward declaration
namespace flatbuffers {
    class DetachedBuffer;
}

using millis_t = std::chrono::milliseconds;

namespace nplex {

// Forward declarations
class session_t;

struct connection_stats_t
{
    millis_t time0{0};                              // Timestamp connection was established (UTC millis since epoch)
    millis_t time1{0};                              // Timestamp connection was closed (UTC millis since epoch)
    std::size_t unack_msgs = 0;                     // Number of msgs in the output queue
    std::size_t unack_bytes = 0;                    // Number of bytes in the output queue
    std::size_t recv_msgs = 0;                      // Total received messages
    std::size_t recv_bytes = 0;                     // Total received bytes
    std::size_t sent_msgs = 0;                      // Total sent messages
    std::size_t sent_bytes = 0;                     // Total sent bytes
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
    addr_t m_addr;                                  // peer address
    uv_timer_t *m_timer_disconnect = nullptr;       // connection-lost timer
    uv_timer_t *m_timer_keepalive = nullptr;        // keepalive timer
    char m_input_buffer[UINT16_MAX] = {0};          // input buffer used by read()
    std::string m_input_msg;                        // current incoming message
    connection_params_t m_params;                   // connection parameters
    connection_stats_t m_stats;                     // connection statistics
    int m_error = 0;                                // disconnection cause

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
    void set_timer(uv_timer_t *&timer, std::uint32_t millis, uv_timer_cb timer_cb);
    session_t * session() const { return reinterpret_cast<session_t *>(m_tcp.data); }
};

/**
 * Class representing a network connection.
 * 
 * This class knows nothing about bussiness logic.
 * Deals with the event loop, messages, timers, etc.
 * 
 * @note This class is not thread-safe.
 * 
 * Main dutties are:
 * - Manage libuv resources
 * - Keep the connection alive (keepalive)
 * - Disconnect on peer inactivity (connection lost)
 * - Send messages
 * - Receive messages
 * - Handle disconnections
 * 
 * Callbacks to the session:
 * - session()->process_request()
 * - session()->process_delivery()
 * - session()->send_keepalive()
 * - session()->release()
 */
class connection_t : private connection_s
{
  public:

    connection_t(session_t *session, uv_stream_t *stream) : connection_s(session, stream) {}
    ~connection_t() = default;

    connection_t(const connection_t&) = delete;
    connection_t& operator=(const connection_t&) = delete;

    /**
     * Get the remote addr object (peer address).
     * 
     * @return The peer address.
     */
    const addr_t & addr() const { return m_addr; }

    /**
     * Get the connection parameters.
     * 
     * @return The connection parameters.
     */
    const auto & params() const { return m_params; }

    /**
     * Get the connection statistics.
     * 
     * @return The connection statistics.
     */
    const auto & stats() const { return m_stats; }

    /**
     * Get the disconnection cause.
     * 
     * @see ERR_* defines.
     * @see uv_strerror().
     * 
     * @return The last error code.
     */
    int error() const { return m_error; }

    /**
     * Configure the connection parameters.
     * 
     * @param[in] params Connection parameters.
     */
    void config(const connection_params_t &params);

    /**
     * Enable or disable the keepalive feature.
     * 
     * If no messages was sent in the last keepalive_millis, then a keepalive message is sent.
     */
    void enable_keepalive();
    void disable_keepalive();

    /**
     * Enable or disable the connection-lost feature.
     * 
     * Disconnects the connection if no peer activity in the last X milliseconds,
     * where X is calculated as keepalive_millis * timeout_factor.
     * 
     * Peer activity is:
     *   - Peer ackowledges a message from local (ex: keepalive msg).
     *   - Local receives a message from peer.
     */
    void enable_connection_lost();
    void disable_connection_lost();

    /**
     * Check if the connection output queue is blocked.
     * 
     * @return true if blocked, false otherwise.
     */
    using connection_s::is_blocked;

    /**
     * Check if the connection is closed.
     * 
     * @return true if closed, false otherwise.
     */
    using connection_s::is_closed;

    /**
     * Disconnect the connection cancelling all pending operations.
     * 
     * @param[in] rc Disconnection cause (error code).
     */
    using connection_s::disconnect;

    /**
     * Shutdown the connection gracefully (send pending messages).
     * 
     * @param[in] rc Disconnection cause (error code).
     */
    using connection_s::shutdown;

    /**
     * Send a message to the peer.
     * 
     * @param[in] buf Message to send.
     */
    using connection_s::send;

};

} // namespace nplex
