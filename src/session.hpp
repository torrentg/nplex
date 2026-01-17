#pragma once

#include <uv.h>
#include <flatbuffers/flatbuffers.h>
#include "common.hpp"
#include "messages.hpp"
#include "connection.hpp"

namespace nplex {

// Forward declarations
struct user_t;
struct context_t;
using user_ptr = std::shared_ptr<user_t>;
using context_ptr = std::shared_ptr<context_t>;

/**
 * Class representing a client session.
 * 
 * server_t is accessed via tcp.loop->data.
 */
class session_t : public std::enable_shared_from_this<session_t>
{
  public:

    enum class state_e : std::uint8_t {
      CONNECTED,
      LOGGED,
      SYNCING,
      SYNCED,
      CLOSED
    };

    // constructor and destructor
    session_t(const context_ptr &context, uv_stream_t *stream);
    ~session_t() = default;
    session_t(const session_t &) = delete;
    session_t & operator=(const session_t &) = delete;

    // const methods
    state_e state() const { return m_state; }
    const std::string & id() const { return m_id; }
    const user_ptr & user() const { return m_user; }
    const char * strerror() const;

    // rest of methods
    void shutdown(int rc);
    void disconnect(int rc);
    void process_request(const msgs::Message *msg);
    void process_delivery(const msgs::Message *msg);
    void push_changes(const std::span<update_t> &updates);
    void send(flatbuffers::DetachedBuffer &&buf);
    void sync_task_complete() { m_ongoing_sync_task = false; do_sync(); }
    void do_sync();

  private:

    user_ptr m_user;                            // session user
    context_ptr m_context;                      // server context
    connection_t m_con;                         // connection object
    std::string m_id;                           // session identifier (user@addr)
    rev_t m_lrev = 0;                           // last revision (maybe unacked, maybe not sent because no data)
    state_e m_state = state_e::CLOSED;          // connection state
    bool m_ongoing_sync_task = false;           // true if a sync task is running
    std::size_t m_load_cid = 0;                 // load correlation id

    void process_login_request(const nplex::msgs::LoginRequest *req);
    void process_load_request(const nplex::msgs::LoadRequest *req);
    void process_submit_request(const nplex::msgs::SubmitRequest *req);
    void process_ping_request(const nplex::msgs::PingRequest *req);

    void send_last_snapshot(std::size_t cid);
    void send_fixed_snapshot(std::size_t cid, rev_t rev);
    void send_only_updates(std::size_t cid, rev_t rev);
};

using session_ptr = std::shared_ptr<session_t>;

} // namespace nplex
