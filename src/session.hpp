#pragma once

#include "common.hpp"
#include "schema.hpp"
#include "connection.hpp"
#include <flatbuffers/flatbuffers.h>
#include <uv.h>

namespace nplex {

// Forward declarations
struct user_t;
struct context_t;
class store_t;
struct read_updates_task_t;
struct build_snapshot_task_t;
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

    // constructor and destructor
    session_t(const context_ptr &context, uv_stream_t *stream);
    ~session_t() = default;
    session_t(const session_t &) = delete;
    session_t & operator=(const session_t &) = delete;

    // const methods
    const std::string & id() const { return m_id; }
    const user_ptr & user() const { return m_user; }
    const addr_t & addr() const { return m_con.addr(); }
    const millis_t & created_at() const { return m_con.stats().time0; }
    const millis_t & disconnected_at() const { return m_con.stats().time1; }
    const context_ptr & context() const { return m_context; }
    bool is_logged() const { return (!!m_user); }
    bool is_closed() const { return (m_con.is_closed()); }
    int error() const { return m_con.error(); }
    const char * strerror() const;

    // rest of methods
    void shutdown(int rc);
    void disconnect(int rc);
    void process_requests(std::span<const msgs::Message *> msgs);
    void process_delivery(const msgs::Message *msg);
    void push_updates(std::span<const update_t> updates, bool syncing = false);
    void push_session(const std::shared_ptr<session_t> &session);
    void send_keepalive();
    void release();

  private:

    user_ptr m_user;                                // Session user
    context_ptr m_context;                          // Server context
    connection_t m_con;                             // Connection object
    std::string m_id;                               // Session identifier (user@addr)
    rev_t m_lrev = 0;                               // Last revision sent (maybe unacked or not sent because no data)
    std::size_t m_updates_cid = 0;                  // Updates correlation id (0 means no push)
    std::size_t m_sessions_cid = 0;                 // Sessions correlation id (0 means no push)
    bool m_sync_in_progress = false;                // There is a sync task operation in progress
    std::vector<DetachedBuffer> m_buffered_msgs;    // Messages waiting to be sent
    std::size_t m_buffered_bytes = 0;               // Total size of buffered messages

    void send(DetachedBuffer &&buf);
    void send_buffered_msg(DetachedBuffer &&buf);
    void trace_output_msg(const DetachedBuffer &buf);
    void flush_buffered_msgs();
    bool should_throttle() const;

    void process_request(const msgs::Message *msg);
    void process_login_request(const msgs::LoginRequest *req);
    void process_snapshot_request(const msgs::SnapshotRequest *req);
    void process_updates_request(const msgs::UpdatesRequest *req);
    void process_sessions_request(const msgs::SessionsRequest *req);
    void process_submit_request(const msgs::SubmitRequest *req);
    void process_ping_request(const msgs::PingRequest *req);

    void process_read_updates_task_result(std::size_t cid, read_updates_task_t *task);
    void process_build_snapshot_task_result(std::size_t cid, build_snapshot_task_t *task);

    void do_sync(std::size_t cid);
};

using session_ptr = std::shared_ptr<session_t>;

} // namespace nplex
