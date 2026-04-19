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
    void process_request(const msgs::Message *msg);
    void process_delivery(const msgs::Message *msg);
    void send_snapshot(std::size_t cid, const store_t &store, const user_ptr &user = nullptr);
    void send_updates(std::size_t cid, rev_t from_rev, rev_t to_rev, const std::span<const update_dto_t> &updates);
    void push_changes(const std::span<const update_t> &updates, bool syncing = false);
    void push_session(const std::shared_ptr<session_t> &session);
    void do_sync(std::size_t cid);
    void send_keepalive();
    void release();

  private:

    user_ptr m_user;                            // session user
    context_ptr m_context;                      // server context
    connection_t m_con;                         // connection object
    std::string m_id;                           // session identifier (user@addr)
    rev_t m_lrev = 0;                           // last revision sent (maybe unacked, maybe not sent because no data)
    std::size_t m_updates_cid = 0;              // updates correlation id (0 means no push)
    std::size_t m_sessions_cid = 0;             // sessions correlation id (0 means no push)
    bool m_sync_in_progress = false;            // there is a sync task operation in progress

    void send(flatbuffers::DetachedBuffer &&buf);
    void process_login_request(const nplex::msgs::LoginRequest *req);
    void process_snapshot_request(const nplex::msgs::SnapshotRequest *req);
    void process_updates_request(const nplex::msgs::UpdatesRequest *req);
    void process_sessions_request(const nplex::msgs::SessionsRequest *req);
    void process_submit_request(const nplex::msgs::SubmitRequest *req);
    void process_ping_request(const nplex::msgs::PingRequest *req);
};

using session_ptr = std::shared_ptr<session_t>;

} // namespace nplex
