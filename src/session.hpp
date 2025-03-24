#pragma once

#include <uv.h>
#include <flatbuffers/flatbuffers.h>
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
class session_t
{
  public:

    // constructor and destructor
    session_t(const context_ptr &context, uv_stream_t *stream);
    ~session_t() = default;
    session_t(const session_t&) = delete;
    session_t& operator=(const session_t&) = delete;

    // const methods
    conn_state_e state() const { return m_con.state(); }
    const std::string & id() const { return m_id; }
    user_ptr user() const { return m_user; }
    const char * strerror() const;

    // rest of methods
    void disconnect(int rc = 0) { m_con.disconnect(rc); }
    void send(flatbuffers::DetachedBuffer &&buf);
    void set_user(const user_ptr &user);
    void do_sync();

    // TODO: move to private
    std::size_t m_load_cid = 0;                 // load correlation id

  private:

    user_ptr m_user;                            // session user
    context_ptr m_context;                      // server context
    connection_t m_con;                         // connection object
    std::string m_id;                           // session identifier (user@addr)
    rev_t m_lrev = 0;                           // last revision (maybe unacked, maybe not sent because no data)
    bool m_ongoing_sync_task = false;           // true if a sync task is running
};

using session_ptr = std::shared_ptr<session_t>;

} // namespace nplex
