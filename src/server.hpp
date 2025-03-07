#pragma once

#include <map>
#include <set>
#include <uv.h>
#include "params.hpp"
#include "messages.hpp"
#include "repository.hpp"
#include "session.hpp"
#include "journal.h"
#include "user.hpp"

namespace nplex {

class server_t 
{
  public:

    void init(const params_t &params);
    void run();
    void stop();

    void append_session(uv_stream_t *stream);
    void release_session(session_t *session);
    void on_msg_received(session_t *session, const msgs::Message *msg);

    // TODO: remove this test method
    void simule_submit();

  private:

    std::uint32_t m_max_connections = 0;
    std::unique_ptr<uv_loop_t> m_loop;
    std::unique_ptr<uv_tcp_t> m_tcp;
    std::unique_ptr<uv_async_t> m_async;
    std::unique_ptr<uv_signal_t> m_signal;
    std::map<std::string, user_ptr> m_users;
    std::set<session_ptr, shared_ptr_compare<session_t>> m_sessions;
    std::shared_ptr<ldb::journal_t> m_journal;
    repo_t m_repo;

    void process_login_request(session_t *session, const nplex::msgs::LoginRequest *req);
    void process_load_request(session_t *session, const nplex::msgs::LoadRequest *req);
    void process_submit_request(session_t *session, const nplex::msgs::SubmitRequest *req);
    void process_ping_request(session_t *session, const nplex::msgs::PingRequest *req);

    void send_last_snapshot(session_t *session, std::size_t cid);     // send last snapshot to a session
    void push_update(const update_t &update);                         // push update to all synchronized sessions
    void push_update(const update_t &update, const user_ptr &user);   // push update to all synchronized sessions of a user
    void push_update(const update_t &update, session_t *session);     // push update to a specific session

    void init_users(const params_t &params);
    void init_event_loop(const params_t &params);
    void init_network(const params_t &params);
};

} // namespace nplex
