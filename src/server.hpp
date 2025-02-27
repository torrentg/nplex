#pragma once

#include <map>
#include <set>
#include <uv.h>
#include "params.hpp"
#include "messages.hpp"
#include "session.hpp"
#include "user.hpp"

namespace nplex {

class server_t 
{
  public:

    explicit server_t(const params_t &params);

    void run();

    // methods called from libuv
    void append_session(session_t *session);
    void release_session(session_t *session);
    void on_msg_received(session_t *session, const msgs::Message *msg);
    void on_msg_delivered(session_t *session, const msgs::Message *msg);

  private:

    params_t m_params;
    std::unique_ptr<uv_loop_t> m_loop;
    std::unique_ptr<uv_tcp_t> m_tcp;
    std::unique_ptr<uv_async_t> m_async;
    std::unique_ptr<uv_signal_t> m_signal;
    std::map<std::string, user_ptr> m_users;
    std::set<session_t *> m_sessions;
    //cache_t m_cache;

    void process_login_request(session_t *session, const nplex::msgs::LoginRequest *req);
    void process_load_request(session_t *session, const nplex::msgs::LoadRequest *req);
    void process_submit_request(session_t *session, const nplex::msgs::SubmitRequest *req);
    void process_ping_request(session_t *session, const nplex::msgs::PingRequest *req);

};

} // namespace nplex
