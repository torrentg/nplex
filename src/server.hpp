#pragma once

#include <uv.h>
#include "params.hpp"
#include "messages.hpp"
#include "context.hpp"
#include "tasks.hpp"
#include "user.hpp"

namespace nplex {

class server_t 
{
  public:

    void init(const params_t &params);
    void run();
    void stop();

    rev_t rev() const { return m_context->repo.rev(); }
    void append_session(uv_stream_t *stream);
    void release_session(session_t *session);
    void on_msg_received(session_t *session, const msgs::Message *msg);

    // TODO: remove this test method
    void simule_submit();
    void release_task(task_t *);

  private:

    bool m_running = false;
    std::uint32_t m_max_connections = 0;
    std::uint32_t m_num_running_tasks = 0;
    std::unique_ptr<uv_loop_t> m_loop;
    std::unique_ptr<uv_tcp_t> m_tcp;          // tcp listener
    std::unique_ptr<uv_async_t> m_async;      // used to stop the server and to notify that journal write done
    std::unique_ptr<uv_signal_t> m_signal;    // used to manage the SIGINT signal (Ctrl-C)
    context_ptr m_context;

    void process_login_request(session_t *session, const nplex::msgs::LoginRequest *req);
    void process_load_request(session_t *session, const nplex::msgs::LoadRequest *req);
    void process_submit_request(session_t *session, const nplex::msgs::SubmitRequest *req);
    void process_ping_request(session_t *session, const nplex::msgs::PingRequest *req);

    void send_last_snapshot(session_t *session, std::size_t cid);     // send last snapshot to a session
    void send_fixed_snapshot(session_t *session, rev_t rev, std::size_t cid);    // send a generic snapshot to a session
    void sync_session(session_t *session, rev_t rev, std::size_t cid);// synchronize a session

    void push_changes(const std::span<update_t> &updates);                         // push update to all synchronized sessions

    void init_event_loop(const params_t &params);
    void init_context(const params_t &params);
    void init_async(const params_t &params);
    void init_network(const params_t &params);

    void submit_task(task_t *task);
};

} // namespace nplex
