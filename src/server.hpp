#pragma once

#include <map>
#include <set>
#include <uv.h>
#include "params.hpp"
#include "messages.hpp"
#include "client.hpp"
#include "user.hpp"

namespace nplex {

class server_t 
{
    using client_ptr = std::unique_ptr<client_t>;

  public:
    explicit server_t(const server_params_t &params);

    void run();

    // methods called from libuv
    void append_client(client_t *client);
    void release_client(client_t *client);
    void on_msg_received(client_t *client, const msgs::Message *msg);
    void on_msg_delivered(client_t *client, const msgs::Message *msg);

  private:
    server_params_t m_params;
    std::unique_ptr<uv_loop_t> m_loop;
    std::unique_ptr<uv_tcp_t> m_tcp;
    std::unique_ptr<uv_async_t> m_async;
    std::unique_ptr<uv_signal_t> m_signal;
    std::map<std::string, user_ptr> m_users;
    std::set<client_t *> m_clients;
    //cache_t m_cache;

    void process_login_request(client_t *client, const nplex::msgs::LoginRequest *req);
    void process_load_request(client_t *client, const nplex::msgs::LoadRequest *req);
    void process_submit_request(client_t *client, const nplex::msgs::SubmitRequest *req);
    void process_ping_request(client_t *client, const nplex::msgs::PingRequest *req);

};

} // namespace nplex
