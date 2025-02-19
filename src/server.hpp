#pragma once

#include <uv.h>
#include <map>
#include <list>
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
    void append_client(uv_stream_t *stream);
    void release_client(client_t *con);

  private:
    server_params_t m_params;
    std::unique_ptr<uv_loop_t> m_loop;
    std::unique_ptr<uv_tcp_t> m_tcp;
    std::unique_ptr<uv_async_t> m_async;
    std::unique_ptr<uv_signal_t> m_signal;
    std::map<gto::cstring, user_ptr, gto::cstring_compare> m_users;
    std::list<client_ptr> m_clients;
    //cache_t m_cache;
};

} // namespace nplex
