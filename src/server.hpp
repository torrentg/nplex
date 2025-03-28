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

    // TODO: remove this test method
    void simule_submit();

  private:

    std::unique_ptr<uv_loop_t> m_loop;
    std::unique_ptr<uv_tcp_t> m_tcp;          // tcp listener
    std::unique_ptr<uv_async_t> m_async;      // used to stop the server and to notify that journal write done
    std::unique_ptr<uv_signal_t> m_signal;    // used to manage the SIGINT signal (Ctrl-C)
    context_ptr m_context;

    void init_event_loop(const params_t &params);
    void init_context(const params_t &params);
    void init_async(const params_t &params);
    void init_network(const params_t &params);
};

} // namespace nplex
