#pragma once

#include <uv.h>
#include <memory>
#include "params.hpp"

namespace nplex {

// Forward declarations
struct context_t;

class server_t 
{
  public:

    /**
     * Initialize the server.
     * 
     * @param[in] params Server parameters.
     * 
     * @exception std::exception Any error (network error, etc).
     */
    void init(const params_t &params);

    /**
     * Run the server.
     * This function blocks until the server is stopped.
     */
    void run() noexcept;

    /**
     * Stop the server.
     * This function is called from a signal handler.
     */
    void stop() noexcept;

    // TODO: remove this test method
    void simule_submit();

  private:

    std::unique_ptr<uv_loop_t> m_loop;
    std::unique_ptr<uv_tcp_t> m_tcp;          // tcp listener
    std::unique_ptr<uv_async_t> m_async;      // used to stop the server and to notify that journal write done
    std::unique_ptr<uv_signal_t> m_signal;    // used to manage the SIGINT signal (Ctrl-C)
    std::shared_ptr<context_t> m_context;     // see context_ptr

    void init_event_loop(const params_t &params);
    void init_context(const params_t &params);
    void init_async(const params_t &params);
    void init_network(const params_t &params);
};

} // namespace nplex
