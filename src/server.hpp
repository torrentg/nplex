#pragma once

#include <uv.h>
#include <memory>

namespace nplex {

// Forward declarations
struct context_t;
struct config_t;

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
    void init(const config_t &config);

    /**
     * Run the server.
     * This function blocks until the server is stopped.
     */
    void run() noexcept;

    // TODO: remove this test method
    void simule_submit();

  private:

    std::unique_ptr<uv_loop_t> m_loop;        // event loop
    std::unique_ptr<uv_tcp_t> m_tcp;          // tcp listener
    std::unique_ptr<uv_signal_t> m_sigint;    // used to manage the SIGINT signal (Ctrl-C)
    std::unique_ptr<uv_signal_t> m_sigterm;   // used to manage the SIGTERM signal (terminate)
    std::shared_ptr<context_t> m_context;     // see context_ptr

    void init_event_loop(const config_t &config);
    void init_signals(const config_t &config);
    void init_context(const config_t &config);
    void init_network(const config_t &config);
    void init_test(const config_t &config);
};

} // namespace nplex
