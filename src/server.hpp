#include <uv.h>
#include <map>
#include <list>
#include "params.hpp"
#include "messages.hpp"
#include "connection.hpp"

namespace nplex {

struct user_t
{
    user_params_t params;
    std::uint32_t num_connections = 0;
};

using user_ptr = std::shared_ptr<user_t>;
using connection_ptr = std::shared_ptr<connection_t>;

class server_t 
{
  public:
    explicit server_t(const server_params_t &params);

    void run();

  private:
    server_params_t m_params;
    std::unique_ptr<uv_loop_t> m_loop;
    std::unique_ptr<uv_tcp_t> m_tcp;
    std::unique_ptr<uv_async_t> m_async;
    std::unique_ptr<uv_signal_t> m_signal;
    std::map<gto::cstring, user_ptr, gto::cstring_compare> m_users;
    std::list<connection_ptr> m_connections;
    //cache_t m_cache;
};

}
