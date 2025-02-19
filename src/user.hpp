#pragma once

#include "nplex.hpp"
#include "params.hpp"

namespace nplex {

struct user_t
{
    user_params_t params;
    std::uint32_t num_connections = 0;
};

using user_ptr = std::shared_ptr<user_t>;

} // namespace nplex
