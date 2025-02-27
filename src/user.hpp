#pragma once

#include "nplex.hpp"

namespace nplex {

struct user_t
{
    std::string name;                                   // User name.

    // user values
    std::string password;                               // User password.
    std::uint32_t max_connections = MAX_CONNECTIONS;    // Maximum number of simultaneous connections.
    std::vector<acl_t> permissions;                     // List of permissions.
    bool can_force = false;                             // Can force to accept dirty transactions.
    bool active = true;                                 // User is active or disabled.

    // session values
    std::uint32_t keepalive_millis = 0;                 // Delay between keepalive messages (0 = disabled).
    std::uint32_t max_unack_msgs = 0;                   // Maximum number of unacknowledged messages (0 = unlimited).
    std::uint32_t max_unack_bytes = 0;                  // Maximum number of unacknowledged bytes (0 = unlimited).
    std::uint32_t max_msg_bytes = 0;                    // Maximum incomming message size (0 = unlimited).

    std::uint32_t num_connections = 0;                  // Number of active connections.

    bool is_authorized(uint8_t mode, const char *key) const;
};

using user_ptr = std::shared_ptr<user_t>;

} // namespace nplex
