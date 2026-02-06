#pragma once

#include "common.hpp"

namespace nplex {

struct acl_t
{
    std::uint8_t mode;                      // Attributes (1=CREATE, 2=READ, 4=UPDATE, 8=DELETE).
    std::string pattern;                    // Pattern (glob).
};

struct user_t
{
    std::string name;                       // User name.

    // user values
    std::string password;                   // User password.
    std::vector<acl_t> permissions;         // List of permissions.
    std::uint32_t max_connections = 0;      // Maximum number of simultaneous connections (0 = unlimited).
    bool can_force = false;                 // Can force to accept dirty transactions.
    bool active = true;                     // User is active or disabled.

    // session values
    std::uint32_t keepalive_millis = 0;     // Delay between keepalive messages (0 = disabled).
    std::uint32_t max_msg_bytes = 0;        // Maximum incomming message size (0 = unlimited).
    std::uint32_t max_unack_msg = 0;        // Maximum number of unacknowledged messages (0 = unlimited).
    std::uint32_t max_unack_bytes = 0;      // Maximum number of unacknowledged bytes (0 = unlimited).
    float timeout_factor = 3.0f;            // Timeout factor (> 1.0).

    std::uint32_t num_connections = 0;      // Number of active connections.

    bool is_authorized(uint8_t mode, const char *key) const;
};

using user_ptr = std::shared_ptr<user_t>;

} // namespace nplex
