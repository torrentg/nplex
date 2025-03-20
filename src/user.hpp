#pragma once

#include "common.hpp"

#define KEEPALIVE_MILLIS 3000
#define MAX_MSG_BYTES (50 * 1024 * 1024)
#define MAX_QUEUE_LENGTH 1000
#define MAX_QUEUE_BYTES (100 * 1024 * 1024)
#define TIMEOUT_FACTOR 3.0
#define MAX_CONNECTIONS 5
#define CAN_FORCE false
#define IS_ACTIVE true

namespace nplex {

struct acl_t
{
    std::uint8_t mode;                                  // Attributes (1=CREATE, 2=READ, 4=UPDATE, 8=DELETE).
    std::string pattern;                                // Pattern (glob).
};

struct user_t
{
    std::string name;                                   // User name.

    // user values
    std::string password;                               // User password.
    std::vector<acl_t> permissions;                     // List of permissions.
    std::uint32_t max_connections = MAX_CONNECTIONS;    // Maximum number of simultaneous connections (0 = unlimited).
    bool can_force = CAN_FORCE;                         // Can force to accept dirty transactions.
    bool active = IS_ACTIVE;                            // User is active or disabled.

    // session values
    std::uint32_t keepalive_millis = KEEPALIVE_MILLIS;  // Delay between keepalive messages (0 = disabled).
    std::uint32_t max_msg_bytes = MAX_MSG_BYTES;        // Maximum incomming message size (0 = unlimited).
    std::uint32_t max_queue_length = MAX_QUEUE_LENGTH;  // Maximum number of messages in the output queue (0 = unlimited).
    std::uint32_t max_queue_bytes = MAX_QUEUE_BYTES;    // Maximum number of bytes in the output queue (0 = unlimited).
    float timeout_factor = TIMEOUT_FACTOR;              // Timeout factor.

    std::uint32_t num_connections = 0;                  // Number of active connections.

    bool is_authorized(uint8_t mode, const char *key) const;
};

using user_ptr = std::shared_ptr<user_t>;

} // namespace nplex

#undef KEEPALIVE_MILLIS
#undef MAX_MSG_BYTES
#undef MAX_QUEUE_LENGTH
#undef MAX_QUEUE_BYTES
#undef TIMEOUT_FACTOR
#undef MAX_CONNECTIONS
#undef CAN_FORCE
#undef IS_ACTIVE
