#pragma once

#include <cstdint>
#include "addr.hpp"

namespace nplex {

struct context_params_t
{
    addr_t addr;                                    // IP address to listen on.
    std::uint32_t max_sessions = 0;                 // Maximum number of simultaneous active sessions (0 = unlimited).
    std::uint32_t max_msg_bytes = 0;                // Maximum output message size (0 = unlimited).
    std::uint32_t snapshot_max_entries = 0;         // Maximum number of updates between two snapshots (0 = unlimited).
    std::uint32_t snapshot_max_bytes = 0;           // Maximum bytes between two snapshots (0 = unlimited).
    std::uint32_t cache_max_entries = 0;            // Maximum number of entries in the cache (0 = unlimited).
    std::uint32_t cache_max_bytes = 0;              // Maximum bytes in the cache (0 = unlimited).
};

struct connection_params_t
{
    std::uint32_t max_unack_msgs = 0;               // Maximum number of unack msgs (0 = unlimited).
    std::uint32_t max_unack_bytes = 0;              // Maximum number of unack bytes (0 = unlimited).
    std::uint32_t keepalive_millis = 0;             // Delay between keepalive messages (0 = disabled).
    float timeout_factor = 3.0f;                    // Timeout factor (> 1.0).
};

struct user_params_t
{
    bool active = false;                            // User is active or disabled.
    bool can_force = false;                         // Can force to accept dirty transactions.
    bool can_monitor = false;                       // Can monitor active sessions.
    std::uint32_t max_connections = 0;              // Maximum number of simultaneous connections (0 = unlimited).
    connection_params_t connection;                 // Connection parameters.
};

struct journal_params_t
{
    bool check = false;                             // Check journal file at startup.
    bool fsync = true;                              // Enable/disable fsync for write updates.
    std::uint32_t write_queue_max_entries = 0;      // Maximum entries pending to be written (0 = unlimited).
    std::uint32_t write_queue_max_bytes = 0;        // Maximum bytes pending to be written (0 = unlimited).
    std::uint32_t flush_max_entries = 0;            // Maximum entries per batch write (0 = unlimited).
    std::uint32_t flush_max_bytes = 0;              // Maximum bytes per batch write (0 = unlimited).
};

struct store_params_t
{
    std::uint32_t retention_max = 0;                // Maximum number of revisions to keep for.
    std::uint32_t retention_min = 0;                // Minimum number of revisions guaranteed to keep for.
    std::uint32_t max_tombstones = 0;               // Maximum number of tombstones (0 = unlimited).
};

} // namespace nplex
