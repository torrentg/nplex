#pragma once

#include <string>
#include <vector>
#include <filesystem>
#include "common.hpp"
#include "user.hpp"
#include "addr.hpp" 

namespace fs = std::filesystem;

namespace nplex {

struct params_t
{
    fs::path datadir;                           // Database directory.
    bool check_journal;                         // Check journal files at startup.
    addr_t addr;                                // IP address to listen on.
    log_level_e log_level;                      // Log level.
    std::uint32_t max_connections;              // Maximum number of allowed connections (0 = unlimited).
    user_t default_user;                        // Default user.
    std::vector<user_t> users;                  // List of users.
    bool disable_fsync;                         // Disable fsync for write updates.
    std::uint32_t write_queue_max_length;       // Maximum messages in the output write queue (0 = unlimited).
    std::uint32_t write_queue_max_bytes;        // Maximum bytes in the output write queue (0 = unlimited).
    std::uint32_t flush_max_entries;            // Maximum entries before flushing write updates (0 = unlimited).
    std::uint32_t flush_max_bytes;              // Maximum bytes before flushing write updates (0 = unlimited).
    std::uint32_t max_updates_between_snapshots;// Maximum updates between two snapshots (0 = unlimited).
    std::uint32_t max_bytes_between_snapshots;  // Maximum bytes between two snapshots (0 = unlimited).

    params_t(const fs::path &path = fs::path());

    /**
    * Load the configuration from an INI file.
    * @param[in] path Path to the configuration file to load.
    * @exception std::exception Any error (file not found, invalid format, etc.).
     */
    void load(const fs::path &path);

    /**
    * Save the configuration to an INI file.
    * Overwrite the existing file or create a new one if it does not exist.
    * @param[in] path Path to write the configuration to.
    * @exception std::exception Any error (not writable, no space left, etc.).
     */
    void save(const fs::path &path) const;
};

} // namespace nplex
