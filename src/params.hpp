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
    addr_t addr = DEFAULT_ADDR;                         // IP address to listen on.
    fs::path datadir;                                   // Database directory.
    std::uint32_t max_connections = MAX_CONNECTIONS;    // Maximum number of allowed connections.
    log_level_e log_level = log_level_e::INFO;          // Log level.
    user_t default_user;                                // Default user.
    std::vector<user_t> users;                          // List of users.
    bool disable_fsync = false;                         // Disable fsync.
    bool daemonize = false;                             // Run as daemon.
    std::uint32_t write_queue_max_length = 1000;
    std::uint32_t write_queue_max_bytes = 350 * 1024 * 1024;
    std::uint32_t flush_max_entries = 50;
    std::uint32_t flush_max_bytes = 25 * 1024 * 1024;

    params_t() = default;
    params_t(const fs::path &path);

    /**
     * Load the configuration from an INI file.
     * Does not reset previous values.
     * @param[in] path Filepath to load the configuration.
     * @exception std::exception Any error (file-not-exist, invalid-format, etc).
     */
    void load(const fs::path &path);

    /**
     * Save the configuration to an INI file.
     * Overwrite existing file or create a new one if not exists.
     * @param[in] path Filepath to write the configuration.
     * @exception std::exception Any error (not-writeable, no-space-left, etc).
     */
    void save(const fs::path &path) const;
};

} // namespace nplex
