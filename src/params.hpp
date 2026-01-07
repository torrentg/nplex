#pragma once

#include <string>
#include <vector>
#include <filesystem>
#include "common.hpp"
#include "user.hpp"
#include "addr.hpp" 

#define DEFAULT_ADDR "localhost:14022"
#define MAX_CONNECTIONS 256
#define DISABLE_FSYNC false
#define QUEUE_MAX_LENGTH 1000
#define QUEUE_MAX_BYTES (350 * 1024 * 1024)
#define FLUSH_MAX_ENTRIES 50
#define FLUSH_MAX_BYTES (25 * 1024 * 1024)

namespace fs = std::filesystem;

namespace nplex {

struct params_t
{
    fs::path datadir;                                           // Database directory.
    bool check_journal = false;                                 // Check journal files at startup.

    addr_t addr = DEFAULT_ADDR;                                 // IP address to listen on.
    log_level_e log_level = log_level_e::DEFAULT;               // Log level.
    std::uint32_t max_connections = MAX_CONNECTIONS;            // Maximum number of allowed connections (0 = unlimited).
    user_t default_user;                                        // Default user.
    std::vector<user_t> users;                                  // List of users.
    bool disable_fsync = DISABLE_FSYNC;                         // Disable fsync.
    std::uint32_t write_queue_max_length = QUEUE_MAX_LENGTH;    // Maximum number of messages in the write queue (0 = unlimited).
    std::uint32_t write_queue_max_bytes = QUEUE_MAX_BYTES;      // Maximum number of bytes in the write queue (0 = unlimited).
    std::uint32_t flush_max_entries = FLUSH_MAX_ENTRIES;        // Maximum number of entries to flush (0 = unlimited).
    std::uint32_t flush_max_bytes = FLUSH_MAX_BYTES;            // Maximum number of bytes to flush (0 = unlimited).

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

#undef DEFAULT_ADDR
#undef MAX_CONNECTIONS
#undef DISABLE_FSYNC
#undef QUEUE_MAX_LENGTH
#undef QUEUE_MAX_BYTES
#undef FLUSH_MAX_ENTRIES
#undef FLUSH_MAX_BYTES
