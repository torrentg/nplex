#pragma once

#include <string>
#include <vector>
#include <filesystem>
#include "cstring.hpp"
#include "nplex.hpp"
#include "addr.hpp" 

namespace fs = std::filesystem;

namespace nplex {

struct user_params_t
{
    gto::cstring name;                                  // User name.
    std::string password;                               // User password.
    std::uint32_t max_connections = MAX_CONNECTIONS;    // Maximum number of simultaneous connections.
    std::uint32_t keepalive_millis = 0;                 // Delay between keepalive messages (0=disabled).
    std::vector<acl_t> permissions;                     // List of permissions.
    bool can_force = false;                             // Can force to accept dirty transactions.
    bool active = true;                                 // User is active or disabled.

    bool is_valid() const {
        return (active && !password.empty() && max_connections > 0 && !permissions.empty());
    }
};

struct server_params_t
{
    addr_t addr = DEFAULT_ADDR;                         // IP address to listen on.
    fs::path datadir;                                   // Database directory.
    std::uint32_t max_connections = MAX_CONNECTIONS;    // Maximum number of allowed connections.
    log_level_e log_level = log_level_e::INFO;          // Log level.
    user_params_t default_user;                         // Default user.
    std::vector<user_params_t> users;                   // List of users.
    bool disable_fsync = false;                         // Disable fsync.
    bool daemonize = false;                             // Run as daemon.

    server_params_t() = default;
    server_params_t(const fs::path &path);

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
