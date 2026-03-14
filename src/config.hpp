#pragma once

#include <string>
#include <vector>
#include <filesystem>
#include "common.hpp"
#include "addr.hpp"
#include "params.hpp"
#include "user.hpp"

namespace fs = std::filesystem;

namespace nplex {

struct config_t
{
    log_level_e log_level;          // Log level.

    context_params_t context;       // Context parameters (network + snapshot + cache).
    journal_params_t journal;       // Journal parameters.
    store_params_t store;           // Store parameters.

    user_t default_user;            // Default user parameters.
    std::vector<user_t> users;      // List of users.

    /**
     * Set default values for all parameters, then load the configuration from an INI file if provided.
     *
     * @param[in] filepath Path to the configuration file to load.
     *
     * @exception std::exception Any error (file not found, invalid format, etc.).
     */
    config_t(const fs::path &filepath = fs::path());

    /**
     * Load the configuration from an INI file.
     *
     * @param[in] filepath Path to the configuration file to load.
     *
     * @exception std::exception Any error (file not found, invalid format, etc.).
     */
    void load(const fs::path &filepath);

    /**
     * Save the configuration to an INI file.
     * Overwrite the existing file or create a new one if it does not exist.
     *
     * @param[in] filepath Path to write the configuration to.
     *
     * @exception std::exception Any error (not writable, no space left, etc.).
     */
    void save(const fs::path &filepath) const;
};

} // namespace nplex
