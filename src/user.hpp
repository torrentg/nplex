#pragma once

#include "common.hpp"
#include "params.hpp"

namespace nplex {

struct acl_t
{
    std::uint8_t mode;                      // Attributes (1=CREATE, 2=READ, 4=UPDATE, 8=DELETE).
    std::string pattern;                    // Glob pattern (ex: "foo?", "**/logs/*.txt").
};

struct user_t
{
    std::string name;                       // User name.
    std::string password;                   // User password.
    std::vector<acl_t> permissions;         // List of permissions.
    std::uint32_t num_connections = 0;      // Number of active connections.
    user_params_t params;                   // User parameters.

    /**
     * Check if the user is authorized to perform all the requested
     * operations on a key according to the user's permissions.
     *
     * ACLs are evaluated in order, and the last ACL whose pattern matches
     * the key determines the final result (later ACLs override previous ones).
     * If the last matching ACL does not allow all the requested operations,
     * the result is false.
     *
     * Note that if the user is disabled, the function will always return false.
     *
     * @param[in] mode Bitwise combination of NPLEX_CREATE, NPLEX_READ,
     *                 NPLEX_UPDATE, NPLEX_DELETE indicating the requested
     *                 operations.
     * @param[in] key  The key on which the operation is to be performed.
     *
     * @return true  = authorized to perform all requested operations,
     *         false = not authorized for at least one of them.
     */
    bool is_authorized(uint8_t mode, const char *key) const;
};

using user_ptr = std::shared_ptr<user_t>;

} // namespace nplex
