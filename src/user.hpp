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
     * ACLs are evaluated in order, and the first ACL whose pattern matches
     * the key determines the final result (later ACLs are ignored).
     *
     * @param[in] mode Bitwise combination of CRUD_CREATE, CRUD_READ,
     *                 CRUD_UPDATE, CRUD_DELETE indicating the requested
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
