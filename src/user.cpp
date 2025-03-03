#include "match.h"
#include "user.hpp"

bool nplex::user_t::is_authorized(uint8_t mode, const char *key) const
{
    if (!active || !key || !mode)
        return false;

    if (permissions.empty())
        return false;

    bool authorized = false;

    for (const auto &acl : permissions)
    {
        if (!glob_match(key, acl.pattern.c_str()))
            continue;

        authorized = (mode & acl.mode);
    }

    return authorized;
}
