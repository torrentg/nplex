#include <spdlog/spdlog.h>
#include <fmt/core.h>
#include <fmt/ranges.h>
#include "exception.hpp"
#include "context.hpp"

// ==========================================================
// Internal (static) functions
// ==========================================================

static bool is_valid_user(const nplex::user_t &user)
{
    if (!user.active)
        return false;

    if (user.password.empty()) {
        SPDLOG_WARN("User {} discarded (no password)", user.name);
        return false;
    }

    if (user.timeout_factor <= 1.0) {
        SPDLOG_WARN("User {} discarded (invalid timeout factor)", user.name);
        return false;
    }

    int sum = 0;

    for (const auto &perm : user.permissions)
        sum += perm.mode;

    if (sum == 0) {
        SPDLOG_WARN("User {} discarded (no acls)", user.name);
        return false;
    }

    return true;
}

static std::map<std::string, nplex::user_ptr> create_users(const nplex::params_t &params)
{
    using namespace nplex;

    std::map<std::string, nplex::user_ptr> ret;
    std::vector<std::string> valid_users;

    for (const auto &user : params.users)
    {
        if (!::is_valid_user(user))
            continue;

        ret[user.name] = std::make_shared<user_t>(user);
        valid_users.push_back(user.name);
    }

    if (ret.empty())
        throw nplex_exception("No valid users found");

    SPDLOG_INFO("Users: [{}]", fmt::join(valid_users, ", "));

    return ret;
}

// ==========================================================
// context_t methods
// ==========================================================

nplex::context_t::context_t(uv_loop_t *loop_, const params_t &params) : loop(loop_)
{
    users = ::create_users(params);

    storage = std::make_shared<storage_t>(params);

    auto [min_rev, max_rev] = storage->get_range();

    repo = storage->get_repo(max_rev);
}
