#include <fstream>
#include <algorithm>
#include <fmt/core.h>
#include "ini.h"
#include "params.hpp"

#define GENERAL_ADDR "addr"
#define GENERAL_LOG_LEVEL "log-level"
#define GENERAL_MAX_CLIENTS "max-clients"
#define GENERAL_DISABLE_FSYNC "disable-fsync"
#define SECTION_DEFAULTS "defaults"
#define DEFAULTS_ACTIVE "active"
#define DEFAULTS_CAN_FORCE "can-force"
#define DEFAULTS_MAX_CONNECTIONS "max-connections"
#define DEFAULTS_KEEPALIVE_MILLIS "keepalive-millis"
#define USER_PASSWORD "password"
#define USER_ACTIVE "active"
#define USER_CAN_FORCE "can-force"
#define USER_MAX_CONNECTIONS "max-connections"
#define USER_KEEPALIVE_MILLIS "keepalive-millis"
#define USER_ACL "acl"

static std::string mode_to_string(std::uint8_t mode)
{
    std::string str;

    str += ((mode & NPLEX_CREATE) ? "c" : "-");
    str += ((mode & NPLEX_READ)   ? "r" : "-");
    str += ((mode & NPLEX_UPDATE) ? "u" : "-");
    str += ((mode & NPLEX_DELETE) ? "d" : "-");

    return str;
}

static std::uint8_t parse_mode(const std::string_view &str)
{
    static const char *crud = "crud";
    std::uint8_t mode = 0;

    if (str.size() != 4)
        throw std::invalid_argument(fmt::format("Invalid crud ({})", str));

    for (std::size_t i = 0; i < 4; i++)
    {
        char c = str[i];

        if (c != crud[i] && c != '-')
            throw std::invalid_argument(fmt::format("Invalid crud ({})", str));

        switch (c)
        {
            case 'c': mode |= NPLEX_CREATE; break;
            case 'r': mode |= NPLEX_READ;   break;
            case 'u': mode |= NPLEX_UPDATE; break;
            case 'd': mode |= NPLEX_DELETE; break;
            default: break;
        }
    }

    return mode;
}

static bool parse_bool(const std::string_view &str)
{
    if (str == "true")
        return true;

    if (str == "false")
        return false;

    throw std::invalid_argument(fmt::format("Invalid bool ({})", str));
}

static std::uint32_t parse_uint32(const std::string_view &str)
{
    for (const auto &c : str)
        if (!isdigit(c))
            throw std::invalid_argument(fmt::format("Invalid number ({})", str));

    auto num = atol(str.data());

    if (num == 0 && (str.size() != 1 || str[0] != '0'))
        throw std::invalid_argument(fmt::format("Invalid number ({})", str));

    if (num < 0 || num > std::numeric_limits<std::uint32_t>::max())
        throw std::invalid_argument(fmt::format("Number out of range ({})", str));

    return static_cast<std::uint32_t>(num);
}

static nplex::acl_t parse_acl(const std::string_view &str)
{
    if (str.size() < 6 || str[4] != ':')
        throw std::invalid_argument(fmt::format("Invalid acl ({})", str));
    
    auto mode =  parse_mode(str.substr(0, 4));
    std::string pattern{str.substr(5)};

    return nplex::acl_t{mode, pattern};
}

static int cb_inih_inner(void *obj, const char *section, const char *name, const char *value)
{
    using namespace nplex;

    auto params = (server_params_t *) obj;

    if (strcmp(section, "") == 0)
    {
        if (strcmp(name, GENERAL_ADDR) == 0) {
            params->addr = addr_t{value};
        } else if (strcmp(name, GENERAL_LOG_LEVEL) == 0) {
            params->log_level = parse_log_level(value);
        } else if (strcmp(name, GENERAL_MAX_CLIENTS) == 0) {
            params->max_connections = parse_uint32(value);
        } else if (strcmp(name, GENERAL_DISABLE_FSYNC) == 0) {
            params->disable_fsync = parse_bool(value);
        } else {
            throw std::invalid_argument(fmt::format("Unrecognized entry ({})", name));
        }

        return true;
    }

    if (strcmp(section, SECTION_DEFAULTS) == 0)
    {
        if (strcmp(name, DEFAULTS_ACTIVE) == 0) {
            params->default_user.active = parse_bool(value);
        } else if (strcmp(name, DEFAULTS_CAN_FORCE) == 0) {
            params->default_user.can_force = parse_bool(value);
        } else if (strcmp(name, DEFAULTS_MAX_CONNECTIONS) == 0) {
            params->default_user.max_connections = parse_uint32(value);
        } else if (strcmp(name, DEFAULTS_KEEPALIVE_MILLIS) == 0) {
            params->default_user.keepalive_millis = parse_uint32(value);
        } else {
            throw std::invalid_argument(fmt::format("Unrecognized entry ({})", name));
        }

        return true;
    }

    auto it = std::find_if(params->users.begin(), params->users.end(), [section](const user_params_t &x) { 
        return (x.name == section); 
    });

    if (it == params->users.end()) {
        params->users.push_back(params->default_user);
        it = std::prev(params->users.end());
        it->name = section;
    }

    if (strcmp(name, USER_PASSWORD) == 0) {
        it->password = value;
    } else if (strcmp(name, USER_ACTIVE) == 0) {
        it->active = parse_bool(value);
    } else if (strcmp(name, USER_CAN_FORCE) == 0) {
        it->can_force = parse_bool(value);
    } else if (strcmp(name, USER_MAX_CONNECTIONS) == 0) {
        it->max_connections = parse_uint32(value);
    } else if (strcmp(name, USER_KEEPALIVE_MILLIS) == 0) {
        it->keepalive_millis = parse_uint32(value);
    } else if (strcmp(name, USER_ACL) == 0) {
        it->permissions.push_back(parse_acl(value));
    } else
        throw std::invalid_argument(fmt::format("Unrecognized entry ({})", name));

    return true;
}

static int cb_inih(void *obj, const char *section, const char *name, const char *value)
{
    try {
        return cb_inih_inner(obj, section, name, value);
    }
    catch (std::exception &e) {
        throw std::invalid_argument(fmt::format("{} in {} section", e.what(), (*section ? section : "global")));
    }
}

nplex::server_params_t::server_params_t(const fs::path &path)
{
    load(path);
}

void nplex::server_params_t::load(const fs::path &path)
{
    int rc = ini_parse(path.c_str(), cb_inih, this);

    if (rc < 0)
        throw std::runtime_error("Unable to read file");
    else if (rc > 0)
        throw std::runtime_error("Syntax error at line " + std::to_string(rc));

    int num_valid_users = 0;

    for (const auto &user : users)
        if (user.active && !user.password.empty() && user.max_connections > 0 && !user.permissions.empty())
            num_valid_users++;

    if (num_valid_users == 0)
        throw std::runtime_error("No valid users");
}

void nplex::server_params_t::save(const fs::path &path) const
{
    std::ofstream ofs(path, std::ios::trunc);

    ofs << "# " << PROJECT_NAME << " configuration file." << std::endl;
    ofs << "# see " PROJECT_URL << std::endl;
    ofs << std::endl;

    ofs << GENERAL_ADDR << " = " << addr.str() << std::endl;
    ofs << GENERAL_LOG_LEVEL << " = " << to_string(log_level) << std::endl;
    ofs << GENERAL_MAX_CLIENTS << " = " << max_connections << std::endl;
    ofs << GENERAL_DISABLE_FSYNC << " = " << (disable_fsync ? "true" : "false") << std::endl;
    ofs << std::endl;

    ofs << "[" << SECTION_DEFAULTS << "]" << std::endl;
    ofs << DEFAULTS_ACTIVE << " = " << (default_user.active ? "true" : "false") << std::endl;
    ofs << DEFAULTS_MAX_CONNECTIONS << " = " << default_user.max_connections << std::endl;
    ofs << DEFAULTS_CAN_FORCE << " = " << (default_user.can_force ? "true" : "false") << std::endl;
    ofs << DEFAULTS_KEEPALIVE_MILLIS << " = " << default_user.keepalive_millis << std::endl;
    ofs << std::endl;

    for (const auto &user : users)
    {
        if (user.name.empty())
            continue;

        ofs << "[" << user.name << "]" << std::endl;
        ofs << USER_PASSWORD << " = " << user.password << std::endl;
        if (user.active != default_user.active)
            ofs << USER_ACTIVE << " = " << (user.active ? "true" : "false") << std::endl;
        if (user.max_connections != default_user.max_connections)
            ofs << USER_MAX_CONNECTIONS << " = " << user.max_connections << std::endl;
        if (user.keepalive_millis != default_user.keepalive_millis)
            ofs << USER_KEEPALIVE_MILLIS << " = " << user.keepalive_millis << std::endl;
        if (user.can_force != default_user.can_force)
            ofs << USER_CAN_FORCE << " = " << (user.can_force ? "true" : "false") << std::endl;

        for (const auto &acl : user.permissions)
            if (!acl.pattern.empty())
                ofs << USER_ACL << " = " << mode_to_string(acl.mode) << ":" << acl.pattern << std::endl;

        ofs << std::endl;
    }

    if (users.empty())
    {
        ofs << "[admin]" << std::endl;
        ofs << USER_ACTIVE << " = true" << std::endl;
        ofs << USER_PASSWORD << " = s3cr3t" << std::endl;
        ofs << USER_MAX_CONNECTIONS << " = 10" << std::endl;
        ofs << USER_KEEPALIVE_MILLIS << " = 1000" << std::endl;
        ofs << USER_CAN_FORCE << " = true" << std::endl;
        ofs << USER_ACL << " = crud:**" << std::endl;
        ofs << std::endl;
    }

    if (!ofs)
        throw std::runtime_error("Error writing file");

    ofs.close();
}
