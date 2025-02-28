#include <limits>
#include <fstream>
#include <charconv>
#include <algorithm>
#include <fmt/core.h>
#include "ini.h"
#include "params.hpp"

#define GENERAL_ADDR "addr"
#define GENERAL_LOG_LEVEL "log-level"
#define GENERAL_MAX_CLIENTS "max-clients"
#define GENERAL_DISABLE_FSYNC "disable-fsync"
#define SECTION_DEFAULTS "defaults"
#define USER_PASSWORD "password"
#define USER_ACTIVE "active"
#define USER_CAN_FORCE "can-force"
#define USER_MAX_CONNECTIONS "max-connections"
#define USER_KEEPALIVE_MILLIS "keepalive-millis"
#define USER_MAX_MSG_BYTES "max-msg-bytes"
#define USER_MAX_UNACK_MSGS "max-unack-msgs"
#define USER_MAX_UNACK_BYTES "max-unack-bytes"
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

static std::uint32_t parse_bytes(const std::string_view &str)
{
    if (str.empty() || !isdigit(str[0]))
        throw std::invalid_argument(fmt::format("Invalid bytes ({})", str));

    float num = 0;

    auto [ptr, ec] = std::from_chars(str.begin(), str.end(), num);
    if (ec != std::errc())
        throw std::invalid_argument(fmt::format("Invalid bytes ({})", str));

    if (isspace(*ptr)) ptr++;
    std::string_view unit{ptr};

    if (unit.empty() || unit == "B")
        num *= 1;
    else if (unit == "KB")
        num *= 1024;
    else if (unit == "MB")
        num *= 1024 * 1024;
    else if (unit == "GB")
        num *= 1024 * 1024 * 1024;
    else
        throw std::invalid_argument(fmt::format("Invalid bytes ({})", str));

    return static_cast<uint32_t>(num);
}

static nplex::acl_t parse_acl(const std::string_view &str)
{
    if (str.size() < 6 || str[4] != ':')
        throw std::invalid_argument(fmt::format("Invalid acl ({})", str));

    auto mode =  parse_mode(str.substr(0, 4));
    std::string pattern{str.substr(5)};

    return nplex::acl_t{mode, pattern};
}

static std::string format_double(double value)
{
    std::ostringstream oss;
    oss << std::fixed << std::setprecision(2) << value;
    std::string str = oss.str();

    str.erase(str.find_last_not_of('0') + 1, std::string::npos);
    if (str.back() == '.')
        str.pop_back();

    return str;
}

static std::string bytes_to_string(std::uint32_t bytes)
{
    if (bytes < 1024)
        return format_double(bytes);
    else if (bytes < 1024 * 1024)
        return fmt::format("{}KB", format_double(bytes / 1024.0));
    else if (bytes < 1024 * 1024 * 1024)
        return fmt::format("{}MB", format_double(bytes / (1024.0 * 1024)));
    else
        return fmt::format("{}GB", format_double(bytes / (1024.0 * 1024 * 1024)));
}

static int cb_inih_inner(void *obj, const char *section, const char *name, const char *value)
{
    using namespace nplex;

    auto params = (params_t *) obj;

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
        if (strcmp(name, USER_ACTIVE) == 0) {
            params->default_user.active = parse_bool(value);
        } else if (strcmp(name, USER_CAN_FORCE) == 0) {
            params->default_user.can_force = parse_bool(value);
        } else if (strcmp(name, USER_MAX_CONNECTIONS) == 0) {
            params->default_user.max_connections = parse_uint32(value);
        } else if (strcmp(name, USER_KEEPALIVE_MILLIS) == 0) {
            params->default_user.keepalive_millis = parse_uint32(value);
        } else if (strcmp(name, USER_MAX_MSG_BYTES) == 0) {
            params->default_user.max_msg_bytes = parse_bytes(value);
        } else if (strcmp(name, USER_MAX_UNACK_MSGS) == 0) {
            params->default_user.max_unack_msgs = parse_uint32(value);
        } else if (strcmp(name, USER_MAX_UNACK_BYTES) == 0) {
            params->default_user.max_unack_bytes = parse_bytes(value);
        } else {
            throw std::invalid_argument(fmt::format("Unrecognized entry ({})", name));
        }

        return true;
    }

    auto it = std::find_if(params->users.begin(), params->users.end(), [section](const user_t &usr) { 
        return (usr.name == section); 
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
    } else if (strcmp(name, USER_MAX_MSG_BYTES) == 0) {
        it->max_msg_bytes = parse_bytes(value);
    } else if (strcmp(name, USER_MAX_UNACK_MSGS) == 0) {
        it->max_unack_msgs = parse_uint32(value);
    } else if (strcmp(name, USER_MAX_UNACK_BYTES) == 0) {
        it->max_unack_bytes = parse_bytes(value);
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

nplex::params_t::params_t(const fs::path &path)
{
    load(path);
}

void nplex::params_t::load(const fs::path &path)
{
    int rc = ini_parse(path.c_str(), cb_inih, this);

    if (rc < 0)
        throw std::runtime_error("Unable to read file");
    else if (rc > 0)
        throw std::runtime_error("Syntax error at line " + std::to_string(rc));
}

void nplex::params_t::save(const fs::path &path) const
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
    ofs << USER_ACTIVE << " = " << (default_user.active ? "true" : "false") << std::endl;
    ofs << USER_MAX_CONNECTIONS << " = " << default_user.max_connections << std::endl;
    ofs << USER_CAN_FORCE << " = " << (default_user.can_force ? "true" : "false") << std::endl;
    ofs << USER_KEEPALIVE_MILLIS << " = " << default_user.keepalive_millis << std::endl;
    ofs << USER_MAX_MSG_BYTES << " = " << ::bytes_to_string(default_user.max_msg_bytes) << std::endl;
    ofs << USER_MAX_UNACK_MSGS << " = " << default_user.max_unack_msgs << std::endl;
    ofs << USER_MAX_UNACK_BYTES << " = " << ::bytes_to_string(default_user.max_unack_bytes) << std::endl;
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
        if (user.max_msg_bytes != default_user.max_msg_bytes)
            ofs << USER_MAX_MSG_BYTES << " = " << ::bytes_to_string(user.max_msg_bytes) << std::endl;
        if (user.max_unack_msgs != default_user.max_unack_msgs)
            ofs << USER_MAX_UNACK_MSGS << " = " << user.max_unack_msgs << std::endl;
        if (user.max_unack_bytes != default_user.max_unack_bytes)
            ofs << USER_MAX_UNACK_BYTES << " = " << ::bytes_to_string(user.max_unack_bytes) << std::endl;  

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
