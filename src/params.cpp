#include <limits>
#include <fstream>
#include <charconv>
#include <algorithm>
#include <fmt/core.h>
#include "ini.h"
#include "utils.hpp"
#include "params.hpp"

#define GENERAL_ADDR                    "addr"
#define GENERAL_LOG_LEVEL               "log-level"
#define GENERAL_MAX_CLIENTS             "max-clients"
#define GENERAL_DISABLE_FSYNC           "disable-fsync"
#define SECTION_USER_DEFAULTS           "user-defaults"
#define USER_PASSWORD                   "password"
#define USER_ACTIVE                     "active"
#define USER_CAN_FORCE                  "can-force"
#define USER_MAX_CONNECTIONS            "max-connections"
#define USER_KEEPALIVE_MILLIS           "keepalive-millis"
#define USER_MAX_MSG_BYTES              "max-msg-bytes"
#define USER_MAX_QUEUE_LENGTH           "max-queue-length"
#define USER_MAX_QUEUE_BYTES            "max-queue-bytes"
#define USER_TIMEOUT_FACTOR             "timeout-factor"
#define USER_ACL                        "acl"
#define WRITE_QUEUE_MAX_LENGTH          "write-queue-max-length"
#define WRITE_QUEUE_MAX_BYTES           "write-queue-max-bytes"
#define FLUSH_MAX_ENTRIES               "flush-max-entries"
#define FLUSH_MAX_BYTES                 "flush-max-bytes"

#define DEFAULT_CHECK_JOURNAL           false
#define DEFAULT_ADDR                    "localhost:14022"
#define DEFAULT_LOG_LEVEL               nplex::log_level_e::INFO
#define DEFAULT_MAX_CONNECTIONS         256
#define DEFAULT_DISABLE_FSYNC           false
#define DEFAULT_QUEUE_MAX_LENGTH        1000
#define DEFAULT_QUEUE_MAX_BYTES         (350 * 1024 * 1024)
#define DEFAULT_FLUSH_MAX_ENTRIES       50
#define DEFAULT_FLUSH_MAX_BYTES         (25 * 1024 * 1024)
#define DEFAULT_USER_ACTIVE             true
#define DEFAULT_USER_CAN_FORCE          false
#define DEFAULT_USER_MAX_CONNECTIONS    5
#define DEFAULT_USER_KEEPALIVE_MILLIS   3000
#define DEFAULT_USER_MAX_MSG_BYTES      (50 * 1024 * 1024)
#define DEFAULT_USER_MAX_QUEUE_LENGTH   1000
#define DEFAULT_USER_MAX_QUEUE_BYTES    (100 * 1024 * 1024)
#define DEFAULT_USER_TIMEOUT_FACTOR     3.0

static std::string crud_to_string(std::uint8_t crud)
{
    return std::string{
        ((crud & NPLEX_CREATE) ? 'c' : '-'),
        ((crud & NPLEX_READ)   ? 'r' : '-'),
        ((crud & NPLEX_UPDATE) ? 'u' : '-'),
        ((crud & NPLEX_DELETE) ? 'd' : '-')
    };
}

static std::uint8_t parse_crud(const std::string_view &str)
{
    static const char *crud_str = "crud";
    std::uint8_t crud = 0;

    if (str.size() != 4)
        throw std::invalid_argument(fmt::format("Invalid crud ({})", str));

    for (std::size_t i = 0; i < 4; i++)
    {
        char c = str[i];

        if (c != crud_str[i] && c != '-')
            throw std::invalid_argument(fmt::format("Invalid crud ({})", str));

        switch (c)
        {
            case 'c': crud |= NPLEX_CREATE; break;
            case 'r': crud |= NPLEX_READ;   break;
            case 'u': crud |= NPLEX_UPDATE; break;
            case 'd': crud |= NPLEX_DELETE; break;
            default: break;
        }
    }

    return crud;
}

static bool parse_bool(const std::string_view &str)
{
    if (str == "true")
        return true;

    if (str == "false")
        return false;

    throw std::invalid_argument(fmt::format("Invalid bool ({})", str));
}

static float parse_float(const std::string_view &str)
{
    if (str.empty() || !isdigit(str[0]))
        throw std::invalid_argument(fmt::format("Invalid float value ({})", str));

    float num = 0;

    auto [ptr, ec] = std::from_chars(str.begin(), str.end(), num);

    if (ec != std::errc())
        throw std::invalid_argument(fmt::format("Invalid float value ({})", str));

    while (ptr != str.end() && isspace(*ptr)) ptr++;

    if (ptr != str.end())
        throw std::invalid_argument(fmt::format("Invalid float value ({})", str));

    return num;
}

static std::uint32_t parse_uint32(const std::string_view &str)
{
    if (str.empty() || !isdigit(str[0]))
        throw std::invalid_argument(fmt::format("Invalid number ({})", str));

    std::uint32_t num = 0;

    auto [ptr, ec] = std::from_chars(str.begin(), str.end(), num);

    if (ec != std::errc())
        throw std::invalid_argument(fmt::format("Invalid number ({})", str));

    while (ptr != str.end() && isspace(*ptr)) ptr++;

    if (ptr != str.end())
        throw std::invalid_argument(fmt::format("Invalid number ({})", str));

    return num;
}

static std::uint32_t parse_bytes(const std::string_view &str)
{
    if (str.empty() || !isdigit(str[0]))
        throw std::invalid_argument(fmt::format("Invalid bytes ({})", str));

    double num = 0;

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

    if (num > std::numeric_limits<std::uint32_t>::max())
        throw std::invalid_argument(fmt::format("Bytes value out of range ({})", str));

    return static_cast<std::uint32_t>(num);
}

static nplex::acl_t parse_acl(const std::string_view &str)
{
    if (str.size() < 6 || str[4] != ':')
        throw std::invalid_argument(fmt::format("Invalid acl ({})", str));

    auto mode = parse_crud(str.substr(0, 4));
    std::string pattern{str.substr(5)};

    return nplex::acl_t{mode, pattern};
}

static int cb_inih_inner(void *obj, const char *section, const char *name, const char *value)
{
    using namespace nplex;

    auto params = static_cast<params_t *>(obj);

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
        } else if (strcmp(name, WRITE_QUEUE_MAX_LENGTH) == 0) {
            params->write_queue_max_length = parse_uint32(value);
        } else if (strcmp(name, WRITE_QUEUE_MAX_BYTES) == 0) {
            params->write_queue_max_bytes = parse_bytes(value);
        } else if (strcmp(name, FLUSH_MAX_ENTRIES) == 0) {
            params->flush_max_entries = parse_uint32(value);
        } else if (strcmp(name, FLUSH_MAX_BYTES) == 0) {
            params->flush_max_bytes = parse_bytes(value);
        } else {
            throw std::invalid_argument(fmt::format("Unrecognized entry ({})", name));
        }

        return true;
    }

    if (strcmp(section, SECTION_USER_DEFAULTS) == 0)
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
        } else if (strcmp(name, USER_MAX_QUEUE_LENGTH) == 0) {
            params->default_user.max_queue_length = parse_uint32(value);
        } else if (strcmp(name, USER_MAX_QUEUE_BYTES) == 0) {
            params->default_user.max_queue_bytes = parse_bytes(value);
        } else if (strcmp(name, USER_TIMEOUT_FACTOR) == 0) {
            params->default_user.timeout_factor = parse_float(value);
            if (params->default_user.timeout_factor <= 1.0)
                throw std::invalid_argument(fmt::format("Invalid timeout factor ({})", value));
        } else {
            throw std::invalid_argument(fmt::format("Unrecognized entry ({})", name));
        }

        return true;
    }

    auto it = std::find_if(params->users.begin(), params->users.end(), [section](const user_t &usr) { 
        return (usr.name == section); 
    });

    // TODO: trap duplicated users (=section)
    // TODO: save section in params_t and check it here

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
    } else if (strcmp(name, USER_MAX_QUEUE_LENGTH) == 0) {
        it->max_queue_length = parse_uint32(value);
    } else if (strcmp(name, USER_MAX_QUEUE_BYTES) == 0) {
        it->max_queue_bytes = parse_bytes(value);
    } else if (strcmp(name, USER_TIMEOUT_FACTOR) == 0) {
        it->timeout_factor = parse_float(value);
        if (it->timeout_factor <= 1.0)
            throw std::invalid_argument(fmt::format("Invalid timeout factor ({})", value));
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

static void set_defaults(nplex::params_t &params)
{
    params.check_journal = DEFAULT_CHECK_JOURNAL;

    params.addr = DEFAULT_ADDR;
    params.log_level = DEFAULT_LOG_LEVEL;
    params.max_connections = DEFAULT_MAX_CONNECTIONS;
    params.disable_fsync = DEFAULT_DISABLE_FSYNC;
    params.write_queue_max_length = DEFAULT_QUEUE_MAX_LENGTH;
    params.write_queue_max_bytes = DEFAULT_QUEUE_MAX_BYTES;
    params.flush_max_entries = DEFAULT_FLUSH_MAX_ENTRIES;
    params.flush_max_bytes = DEFAULT_FLUSH_MAX_BYTES;

    params.default_user.active = DEFAULT_USER_ACTIVE;
    params.default_user.can_force = DEFAULT_USER_CAN_FORCE;
    params.default_user.max_connections = DEFAULT_USER_MAX_CONNECTIONS;
    params.default_user.keepalive_millis = DEFAULT_USER_KEEPALIVE_MILLIS;
    params.default_user.max_msg_bytes = DEFAULT_USER_MAX_MSG_BYTES;
    params.default_user.max_queue_length = DEFAULT_USER_MAX_QUEUE_LENGTH;
    params.default_user.max_queue_bytes = DEFAULT_USER_MAX_QUEUE_BYTES;
    params.default_user.timeout_factor = DEFAULT_USER_TIMEOUT_FACTOR;
}

nplex::params_t::params_t(const fs::path &path)
{
    set_defaults(*this);

    if (!path.empty())
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
    ofs << GENERAL_DISABLE_FSYNC << " = " << (disable_fsync ? "true" : "false") << std::endl;
    ofs << std::endl;

    ofs << GENERAL_MAX_CLIENTS << " = " << max_connections << std::endl;
    ofs << WRITE_QUEUE_MAX_LENGTH << " = " << write_queue_max_length << std::endl;
    ofs << WRITE_QUEUE_MAX_BYTES << " = " << bytes_to_string(write_queue_max_bytes) << std::endl;
    ofs << FLUSH_MAX_ENTRIES << " = " << flush_max_entries << std::endl;
    ofs << FLUSH_MAX_BYTES << " = " << bytes_to_string(flush_max_bytes) << std::endl;
    ofs << std::endl;

    ofs << "[" << SECTION_USER_DEFAULTS << "]" << std::endl;
    ofs << USER_ACTIVE << " = " << (default_user.active ? "true" : "false") << std::endl;
    ofs << USER_CAN_FORCE << " = " << (default_user.can_force ? "true" : "false") << std::endl;
    ofs << USER_MAX_CONNECTIONS << " = " << default_user.max_connections << std::endl;
    ofs << USER_KEEPALIVE_MILLIS << " = " << default_user.keepalive_millis << std::endl;
    ofs << USER_TIMEOUT_FACTOR << " = " << fmt::format("{:.1f}", default_user.timeout_factor) << std::endl;
    ofs << USER_MAX_MSG_BYTES << " = " << bytes_to_string(default_user.max_msg_bytes) << std::endl;
    ofs << USER_MAX_QUEUE_LENGTH << " = " << default_user.max_queue_length << std::endl;
    ofs << USER_MAX_QUEUE_BYTES << " = " << bytes_to_string(default_user.max_queue_bytes) << std::endl;
    ofs << std::endl;

    for (const auto &user : users)
    {
        if (user.name.empty())
            continue;

        ofs << "[" << user.name << "]" << std::endl;
        ofs << USER_PASSWORD << " = " << user.password << std::endl;
        if (user.active != default_user.active)
            ofs << USER_ACTIVE << " = " << (user.active ? "true" : "false") << std::endl;
        if (user.can_force != default_user.can_force)
            ofs << USER_CAN_FORCE << " = " << (user.can_force ? "true" : "false") << std::endl;
        if (user.max_connections != default_user.max_connections)
            ofs << USER_MAX_CONNECTIONS << " = " << user.max_connections << std::endl;
        if (user.keepalive_millis != default_user.keepalive_millis)
            ofs << USER_KEEPALIVE_MILLIS << " = " << user.keepalive_millis << std::endl;
        if (user.timeout_factor != default_user.timeout_factor)
            ofs << USER_TIMEOUT_FACTOR << " = " << fmt::format("{:.1f}", user.timeout_factor) << std::endl;
        if (user.max_msg_bytes != default_user.max_msg_bytes)
            ofs << USER_MAX_MSG_BYTES << " = " << bytes_to_string(user.max_msg_bytes) << std::endl;
        if (user.max_queue_length != default_user.max_queue_length)
            ofs << USER_MAX_QUEUE_LENGTH << " = " << user.max_queue_length << std::endl;
        if (user.max_queue_bytes != default_user.max_queue_bytes)
            ofs << USER_MAX_QUEUE_BYTES << " = " << bytes_to_string(user.max_queue_bytes) << std::endl;

        for (const auto &acl : user.permissions)
            if (!acl.pattern.empty())
                ofs << USER_ACL << " = " << crud_to_string(acl.mode) << ":" << acl.pattern << std::endl;

        ofs << std::endl;
    }

    if (users.empty())
    {
        ofs << "[admin]" << std::endl;
        ofs << USER_ACTIVE << " = true" << std::endl;
        ofs << USER_PASSWORD << " = s3cr3t" << std::endl;
        ofs << USER_CAN_FORCE << " = true" << std::endl;
        ofs << USER_ACL << " = crud:**" << std::endl;
        ofs << std::endl;
    }

    if (!ofs)
        throw std::runtime_error("Error writing file");

    ofs.close();
}
