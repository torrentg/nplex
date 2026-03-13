#include <limits>
#include <fstream>
#include <charconv>
#include <algorithm>
#include <fmt/core.h>
#include "ini.h"
#include "utils.hpp"
#include "params.hpp"
#include "user.hpp"
#include "config.hpp"

using namespace std::literals;

namespace nplex {

    // INI keys
#define SECTION_USER_DEFAULTS                   "user-defaults"
#define LOG_LEVEL                               "log-level"
#define DISABLE_FSYNC                           "disable-fsync"
#define NETWORK_ADDR                            "addr"
#define MAX_SESSIONS                            "max-sessions"
#define USER_PASSWORD                           "password"
#define USER_ACTIVE                             "active"
#define USER_CAN_FORCE                          "can-force"
#define USER_MAX_CONNECTIONS                    "max-connections"
#define USER_KEEPALIVE_MILLIS                   "keepalive-millis"
#define USER_MAX_UNACK_MSG                      "max-unack-msg"
#define USER_MAX_UNACK_BYTES                    "max-unack-bytes"
#define USER_TIMEOUT_FACTOR                     "timeout-factor"
#define USER_ACL                                "acl"
#define MAX_MSG_BYTES                           "max-msg-bytes"
#define WRITE_QUEUE_MAX_LENGTH                  "write-queue-max-length"
#define WRITE_QUEUE_MAX_BYTES                   "write-queue-max-bytes"
#define FLUSH_MAX_ENTRIES                       "flush-max-entries"
#define FLUSH_MAX_BYTES                         "flush-max-bytes"
#define MAX_UPDATES_BETWEEN_SNAPSHOTS           "max-updates-between-snapshots"
#define MAX_BYTES_BETWEEN_SNAPSHOTS             "max-bytes-between-snapshots"
#define TOMBSTONE_RETENTION_MAX                 "tombstone-retention-max"
#define TOMBSTONE_RETENTION_MIN                 "tombstone-retention-min"
#define MAX_TOMBSTONES                          "max-tombstones"
#define CACHE_MAX_BYTES                         "cache-max-bytes"
#define CACHE_MAX_ENTRIES                       "cache-max-entries"

// Defaults
#define DEFAULT_CHECK_JOURNAL                   false
#define DEFAULT_LOG_LEVEL                       nplex::log_level_e::INFO
#define DEFAULT_DISABLE_FSYNC                   false
#define DEFAULT_NETWORK_ADDR                    "localhost:14022"
#define DEFAULT_MAX_SESSIONS                    64
#define DEFAULT_MAX_MSG_BYTES                   (2 * 1024 * 1024)
#define DEFAULT_QUEUE_MAX_LENGTH                1000
#define DEFAULT_QUEUE_MAX_BYTES                 (350 * 1024 * 1024)
#define DEFAULT_FLUSH_MAX_ENTRIES               50
#define DEFAULT_FLUSH_MAX_BYTES                 (25 * 1024 * 1024)
#define DEFAULT_USER_ACTIVE                     true
#define DEFAULT_USER_CAN_FORCE                  false
#define DEFAULT_USER_MAX_CONNECTIONS            5
#define DEFAULT_USER_KEEPALIVE_MILLIS           3000
#define DEFAULT_USER_MAX_UNACK_MSG              1000
#define DEFAULT_USER_MAX_UNACK_BYTES            (100 * 1024 * 1024)
#define DEFAULT_USER_TIMEOUT_FACTOR             3.0
#define DEFAULT_MAX_UPDATES_BETWEEN_SNAPSHOTS   50000
#define DEFAULT_MAX_BYTES_BETWEEN_SNAPSHOTS     (100 * 1024 * 1024)
#define DEFAULT_TOMBSTONE_RETENTION_MAX         20000
#define DEFAULT_TOMBSTONE_RETENTION_MIN         5
#define DEFAULT_MAX_TOMBSTONES                  1500
#define DEFAULT_CACHE_MAX_ENTRIES               50000
#define DEFAULT_CACHE_MAX_BYTES                 (500 * 1024 * 1024)

static std::string crud_to_string(std::uint8_t crud)
{
    return std::string{
        ((crud & CRUD_CREATE) ? 'c' : '-'),
        ((crud & CRUD_READ)   ? 'r' : '-'),
        ((crud & CRUD_UPDATE) ? 'u' : '-'),
        ((crud & CRUD_DELETE) ? 'd' : '-')
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
            case 'c': crud |= CRUD_CREATE; break;
            case 'r': crud |= CRUD_READ;   break;
            case 'u': crud |= CRUD_UPDATE; break;
            case 'd': crud |= CRUD_DELETE; break;
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

    if (ptr != str.end() && isspace(*ptr)) ptr++;
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

static acl_t parse_acl(const std::string_view &str)
{
    if (str.size() < 6 || str[4] != ':')
        throw std::invalid_argument(fmt::format("Invalid acl ({})", str));

    auto mode = parse_crud(str.substr(0, 4));
    std::string pattern{str.substr(5)};

    return acl_t{mode, pattern};
}

static int cb_inih_inner(void *obj, const char *section, const char *name, const char *value)
{
    auto cfg = static_cast<config_t *>(obj);

    if (strcmp(section, "") == 0)
    {
        if (strcmp(name, NETWORK_ADDR) == 0) {
            cfg->context.addr = addr_t{value};
        } else if (strcmp(name, LOG_LEVEL) == 0) {
            cfg->log_level = parse_log_level(value);
        } else if (strcmp(name, MAX_SESSIONS) == 0) {
            cfg->context.max_sessions = parse_uint32(value);
        } else if (strcmp(name, DISABLE_FSYNC) == 0) {
            cfg->journal.fsync = !parse_bool(value);
        } else if (strcmp(name, MAX_MSG_BYTES) == 0) {
            cfg->context.max_msg_bytes = parse_bytes(value);
        } else if (strcmp(name, WRITE_QUEUE_MAX_LENGTH) == 0) {
            cfg->journal.write_queue_max_entries = parse_uint32(value);
        } else if (strcmp(name, WRITE_QUEUE_MAX_BYTES) == 0) {
            cfg->journal.write_queue_max_bytes = parse_bytes(value);
        } else if (strcmp(name, FLUSH_MAX_ENTRIES) == 0) {
            cfg->journal.flush_max_entries = parse_uint32(value);
        } else if (strcmp(name, FLUSH_MAX_BYTES) == 0) {
            cfg->journal.flush_max_bytes = parse_bytes(value);
        } else if (strcmp(name, MAX_UPDATES_BETWEEN_SNAPSHOTS) == 0) {
            cfg->context.snapshot_max_entries = parse_uint32(value);
        } else if (strcmp(name, MAX_BYTES_BETWEEN_SNAPSHOTS) == 0) {
            cfg->context.snapshot_max_bytes = parse_bytes(value);
        } else if (strcmp(name, TOMBSTONE_RETENTION_MAX) == 0) {
            cfg->repo.retention_max = parse_uint32(value);
        } else if (strcmp(name, TOMBSTONE_RETENTION_MIN) == 0) {
            cfg->repo.retention_min = parse_uint32(value);
        } else if (strcmp(name, MAX_TOMBSTONES) == 0) {
            cfg->repo.max_tombstones = parse_uint32(value);
        } else if (strcmp(name, CACHE_MAX_ENTRIES) == 0) {
            cfg->context.cache_max_entries = parse_uint32(value);
        } else if (strcmp(name, CACHE_MAX_BYTES) == 0) {
            cfg->context.cache_max_bytes = parse_bytes(value);
        } else {
            throw std::invalid_argument(fmt::format("Unrecognized entry ({})", name));
        }

        return true;
    }

    if (strcmp(section, SECTION_USER_DEFAULTS) == 0)
    {
        if (strcmp(name, USER_ACTIVE) == 0) {
            cfg->default_user.params.active = parse_bool(value);
        } else if (strcmp(name, USER_CAN_FORCE) == 0) {
            cfg->default_user.params.can_force = parse_bool(value);
        } else if (strcmp(name, USER_MAX_CONNECTIONS) == 0) {
            cfg->default_user.params.max_connections = parse_uint32(value);
        } else if (strcmp(name, USER_KEEPALIVE_MILLIS) == 0) {
            cfg->default_user.params.connection.keepalive_millis = parse_uint32(value);
        } else if (strcmp(name, USER_MAX_UNACK_MSG) == 0) {
            cfg->default_user.params.connection.max_unack_msgs = parse_uint32(value);
        } else if (strcmp(name, USER_MAX_UNACK_BYTES) == 0) {
            cfg->default_user.params.connection.max_unack_bytes = parse_bytes(value);
        } else if (strcmp(name, USER_TIMEOUT_FACTOR) == 0) {
            cfg->default_user.params.connection.timeout_factor = parse_float(value);
            if (cfg->default_user.params.connection.timeout_factor != 0.0f &&
                cfg->default_user.params.connection.timeout_factor <= 1.0f)
                throw std::invalid_argument(fmt::format("Invalid timeout factor ({})", value));
        } else {
            throw std::invalid_argument(fmt::format("Unrecognized entry ({})", name));
        }

        return true;
    }

    auto it = std::find_if(cfg->users.begin(), cfg->users.end(), [section](const user_t &usr) {
        return (usr.name == section);
    });

    if (it == cfg->users.end()) {
        cfg->users.push_back(cfg->default_user);
        it = std::prev(cfg->users.end());
        it->name = section;
    }

    if (strcmp(name, USER_PASSWORD) == 0) {
        it->password = value;
    } else if (strcmp(name, USER_ACTIVE) == 0) {
        it->params.active = parse_bool(value);
    } else if (strcmp(name, USER_CAN_FORCE) == 0) {
        it->params.can_force = parse_bool(value);
    } else if (strcmp(name, USER_MAX_CONNECTIONS) == 0) {
        it->params.max_connections = parse_uint32(value);
    } else if (strcmp(name, USER_KEEPALIVE_MILLIS) == 0) {
        it->params.connection.keepalive_millis = parse_uint32(value);
    } else if (strcmp(name, USER_MAX_UNACK_MSG) == 0) {
        it->params.connection.max_unack_msgs = parse_uint32(value);
    } else if (strcmp(name, USER_MAX_UNACK_BYTES) == 0) {
        it->params.connection.max_unack_bytes = parse_bytes(value);
    } else if (strcmp(name, USER_TIMEOUT_FACTOR) == 0) {
        it->params.connection.timeout_factor = parse_float(value);
        if (it->params.connection.timeout_factor != 0.0f && it->params.connection.timeout_factor <= 1.0f)
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

static void set_defaults(config_t &cfg)
{
    // Global/general defaults
    cfg.log_level = DEFAULT_LOG_LEVEL;

    // Network defaults
    cfg.context.addr = addr_t{DEFAULT_NETWORK_ADDR};
    cfg.context.max_sessions = DEFAULT_MAX_SESSIONS;
    cfg.context.max_msg_bytes = DEFAULT_MAX_MSG_BYTES;

    // Journal defaults
    cfg.journal.check = DEFAULT_CHECK_JOURNAL;
    cfg.journal.fsync = !DEFAULT_DISABLE_FSYNC;
    cfg.journal.write_queue_max_entries = DEFAULT_QUEUE_MAX_LENGTH;
    cfg.journal.write_queue_max_bytes = DEFAULT_QUEUE_MAX_BYTES;
    cfg.journal.flush_max_entries = DEFAULT_FLUSH_MAX_ENTRIES;
    cfg.journal.flush_max_bytes = DEFAULT_FLUSH_MAX_BYTES;

    // Snapshot defaults
    cfg.context.snapshot_max_entries = DEFAULT_MAX_UPDATES_BETWEEN_SNAPSHOTS;
    cfg.context.snapshot_max_bytes = DEFAULT_MAX_BYTES_BETWEEN_SNAPSHOTS;

    // Tombstone defaults
    cfg.repo.retention_max = DEFAULT_TOMBSTONE_RETENTION_MAX;
    cfg.repo.retention_min = DEFAULT_TOMBSTONE_RETENTION_MIN;
    cfg.repo.max_tombstones = DEFAULT_MAX_TOMBSTONES;

    // Cache defaults
    cfg.context.cache_max_entries = DEFAULT_CACHE_MAX_ENTRIES;
    cfg.context.cache_max_bytes = DEFAULT_CACHE_MAX_BYTES;

    // User defaults
    cfg.default_user.params.active = DEFAULT_USER_ACTIVE;
    cfg.default_user.params.can_force = DEFAULT_USER_CAN_FORCE;
    cfg.default_user.params.max_connections = DEFAULT_USER_MAX_CONNECTIONS;
    cfg.default_user.params.connection.keepalive_millis = DEFAULT_USER_KEEPALIVE_MILLIS;
    cfg.default_user.params.connection.max_unack_msgs = DEFAULT_USER_MAX_UNACK_MSG;
    cfg.default_user.params.connection.max_unack_bytes = DEFAULT_USER_MAX_UNACK_BYTES;
    cfg.default_user.params.connection.timeout_factor = DEFAULT_USER_TIMEOUT_FACTOR;

    // Sanity: ensure retention_min does not exceed retention_max when both are set.
    if (cfg.repo.retention_max > 0 && cfg.repo.retention_min > cfg.repo.retention_max)
        cfg.repo.retention_min = cfg.repo.retention_max;
}

template <typename T>
static void normalize_unlimited(T &value)
{
    if (value == static_cast<T>(0))
        value = std::numeric_limits<T>::max();
}

// Validació global + transformació de paràmetres
static void normalize(config_t &cfg)
{
    normalize_unlimited(cfg.context.max_sessions);
    normalize_unlimited(cfg.context.max_msg_bytes);
    normalize_unlimited(cfg.context.snapshot_max_entries);
    normalize_unlimited(cfg.context.snapshot_max_bytes);
    normalize_unlimited(cfg.context.cache_max_entries);
    normalize_unlimited(cfg.context.cache_max_bytes);
    normalize_unlimited(cfg.journal.write_queue_max_entries);
    normalize_unlimited(cfg.journal.write_queue_max_bytes);
    normalize_unlimited(cfg.journal.flush_max_entries);
    normalize_unlimited(cfg.journal.flush_max_bytes);
    normalize_unlimited(cfg.repo.max_tombstones);

    for (auto &user : cfg.users)
    {
        normalize_unlimited(user.params.max_connections);

        auto &con = user.params.connection;

        normalize_unlimited(con.max_unack_msgs);
        normalize_unlimited(con.max_unack_bytes);
    }
}

config_t::config_t(const fs::path &filepath)
{
    set_defaults(*this);

    if (!filepath.empty())
        load(filepath);
}

void config_t::load(const fs::path &filepath)
{
    int rc = ini_parse(filepath.c_str(), cb_inih, this);

    if (rc < 0)
        throw std::runtime_error("Unable to read file");
    else if (rc > 0)
        throw std::runtime_error("Syntax error at line " + std::to_string(rc));

    if (repo.retention_min > repo.retention_max)
        throw std::invalid_argument("tombstone-retention-min cannot exceed tombstone-retention-max");

    normalize(*this);
}

void config_t::save(const fs::path &filepath) const
{
    std::ofstream ofs(filepath, std::ios::trunc);

    ofs << "# " << PROJECT_NAME << " configuration file." << std::endl;
    ofs << "# see " PROJECT_URL << std::endl;
    ofs << std::endl;

    ofs << NETWORK_ADDR << " = " << context.addr.str() << std::endl;
    ofs << LOG_LEVEL << " = " << to_string(log_level) << std::endl;
    ofs << DISABLE_FSYNC << " = " << (journal.fsync ? "false" : "true") << std::endl;
    ofs << std::endl;

    ofs << MAX_SESSIONS << " = " << context.max_sessions << std::endl;
    ofs << MAX_MSG_BYTES << " = " << bytes_to_string(context.max_msg_bytes) << std::endl;
    ofs << WRITE_QUEUE_MAX_LENGTH << " = " << journal.write_queue_max_entries << std::endl;
    ofs << WRITE_QUEUE_MAX_BYTES << " = " << bytes_to_string(journal.write_queue_max_bytes) << std::endl;
    ofs << FLUSH_MAX_ENTRIES << " = " << journal.flush_max_entries << std::endl;
    ofs << FLUSH_MAX_BYTES << " = " << bytes_to_string(journal.flush_max_bytes) << std::endl;
    ofs << MAX_UPDATES_BETWEEN_SNAPSHOTS << " = " << context.snapshot_max_entries << std::endl;
    ofs << MAX_BYTES_BETWEEN_SNAPSHOTS << " = " << bytes_to_string(context.snapshot_max_bytes) << std::endl;
    ofs << TOMBSTONE_RETENTION_MAX << " = " << repo.retention_max << std::endl;
    ofs << TOMBSTONE_RETENTION_MIN << " = " << repo.retention_min << std::endl;
    ofs << MAX_TOMBSTONES << " = " << repo.max_tombstones << std::endl;
    ofs << CACHE_MAX_BYTES << " = " << bytes_to_string(context.cache_max_bytes) << std::endl;
    ofs << CACHE_MAX_ENTRIES << " = " << context.cache_max_entries << std::endl;
    ofs << std::endl;

    ofs << "[" << SECTION_USER_DEFAULTS << "]" << std::endl;
    ofs << USER_ACTIVE << " = " << (default_user.params.active ? "true" : "false") << std::endl;
    ofs << USER_CAN_FORCE << " = " << (default_user.params.can_force ? "true" : "false") << std::endl;
    ofs << USER_MAX_CONNECTIONS << " = " << default_user.params.max_connections << std::endl;
    ofs << USER_KEEPALIVE_MILLIS << " = " << default_user.params.connection.keepalive_millis << std::endl;
    ofs << USER_TIMEOUT_FACTOR << " = " << fmt::format("{:.1f}", default_user.params.connection.timeout_factor) << std::endl;
    ofs << USER_MAX_UNACK_MSG << " = " << default_user.params.connection.max_unack_msgs << std::endl;
    ofs << USER_MAX_UNACK_BYTES << " = " << bytes_to_string(default_user.params.connection.max_unack_bytes) << std::endl;
    ofs << std::endl;

    for (const auto &user : users)
    {
        if (user.name.empty())
            continue;

        ofs << "[" << user.name << "]" << std::endl;
        ofs << USER_PASSWORD << " = " << user.password << std::endl;
        if (user.params.active != default_user.params.active)
            ofs << USER_ACTIVE << " = " << (user.params.active ? "true" : "false") << std::endl;
        if (user.params.can_force != default_user.params.can_force)
            ofs << USER_CAN_FORCE << " = " << (user.params.can_force ? "true" : "false") << std::endl;
        if (user.params.max_connections != default_user.params.max_connections)
            ofs << USER_MAX_CONNECTIONS << " = " << user.params.max_connections << std::endl;
        if (user.params.connection.keepalive_millis != default_user.params.connection.keepalive_millis)
            ofs << USER_KEEPALIVE_MILLIS << " = " << user.params.connection.keepalive_millis << std::endl;
        if (user.params.connection.timeout_factor != default_user.params.connection.timeout_factor)
            ofs << USER_TIMEOUT_FACTOR << " = " << fmt::format("{:.1f}", user.params.connection.timeout_factor) << std::endl;
        if (user.params.connection.max_unack_msgs != default_user.params.connection.max_unack_msgs)
            ofs << USER_MAX_UNACK_MSG << " = " << user.params.connection.max_unack_msgs << std::endl;
        if (user.params.connection.max_unack_bytes != default_user.params.connection.max_unack_bytes)
            ofs << USER_MAX_UNACK_BYTES << " = " << bytes_to_string(user.params.connection.max_unack_bytes) << std::endl;

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

} // namespace nplex
