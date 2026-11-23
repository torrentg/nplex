#include "config.hpp"
#include "exception.hpp"
#include "params.hpp"
#include "utils.hpp"
#include "user.hpp"
#include "ini.h"
#include <fmt/core.h>
#include <algorithm>
#include <charconv>
#include <fstream>
#include <limits>

using namespace std::literals;

// INI keys
static constexpr const char *SECTION_USER_DEFAULTS                      = "user-defaults";
static constexpr const char *LOG_LEVEL                                  = "log-level";
static constexpr const char *FSYNC                                      = "fsync";
static constexpr const char *NETWORK_ADDR                               = "addr";
static constexpr const char *MAX_SESSIONS                               = "max-sessions";
static constexpr const char *MSG_MAX_BYTES                              = "msg-max-bytes";
static constexpr const char *WRITE_QUEUE_MAX_ENTRIES                    = "write-queue-max-entries";
static constexpr const char *WRITE_QUEUE_MAX_BYTES                      = "write-queue-max-bytes";
static constexpr const char *WRITE_FLUSH_MAX_ENTRIES                    = "write-flush-max-entries";
static constexpr const char *WRITE_FLUSH_MAX_BYTES                      = "write-flush-max-bytes";
static constexpr const char *TOMBSTONE_MAX_ENTRIES                      = "tombstone-max-entries";
static constexpr const char *TOMBSTONE_MAX_AGE                          = "tombstone-max-age";
static constexpr const char *TOMBSTONE_MIN_AGE                          = "tombstone-min-age";
static constexpr const char *CACHE_MAX_ENTRIES                          = "cache-max-entries";
static constexpr const char *CACHE_MAX_BYTES                            = "cache-max-bytes";
static constexpr const char *JOURNAL_MAX_ENTRIES                        = "journal-max-entries";
static constexpr const char *JOURNAL_MAX_BYTES                          = "journal-max-bytes";
static constexpr const char *SNAPSHOT_MAX_ENTRIES                       = "snapshot-max-entries";
static constexpr const char *SNAPSHOT_MAX_BYTES                         = "snapshot-max-bytes";
static constexpr const char *USER_PASSWORD                              = "password";
static constexpr const char *USER_ACTIVE                                = "active";
static constexpr const char *USER_CAN_FORCE                             = "can-force";
static constexpr const char *USER_CAN_MONITOR                           = "can-monitor";
static constexpr const char *USER_MAX_CONNECTIONS                       = "max-connections";
static constexpr const char *USER_KEEPALIVE_MILLIS                      = "keepalive-millis";
static constexpr const char *USER_TIMEOUT_FACTOR                        = "timeout-factor";
static constexpr const char *USER_MAX_UNACK_MSG                         = "max-unack-msg";
static constexpr const char *USER_MAX_UNACK_BYTES                       = "max-unack-bytes";
static constexpr const char *USER_ACL                                   = "acl";

// Defaults
static constexpr nplex::log_level_e DEFAULT_LOG_LEVEL                   = nplex::log_level_e::INFO;
static constexpr bool          DEFAULT_FSYNC                            = false;
static constexpr const char   *DEFAULT_NETWORK_ADDR                     = "localhost:14022";
static constexpr std::uint32_t DEFAULT_MAX_SESSIONS                     = 64;
static constexpr std::uint32_t DEFAULT_MSG_MAX_BYTES                    = 2 * 1024 * 1024;
static constexpr std::uint32_t DEFAULT_WRITE_QUEUE_MAX_ENTRIES          = 1000;
static constexpr std::uint32_t DEFAULT_WRITE_QUEUE_MAX_BYTES            = 350 * 1024 * 1024;
static constexpr std::uint32_t DEFAULT_WRITE_FLUSH_MAX_ENTRIES          = 50;
static constexpr std::uint32_t DEFAULT_WRITE_FLUSH_MAX_BYTES            = 25 * 1024 * 1024;
static constexpr std::uint32_t DEFAULT_TOMBSTONE_MAX_ENTRIES            = 1500;
static constexpr std::uint32_t DEFAULT_TOMBSTONE_MAX_AGE                = 20000;
static constexpr std::uint32_t DEFAULT_TOMBSTONE_MIN_AGE                = 5;
static constexpr std::uint32_t DEFAULT_CACHE_MAX_ENTRIES                = 50000;
static constexpr std::uint32_t DEFAULT_CACHE_MAX_BYTES                  = 500 * 1024 * 1024;
static constexpr std::uint32_t DEFAULT_JOURNAL_MAX_ENTRIES              = 100000;
static constexpr std::uint32_t DEFAULT_JOURNAL_MAX_BYTES                = 100 * 1024 * 1024;
static constexpr std::uint32_t DEFAULT_SNAPSHOT_MAX_ENTRIES             = 50000;
static constexpr std::uint32_t DEFAULT_SNAPSHOT_MAX_BYTES               = 50 * 1024 * 1024;
static constexpr bool          DEFAULT_USER_ACTIVE                      = true;
static constexpr bool          DEFAULT_USER_CAN_FORCE                   = false;
static constexpr bool          DEFAULT_USER_CAN_MONITOR                 = false;
static constexpr std::uint32_t DEFAULT_USER_MAX_CONNECTIONS             = 5;
static constexpr std::uint32_t DEFAULT_USER_KEEPALIVE_MILLIS            = 3000;
static constexpr float         DEFAULT_USER_TIMEOUT_FACTOR              = 3.0;
static constexpr std::uint32_t DEFAULT_USER_MAX_UNACK_MSG               = 1000;
static constexpr std::uint32_t DEFAULT_USER_MAX_UNACK_BYTES             = 100 * 1024 * 1024;

// ==========================================================
// Internal (static) functions
// ==========================================================

static bool parse_bool(const std::string_view &str)
{
    if (str == "true")
        return true;

    if (str == "false")
        return false;

    throw nplex::nplex_exception("Invalid bool ({})", str);
}

static float parse_float(const std::string_view &str)
{
    if (str.empty() || !isdigit(str[0]))
        throw nplex::nplex_exception("Invalid float value ({})", str);

    float num = 0;

    auto [ptr, ec] = std::from_chars(str.begin(), str.end(), num);

    if (ec != std::errc())
        throw nplex::nplex_exception("Invalid float value ({})", str);

    while (ptr != str.end() && isspace(*ptr)) ptr++;

    if (ptr != str.end())
        throw nplex::nplex_exception("Invalid float value ({})", str);

    return num;
}

static std::uint32_t parse_uint32(const std::string_view &str)
{
    if (str.empty() || !isdigit(str[0]))
        throw nplex::nplex_exception("Invalid number ({})", str);

    std::uint32_t num = 0;

    auto [ptr, ec] = std::from_chars(str.begin(), str.end(), num);

    if (ec != std::errc())
        throw nplex::nplex_exception("Invalid number ({})", str);

    while (ptr != str.end() && isspace(*ptr)) ptr++;

    if (ptr != str.end())
        throw nplex::nplex_exception("Invalid number ({})", str);

    return num;
}

static std::uint32_t parse_bytes(const std::string_view &str)
{
    if (str.empty() || !isdigit(str[0]))
        throw nplex::nplex_exception("Invalid bytes ({})", str);

    double num = 0;

    auto [ptr, ec] = std::from_chars(str.begin(), str.end(), num);
    if (ec != std::errc())
        throw nplex::nplex_exception("Invalid bytes ({})", str);

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
        throw nplex::nplex_exception("Invalid bytes ({})", str);

    if (num > std::numeric_limits<std::uint32_t>::max())
        throw nplex::nplex_exception("Bytes value out of range ({})", str);

    return static_cast<std::uint32_t>(num);
}

static nplex::acl_t parse_acl(const std::string_view &str)
{
    if (str.size() < 6 || str[4] != ':')
        throw nplex::nplex_exception("Invalid acl ({})", str);

    auto mode = nplex::parse_crud(str.substr(0, 4));
    std::string pattern{str.substr(5)};

    return nplex::acl_t{mode, pattern};
}

static void parse_section_main(nplex::config_t *cfg, const char *name, const char *value)
{
    if (strcmp(name, NETWORK_ADDR) == 0) {
        cfg->context.addr = nplex::addr_t{value};
    } else if (strcmp(name, LOG_LEVEL) == 0) {
        cfg->log_level = nplex::parse_log_level(value);
    } else if (strcmp(name, MAX_SESSIONS) == 0) {
        cfg->context.max_sessions = parse_uint32(value);
    } else if (strcmp(name, FSYNC) == 0) {
        cfg->storage.fsync = parse_bool(value);
    } else if (strcmp(name, MSG_MAX_BYTES) == 0) {
        cfg->context.msg_max_bytes = parse_bytes(value);
    } else if (strcmp(name, WRITE_QUEUE_MAX_ENTRIES) == 0) {
        cfg->storage.queue_max_entries = parse_uint32(value);
    } else if (strcmp(name, WRITE_QUEUE_MAX_BYTES) == 0) {
        cfg->storage.queue_max_bytes = parse_bytes(value);
    } else if (strcmp(name, WRITE_FLUSH_MAX_ENTRIES) == 0) {
        cfg->storage.flush_max_entries = parse_uint32(value);
    } else if (strcmp(name, WRITE_FLUSH_MAX_BYTES) == 0) {
        cfg->storage.flush_max_bytes = parse_bytes(value);
    } else if (strcmp(name, JOURNAL_MAX_ENTRIES) == 0) {
        cfg->storage.journal_max_entries = parse_uint32(value);
    } else if (strcmp(name, JOURNAL_MAX_BYTES) == 0) {
        cfg->storage.journal_max_bytes = parse_bytes(value);
    } else if (strcmp(name, SNAPSHOT_MAX_ENTRIES) == 0) {
        cfg->context.snapshot_max_entries = parse_uint32(value);
    } else if (strcmp(name, SNAPSHOT_MAX_BYTES) == 0) {
        cfg->context.snapshot_max_bytes = parse_bytes(value);
    } else if (strcmp(name, TOMBSTONE_MAX_ENTRIES) == 0) {
        cfg->store.tombstone_max_entries = parse_uint32(value);
    } else if (strcmp(name, TOMBSTONE_MAX_AGE) == 0) {
        cfg->store.tombstone_max_age = parse_uint32(value);
    } else if (strcmp(name, TOMBSTONE_MIN_AGE) == 0) {
        cfg->store.tombstone_min_age = parse_uint32(value);
    } else if (strcmp(name, CACHE_MAX_ENTRIES) == 0) {
        cfg->context.cache_max_entries = parse_uint32(value);
    } else if (strcmp(name, CACHE_MAX_BYTES) == 0) {
        cfg->context.cache_max_bytes = parse_bytes(value);
    } else {
        throw nplex::nplex_exception("Unrecognized entry ({})", name);
    }
}

static void parse_section_default_user(nplex::user_t &user, const char *name, const char *value)
{
    if (strcmp(name, USER_ACTIVE) == 0) {
        user.params.active = parse_bool(value);
    } else if (strcmp(name, USER_CAN_FORCE) == 0) {
        user.params.can_force = parse_bool(value);
    } else if (strcmp(name, USER_CAN_MONITOR) == 0) {
        user.params.can_monitor = parse_bool(value);
    } else if (strcmp(name, USER_MAX_CONNECTIONS) == 0) {
        user.params.max_connections = parse_uint32(value);
    } else if (strcmp(name, USER_KEEPALIVE_MILLIS) == 0) {
        user.params.connection.keepalive_millis = parse_uint32(value);
    } else if (strcmp(name, USER_MAX_UNACK_MSG) == 0) {
        user.params.connection.max_unack_msgs = parse_uint32(value);
    } else if (strcmp(name, USER_MAX_UNACK_BYTES) == 0) {
        user.params.connection.max_unack_bytes = parse_bytes(value);
    } else if (strcmp(name, USER_TIMEOUT_FACTOR) == 0) {
        user.params.connection.timeout_factor = parse_float(value);
        if (user.params.connection.timeout_factor != 0.0f && user.params.connection.timeout_factor <= 1.0f)
        throw nplex::nplex_exception("Invalid timeout factor ({})", value);
    } else {
        throw nplex::nplex_exception("Unrecognized entry ({})", name);
    }
}

static void parse_section_user(nplex::user_t &user, const char *name, const char *value)
{
    if (strcmp(name, USER_PASSWORD) == 0) {
        user.password = value;
    } else if (strcmp(name, USER_ACTIVE) == 0) {
        user.params.active = parse_bool(value);
    } else if (strcmp(name, USER_CAN_FORCE) == 0) {
        user.params.can_force = parse_bool(value);
    } else if (strcmp(name, USER_CAN_MONITOR) == 0) {
        user.params.can_monitor = parse_bool(value);
    } else if (strcmp(name, USER_MAX_CONNECTIONS) == 0) {
        user.params.max_connections = parse_uint32(value);
    } else if (strcmp(name, USER_KEEPALIVE_MILLIS) == 0) {
        user.params.connection.keepalive_millis = parse_uint32(value);
    } else if (strcmp(name, USER_MAX_UNACK_MSG) == 0) {
        user.params.connection.max_unack_msgs = parse_uint32(value);
    } else if (strcmp(name, USER_MAX_UNACK_BYTES) == 0) {
        user.params.connection.max_unack_bytes = parse_bytes(value);
    } else if (strcmp(name, USER_TIMEOUT_FACTOR) == 0) {
        user.params.connection.timeout_factor = parse_float(value);
        if (user.params.connection.timeout_factor != 0.0f && user.params.connection.timeout_factor <= 1.0f)
            throw nplex::nplex_exception("Invalid timeout factor ({})", value);
    } else if (strcmp(name, USER_ACL) == 0) {
        user.permissions.push_back(parse_acl(value));
    } else {
        throw nplex::nplex_exception("Unrecognized entry ({})", name);
    }
}

static int cb_inih_inner(void *obj, const char *section, const char *name, const char *value)
{
    auto cfg = static_cast<nplex::config_t *>(obj);

    if (strcmp(section, "") == 0) {
        parse_section_main(cfg, name, value);
        return true;
    }

    if (strcmp(section, SECTION_USER_DEFAULTS) == 0) {
        parse_section_default_user(cfg->default_user, name, value);
        return true;
    }

    // other sections are treated as user sections
    auto it = std::find_if(cfg->users.begin(), cfg->users.end(), [section](const nplex::user_t &usr) {
        return (usr.name == section);
    });

    if (it == cfg->users.end()) {
        cfg->users.push_back(cfg->default_user);
        it = std::prev(cfg->users.end());
        it->name = section;
    }

    parse_section_user(*it, name, value);

    return true;
}

static int cb_inih(void *obj, const char *section, const char *name, const char *value)
{
    try {
        return cb_inih_inner(obj, section, name, value);
    }
    catch (std::exception &e) {
        throw nplex::nplex_exception("{} in {} section", e.what(), (*section ? section : "global"));
    }
}

static void set_defaults(nplex::config_t &cfg)
{
    // Global/general defaults
    cfg.log_level = DEFAULT_LOG_LEVEL;

    // Network defaults
    cfg.context.addr = nplex::addr_t{DEFAULT_NETWORK_ADDR};
    cfg.context.max_sessions = DEFAULT_MAX_SESSIONS;
    cfg.context.msg_max_bytes = DEFAULT_MSG_MAX_BYTES;

    // Storage defaults
    cfg.storage.fsync = DEFAULT_FSYNC;
    cfg.storage.queue_max_entries = DEFAULT_WRITE_QUEUE_MAX_ENTRIES;
    cfg.storage.queue_max_bytes = DEFAULT_WRITE_QUEUE_MAX_BYTES;
    cfg.storage.flush_max_entries = DEFAULT_WRITE_FLUSH_MAX_ENTRIES;
    cfg.storage.flush_max_bytes = DEFAULT_WRITE_FLUSH_MAX_BYTES;
    cfg.storage.journal_max_entries = DEFAULT_JOURNAL_MAX_ENTRIES;
    cfg.storage.journal_max_bytes = DEFAULT_JOURNAL_MAX_BYTES;

    // Snapshot defaults
    cfg.context.snapshot_max_entries = DEFAULT_SNAPSHOT_MAX_ENTRIES;
    cfg.context.snapshot_max_bytes = DEFAULT_SNAPSHOT_MAX_BYTES;

    // Tombstone defaults
    cfg.store.tombstone_max_entries = DEFAULT_TOMBSTONE_MAX_ENTRIES;
    cfg.store.tombstone_max_age = DEFAULT_TOMBSTONE_MAX_AGE;
    cfg.store.tombstone_min_age = DEFAULT_TOMBSTONE_MIN_AGE;

    // Cache defaults
    cfg.context.cache_max_entries = DEFAULT_CACHE_MAX_ENTRIES;
    cfg.context.cache_max_bytes = DEFAULT_CACHE_MAX_BYTES;

    // User defaults
    cfg.default_user.params.active = DEFAULT_USER_ACTIVE;
    cfg.default_user.params.can_force = DEFAULT_USER_CAN_FORCE;
    cfg.default_user.params.can_monitor = DEFAULT_USER_CAN_MONITOR;
    cfg.default_user.params.max_connections = DEFAULT_USER_MAX_CONNECTIONS;
    cfg.default_user.params.connection.keepalive_millis = DEFAULT_USER_KEEPALIVE_MILLIS;
    cfg.default_user.params.connection.max_unack_msgs = DEFAULT_USER_MAX_UNACK_MSG;
    cfg.default_user.params.connection.max_unack_bytes = DEFAULT_USER_MAX_UNACK_BYTES;
    cfg.default_user.params.connection.timeout_factor = DEFAULT_USER_TIMEOUT_FACTOR;

    // Sanity: ensure tombstone_min_age does not exceed tombstone_max_age when both are set.
    if (cfg.store.tombstone_max_age > 0 && cfg.store.tombstone_min_age > cfg.store.tombstone_max_age)
        cfg.store.tombstone_min_age = cfg.store.tombstone_max_age;
}

template <typename T>
static void normalize_unlimited(T &value)
{
    if (value == static_cast<T>(0))
        value = std::numeric_limits<T>::max();
}

static void normalize(nplex::config_t &cfg)
{
    normalize_unlimited(cfg.context.max_sessions);
    normalize_unlimited(cfg.context.msg_max_bytes);
    normalize_unlimited(cfg.context.snapshot_max_entries);
    normalize_unlimited(cfg.context.snapshot_max_bytes);
    normalize_unlimited(cfg.context.cache_max_entries);
    normalize_unlimited(cfg.context.cache_max_bytes);
    normalize_unlimited(cfg.storage.queue_max_entries);
    normalize_unlimited(cfg.storage.queue_max_bytes);
    normalize_unlimited(cfg.storage.flush_max_entries);
    normalize_unlimited(cfg.storage.flush_max_bytes);
    normalize_unlimited(cfg.store.tombstone_max_entries);

    for (auto &user : cfg.users)
    {
        normalize_unlimited(user.params.max_connections);

        auto &con = user.params.connection;

        normalize_unlimited(con.max_unack_msgs);
        normalize_unlimited(con.max_unack_bytes);
    }
}

// ==========================================================
// config_t methods
// ==========================================================

nplex::config_t::config_t(const fs::path &filepath)
{
    set_defaults(*this);

    if (!filepath.empty())
        load(filepath);
}

void nplex::config_t::load(const fs::path &filepath)
{
    int rc = ini_parse(filepath.c_str(), cb_inih, this);

    if (rc < 0)
        throw nplex_exception("Unable to read file");
    else if (rc > 0)
        throw nplex_exception("Syntax error at line {}", rc);

    if (store.tombstone_min_age > store.tombstone_max_age)
        throw nplex_exception("{} cannot exceed {}", TOMBSTONE_MIN_AGE, TOMBSTONE_MAX_AGE);

    normalize(*this);
}

void nplex::config_t::save(const fs::path &filepath) const
{
    std::ofstream ofs(filepath, std::ios::trunc);

    ofs << "# " << PROJECT_NAME << " configuration file." << std::endl;
    ofs << "# see " PROJECT_URL << std::endl;
    ofs << std::endl;

    ofs << NETWORK_ADDR << " = " << context.addr.str() << std::endl;
    ofs << LOG_LEVEL << " = " << to_string(log_level) << std::endl;
    ofs << FSYNC << " = " << fmt::format("{}", storage.fsync) << std::endl;
    ofs << std::endl;

    ofs << MAX_SESSIONS << " = " << context.max_sessions << std::endl;
    ofs << MSG_MAX_BYTES << " = " << bytes_to_string(context.msg_max_bytes) << std::endl;
    ofs << WRITE_QUEUE_MAX_ENTRIES << " = " << storage.queue_max_entries << std::endl;
    ofs << WRITE_QUEUE_MAX_BYTES << " = " << bytes_to_string(storage.queue_max_bytes) << std::endl;
    ofs << WRITE_FLUSH_MAX_ENTRIES << " = " << storage.flush_max_entries << std::endl;
    ofs << WRITE_FLUSH_MAX_BYTES << " = " << bytes_to_string(storage.flush_max_bytes) << std::endl;
    ofs << TOMBSTONE_MAX_ENTRIES << " = " << store.tombstone_max_entries << std::endl;
    ofs << TOMBSTONE_MAX_AGE << " = " << store.tombstone_max_age << std::endl;
    ofs << TOMBSTONE_MIN_AGE << " = " << store.tombstone_min_age << std::endl;
    ofs << CACHE_MAX_BYTES << " = " << bytes_to_string(context.cache_max_bytes) << std::endl;
    ofs << CACHE_MAX_ENTRIES << " = " << context.cache_max_entries << std::endl;
    ofs << JOURNAL_MAX_ENTRIES << " = " << storage.journal_max_entries << std::endl;
    ofs << JOURNAL_MAX_BYTES << " = " << bytes_to_string(storage.journal_max_bytes) << std::endl;
    ofs << SNAPSHOT_MAX_ENTRIES << " = " << context.snapshot_max_entries << std::endl;
    ofs << SNAPSHOT_MAX_BYTES << " = " << bytes_to_string(context.snapshot_max_bytes) << std::endl;
    ofs << std::endl;

    ofs << "[" << SECTION_USER_DEFAULTS << "]" << std::endl;
    ofs << USER_ACTIVE << " = " << fmt::format("{}", default_user.params.active) << std::endl;
    ofs << USER_CAN_FORCE << " = " << fmt::format("{}", default_user.params.can_force) << std::endl;
    ofs << USER_CAN_MONITOR << " = " << fmt::format("{}", default_user.params.can_monitor) << std::endl;
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
            ofs << USER_ACTIVE << " = " << fmt::format("{}", user.params.active) << std::endl;
        if (user.params.can_force != default_user.params.can_force)
            ofs << USER_CAN_FORCE << " = " << fmt::format("{}", user.params.can_force) << std::endl;
        if (user.params.can_monitor != default_user.params.can_monitor)
            ofs << USER_CAN_MONITOR << " = " << fmt::format("{}", user.params.can_monitor) << std::endl;
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
        ofs << USER_CAN_MONITOR << " = true" << std::endl;
        ofs << USER_ACL << " = crud:**" << std::endl;
        ofs << std::endl;
    }

    if (!ofs)
        throw nplex_exception("Error writing file {}", filepath.filename().string());

    ofs.close();
}
