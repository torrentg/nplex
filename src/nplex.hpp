#pragma once

#include <string>
#include <cstdint>
#include <stdexcept>

#define PROJECT_NAME "nplex"
#define PROJECT_VERSION "0.1.0"
#define PROJECT_URL "https://github.com/torrentg/nplex"

#define CONFIG_FILENAME "nplex.ini"
#define LOCK_FILENAME "nplex.lock"
#define LOG_FILENAME "nplex.log"
#define DATA_FILENAME "nplex.dat"
#define INDEX_FILENAME "nplex.idx"

#define MAX_CONNECTIONS 256

#define NPLEX_CREATE 1
#define NPLEX_READ   2
#define NPLEX_UPDATE 4
#define NPLEX_DELETE 8

#define UNUSED(x) (void)(x)

namespace nplex {

struct acl_t
{
    std::uint8_t mode;      // Attributes (1=CREATE, 2=READ, 4=UPDATE, 8=DELETE).
    std::string pattern;    // Pattern (glob).
};

enum log_level_e : std::uint8_t {
    DEBUG,
    INFO,
    WARN,
    ERROR
};

inline log_level_e parse_log_level(const std::string_view &str)
{
    using namespace std::string_literals;

    if ("debug"s == str)
        return log_level_e::DEBUG;
    if ("info"s == str)
        return log_level_e::INFO;
    if ("warning"s == str)
        return log_level_e::WARN;
    if ("error"s == str)
        return log_level_e::ERROR;

    throw std::invalid_argument("Invalid log level");
}

inline std::string to_string(log_level_e level)
{
    switch (level)
    {
        case log_level_e::DEBUG: return "debug";
        case log_level_e::INFO: return "info";
        case log_level_e::WARN: return "warning";
        case log_level_e::ERROR: return "error";
        default: return "unknown";
    }
}

} // namespace nplex
