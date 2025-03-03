#pragma once

#include <cstdint>
#include <string>
#include <chrono>
#include "cstring.hpp"

#define PROJECT_NAME "nplex"
#define PROJECT_VERSION "0.1.0"
#define PROJECT_URL "https://github.com/torrentg/nplex"

#define API_VERSION 10

#define CONFIG_FILENAME "nplex.ini"
#define LOG_FILENAME "nplex.log"
#define DATA_FILENAME "nplex.dat"
#define INDEX_FILENAME "nplex.idx"

#define DEFAULT_ADDR "localhost:14022"
#define MAX_CONNECTIONS 256

#define NPLEX_CREATE 1
#define NPLEX_READ   2
#define NPLEX_UPDATE 4
#define NPLEX_DELETE 8

// Error codes
#define ERR_CLOSED_BY_LOCAL 1000
#define ERR_CLOSED_BY_PEER 1001
#define ERR_MSG_ERROR 1002
#define ERR_MSG_UNEXPECTED 1003
#define ERR_MSG_SIZE 1004
#define ERR_USR_NOT_FOUND 1005
#define ERR_USR_INVL_PWD 1006
#define ERR_USR_MAX_CONN 1007
#define ERR_MAX_CONN 1008
#define ERR_API_VERSION 1009
#define ERR_TIMEOUT_STEP_1 1010
#define ERR_TIMEOUT_STEP_2 1011


#define UNUSED(x) (void)(x)

namespace nplex {

//! Database revision number.
using rev_t = std::size_t;

//! Database key type.
using key_t = gto::cstring;

//! Database timestamps (milliseconds from epoch time).
using millis_t = std::chrono::milliseconds;

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

// Key support functions.
inline bool is_valid_key(const std::string_view &key) { return !key.empty(); }
inline bool is_valid_key(const char *key) { return (key && is_valid_key(std::string_view{key})); }
inline bool is_valid_key(const key_t &key) { return is_valid_key(key.view()); }

} // namespace nplex
