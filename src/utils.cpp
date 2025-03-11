#include <arpa/inet.h>
#include "utf8.h"
#include "utils.hpp"

std::uint32_t nplex::ntohl_ptr(const char *ptr)
{
    return ntohl(*reinterpret_cast<const std::uint32_t *>(ptr));
}

bool nplex::is_valid_key(const std::string_view &key)
{
    if (key.empty())
        return false;

    if (key.find('\0') != std::string_view::npos)
        return false;

    if (utf8nvalid(reinterpret_cast<const utf8_int8_t *>(key.data()), key.length()) != 0)
        return false;

    return true;
}

nplex::log_level_e nplex::parse_log_level(const std::string_view &str)
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

std::string nplex::to_string(log_level_e level)
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
