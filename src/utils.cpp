#include <arpa/inet.h>
#include <fmt/core.h>
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

    if ("trace"s == str)
        return log_level_e::TRACE;
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
        case log_level_e::TRACE: return "trace";
        case log_level_e::DEBUG: return "debug";
        case log_level_e::INFO: return "info";
        case log_level_e::DEFAULT: return "info";
        case log_level_e::WARN: return "warning";
        case log_level_e::ERROR: return "error";
        default: return "unknown";
    }
}

gto::cstring nplex::create_cstring(const flatbuffers::Vector<std::uint8_t> *value) {
    return gto::cstring{reinterpret_cast<const char *>(value->data()), static_cast<std::size_t>(value->size())};
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

std::string nplex::bytes_to_string(std::size_t bytes)
{
    if (bytes < 1024)
        return std::to_string(bytes);
    else if (bytes < 1024 * 1024)
        return fmt::format("{}KB", format_double(static_cast<double>(bytes) / 1024.0));
    else if (bytes < 1024 * 1024 * 1024)
        return fmt::format("{}MB", format_double(static_cast<double>(bytes) / (1024.0 * 1024)));
    else
        return fmt::format("{}GB", format_double(static_cast<double>(bytes) / (1024.0 * 1024 * 1024)));
}
