#include <ctime>
#include <sstream>
#include <iomanip>
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
    const auto *ptr = reinterpret_cast<const utf8_int8_t *>(key.data());
    const auto *end = ptr + key.length();

    if (key.empty())
        return false;

    if (key.front() == ' ' || key.back() == ' ')
        return false;

    if (key.find("//") != std::string_view::npos)
        return false;

    if (key.find('\0') != std::string_view::npos)
        return false;

    if (utf8nvalid(ptr, key.length()) != 0)
        return false;

    // Check for control characters (C0, DEL, C1)
    while (ptr < end)
    {
        utf8_int32_t cp = 0;

        ptr = utf8codepoint(ptr, &cp);

        if ((cp >= 0x00 && cp <= 0x1F) || (cp >= 0x7F && cp <= 0x9F))
            return false;
    }

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
        case log_level_e::INFO:  return "info";
        case log_level_e::WARN:  return "warning";
        case log_level_e::ERROR: return "error";
        case log_level_e::NONE:  return "none";
        default: return "unknown";
    }
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

std::size_t nplex::estimate_bytes(const update_dto_t &update)
{
    if (update.deletes.empty() && update.upserts.empty())
        return 0;

    std::size_t bytes = 0;

    bytes += sizeof(std::uint64_t);             // rev
    bytes += sizeof(std::uint64_t);             // timestamp
    bytes += sizeof(std::uint32_t);             // type
    bytes += sizeof(std::uint32_t);             // num upserts
    bytes += sizeof(std::uint32_t);             // num deletes
    bytes += sizeof(std::uint32_t);             // user length
    bytes += update.user.size();                // user
    bytes += sizeof(std::uint32_t);             // num upserts
    bytes += sizeof(std::uint32_t);             // num deletes

    for (const auto &pair : update.upserts)
    {
        bytes += sizeof(std::uint32_t);         // key length
        bytes += pair.first.size();             // key data
        bytes += sizeof(std::uint32_t);         // value length
        bytes += pair.second.size();            // value data
    }

    for (const auto &key : update.deletes)
    {
        bytes += sizeof(std::uint32_t);         // key length
        bytes += key.size();                    // key data
    }

    return bytes;
}

std::size_t nplex::estimate_bytes(const update_t &update)
{
    if (!update.meta || (update.upserts.empty() && update.deletes.empty()))
        return 0;

    std::size_t bytes = 0;

    bytes += sizeof(std::uint64_t);             // rev
    bytes += sizeof(std::uint64_t);             // timestamp
    bytes += sizeof(std::uint32_t);             // type
    bytes += sizeof(std::uint32_t);             // num upserts
    bytes += sizeof(std::uint32_t);             // num deletes
    bytes += sizeof(std::uint32_t);             // user length
    bytes += update.meta->user.size();          // user
    bytes += sizeof(std::uint32_t);             // num upserts
    bytes += sizeof(std::uint32_t);             // num deletes

    for (const auto &pair : update.upserts)
    {
        bytes += sizeof(std::uint32_t);         // key length
        bytes += pair.first.size();             // key data
        bytes += sizeof(std::uint32_t);         // value length
        bytes += pair.second->data().size();    // value data
    }

    for (const auto &key : update.deletes)
    {
        bytes += sizeof(std::uint32_t);         // key length
        bytes += key.size();                    // key data
    }

    return bytes;
}

std::string nplex::to_iso8601(std::chrono::milliseconds ms_since_epoch)
{
    using namespace std::chrono;

    auto secs = duration_cast<seconds>(ms_since_epoch); 
    auto millis = duration_cast<milliseconds>(ms_since_epoch - secs).count(); 

    std::ostringstream oss; 
    std::time_t t = secs.count(); 
    std::tm tm_utc; 

    gmtime_r(&t, &tm_utc);

    oss << std::put_time(&tm_utc, "%Y-%m-%dT%H:%M:%S"); 
    oss << '.' << std::setw(3) << std::setfill('0') << millis << 'Z'; 

    return oss.str();
}
