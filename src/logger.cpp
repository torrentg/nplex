#include "logger.hpp"
#include "common.hpp"
#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <stdexcept>

// ==========================================================
// Internal (static) variables and functions
// ==========================================================

static spdlog::level::level_enum to_spdlog(nplex::log_level_e level) noexcept
{
    using nplex::log_level_e;

    switch (level)
    {
        case log_level_e::TRACE: return spdlog::level::trace;
        case log_level_e::DEBUG: return spdlog::level::debug;
        case log_level_e::INFO:  return spdlog::level::info;
        case log_level_e::WARN:  return spdlog::level::warn;
        case log_level_e::ERROR: return spdlog::level::err;
        case log_level_e::NONE:  return spdlog::level::off;
        default:                 return spdlog::level::info;
    }
}

static std::string get_pattern(spdlog::level::level_enum level)
{
    // @see https://github.com/gabime/spdlog/wiki/3.-Custom-formatting#pattern-flags
    if (level <= spdlog::level::debug)
        return "[%Y-%m-%d %H:%M:%S.%e] [%t] [%s:%#] [%-5l] %v";
    else
        return "[%Y-%m-%d %H:%M:%S.%e] [%-5l] %v";
}

// ==========================================================
// nplex methods
// ==========================================================

void nplex::init_logger(bool daemonize, log_level_e level)
{
    auto spdlog_level = to_spdlog(level);
    auto spdlog_pattern = get_pattern(spdlog_level);

    spdlog::set_default_logger(
        daemonize ?
            spdlog::basic_logger_mt(PROJECT_NAME, LOG_FILENAME) :
            spdlog::stdout_color_mt(PROJECT_NAME)
    );

    spdlog::flush_on(spdlog::level::debug);
    spdlog::set_pattern(spdlog_pattern);
    spdlog::set_level(spdlog_level);
}

void nplex::reopen_logger()
{
    auto spdlog_level = spdlog::get_level();
    auto spdlog_pattern = get_pattern(spdlog_level);

    spdlog::drop(PROJECT_NAME);
    auto logger = spdlog::basic_logger_mt(PROJECT_NAME, LOG_FILENAME, true);
    spdlog::set_default_logger(logger);

    spdlog::flush_on(spdlog::level::debug);
    spdlog::set_pattern(spdlog_pattern);
    spdlog::set_level(spdlog_level);
}
