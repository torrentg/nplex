#pragma once

#include "common.hpp"
#include <spdlog/spdlog.h>
#include <string_view>
#include <cstdint>
#include <string>

namespace nplex {

/**
 * Initialize logging backend and default logger instance.
 *
 * @param[in] daemonize true to log to file, false to log to stdout.
 * @param[in] level Initial minimum level enabled for runtime logging.
 */
void init_logger(bool daemonize, log_level_e level);

/**
 * Reopen log destination (used after logrotate/SIGHUP).
 */
void reopen_logger();

} // namespace nplex

#define LOG_TRACE(...) do { \
    if (spdlog::should_log(spdlog::level::trace)) \
        SPDLOG_TRACE(__VA_ARGS__); \
} while (0)

#define LOG_DEBUG(...) do { \
    if (spdlog::should_log(spdlog::level::debug)) \
        SPDLOG_DEBUG(__VA_ARGS__); \
} while (0)

#define LOG_INFO(...) do { \
    if (spdlog::should_log(spdlog::level::info)) \
        SPDLOG_INFO(__VA_ARGS__); \
} while (0)

#define LOG_WARN(...) do { \
    if (spdlog::should_log(spdlog::level::warn)) \
        SPDLOG_WARN(__VA_ARGS__); \
} while (0)

#define LOG_ERROR(...) do { \
    if (spdlog::should_log(spdlog::level::err)) \
        SPDLOG_ERROR(__VA_ARGS__); \
} while (0)
