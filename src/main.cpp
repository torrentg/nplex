#include <getopt.h>
#include <cstdlib>
#include <cstdint>
#include <csignal>
#include <iostream>
#include <filesystem>
#include <limits>
#include <string>
#include <spdlog/spdlog.h>
#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include "params.hpp"
#include "addr.hpp"
#include "utils.hpp"
#include "server.hpp"
#include "common.hpp"

using namespace std;
using namespace nplex;

namespace fs = std::filesystem;

static server_t server;

static spdlog::level::level_enum to_spdlog(log_level_e level)
{
    switch (level)
    {
        case log_level_e::TRACE:   return spdlog::level::trace;
        case log_level_e::DEBUG:   return spdlog::level::debug;
        case log_level_e::INFO:    return spdlog::level::info;
        case log_level_e::WARN:    return spdlog::level::warn;
        case log_level_e::ERROR:   return spdlog::level::err;
        default: return spdlog::level::info;
    }
}

static void help()
{
    std::cout <<
    "Nplex is a key-value stream database.\n"
    "\n"
    "Usage:\n"
    "  nplex -D datadir [OPTION]...\n"
    "\n"
    "Options:\n"
    "  -D DATADIR       Database directory.\n"
    "  -a HOST:PORT     Address to listen on (ex: localhost:14022).\n"
    "  -l LOGLEVEL      Log level (trace, debug, info, warning, error).\n"
    "  -c               Check journal files at startup.\n"
    "  -d               Run the program as a daemon.\n"
    "  -F               Turn fsync off.\n"
    "  -V, --version    Output version information, then exit.\n"
    "  -h, --help       Show this help, then exit.\n"
    "\n"
    "Signals:\n"
    "  SIGHUP           Recreate log file (when daemonized).\n"
    "  SIGINT           Graceful shutdown.\n"
    "  SIGTERM          Graceful shutdown.\n"
    "  SIGPIPE          Ignored.\n"
    "\n"
    "Exit status:\n"
    "  0   finished without errors\n"
    "  1   finished with errors\n"
    "\n"
    "Nplex home page: <" PROJECT_URL ">."
    << std::endl;
}

static void version()
{
    std::cout <<
    PROJECT_NAME << " " << PROJECT_VERSION << "\n"
    "Copyright (c) 2026 Gerard Torrent.\n"
    "License MIT: MIT License <https://opensource.org/licenses/MIT>."
    << std::endl;
}

static void handle_sig_logrotate(int signal)
{
    if (signal != SIGHUP)
        return;

    try {
        SPDLOG_TRACE("SIGHUP signal received. Recreating log file.");
        spdlog::drop(PROJECT_NAME);
        auto logger = spdlog::basic_logger_mt(PROJECT_NAME, LOG_FILENAME, true);
        spdlog::set_default_logger(logger);
    } catch (const spdlog::spdlog_ex &e) {
        std::cerr << "Failed to recreate log file: " << e.what() << std::endl;
    }
}

static void install_signal_handler(int signal, void (*handle)(int))
{
    struct sigaction sa;
    sa.sa_handler = handle;
    sa.sa_flags = 0;
    sigemptyset(&sa.sa_mask);

    if (sigaction(signal, &sa, nullptr) == -1) {
        std::cerr << "Error: Failed to install signal handler." << std::endl;
        std::exit(EXIT_FAILURE);
    }
}

int main(int argc, char *argv[])
{
    fs::path datadir_arg;
    addr_t addr_arg;
    log_level_e log_level_arg = log_level_e::NONE;
    bool check_journal_arg = false;
    bool disable_fsync_arg = false;
    bool daemonize_arg = false;

    // short options
    const char* const options1 = "cdVhFD:l:a:";

    // long options (name + has_arg + flag + val)
    const struct option options2[] = {
        { "help",    0, nullptr, 'h' },
        { "version", 0, nullptr, 'V' },
        { nullptr,   0, nullptr,  0  }
    };

    if (argc <= 1) {
        std::cerr << "Error: No arguments provided." << std::endl;
        std::cerr << "Use the --help option for more information." << std::endl;
        return EXIT_FAILURE;
    }

    while (true)
    {
        int curropt = getopt_long(argc, argv, options1, options2, nullptr);

        if (curropt == -1)
            break;

        switch (curropt)
        {
            case 'h': // -h or --help (show help and exit)
                help();
                return EXIT_SUCCESS;

            case 'V': // -V or --version (show version and exit)
                version();
                return EXIT_SUCCESS;

            case 'D': // -D datadir (set data directory)
                datadir_arg = optarg;
                break;

            case 'l': // -l loglevel (set log level)
                try {
                    log_level_arg = parse_log_level(optarg);
                }
                catch (const std::invalid_argument &e) {
                    std::cerr << "Error: Invalid log level (" << optarg << ")." << std::endl;
                    return EXIT_FAILURE;
                }
                break;

            case 'a': // -a host:port (set address to listen on)
                try {
                    addr_arg = addr_t{optarg};
                }
                catch (const std::exception &e) {
                    std::cerr << "Error: " << e.what() << std::endl;
                    return EXIT_FAILURE;
                }
                break;

            case 'c': // -c (check journal at startup)
                check_journal_arg = true;
                break;

            case 'd': // -d (run as daemon)
                daemonize_arg = true;
                break;

            case 'F': // -F (turn fsync off)
                disable_fsync_arg = true;
                break;

            default: // '?' (unexpected argument)
                // getopt() prints message 'invalid option' (see opterr)
                std::cerr << "Use the --help option for more information." << std::endl;
                return EXIT_FAILURE;
        }
    }

    if (argc != optind) {
        std::cerr << "Error: Unexpected argument (" << argv[optind] << ")." << std::endl;
        std::cerr << "Use the --help option for more information." << std::endl;
        return EXIT_FAILURE;
    }

    if (datadir_arg.empty()) {
        std::cerr << "Error: Missing required argument -D <datadir>." << std::endl;
        return EXIT_FAILURE;
    }

    if (!fs::exists(datadir_arg))
    {
        std::error_code ec;
        fs::create_directories(datadir_arg, ec);
        if (ec) {
            std::cerr << "Error: Unable to create directory " << datadir_arg << " (" << ec.message() << ")" << std::endl;
            return EXIT_FAILURE;
        }
    }

    if (!fs::is_directory(datadir_arg)) {
        std::cerr << "Error: Path " << datadir_arg << " is not a directory." << std::endl;
        return EXIT_FAILURE;
    }

    try {
        std::filesystem::current_path(datadir_arg);
    }
    catch (const std::exception &) {
        std::cerr << "Error: Unable to change to directory " << datadir_arg << std::endl;
        return EXIT_FAILURE;
    }

    params_t params;

    try
    {
        fs::path config_file = CONFIG_FILENAME;

        if (!fs::exists(config_file))
        {
            if (addr_arg.port())
                params.addr = addr_arg;

            if (log_level_arg != log_level_e::NONE)
                params.log_level = log_level_arg;

            params.disable_fsync = disable_fsync_arg;

            params.save(config_file);
        }

        params.load(config_file);

        if (addr_arg.port())
            params.addr = addr_arg;

        if (log_level_arg != log_level_e::NONE)
            params.log_level = log_level_arg;

        params.datadir = std::filesystem::current_path();
        params.check_journal = check_journal_arg;
        params.disable_fsync = disable_fsync_arg;
    }
    catch(const std::exception &e) {
        std::cerr << "Error accessing " << datadir_arg / CONFIG_FILENAME << ": " << e.what() << std::endl;
        return EXIT_FAILURE;
    }

    try
    {
        spdlog::set_default_logger(
            daemonize_arg ?
                spdlog::basic_logger_mt(PROJECT_NAME, LOG_FILENAME):
                spdlog::stdout_color_mt(PROJECT_NAME)
        );

        spdlog::flush_on(spdlog::level::debug);
        spdlog::set_level(to_spdlog(params.log_level));

        // @see https://github.com/gabime/spdlog/wiki/3.-Custom-formatting#pattern-flags
        if (params.log_level <= log_level_e::DEBUG)
            spdlog::set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%t] [%s:%#] [%-5l] %v");
        else
            spdlog::set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%-5l] %v");

        if (daemonize_arg)
            install_signal_handler(SIGHUP, handle_sig_logrotate);
        else
            signal(SIGHUP, SIG_IGN);
    }
    catch (const spdlog::spdlog_ex &e) {
        std::cerr << e.what() << std::endl;
        return EXIT_FAILURE;
    }

    if (daemonize_arg && daemon(1, 0) == -1) {
        std::cerr << "Error: Failed to daemonize the process." << std::endl;
        return EXIT_FAILURE;
    }

    try {
        server.init(params);
        signal(SIGPIPE, SIG_IGN); // SIGTERM and SIGINT are handled in server.cpp
        server.run();
        return EXIT_SUCCESS;
    }
    catch (const std::exception &e) {
        SPDLOG_ERROR("{}", e.what());
        return EXIT_FAILURE;
    }
}
