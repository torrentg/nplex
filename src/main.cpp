#include <getopt.h>
#include <cstdlib>
#include <cstdint>
#include <cctype>
#include <iostream>
#include <filesystem>
#include <limits>
#include <string>
#include <spdlog/spdlog.h>
#include <spdlog/sinks/basic_file_sink.h>
#include "params.hpp"
#include "journal.h"
#include "addr.hpp"
#include "server.hpp"
#include "common.hpp"

using namespace std;
using namespace nplex;

namespace fs = std::filesystem;

static spdlog::level::level_enum to_spdlog(log_level_e level)
{
    switch (level)
    {
        case log_level_e::DEBUG: return spdlog::level::debug;
        case log_level_e::INFO:  return spdlog::level::info;
        case log_level_e::WARN:  return spdlog::level::warn;
        case log_level_e::ERROR: return spdlog::level::err;
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
    "  -N MAX-CONNECT   Maximum number of allowed connections.\n"
    "  -l LOGLEVEL      Log level (debug, info, warning, error).\n"
    "  -d               Run the program as a daemon.\n"
    "  -F               Turn fsync off.\n"
    "  -V, --version    Output version information, then exit.\n"
    "  -h, --help       Show this help, then exit.\n"
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
    "Copyright (c) 2025 Gerard Torrent.\n"
    "License MIT: MIT License <https://opensource.org/licenses/MIT>."
    << std::endl;
}

int main(int argc, char *argv[])
{
    params_t params;

    // short options
    const char* const options1 = "dVhFD:l:a:N:";

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
                params.datadir = optarg;
                break;

            case 'l': // -l loglevel (set log level)
                try {
                    params.log_level = parse_log_level(optarg);
                }
                catch (const std::invalid_argument &e) {
                    std::cerr << "Error: Invalid log level (" << optarg << ")." << std::endl;
                    return EXIT_FAILURE;
                }
                break;

            case 'a': // -a host:port (set address to listen on)
                try {
                    params.addr = addr_t{optarg};
                }
                catch (const std::exception &e) {
                    std::cerr << "Error: " << e.what() << std::endl;
                    return EXIT_FAILURE;
                }
                break;

            case 'N': // -N max-connections (set maximum number of connections)
                try {
                    int num = std::stoi(optarg);

                    if (!isdigit(*optarg) || *optarg == '0' || num <= 0 || num > MAX_CONNECTIONS)
                        throw std::invalid_argument("Invalid number of connections");

                    params.max_connections = static_cast<std::uint32_t>(num);
                }
                catch (...) {
                    std::cerr << "Error: Invalid maximum number of connections (" << optarg << ")." << std::endl;
                    return EXIT_FAILURE;
                }
                break;

            case 'd': // -d (run as daemon)
                params.daemonize = true;
                break;

            case 'F': // -F (turn fsync off)
                params.disable_fsync = true;
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

    if (params.datadir.empty()) {
        std::cerr << "Error: Missing required argument -D <datadir>." << std::endl;
        return EXIT_FAILURE;
    }

    if (!fs::exists(params.datadir))
    {
        std::error_code ec;
        fs::create_directories(params.datadir, ec);
        if (ec) {
            std::cerr << "Error: Unable to create directory " << params.datadir << " (" << ec.message() << ")" << std::endl;
            return EXIT_FAILURE;
        }
    }

    if (!fs::is_directory(params.datadir)) {
        std::cerr << "Error: Path " << params.datadir << " is not a directory." << std::endl;
        return EXIT_FAILURE;
    }

    try {
        std::filesystem::current_path(params.datadir);
        params.datadir = fs::current_path();
    }
    catch (const std::exception &) {
        std::cerr << "Error: Unable to change to directory " << params.datadir << std::endl;
        return EXIT_FAILURE;
    }

    fs::path config_file = CONFIG_FILENAME;

    try
    {
        if (!fs::exists(config_file))
        {
            params.default_user.keepalive_millis = 3000;
            params.default_user.max_msg_bytes = 50 * 1024 * 1024;
            params.default_user.max_unack_bytes = 100 * 1024 * 1024;
            params.default_user.max_unack_msgs = 100;
            params.save(config_file);
        }

        params_t aux(config_file);

        aux.datadir = params.datadir;
        aux.addr = params.addr;
        aux.log_level = params.log_level;
        aux.max_connections = params.max_connections;
        aux.disable_fsync = params.disable_fsync;
        aux.daemonize = params.daemonize;

        params = aux;
    }
    catch(const std::exception &e) {
        std::cerr << "Error accessing " << fs::absolute(config_file) << ": " << e.what() << std::endl;
        return EXIT_FAILURE;
    }

    std::shared_ptr<ldb::journal_t> journal;

    try {
        journal = std::make_shared<ldb::journal_t>(fs::current_path(), PROJECT_NAME);
        journal->set_fsync(!params.disable_fsync);
    }
    catch (const std::exception &e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return EXIT_FAILURE;
    }

    if (params.daemonize)
    {
        spdlog::filename_t log_file = LOG_FILENAME;

        try {
            auto logger = spdlog::basic_logger_mt("basic_logger", log_file);
            logger->flush_on(spdlog::level::info);
            spdlog::set_default_logger(logger);
        }
        catch (const spdlog::spdlog_ex &e) {
            std::cerr << e.what() << std::endl;
            return EXIT_FAILURE;
        }

        if (daemon(1, 0) == -1) {
            std::cerr << "Error: Failed to daemonize the process." << std::endl;
            return EXIT_FAILURE;
        }
    }

    spdlog::set_level(to_spdlog(params.log_level));
    // @see https://github.com/gabime/spdlog/wiki/3.-Custom-formatting#pattern-flags
    spdlog::set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%s:%#] [%-5l] %v");

    // TODO: install signal catcher

    try {
        server_t server{params};
        server.run();
        return EXIT_SUCCESS;
    }
    catch (const std::exception &e) {
        SPDLOG_ERROR("Error: {}", e.what());
        return EXIT_FAILURE;
    }
}
