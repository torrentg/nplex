#include <cstdlib>
#include <cstdint>
#include <cctype>
#include <getopt.h>
#include <iostream>
#include <filesystem>
#include <limits>
#include <string>
#include "params.hpp"
#include "addr.hpp"
#include "nplex.hpp"

using namespace std;
using namespace nplex;

namespace fs = std::filesystem;

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
    "  -a HOST:PORT     Address to listen on (ex: localhost:8444).\n"
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
    "License MIT: MIT License <https://opensource.org/licenses/MIT>.\n"
    << std::endl;
}

int main(int argc, char *argv[])
{
    server_params_t params;

    // short options
    const char* const options1 = "D:l:a:N:dVhF";

    // long options (name + has_arg + flag + val)
    const struct option options2[] = {
        { "help",         0,  nullptr,  'h' },
        { "version",      0,  nullptr,  'V' },
        { nullptr,        0,  nullptr,   0  }
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
                catch (std::invalid_argument &e) {
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

    if (argc - optind > 1) {
        std::cerr << "Error: Unexpected argument." << std::endl;
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

    // TODO: check and set lock file (containing PID)
    // TODO: install signal catcher
    // TODO: set_terminate(exception_handler)
    // TODO: create nplex.conf if does not exist
    // TODO: read configuration file (nplex.conf) overwriting command line options
    // TODO: setup the log system (nplex.log)
    // TODO: open database files (nplex.dat, nplex.idx)
    // TODO: create the event-loop
    // TODO: daemonize if required
    // TODO: run the event-loop

    return EXIT_SUCCESS;
}
