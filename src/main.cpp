#include "common.hpp"
#include "config.hpp"
#include "utils.hpp"
#include "addr.hpp"
#include "server.hpp"
#include "readme_template.h"
#include "logger.hpp"
#include <string>
#include <filesystem>
#include <fstream>
#include <cstdlib>
#include <cstdint>
#include <csignal>
#include <ctime>
#include <optional>
#include <getopt.h>

using namespace std;
using namespace nplex;

namespace fs = std::filesystem;

static server_t server;

struct args_t
{
    fs::path            datadir;
    addr_t              addr;
    log_level_e         log_level = log_level_e::NONE;
    bool                check     = false;
    bool                daemonize = false;
    std::optional<bool> fsync_off;
};

static void help()
{
    fprintf(stdout,
        "Nplex is a key-value stream database.\n"
        "\n"
        "Usage:\n"
        "  nplex -D datadir [OPTION]...\n"
        "\n"
        "Options:\n"
        "  -D DATADIR       Database directory.\n"
        "  -a HOST:PORT     Address to listen on (ex: localhost:14022).\n"
        "  -l LOGLEVEL      Log level (trace, debug, info, warning, error).\n"
        "  -c               Check journal file at startup.\n"
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
        "Nplex home page: <" PROJECT_URL ">.\n");
}

static void version()
{
    fprintf(stdout,
        PROJECT_NAME " " PROJECT_VERSION "\n"
        "schemas = [0x%08x, 0x%08x, 0x%08x]\n"
        "Copyright (c) 2026 Gerard Torrent.\n"
        "License MIT: MIT License <https://opensource.org/licenses/MIT>.\n",
        SCHEMA1_HASH, SCHEMA2_HASH, SCHEMA3_HASH);
}

static void handle_sig_logrotate(int signal)
{
    if (signal != SIGHUP)
        return;

    try {
        LOG_TRACE("SIGHUP signal received. Recreating log file.");
        reopen_logger();
    } catch (const std::exception &e) {
        fprintf(stderr, "Failed to recreate log file: %s\n", e.what());
    }
}

static void install_signal_handler(int signal, void (*handle)(int))
{
    struct sigaction sa;
    sa.sa_handler = handle;
    sa.sa_flags = 0;
    sigemptyset(&sa.sa_mask);

    if (sigaction(signal, &sa, nullptr) == -1) {
        fprintf(stderr, "Error: Failed to install signal handler.\n");
        std::exit(EXIT_FAILURE);
    }
}

static args_t parse_args(int argc, char *argv[])
{
    args_t args;

    // short options
    const char* const options1 = "cdVhFD:l:a:";

    // long options (name + has_arg + flag + val)
    const struct option options2[] = {
        { "help",    0, nullptr, 'h' },
        { "version", 0, nullptr, 'V' },
        { nullptr,   0, nullptr,  0  }
    };

    if (argc <= 1) {
        fprintf(stderr, "Error: No arguments provided.\n");
        fprintf(stderr, "Use the --help option for more information.\n");
        std::exit(EXIT_FAILURE);
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
                std::exit(EXIT_SUCCESS);

            case 'V': // -V or --version (show version and exit)
                version();
                std::exit(EXIT_SUCCESS);

            case 'D': // -D datadir (set data directory)
                args.datadir = optarg;
                break;

            case 'l': // -l loglevel (set log level)
                try {
                    args.log_level = parse_log_level(optarg);
                }
                catch (const std::invalid_argument &) {
                    fprintf(stderr, "Error: Invalid log level (%s).\n", optarg);
                    std::exit(EXIT_FAILURE);
                }
                break;

            case 'a': // -a host:port (set address to listen on)
                try {
                    args.addr = addr_t{optarg};
                }
                catch (const std::exception &e) {
                    fprintf(stderr, "Error: %s\n", e.what());
                    std::exit(EXIT_FAILURE);
                }
                break;

            case 'c': // -c (check journal at startup)
                args.check = true;
                break;

            case 'd': // -d (run as daemon)
                args.daemonize = true;
                break;

            case 'F': // -F (turn fsync off)
                args.fsync_off = true;
                break;

            default: // '?' (unexpected argument)
                // getopt() prints message 'invalid option' (see opterr)
                fprintf(stderr, "Use the --help option for more information.\n");
                std::exit(EXIT_FAILURE);
        }
    }

    if (argc != optind) {
        fprintf(stderr, "Error: Unexpected argument (%s).\n", argv[optind]);
        fprintf(stderr, "Use the --help option for more information.\n");
        std::exit(EXIT_FAILURE);
    }

    if (args.datadir.empty()) {
        fprintf(stderr, "Error: Missing required argument -D <datadir>.\n");
        std::exit(EXIT_FAILURE);
    }

    return args;
}

static void setup_datadir(const fs::path &datadir)
{
    if (!fs::exists(datadir))
    {
        std::error_code ec;
        fs::create_directories(datadir, ec);
        if (ec)
            throw std::runtime_error("Error: Unable to create directory " + datadir.string() + " (" + ec.message() + ")");
    }

    if (!fs::is_directory(datadir))
        throw std::runtime_error("Error: Path " + datadir.string() + " is not a directory.");

    try {
        fs::current_path(datadir);
    }
    catch (const std::exception &) {
        throw std::runtime_error("Error: Unable to change to directory " + datadir.string());
    }
}

static config_t init_config(const args_t &args)
{
    config_t config;

    try
    {
        fs::path config_file = CONFIG_FILENAME;

        if (!fs::exists(config_file))
        {
            if (args.addr.port())
                config.context.addr = args.addr;

            if (args.log_level != log_level_e::NONE)
                config.log_level = args.log_level;

            if (args.fsync_off.has_value())
                config.journal.fsync = !args.fsync_off.value();

            config.save(config_file);
        }

        config.load(config_file);

        if (args.addr.port())
            config.context.addr = args.addr;

        if (args.log_level != log_level_e::NONE)
            config.log_level = args.log_level;

        if (args.fsync_off.has_value())
            config.journal.fsync = !args.fsync_off.value();

        config.journal.check = args.check;
    }
    catch (const std::exception &e) {
        throw std::runtime_error(std::string("Error accessing ") + CONFIG_FILENAME + ": " + e.what());
    }

    return config;
}

static void setup_readme()
{
    try
    {
        fs::path readme_file = README_FILENAME;

        if (!fs::exists(readme_file))
        {
            std::ofstream f(readme_file, std::ios::binary);
            if (!f)
                throw std::runtime_error("cannot open file for writing");

            std::time_t t = std::time(nullptr);
            char datebuf[11] = {};
            std::strftime(datebuf, sizeof(datebuf), "%Y-%m-%d", std::localtime(&t));
            f << "<!-- created by " PROJECT_NAME " " PROJECT_VERSION " on " << datebuf << " -->\n";
            f.write(reinterpret_cast<const char *>(readme_content), readme_content_len);
        }
    }
    catch (const std::exception &e) {
        fprintf(stderr, "Warning: Unable to create %s: %s\n", README_FILENAME, e.what());
    }
}

int main(int argc, char *argv[])
{
    args_t args{};
    config_t config{};

    try {
        args = parse_args(argc, argv);
        setup_datadir(args.datadir);
        config = init_config(args);
        setup_readme();
        init_logger(args.daemonize, config.log_level);
    }
    catch (const std::exception &e) {
        fprintf(stderr, "%s\n", e.what());
        return EXIT_FAILURE;
    }

    if (args.daemonize)
        install_signal_handler(SIGHUP, handle_sig_logrotate);
    else
        signal(SIGHUP, SIG_IGN);

    if (args.daemonize && daemon(1, 0) == -1) {
        fprintf(stderr, "Error: Failed to daemonize the process.\n");
        return EXIT_FAILURE;
    }

    try {
        server.init(config);
        signal(SIGPIPE, SIG_IGN); // SIGTERM and SIGINT are handled in server.cpp
        server.run();
        return EXIT_SUCCESS;
    }
    catch (const std::exception &e) {
        LOG_ERROR("{}", e.what());
        return EXIT_FAILURE;
    }
}
