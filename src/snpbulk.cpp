#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <getopt.h>
#include "storage.hpp"
#include "exception.hpp"

#define APP_NAME    "snpbulk"

extern "C" void print_snapshot(FILE *out, const char *data, size_t len, char mode);

static void print_help(FILE *out)
{
    fprintf(out,
        "%s - show nplex snapshot contents as JSON.\n"
        "\n"
        "Usage:\n"
        "  %s [-h] [-c] file1 [file2 ...]\n"
        "\n"
        "Options:\n"
        "  -h, --help       Show this help message and exit.\n"
        "  -c, --compact    Use compact JSON format. Default is indented.\n",
        APP_NAME, APP_NAME
    );
}

int main(int argc, char *argv[])
{
    if (argc < 2) {
        print_help(stdout);
        return EXIT_FAILURE;
    }

    char mode = 'i';
    int opt = 0;

    static struct option long_opts[] = {
        {"help",    no_argument, 0, 'h'},
        {"compact", no_argument, 0, 'c'},
        {0, 0, 0, 0}
    };

    while ((opt = getopt_long(argc, argv, "hc", long_opts, nullptr)) != -1)
    {
        switch (opt)
        {
            case 'h':
                print_help(stdout);
                return EXIT_SUCCESS;
            case 'c':
                mode = 'c';
                break;
            default:
                print_help(stdout);
                return EXIT_FAILURE;
        }
    }

    if (optind >= argc) {
        fprintf(stderr, "%s: FILE is required\n", APP_NAME);
        return EXIT_FAILURE;
    }

    int exit_code = EXIT_SUCCESS;

    for (int i = optind; i < argc; ++i)
    {
        try {
            std::string content = nplex::read_snapshot(argv[i]);
            print_snapshot(stdout, content.data(), content.size(), mode);
        }
        catch (const std::exception &e) {
            fprintf(stderr, "%s: %s: %s\n", APP_NAME, argv[i], e.what());
            exit_code = EXIT_FAILURE;
        }
    }

    return exit_code;
}
