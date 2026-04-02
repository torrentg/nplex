#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <getopt.h>

#define APP_NAME                "snpbulk"

void print_snapshot(FILE *out, const char *data, size_t len, char mode);

void print_help(FILE *out)
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
        {"help",     no_argument,       0, 'h'},
        {"compact",  no_argument,       0, 'c'},
        {0, 0, 0, 0}
    };

    while ((opt = getopt_long(argc, argv, "hc", long_opts, NULL)) != -1)
    {
        switch (opt)
        {
            case 'h':
                print_help(stdout);
                exit(EXIT_SUCCESS);
            case 'c':
                mode = 'c';
                break;
            default:
                print_help(stdout);
                exit(EXIT_FAILURE);
        }
    }

    if (optind >= argc) {
        fprintf(stderr, "%s: FILE is required\n", APP_NAME);
        exit(EXIT_FAILURE);
    }

    // Process each file
    for (int i = optind; i < argc; ++i)
    {
        const char *file_name = argv[i];
        FILE *file = fopen(file_name, "rb");

        if (!file) {
            perror("Error opening file");
            continue;
        }

        // Read file content into ldb_entry_t
        fseek(file, 0, SEEK_END);
        size_t file_size = (size_t) ftell(file);
        fseek(file, 0, SEEK_SET);

        char *file_content = malloc(file_size);

        if (!file_content) {
            fprintf(stderr, "Not enough memory\n");
            fclose(file);
            continue;
        }

        fread(file_content, 1, file_size, file);
        fclose(file);

        // Print snapshot
        print_snapshot(stdout, file_content, file_size, mode);
        free(file_content);
    }

    return EXIT_SUCCESS;
}
