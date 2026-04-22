#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <filesystem>
#include <fstream>
#include <vector>
#include <getopt.h>
#include <fmt/core.h>
#include "journal.h"
#include "json.hpp"
#include "storage.hpp"
#include "exception.hpp"
#include "common.hpp"

#define APP_NAME    "nplexcat"

namespace fs = std::filesystem;

// ==========================================================
// File type detection
// ==========================================================

enum class file_type_e { 
    SNAPSHOT, 
    JOURNAL, 
    UNKNOWN
};

static file_type_e detect_file_type(const fs::path &path)
{
    char header[8] = {};

    std::ifstream ifs(path, std::ios::binary);
    if (!ifs.read(header, 8))
        return file_type_e::UNKNOWN;

    if (std::memcmp(header, SNAPSHOT_MAGIC, MAGIC_LEN) == 0)
        return file_type_e::SNAPSHOT;

    if (*(uint64_t *)(header) == LDB_DAT_MAGIC_NUMBER)
        return file_type_e::JOURNAL;

    return file_type_e::UNKNOWN;
}

// ==========================================================
// Dump functions
// ==========================================================

static void dump_snapshot(const fs::path &path, char mode)
{
    auto content = nplex::read_snapshot(path);
    auto json = nplex::snapshot_to_json(content.data(), content.size(), mode);
    printf("%s\n", json.c_str());
}

static void dump_journal(const fs::path &path, char mode)
{
    int rc = 0;
    ldb_stats_t stats = {};
    ldb::journal_t journal{};

    journal = nplex::open_journal(path, LDB_OPEN_READONLY);

    if ((rc = journal.stats(0, UINT64_MAX, &stats)) != LDB_OK)
        throw nplex::nplex_exception(fmt::format("Failed to get journal stats ({})", ldb_strerror(rc)));
    
    if (stats.num_entries == 0)
        return;

    constexpr size_t BATCH = 128;
    size_t buf_len = 1024 * 1024;
    std::vector<char> buf(buf_len);
    ldb_entry_t entries[BATCH + 1] = {};
    uint64_t seq = stats.min_seqnum;
    uint64_t to_seq = stats.max_seqnum;

    while (seq <= to_seq)
    {
        size_t want = std::min(BATCH, static_cast<size_t>(to_seq - seq + 1ULL));
        size_t num = 0;

        if ((rc = journal.read(seq, entries, want, buf.data(), buf_len, &num)) != LDB_OK)
        {
            if (rc == LDB_ERR_NOT_FOUND)
                break;

            throw nplex::nplex_exception("error reading journal: {}", ldb_strerror(rc));
        }

        if (num == 0)
            break;

        // buffer exhausted: entries[num] contains next entry but data == NULL
        if (num < want && entries[num].seqnum != 0 && entries[num].data == nullptr)
        {
            size_t need = static_cast<size_t>(entries[num].data_len) + 64;

            while (buf_len < need)
                buf_len *= 2;

            buf.resize(buf_len);
        }

        for (size_t i = 0; i < num; ++i) {
            auto json = nplex::update_to_json(static_cast<const char *>(entries[i].data), entries[i].data_len, mode);
            printf("%s\n", json.c_str());
        }

        seq = entries[num - 1].seqnum + 1;
    }
}

// ==========================================================
// CLI
// ==========================================================

static void version()
{
    std::cout
        << APP_NAME << " " << PROJECT_VERSION << "\n"
        << "schemas = {" << SCHEMA1_HASH << ", " << SCHEMA2_HASH << "}\n"
        << "Copyright (c) 2026 Gerard Torrent.\n"
        << "License MIT: MIT License <https://opensource.org/licenses/MIT>."
        << std::endl;
}

static void print_help(FILE *out)
{
    fprintf(out,
        "%s - dump nplex snapshot or journal files as JSON.\n"
        "\n"
        "Usage:\n"
        "  %s [-h] [-V] [-c] file1 [file2 ...]\n"
        "\n"
        "Options:\n"
        "  -h, --help       Show this help message and exit.\n"
        "  -V, --version    Show version info and exit.\n"
        "  -c, --compact    Use compact JSON format. Default is indented.\n",
        APP_NAME, APP_NAME
    );
}

static int dump_file(const char *argv, char mode)
{
    try
    {
        fs::path path(argv);

        switch (detect_file_type(path))
        {
            case file_type_e::SNAPSHOT:
                dump_snapshot(path, mode);
                break;
            case file_type_e::JOURNAL:
                dump_journal(path, mode);
                break;
            default:
                fprintf(stderr, "%s: %s: unsupported file format\n", APP_NAME, argv);
                break;
        }

        return EXIT_SUCCESS;
    }
    catch (const std::exception &e)
    {
        fprintf(stderr, "%s: %s: %s\n", APP_NAME, argv, e.what());
        return EXIT_FAILURE;
    }
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
        {"version", no_argument, 0, 'V'},
        {"compact", no_argument, 0, 'c'},
        {0, 0, 0, 0}
    };

    while ((opt = getopt_long(argc, argv, "hVc", long_opts, nullptr)) != -1)
    {
        switch (opt)
        {
            case 'h':
                print_help(stdout);
                return EXIT_SUCCESS;
            case 'V':
                version();
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
        exit_code |= dump_file(argv[i], mode);

    return exit_code;
}
