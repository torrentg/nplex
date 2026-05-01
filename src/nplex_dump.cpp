#include <getopt.h>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <vector>
#include <fmt/core.h>
#include "journal.h"
#include "common.hpp"
#include "exception.hpp"
#include "storage.hpp"
#include "json.hpp"

static constexpr char        APP_NAME[]    = "nplex_dump";
static constexpr std::size_t BATCH_SIZE    = 10'000;
static constexpr std::size_t BATCH_BYTES   = 1 * 1024 * 1024;
static constexpr std::size_t BATCH_FACTOR  = 2;

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

    if (*reinterpret_cast<uint64_t *>(header) == LDB_DAT_MAGIC_NUMBER)
        return file_type_e::JOURNAL;

    return file_type_e::UNKNOWN;
}

// ==========================================================
// Dump functions
// ==========================================================

static void dump_snapshot(const fs::path &path)
{
    auto content = nplex::read_snapshot(path);
    auto json = nplex::snapshot_to_json(content.data(), content.size());
    printf("%s\n", json.c_str());
}

static void dump_journal(const fs::path &path)
{
    int rc = 0;
    ldb_stats_t stats = {};
    auto journal = nplex::open_journal(path, LDB_OPEN_READONLY);

    if ((rc = journal->stats(0, UINT64_MAX, &stats)) != LDB_OK)
        throw nplex::nplex_exception("Failed to get journal stats ({})", ldb_strerror(rc));
    
    if (stats.min_seqnum == 0)
        return;

    std::vector<char> buf(BATCH_BYTES, 0);
    ldb_entry_t entries[BATCH_SIZE] = {};
    uint64_t seq = stats.min_seqnum;
    uint64_t to_seq = stats.max_seqnum;

    while (seq <= to_seq)
    {
        size_t want = std::min(BATCH_SIZE, static_cast<size_t>(to_seq - seq + 1ULL));
        size_t num = 0;

        rc = journal->read(seq, entries, want, buf.data(), buf.size(), &num);
        
        if (rc != LDB_OK && rc != LDB_ERR_NOT_FOUND)
            throw nplex::nplex_exception("error reading journal: {}", ldb_strerror(rc));

        for (size_t i = 0; i < num; ++i) {
            auto json = nplex::update_to_json(static_cast<const char *>(entries[i].data), entries[i].data_len);
            printf("%s\n", json.c_str());
        }

        // buffer exhausted: entries[num] contains next entry but data == NULL
        if (num < want)
        {
            // case: reached end of this journal
            if (entries[num].seqnum == 0)
                return;

            // case: buffer too short
            size_t need = static_cast<size_t>(entries[num].data_len) + 128;
            size_t new_size = buf.size();

            while (new_size < need)
                new_size *= BATCH_FACTOR;

            if (new_size > buf.size())
                buf.resize(new_size);
        }

        if (num > 0)
            seq = entries[num - 1].seqnum + 1;
    }
}

// ==========================================================
// CLI
// ==========================================================

static void version()
{
    fprintf(stdout,
        APP_NAME " " PROJECT_VERSION "\n"
        "schemas = [0x%08x, 0x%08x]\n"
        "Copyright (c) 2026 Gerard Torrent.\n"
        "License MIT: MIT License <https://opensource.org/licenses/MIT>.\n",
        SCHEMA1_HASH, SCHEMA2_HASH);
}

static void print_help(FILE *out)
{
    fprintf(out,
        "%s - export nplex snapshot or journal files as JSON.\n"
        "\n"
        "Usage:\n"
        "  %s [-h] [-V] file1 [file2 ...]\n"
        "\n"
        "Options:\n"
        "  -h, --help       Show this help message and exit.\n"
        "  -V, --version    Show version info and exit.\n"
        "\n"
        "Output format:\n"
        "  NDJSON (one JSON object per line).\n",
        APP_NAME, APP_NAME
    );
}

static int dump_file(const char *argv)
{
    try
    {
        fs::path path(argv);

        switch (detect_file_type(path))
        {
            case file_type_e::SNAPSHOT:
                dump_snapshot(path);
                break;
            case file_type_e::JOURNAL:
                dump_journal(path);
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

    int opt = 0;

    static struct option long_opts[] = {
        {"help",    no_argument, 0, 'h'},
        {"version", no_argument, 0, 'V'},
        {0, 0, 0, 0}
    };

    while ((opt = getopt_long(argc, argv, "hV", long_opts, nullptr)) != -1)
    {
        switch (opt)
        {
            case 'h':
                print_help(stdout);
                return EXIT_SUCCESS;
            case 'V':
                version();
                return EXIT_SUCCESS;
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
        exit_code |= dump_file(argv[i]);

    return exit_code;
}
