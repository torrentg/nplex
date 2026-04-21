#include <regex>
#include <limits>
#include <fstream>
#include <charconv>
#include <algorithm>
#include <cstring>
#include <spdlog/spdlog.h>
#include "journal.h"
#include "exception.hpp"
#include "messaging.hpp"
#include "utils.hpp"
#include "storage.hpp"

#define READ_BATCH_ENTRIES  10000
#define READ_BATCH_BYTES    (1 * 1024 * 1024)
#define READ_BATCH_FACTOR   2

struct snapshot_header_t {
    uint8_t  magic[8];
    uint32_t schema2_hash;
};

namespace fs = std::filesystem;

// ==========================================================
// Internal (static) variables and functions
// ==========================================================

static const std::regex snapshot_pattern(R"(.*/snapshot-\d+\.dat)");
static constexpr uint8_t SNAPSHOT_MAGIC[8] = {'N','P','L','E','X','S','N','P'};

template <typename T>
static const std::uint8_t * as_uint8_ptr(T *ptr) {
    return reinterpret_cast<const std::uint8_t *>(ptr);
}

static void check_snapshot_header(std::ifstream &ifs)
{
    snapshot_header_t hdr = {};

    if (!ifs.read(reinterpret_cast<char*>(&hdr), sizeof(hdr)))
        throw nplex::nplex_exception("snapshot unknown format");

    if (std::memcmp(hdr.magic, SNAPSHOT_MAGIC, sizeof(SNAPSHOT_MAGIC)) != 0)
        throw nplex::nplex_exception("snapshot unknown format");

    if (hdr.schema2_hash != SCHEMA2_HASH)
        throw nplex::nplex_exception("snapshot schema mismatch, expected=0x{:08x}, found=0x{:08x}", 
            SCHEMA2_HASH, hdr.schema2_hash);
}

static void check_snapshots(const std::filesystem::path &m_path)
{
    for (auto const &dir_entry : std::filesystem::directory_iterator(m_path))
    {
        if (!std::regex_match(dir_entry.path().c_str(), snapshot_pattern))
            continue;

        std::string filename = dir_entry.path().filename().string();

        std::ifstream ifs(dir_entry.path(), std::ios::binary);
        if (!ifs)
            throw nplex::nplex_exception(fmt::format("Failed to open snapshot file ({})", filename));

        try {
            ::check_snapshot_header(ifs);
        }
        catch (const std::exception &e) {
            throw nplex::nplex_exception(fmt::format("Invalid snapshot file ({}): {}", filename, e.what()));
        }
    }
}

static nplex::rev_t parse_rev(const char* filename)
{
    std::string_view str(filename);

    assert(str.ends_with(".dat"));
    str.remove_suffix(std::string(".dat").length());

    auto pos = str.find_last_of('-');
    assert(pos != std::string::npos);

    str = str.substr(pos + 1);
    assert(!str.empty());

    nplex::rev_t num = 0;
    auto [ptr, ec] = std::from_chars(str.begin(), str.end(), num);
    if (ec != std::errc())
        throw nplex::nplex_exception(fmt::format("Invalid snapshot filename ({})", filename));

    return num;
}

// Returns the latest snapshot revision available that is less-eq/great-eq than rev
static nplex::rev_t get_snapshot_rev(const fs::path &path, nplex::rev_t rev, bool le = true)
{
    nplex::rev_t rev_available = 0;

    for (auto const &dir_entry : std::filesystem::directory_iterator(path)) 
    {
        if (!std::regex_match(dir_entry.path().c_str(), snapshot_pattern))
            continue;

        nplex::rev_t rnum = parse_rev(dir_entry.path().c_str());

        if (le)
        {
            if (rnum <= rev && rnum > rev_available)
                rev_available = rnum;
        }
        else
        {
            if (rev <= rnum && (rev_available == 0 || rnum < rev_available))
                rev_available = rnum;
        }
    }

    return rev_available;
}

// ==========================================================
// storage_t methods
// ==========================================================

std::pair<nplex::rev_t, nplex::rev_t> nplex::storage_t::get_revs_range()
{
    int rc = 0;
    ldb_stats_t stats = {};

    if ((rc = m_journal.stats(0, UINT64_MAX, &stats)) != LDB_OK)
        throw nplex::nplex_exception(fmt::format("Failed to get journal stats ({})", ldb_strerror(rc)));

    rev_t min_rev = stats.min_seqnum;
    rev_t max_rev = stats.max_seqnum;
    SPDLOG_DEBUG("Journal range: [r{}, r{}]", min_rev, max_rev);

    rev_t min_snp = ::get_snapshot_rev(m_path, 0, false);
    rev_t max_snp = ::get_snapshot_rev(m_path, std::numeric_limits<rev_t>::max(), true);
    SPDLOG_DEBUG("Snapshot range: [r{}, r{}]", min_snp, max_snp);

    if (max_snp && max_snp > max_rev)
        throw nplex::nplex_exception(fmt::format("Found snapshot revision greater than max journal revision (r{} > r{})", max_snp, max_rev));

    ::check_snapshots(m_path);

    // get version of first snapshot greater-eq than min_rev
    rev_t rev0 = ::get_snapshot_rev(m_path, min_rev, false);

    if (rev0 == 0 && min_rev > 1)
        throw nplex_exception("No snapshot available in range [r{}, r{}]", min_rev, max_rev);

    if (min_rev > 1 && min_rev <= rev0)
        SPDLOG_INFO("Found unused journal entries, range = [{}-{}]", min_rev, rev0);

    rev_t rev_k = (min_rev == 1 ? 1 : rev0);

    return {rev_k, max_rev};
}

std::string nplex::read_snapshot(const fs::path &file)
{
    std::string filename = file.filename().string();
    std::ifstream ifs(file, std::ios::binary | std::ios::ate);

    if (!ifs)
        throw nplex::nplex_exception(fmt::format("Failed to open snapshot file ({})", filename));

    auto file_size = ifs.tellg();
    ifs.seekg(0, std::ios::beg);

    ::check_snapshot_header(ifs);

    std::size_t data_size = static_cast<std::size_t>(file_size) - sizeof(snapshot_header_t);
    std::string content(data_size, '\0');

    if (!ifs.read(&content[0], static_cast<std::streamsize>(data_size)))
        throw nplex_exception(fmt::format("Failed to read snapshot file ({})", filename));

    auto verifier = flatbuffers::Verifier(as_uint8_ptr(content.c_str()), content.length());

    if (!verifier.VerifyBuffer<nplex::msgs::Snapshot>(nullptr))
        throw nplex_exception(fmt::format("Invalid snapshot file ({})", filename));

    return content;
}

std::string nplex::storage_t::read_snapshot(rev_t rev)
{
    rev_t rev_available = ::get_snapshot_rev(m_path, rev);

    if (rev_available == 0)
        return {};

    return nplex::read_snapshot(m_path / fmt::format(SNAPSHOT_FILENAME, rev_available));
}

nplex::rev_t nplex::storage_t::write_snapshot(const std::string_view &data) const
{
    auto verifier = flatbuffers::Verifier(as_uint8_ptr(data.data()), data.length());

    if (!verifier.VerifyBuffer<nplex::msgs::Snapshot>(nullptr))
        throw nplex_exception("Invalid snapshot data");

    auto snapshot = flatbuffers::GetRoot<nplex::msgs::Snapshot>(data.data());
    rev_t rev = snapshot->rev();

    std::string filename = fmt::format(SNAPSHOT_FILENAME, rev);
    std::ofstream ofs(m_path / filename, std::ios::binary | std::ios::trunc);

    if (!ofs)
        throw nplex::nplex_exception(fmt::format("Failed to open snapshot file ({})", filename));

    snapshot_header_t hdr = {
        .magic = {0}, 
        .schema2_hash = SCHEMA2_HASH
    };

    std::memcpy(hdr.magic, SNAPSHOT_MAGIC, sizeof(SNAPSHOT_MAGIC));

    ofs.write(reinterpret_cast<const char*>(&hdr), sizeof(hdr));
    if (!ofs)
        throw nplex::nplex_exception(fmt::format("Failed to write to snapshot file ({})", filename));

    ofs.write(data.data(), static_cast<std::streamsize>(data.size()));
    if (!ofs)
        throw nplex::nplex_exception(fmt::format("Failed to write to snapshot file ({})", filename));

    return rev;
}

std::size_t nplex::storage_t::read_entries(rev_t rev, const std::function<bool(const msgs::Update *)> &func, std::size_t num)
{
    if (rev == 0 || num == 0)
        return 0;

    int rc = 0;
    std::size_t count = 0;
    std::size_t bytes = 0;
    auto start_time = uv_hrtime();
    ldb_entry_t entries[READ_BATCH_ENTRIES] = {};
    ldb_stats_t stats = {};
    std::vector<char> buf;

    if ((rc = m_journal.stats(rev, rev + std::min(num, std::size_t{READ_BATCH_ENTRIES}) - 1, &stats)) != LDB_OK)
        throw nplex_exception("{}", ldb_strerror(rc));

    buf.resize(std::clamp(stats.data_size, std::size_t{256}, std::size_t{READ_BATCH_BYTES}), 0);

    while (count < num)
    {
        std::size_t num_reads = 0;
        std::size_t len = std::min(std::size_t{READ_BATCH_ENTRIES}, num - count);

        rc = m_journal.read(rev, entries, len, buf.data(), buf.size(), &num_reads);

        SPDLOG_TRACE("Read journal entries, range = [r{}, r{}], rc = {}", 
            rev, rev + num_reads - (num_reads ? 1 : 0), ldb_strerror(rc));

        if (rc != LDB_OK && rc != LDB_ERR_NOT_FOUND)
            throw nplex_exception("{}", ldb_strerror(rc));

        for (std::size_t i = 0; i < num_reads; i++)
        {
            auto verifier = flatbuffers::Verifier(as_uint8_ptr(entries[i].data), entries[i].data_len);

            if (!verifier.VerifyBuffer<nplex::msgs::Update>(nullptr))
                throw nplex_exception("Invalid journal entry (r{})", rev);

            auto update = flatbuffers::GetRoot<nplex::msgs::Update>(entries[i].data);

            assert(update->rev() == rev);

            bytes += entries[i].data_len;
            count++;
            rev++;

            if (!func(update))
                goto READ_END;
        }

        if (num_reads < len)
        {
            // case: reached end of journal
            if (entries[num_reads].seqnum == 0)
                break;

            // case: buffer too short
            size_t new_size = buf.size();

            while (new_size < 128 + entries[num_reads].data_len)
                new_size *= READ_BATCH_FACTOR;

            if (buf.size() < new_size)
                buf.resize(new_size, 0);
        }
    }

READ_END:

    uint64_t duration_us = (uv_hrtime() - start_time) / 1000;

    SPDLOG_TRACE("Journal read completed, revs={}, bytes={}, elapsed={}μs", 
        count, bytes_to_string(bytes), duration_us);

    return count;
}

nplex::store_t nplex::storage_t::get_store(rev_t rev, const user_ptr &user)
{
    store_t repo;

    // step1: retrieve nearest snapshot (less-eq than rev)
    std::string str = read_snapshot(rev);
    if (!str.empty())
    {
        auto snapshot = flatbuffers::GetRoot<nplex::msgs::Snapshot>(str.c_str());
        repo.load(snapshot, user);
        str = std::string{};
    }

    if (repo.rev() == rev)
        return repo;

    // step2: define a function to update the repo with the journal updates
    auto func = [&repo, rev, user](const nplex::msgs::Update *update) -> bool {
        repo.update(update, user);
        return (repo.rev() < rev);
    };

    // step3: apply updates until m_rev
    read_entries(repo.rev() + 1, func);

    if (repo.rev() != rev)
        throw nplex_exception("Failed to load store at revision {}", rev);

    return repo;
}
