#include <regex>
#include <limits>
#include <fstream>
#include <charconv>
#include <algorithm>
#include <cstring>
#include <fmt/core.h>
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

struct journal_meta_t {
    uint8_t  magic[8];
    uint32_t schema1_hash;
};

namespace fs = std::filesystem;

// ==========================================================
// Internal (static) variables and functions
// ==========================================================

static const std::regex snapshot_pattern(R"(.*/?snapshot-\d+\.dat)");

template <typename T>
static const std::uint8_t * as_uint8_ptr(T *ptr) {
    return reinterpret_cast<const std::uint8_t *>(ptr);
}

static void check_snapshot_header(std::ifstream &ifs)
{
    snapshot_header_t hdr = {};

    ifs.seekg(0, std::ios::beg);
    if (!ifs)
        throw nplex::nplex_exception("invalid file");

    if (!ifs.read(reinterpret_cast<char*>(&hdr), sizeof(hdr)))
        throw nplex::nplex_exception("unable to read header");

    if (std::memcmp(hdr.magic, SNAPSHOT_MAGIC, MAGIC_LEN) != 0)
        throw nplex::nplex_exception("invalid file signature");

    if (hdr.schema2_hash != SCHEMA2_HASH)
        throw nplex::nplex_exception("schema2 mismatch");
}

static std::string get_snapshot_content(std::ifstream &ifs)
{
    ifs.seekg(0, std::ios::end);
    if (!ifs)
        throw nplex::nplex_exception("invalid file");

    auto file_size = ifs.tellg();

    if (!ifs || file_size < static_cast<std::streamoff>(sizeof(snapshot_header_t)))
        throw nplex::nplex_exception("invalid snapshot file");

    ifs.seekg(sizeof(snapshot_header_t), std::ios::beg);
    if (!ifs)
        throw nplex::nplex_exception("invalid file");

    std::size_t data_size = static_cast<std::size_t>(file_size) - sizeof(snapshot_header_t);
    std::string content(data_size, '\0');

    if (!ifs.read(&content[0], static_cast<std::streamsize>(data_size)))
        throw nplex::nplex_exception("failed to read content");

    return content;
}

static void check_snapshot_content(const std::string &content)
{
    auto verifier = flatbuffers::Verifier(as_uint8_ptr(content.c_str()), content.length());

    if (!verifier.VerifyBuffer<nplex::msgs::Snapshot>(nullptr))
        throw nplex::nplex_exception("invalid snapshot content");
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
        throw nplex::nplex_exception("Invalid snapshot filename ({})", filename);

    return num;
}

// ==========================================================
// nplex functions
// ==========================================================

std::string nplex::read_snapshot(const fs::path &file, bool check)
{
    std::string filename = file.filename().string();
    std::ifstream ifs(file, std::ios::binary | std::ios::ate);

    if (!ifs)
        throw nplex_exception("Failed to open snapshot file ({})", filename);

    ::check_snapshot_header(ifs);

    std::string content = ::get_snapshot_content(ifs);

    if (check)
        ::check_snapshot_content(content);

    return content;
}

ldb::journal_t nplex::open_journal(const std::filesystem::path &file, int flags)
{
    int rc = 0;
    ldb_stats_t stats = {};
    journal_meta_t meta = {
        .magic = {0},
        .schema1_hash = SCHEMA1_HASH
    };

    auto dir  = file.parent_path();
    auto name = file.stem().string();

    ldb::journal_t journal(dir, name, flags);

    if ((rc = journal.stats(0, UINT64_MAX, &stats)) != LDB_OK)
        throw nplex_exception("Failed to get journal stats ({})", ldb_strerror(rc));

    if (stats.min_seqnum == 0)
    {
        std::memcpy(meta.magic, JOURNAL_MAGIC, MAGIC_LEN);

        if ((rc = journal.set_meta(reinterpret_cast<const char *>(&meta), sizeof(meta))) != LDB_OK)
            throw nplex_exception("Failed to set journal metadata ({})", ldb_strerror(rc));
    }
    else
    {
        char buf[LDB_METADATA_LEN] = {};

        static_assert(LDB_METADATA_LEN >= sizeof(meta), "Metadata buffer too small");

        if ((rc = journal.get_meta(buf, sizeof(buf))) != LDB_OK)
            throw nplex_exception("Failed to get journal metadata ({})", ldb_strerror(rc));

        std::memcpy(&meta, buf, sizeof(meta));

        if (std::memcmp(meta.magic, JOURNAL_MAGIC, MAGIC_LEN) != 0)
            throw nplex_exception("Journal metadata has unknown format (missing magic)");

        if (meta.schema1_hash != SCHEMA1_HASH)
            throw nplex_exception("Journal schema mismatch: expected 0x{:08x}, found 0x{:08x}",
                SCHEMA1_HASH, meta.schema1_hash);
    }

    return journal;
}

// ==========================================================
// storage_t methods
// ==========================================================

nplex::storage_t::storage_t(const std::filesystem::path &path, int flags) : m_path{path}
{
    m_check = flags & LDB_OPEN_CHECK;
    m_journal = open_journal(path / JOURNAL_NAME ".dat", flags);
    rebuild_map_snapshots();
    rebuild_min_rev();
}

nplex::rev_t nplex::storage_t::get_max_rev()
{
    int rc = 0;
    ldb_stats_t stats = {};

    if ((rc = m_journal.stats(0, UINT64_MAX, &stats)) != LDB_OK)
        throw nplex_exception("{}", ldb_strerror(rc));

    return stats.max_seqnum;
}

/**
 * Get the filename of the snapshot with the greatest revision less-eq than rev.
 * 
 * @param[in] rev Revision.
 * 
 * @return Snapshot filename (empty if not found).
 */
std::string nplex::storage_t::get_snapshot_filename(rev_t rev)
{
    std::lock_guard<std::mutex> lock(m_mutex);

    if (m_snapshots.empty())
        return {};

    auto it = m_snapshots.upper_bound(rev);

    if (it == m_snapshots.begin())
        return {};

    return (--it)->second.filename;
}

std::string nplex::storage_t::read_snapshot(rev_t rev)
{
    std::string filename = get_snapshot_filename(rev);

    if (filename.empty())
        return {};

    try
    {
        auto content = nplex::read_snapshot(m_path / filename);
        SPDLOG_DEBUG("Read snapshot {}", filename);
        return content;
    }
    catch(const std::exception &e)
    {
        // Someone has altered the data dir content
        SPDLOG_WARN("Failed to read snapshot {}: {}", filename, e.what());
    }

    rebuild_map_snapshots();

    filename = get_snapshot_filename(rev);

    if (filename.empty())
        return {};

    return nplex::read_snapshot(m_path / filename);
}

nplex::rev_t nplex::storage_t::write_snapshot(const std::string_view &data)
{
    auto verifier = flatbuffers::Verifier(as_uint8_ptr(data.data()), data.length());

    if (!verifier.VerifyBuffer<nplex::msgs::Snapshot>(nullptr))
        throw nplex_exception("Invalid snapshot data");

    auto snapshot = flatbuffers::GetRoot<nplex::msgs::Snapshot>(data.data());
    rev_t rev = snapshot->rev();

    std::string filename = fmt::format(SNAPSHOT_FILENAME, rev);
    std::ofstream ofs(m_path / filename, std::ios::binary | std::ios::trunc);

    if (!ofs)
        throw nplex_exception("Failed to open snapshot file ({})", filename);

    snapshot_header_t hdr = {
        .magic = {0}, 
        .schema2_hash = SCHEMA2_HASH
    };

    std::memcpy(hdr.magic, SNAPSHOT_MAGIC, MAGIC_LEN);

    ofs.write(reinterpret_cast<const char*>(&hdr), sizeof(hdr));
    if (!ofs)
        throw nplex_exception("Failed to write to snapshot file ({})", filename);

    ofs.write(data.data(), static_cast<std::streamsize>(data.size()));
    if (!ofs)
        throw nplex_exception("Failed to write to snapshot file ({})", filename);

    ofs.close();

    std::lock_guard<std::mutex> lock(m_mutex);

    m_snapshots[rev] = snapshot_item_t{
        .rev = rev,
        .filename = filename,
        .checked = true
    };

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
    std::vector<char> buf;

    buf.resize(READ_BATCH_BYTES);

    while (count < num)
    {
        std::size_t num_reads = 0;
        std::size_t len = std::min(std::size_t{READ_BATCH_ENTRIES}, num - count);

        rc = m_journal.read(rev, entries, len, buf.data(), buf.size(), &num_reads);

        if (rc != LDB_OK && rc != LDB_ERR_NOT_FOUND)
            throw nplex_exception("{}", ldb_strerror(rc));

        SPDLOG_TRACE("Read journal entries, range = [r{}, r{}]", rev, rev + num_reads - (num_reads ? 1 : 0));

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

    SPDLOG_TRACE("Journal read completed, num_entries={}, total_bytes={}, elapsed={}μs", 
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

void nplex::storage_t::rebuild_map_snapshots()
{
    decltype( m_snapshots ) snapshots;

    for (auto const &dir_entry : std::filesystem::directory_iterator(m_path))
    {
        if (!std::regex_match(dir_entry.path().c_str(), snapshot_pattern))
            continue;

        std::string filename = dir_entry.path().filename().string();
        nplex::rev_t rnum = 0;

        try {
            rnum = parse_rev(filename.c_str());
        }
        catch (const std::exception &e) {
            SPDLOG_WARN("Invalid snapshot filename ({}), skipping", filename);
            continue;
        }

        if (snapshots.contains(rnum))
        {
            SPDLOG_WARN("Duplicate snapshot revision ({}), skipping", filename);
            continue;
        }

        try
        {
            std::ifstream ifs(dir_entry.path(), std::ios::binary);

            if (!ifs)
                throw nplex_exception("failed to open file");

            ::check_snapshot_header(ifs);

            if (m_check)
            {
                auto content = ::get_snapshot_content(ifs);
                ::check_snapshot_content(content);

                auto *snapshot = flatbuffers::GetRoot<msgs::Snapshot>(content.c_str());

                if (snapshot->rev() != rnum)
                    throw nplex_exception("revision mismatch (filename: r{}, content: r{})", rnum, snapshot->rev());
            }
        }
        catch (const std::exception &e) {
            SPDLOG_WARN("Skipping file {}: {}", filename, e.what());
            continue;
        }

        snapshots[rnum] = snapshot_item_t{
            .rev = rnum,
            .filename = filename,
            .checked = m_check
        };

        SPDLOG_DEBUG("Found snapshot {}, rev = r{}", filename, rnum);
    }

    std::lock_guard<std::mutex> lock(m_mutex);
    m_snapshots = std::move(snapshots);
}

/**
 * @pre m_journal is open and valid
 * @pre m_snapshots is up-to-date (call rebuild_map_snapshots before if needed)
 */
void nplex::storage_t::rebuild_min_rev()
{
    std::lock_guard<std::mutex> lock(m_mutex);
    ldb_stats_t stats = {};
    int rc = 0;

    if ((rc = m_journal.stats(0, UINT64_MAX, &stats)) != LDB_OK)
        throw nplex_exception("Failed to get journal stats ({})", ldb_strerror(rc));

    rev_t min_rev = stats.min_seqnum;
    rev_t max_rev = stats.max_seqnum;
    SPDLOG_DEBUG("Journal range: [r{}, r{}]", min_rev, max_rev);

    rev_t min_snp = (m_snapshots.empty() ? 0 : m_snapshots.begin()->first);
    rev_t max_snp = (m_snapshots.empty() ? 0 : m_snapshots.rbegin()->first);
    SPDLOG_DEBUG("Snapshot range: [r{}, r{}]", min_snp, max_snp);

    if (max_snp && max_snp > max_rev)
        throw nplex_exception("Found snapshot revision greater than max journal revision (r{} > r{})", max_snp, max_rev);

    // get version of first snapshot greater-eq than min_rev
    auto it = m_snapshots.lower_bound(min_rev);
    rev_t rev0 = (it == m_snapshots.end() ? 0 : it->first);

    if (rev0 == 0 && min_rev > 1)
        throw nplex_exception("No snapshot available in range [r{}, r{}]", min_rev, max_rev);

    if (min_rev > 1 && min_rev <= rev0)
        SPDLOG_INFO("Found unused journal entries, range = [{}-{}]", min_rev, rev0);

    m_min_rev = (min_rev == 1 ? 1 : rev0);
}
