#include <regex>
#include <limits>
#include <fstream>
#include <charconv>
#include <algorithm>
#include <cstring>
#include "cppcrc.h"
#include <fmt/std.h>
#include <fmt/core.h>
#include <spdlog/spdlog.h>
#include "journal.h"
#include "exception.hpp"
#include "messaging.hpp"
#include "utils.hpp"
#include "storage.hpp"

static constexpr std::size_t READ_BATCH_ENTRIES = 10'000;
static constexpr std::size_t READ_BATCH_BYTES   = 1 * 1024 * 1024;
static constexpr std::size_t READ_BATCH_FACTOR  = 2;

struct snapshot_header_t {
    uint8_t  magic[8];
    uint32_t schema2_hash;
    uint32_t checksum;
};

struct journal_meta_t {
    uint8_t  magic[8];
    uint32_t schema1_hash;
};

struct read_result_t {
    nplex::rev_t rev = 0;           // next revision to read (i.e. last read revision + 1)
    std::size_t count = 0;          // number of entries read
    std::size_t bytes = 0;          // total bytes read (entries data only)
    bool stopped = false;           // whether the callback requested to stop reading
};

struct read_buffer_t {
    std::array<ldb_entry_t, READ_BATCH_ENTRIES> entries{};
    std::vector<char> arena;
    read_buffer_t() : arena(READ_BATCH_BYTES, 0) {}
};

namespace fs = std::filesystem;

// ==========================================================
// Internal (static) variables and functions
// ==========================================================

static const std::regex snapshot_pattern(R"(.*/?snapshot-\d+\.dat)");
static const std::regex journal_pattern(R"(.*/?journal-\d+\.dat)");

template <typename T>
static const std::uint8_t * as_uint8_ptr(T *ptr) {
    return reinterpret_cast<const std::uint8_t *>(ptr);
}

static snapshot_header_t check_snapshot_header(std::ifstream &ifs)
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

    return hdr;
}

static void check_snapshot_checksum(const std::string_view &content, uint32_t expected)
{
    auto computed = CRC32::CRC32::calc(reinterpret_cast<const uint8_t*>(content.data()), content.size());

    if (computed != expected)
        throw nplex::nplex_exception("checksum mismatch");
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

static void check_snapshot_content(const std::string_view &content)
{
    auto verifier = flatbuffers::Verifier(as_uint8_ptr(content.data()), content.size());

    if (!verifier.VerifyBuffer<nplex::msgs::Snapshot>(nullptr))
        throw nplex::nplex_exception("invalid snapshot content");
}

/**
 * Parse the revision number from a snapshot or journal filename.
 * 
 * @param[in] filename A filename of the form "<dir>/<stem>-<rev><.ext>".
 * 
 * @return Filename revision number.
 */
static nplex::rev_t parse_rev(const fs::path &filename)
{
    // we take the filename stem, without directory or extension
    auto stem = filename.stem().string();
    std::string_view str(stem);

    auto pos = str.find_last_of('-');
    if (pos == std::string::npos)
        throw nplex::nplex_exception("invalid filename");

    str = str.substr(pos + 1);
    if (str.empty())
        throw nplex::nplex_exception("invalid filename");

    nplex::rev_t num = 0;
    auto [ptr, ec] = std::from_chars(str.begin(), str.end(), num);
    if (ec != std::errc())
        throw nplex::nplex_exception("invalid filename");

    return num;
}

static std::pair<nplex::rev_t, nplex::rev_t> get_journal_range(const nplex::journal_ptr &journal)
{
    int rc = 0;
    ldb_stats_t stats = {};

    if ((rc = journal->stats(0, UINT64_MAX, &stats)) != LDB_OK)
        throw nplex::nplex_exception("{}", ldb_strerror(rc));

    return {stats.min_seqnum, stats.max_seqnum};
}

static std::pair<nplex::rev_t, nplex::rev_t> get_archives_range(const std::map<nplex::rev_t, nplex::journal_item_t> &archives)
{
    nplex::rev_t min_rev = (archives.empty() ? 0 : archives.begin()->second.from_rev);
    nplex::rev_t max_rev = (archives.empty() ? 0 : archives.rbegin()->second.to_rev);

    return {min_rev, max_rev};
}

static std::pair<nplex::rev_t, nplex::rev_t> get_updates_range(const nplex::journal_ptr &journal, const std::map<nplex::rev_t, nplex::journal_item_t> &archives)
{
    auto [min_rev, max_rev] = get_journal_range(journal);
    auto [min_arx, max_arx] = get_archives_range(archives);

    // Case: no journal entries
    if (!min_rev) {
        min_rev = min_arx;
        max_rev = max_arx;
    }

    if (min_arx && min_arx < min_rev)
        min_rev = min_arx;

    return {min_rev, max_rev};
}

static std::pair<nplex::rev_t, nplex::rev_t> get_snapshots_range(const std::map<nplex::rev_t, nplex::snapshot_item_t> &snapshots)
{
    nplex::rev_t min_snp = (snapshots.empty() ? 0 : snapshots.begin()->first);
    nplex::rev_t max_snp = (snapshots.empty() ? 0 : snapshots.rbegin()->first);

    return {min_snp, max_snp};
}

/**
 * Read entries from a journal.
 *
 * @param[in] journal  Journal to read from.
 * @param[in] rev      Starting revision (included).
 * @param[in] num      Total entries to read.
 * @param[in,out] rbuf Read buffers (entries array + data buffer).
 * @param[in] func     Callback; returning false stops reading.
 *
 * @return Read result struct.
 */
static read_result_t read_journal(const nplex::journal_ptr &journal, nplex::rev_t rev, std::size_t num, read_buffer_t &rbuf, const std::function<bool(const nplex::msgs::Update *)> &func)
{
    std::size_t count = 0;
    std::size_t bytes = 0;

    while (count < num)
    {
        std::size_t num_reads = 0;
        std::size_t len = std::min(std::size_t{READ_BATCH_ENTRIES}, num - count);

        int rc = journal->read(rev, rbuf.entries.data(), len, rbuf.arena.data(), rbuf.arena.size(), &num_reads);

        if (rc != LDB_OK && rc != LDB_ERR_NOT_FOUND)
            throw nplex::nplex_exception("{}", ldb_strerror(rc));

        if (num_reads)
            SPDLOG_TRACE("Read journal entries, range = [r{}, r{}]", rev, rev + num_reads - 1);

        for (std::size_t i = 0; i < num_reads; i++)
        {
            auto verifier = flatbuffers::Verifier(as_uint8_ptr(rbuf.entries[i].data), rbuf.entries[i].data_len);

            if (!verifier.VerifyBuffer<nplex::msgs::Update>(nullptr))
                throw nplex::nplex_exception("Invalid journal entry (r{})", rev);

            auto update = flatbuffers::GetRoot<nplex::msgs::Update>(rbuf.entries[i].data);

            if (update->rev() != rev)
                throw nplex::nplex_exception("Unexpected revision in journal entry (r{}, expected r{})", update->rev(), rev);

            bytes += rbuf.entries[i].data_len;
            count++;
            rev++;

            if (!func(update))
                return {rev, count, bytes, true};
        }

        if (num_reads < len)
        {
            // case: reached end of this journal
            if (rbuf.entries[num_reads].seqnum == 0)
                return {rev, count, bytes, false};

            // case: buffer too short
            size_t new_size = rbuf.arena.size();

            while (new_size < 128 + rbuf.entries[num_reads].data_len)
                new_size *= READ_BATCH_FACTOR;

            if (rbuf.arena.size() < new_size)
                rbuf.arena.resize(new_size, 0);
        }
    }

    return {rev, count, bytes, false};
}

// ==========================================================
// nplex functions
// ==========================================================

std::string nplex::read_snapshot(const fs::path &file, bool check)
{
    std::string filename = file.filename().string();
    std::ifstream ifs(file, std::ios::binary);

    if (!ifs)
        throw nplex_exception("Failed to open snapshot file ({})", filename);

    auto hdr = ::check_snapshot_header(ifs);

    std::string content = ::get_snapshot_content(ifs);

    ::check_snapshot_checksum(content, hdr.checksum);

    if (check)
        ::check_snapshot_content(content);

    return content;
}

nplex::journal_ptr nplex::open_journal(const fs::path &file, int flags)
{
    auto dir  = file.parent_path();
    auto name = file.stem().string();

    // Open the journal
    auto journal = std::make_shared<ldb::journal_t>(dir, name, flags);

    // Get the journal range minimum revision (if any)
    bool is_empty = (get_journal_range(journal).first == 0);

    // Read the journal metadata
    int rc = 0;
    journal_meta_t meta = {
        .magic = {0},
        .schema1_hash = SCHEMA1_HASH
    };

    static_assert(LDB_METADATA_LEN >= sizeof(meta), "Metadata buffer too small");
    if ((rc = journal->get_meta(reinterpret_cast<char *>(&meta), sizeof(meta))) != LDB_OK)
        throw nplex_exception("Failed to read journal metadata ({})", ldb_strerror(rc));

    if (is_empty)
    {
        std::memcpy(meta.magic, JOURNAL_MAGIC, MAGIC_LEN);
        meta.schema1_hash = SCHEMA1_HASH;

        // Set/update the journal metadata
        if ((rc = journal->set_meta(reinterpret_cast<const char *>(&meta), sizeof(meta))) != LDB_OK)
            throw nplex_exception("Failed to write journal metadata ({})", ldb_strerror(rc));
    }

    // Checking that journal content is a valid nplex journal
    if (std::memcmp(meta.magic, JOURNAL_MAGIC, MAGIC_LEN) != 0)
        throw nplex_exception("Invalid journal (magic mismatch)");

    // Checking that journal content is compatible with current schema
    if (meta.schema1_hash != SCHEMA1_HASH)
        throw nplex_exception("Invalid journal (schema1 mismatch)");

    return journal;
}

// ==========================================================
// storage_t methods
// ==========================================================

nplex::storage_t::storage_t(const fs::path &path, int flags) : m_path{path}
{
    m_check = flags & LDB_OPEN_CHECK;
    m_journal = open_journal(path / JOURNAL_NAME ".dat", flags);

    rebuild_map_archives();
    rebuild_map_snapshots();
}

nplex::rev_t nplex::storage_t::get_min_rev()
{
    std::lock_guard<std::mutex> lock(m_mutex);

    auto [min_rev, max_rev] = get_updates_range(m_journal, m_archives);
    auto [min_snp, max_snp] = get_snapshots_range(m_snapshots);

    assert(!max_snp || max_snp <= max_rev);
    assert( min_snp || min_rev <= 1);

    return (min_rev <= 1 ? min_rev : min_snp);
}

nplex::rev_t nplex::storage_t::get_max_rev()
{
    std::lock_guard<std::mutex> lock(m_mutex);

    return get_updates_range(m_journal, m_archives).second;
}

/**
 * Get the filename of the snapshot with the greatest revision less-eq than rev.
 * 
 * @param[in] rev Revision.
 * 
 * @return Snapshot filename (empty if not found).
 */
std::string nplex::storage_t::get_snapshot_filename(rev_t rev) const
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
    catch (const std::exception &e)
    {
        // Someone has altered the data dir content
        SPDLOG_WARN("Failed to read snapshot {}: {}", filename, e.what());
    }

    SPDLOG_INFO("Rebuilding snapshot map ...");
    rebuild_map_snapshots();

    filename = get_snapshot_filename(rev);

    if (filename.empty())
        return {};

    try {
        auto content = nplex::read_snapshot(m_path / filename);
        SPDLOG_DEBUG("Read snapshot {}", filename);
        return content;
    }
    catch (const std::exception &e)
    {
        throw nplex_exception("Failed to read snapshot {}: {}", filename, e.what());
    }
}

nplex::rev_t nplex::storage_t::write_snapshot(const std::string_view &data)
{
    ::check_snapshot_content(data);

    auto snapshot = flatbuffers::GetRoot<msgs::Snapshot>(data.data());
    rev_t rev = snapshot->rev();

    std::string filename = fmt::format(SNAPSHOT_FILENAME, rev);
    std::ofstream ofs(m_path / filename, std::ios::binary | std::ios::trunc);

    if (!ofs)
        throw nplex_exception("Failed to open snapshot file ({})", filename);

    snapshot_header_t hdr = {
        .magic = {0},
        .schema2_hash = SCHEMA2_HASH,
        .checksum = CRC32::CRC32::calc(reinterpret_cast<const uint8_t*>(data.data()), data.size())
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

nplex::journal_ptr nplex::storage_t::get_archive(rev_t rev)
{
    std::string filename;
    rev_t key = 0;

    {
        std::lock_guard<std::mutex> lock(m_mutex);

        auto it = m_archives.lower_bound(rev);

        if (it == m_archives.end())
            return m_journal;

        key = it->first;

        if (it->second.journal)
            return it->second.journal;

        filename = it->second.filename;
    }

    auto ptr = open_journal(m_path / filename, LDB_OPEN_READONLY);

    {
        std::lock_guard<std::mutex> lock(m_mutex);

        auto it = m_archives.find(key);

        if (it == m_archives.end())
            return ptr;

        it->second.journal = ptr;

        return it->second.journal;
    }
}

std::size_t nplex::storage_t::read_entries(rev_t rev, const std::function<bool(const msgs::Update *)> &func, std::size_t num)
{
    if (rev == 0 || num == 0 || !func)
        return 0;

    auto start_time = uv_hrtime();
    std::size_t count = 0;
    std::size_t bytes = 0;
    read_buffer_t rbuf;

    while (count < num)
    {
        journal_ptr journal = get_archive(rev);

        auto res = read_journal(journal, rev, num - count, rbuf, func);

        rev   = res.rev;
        count += res.count;
        bytes += res.bytes;

        if (res.stopped || res.count == 0)
            break;
    }

    SPDLOG_TRACE("Journal read completed, num_entries={}, total_bytes={}, elapsed={}μs",
        count, bytes_to_string(bytes), (uv_hrtime() - start_time) / 1000);

    return count;
}

nplex::store_t nplex::storage_t::get_store(rev_t rev, const user_ptr &user)
{
    store_t repo;

    // step1: retrieve nearest snapshot (less-eq than rev)
    std::string str = read_snapshot(rev);
    if (!str.empty())
    {
        auto snapshot = flatbuffers::GetRoot<msgs::Snapshot>(str.c_str());
        repo.load(snapshot, user);
        str = std::string{};
    }

    if (repo.rev() == rev)
        return repo;

    // step2: define a function to update the repo with the journal updates
    auto func = [&repo, rev, user](const msgs::Update *update) -> bool {
        repo.update(update, user);
        return (repo.rev() < rev);
    };

    // step3: apply updates until m_rev
    read_entries(repo.rev() + 1, func);

    if (repo.rev() != rev)
        throw nplex_exception("Failed to load store at revision {}", rev);

    return repo;
}

/**
 * @pre m_journal is open and valid
 */
void nplex::storage_t::rebuild_map_archives()
{
    rev_t rev = 0;
    bool has_errors = false;
    decltype(m_archives) archives;

    for (auto const &dir_entry : fs::directory_iterator(m_path))
    {
        if (!std::regex_match(dir_entry.path().c_str(), journal_pattern))
            continue;

        auto filename = dir_entry.path().filename();
        rev_t min_rev = 0;
        rev_t max_rev = 0;

        try
        {
            rev = parse_rev(filename);

            auto journal = open_journal(dir_entry.path(), (m_check ? LDB_OPEN_CHECK : 0));

            std::tie(min_rev, max_rev) = get_journal_range(journal);

            if (rev != max_rev)
                throw nplex_exception("revision mismatch");

            if (archives.contains(rev))
                throw nplex_exception("duplicate journal");
        }
        catch (const std::exception &e)
        {
            SPDLOG_WARN("Skipping file {}: {}", filename, e.what());
            has_errors = true;
            continue;
        }

        archives[max_rev] = journal_item_t{
            .from_rev = min_rev,
            .to_rev = max_rev,
            .filename = filename,
            .journal = nullptr,
            .checked = m_check
        };

        SPDLOG_DEBUG("Found journal {}, revs = [r{}, r{}]", filename, min_rev, max_rev);
    }

    rev = 0;

    // Checks for gaps and overlaps in journal revisions
    for (const auto &[num, item] : archives)
    {
        if (rev && item.from_rev != rev + 1) {
             SPDLOG_WARN("Found gap between archives, range = [r{}, r{}]", rev + 1, item.from_rev - 1);
             has_errors = true;
        }

        rev = item.to_rev;
    }

    if (!archives.empty())
    {
        rev_t min_rev = get_journal_range(m_journal).first;
        const auto &last_item = archives.rbegin()->second;

        if (min_rev && last_item.to_rev + 1 != min_rev) {
            SPDLOG_WARN("Found gap between archives and journal, range = [r{}, r{}]", last_item.to_rev + 1, min_rev - 1);
            has_errors = true;
        }
    }

    if (has_errors)
        throw nplex_exception("Failed to rebuild journals map");

    std::lock_guard<std::mutex> lock(m_mutex);
    m_archives = std::move(archives);
}

/**
 * @pre m_journal is open and valid
 * @pre m_archives is up-to-date (call rebuild_map_archives before if needed)
 */
void nplex::storage_t::rebuild_map_snapshots()
{
    decltype(m_snapshots) snapshots;
    rev_t min_rev = 0;
    rev_t max_rev = 0;

    {
        std::lock_guard<std::mutex> lock(m_mutex);
        std::tie(min_rev, max_rev) = get_updates_range(m_journal, m_archives);
    }

    for (auto const &dir_entry : fs::directory_iterator(m_path))
    {
        if (!std::regex_match(dir_entry.path().c_str(), snapshot_pattern))
            continue;

        auto filename = dir_entry.path().filename();
        rev_t rev = 0;

        try
        {
            rev = parse_rev(filename);

            if (rev + 1 < min_rev || max_rev < rev)
                throw nplex_exception("revision out-of-range");

            std::ifstream ifs(dir_entry.path(), std::ios::binary);

            if (!ifs)
                throw nplex_exception("failed to open file");

            auto hdr = ::check_snapshot_header(ifs);

            if (snapshots.contains(rev))
                throw nplex_exception("duplicate snapshot");

            if (m_check)
            {
                auto content = ::get_snapshot_content(ifs);
                ::check_snapshot_checksum(content, hdr.checksum);
                ::check_snapshot_content(content);

                auto *snapshot = flatbuffers::GetRoot<msgs::Snapshot>(content.c_str());

                if (snapshot->rev() != rev)
                    throw nplex_exception("revision mismatch");
            }
        }
        catch (const std::exception &e)
        {
            SPDLOG_WARN("Skipping file {}: {}", filename, e.what());
            continue;
        }

        snapshots[rev] = snapshot_item_t{
            .rev = rev,
            .filename = filename.string(),
            .checked = m_check
        };

        SPDLOG_DEBUG("Found snapshot {}, rev = r{}", filename, rev);
    }

    std::lock_guard<std::mutex> lock(m_mutex);
    m_snapshots = std::move(snapshots);
}
