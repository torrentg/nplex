#include "journal_updates_persister.hpp"
#include "exception.hpp"
#include "logger.hpp"
#include "journal.h"
#include <algorithm>
#include <vector>
#include <set>

namespace fs = std::filesystem;

// ==========================================================
// Internal (static) functions
// ==========================================================

namespace {

constexpr std::string_view CURRENT_SEGMENT = "journal";
constexpr std::string_view FILENAME_PREFIX = "journal-";
constexpr std::string_view FILENAME_EXTENSION = ".dat";
constexpr std::string_view MAGIC_BYTES = "NPLEXLOG";
constexpr std::size_t READ_BATCH_ENTRIES = 1024;
constexpr std::size_t READ_BATCH_BYTES = 1 * 1024 * 1024;

struct journal_meta_t
{
    uint8_t  magic[8];
    uint32_t schema1_hash;

    journal_meta_t() : schema1_hash(SCHEMA1_HASH) {
        std::memcpy(magic, MAGIC_BYTES.data(), sizeof(magic));
    }
};

void check_journal_metadata(const std::shared_ptr<ldb::journal_t> &journal)
{
    int rc = 0;
    journal_meta_t meta;

    static_assert(LDB_METADATA_LEN >= sizeof(meta), "Metadata buffer too small");

    if ((rc = journal->get_meta(reinterpret_cast<char *>(&meta), sizeof(meta))) != LDB_OK)
        throw nplex::nplex_exception("Failed to read journal metadata ({})", ldb_strerror(rc));

    bool is_empty = (journal->range().first == 0);

    if (is_empty)
    {
        journal_meta_t expected_meta;

        if (std::memcmp(&meta, &expected_meta, sizeof(journal_meta_t)) == 0)
            return;

        if ((rc = journal->set_meta(reinterpret_cast<const char *>(&expected_meta), sizeof(expected_meta))) != LDB_OK)
            throw nplex::nplex_exception("Failed to write journal metadata ({})", ldb_strerror(rc));

        return;
    }

    // Checking that journal content is a valid nplex journal
    if (std::memcmp(meta.magic, MAGIC_BYTES.data(), MAGIC_BYTES.size()) != 0)
        throw nplex::nplex_exception("Invalid journal (magic mismatch)");

    // Checking that journal content is compatible with current schema
    if (meta.schema1_hash != SCHEMA1_HASH)
        throw nplex::nplex_exception("Invalid journal (schema1 mismatch)");
}

size_t next_pow2(size_t n)
{
    static_assert(sizeof(size_t) == 8);

    if (n == 0)
        return 1;

    n--;
    n |= n >> 1;
    n |= n >> 2;
    n |= n >> 4;
    n |= n >> 8;
    n |= n >> 16;
    n |= n >> 32;

    return n + 1;
}

struct read_buffers_t
{
    std::array<ldb_entry_t, READ_BATCH_ENTRIES> entries{};
    std::vector<char> arena;

    read_buffers_t() : arena(READ_BATCH_BYTES, '\0') {}
};

struct read_result_t
{
    nplex::rev_t rev = 0;           // next revision to read (i.e. last read revision + 1)
    std::size_t count = 0;          // number of entries read
    std::size_t bytes = 0;          // total bytes read (entries data only)
    bool stopped = false;           // whether the callback requested to stop reading
};

read_result_t read_updates(const nplex::journal_ptr &journal, nplex::rev_t from_rev, const nplex::journal_updates_persister_t::read_callback_f &on_entry, read_buffers_t &buffers)
{
    if (!journal)
        return read_result_t{from_rev, 0, 0, true};

    std::uint64_t seqnum = from_rev;
    std::size_t num_entries = 0;
    std::size_t total_bytes = 0;

    auto &entries = buffers.entries;
    auto &arena = buffers.arena;

    while (true)
    {
        std::size_t num_reads = 0;

        int rc = journal->read(seqnum, entries.data(), entries.size(), arena.data(), arena.size(), &num_reads);

        if (rc != LDB_OK && rc != LDB_ERR_NOT_FOUND)
            throw nplex::nplex_exception("failed to read journal entries from seqnum {}: {}", seqnum, ldb_strerror(rc));

        for (std::size_t i = 0; i < num_reads; ++i)
        {
            if (entries[i].seqnum != seqnum)
                throw nplex::nplex_exception("Journal corruption at rev {}", seqnum);

            nplex::updates_persister_t::update_entry_t raw = {
                .seqnum = entries[i].seqnum,
                .data_len = entries[i].data_len,
                .data = entries[i].data
            };

            seqnum++;
            num_entries++;
            total_bytes += raw.data_len;

            if (!on_entry(raw))
                return read_result_t{seqnum, num_entries, total_bytes, true};
        }

        if (num_reads < entries.size())
        {
            // case: reached end of this journal
            if (entries[num_reads].seqnum == 0)
                break;

            // case: buffer too short
            size_t need = entries[num_reads].data_len + 32;

            if (need > arena.size())
                arena.resize(next_pow2(need), 0);
        }
    }

    return read_result_t{seqnum, num_entries, total_bytes, false};
}

} // unnamed namespace

// ==========================================================
// journal_updates_persister_t methods
// ==========================================================

nplex::journal_updates_persister_t::journal_updates_persister_t(const fs::path &path, bool fsync) : m_path(path)
{
    std::error_code ec;
    const auto status = fs::status(m_path, ec);

    if (ec || !fs::exists(status) || !fs::is_directory(status))
        throw nplex_exception("journal directory does not exist or is not a directory: {}", m_path.string());

    const auto perms = status.permissions();

    if ((perms & fs::perms::owner_read) == fs::perms::none &&
        (perms & fs::perms::group_read) == fs::perms::none &&
        (perms & fs::perms::others_read) == fs::perms::none)
    {
        throw nplex_exception("journal directory is not readable: {}", m_path.string());
    }

    if ((perms & fs::perms::owner_write) == fs::perms::none &&
        (perms & fs::perms::group_write) == fs::perms::none &&
        (perms & fs::perms::others_write) == fs::perms::none)
    {
        throw nplex_exception("journal directory is not writable: {}", m_path.string());
    }

    m_journal = open_journal_file(m_path, std::string{CURRENT_SEGMENT}, LDB_CREATE | LDB_CHECK | (fsync ? LDB_FSYNC : 0));

    update_catalog();
}

std::pair<nplex::rev_t, nplex::rev_t> nplex::journal_updates_persister_t::get_updates_range()
{
    auto catalog_range = get_catalog_range();

    if (m_journal->empty())
        return catalog_range;

    auto current_range = m_journal->range();

    if (catalog_range.second == 0)
        return current_range;

    assert(current_range.first == 0 || catalog_range.second + 1 == current_range.first);

    return {catalog_range.first, current_range.second};
}

void nplex::journal_updates_persister_t::read_updates(rev_t from_rev, const read_callback_f &on_entry)
{
    journal_ptr journal;
    read_buffers_t buffers;
    read_result_t result{from_rev, 0, 0, false};

    while (!result.stopped)
    {
        journal = get_journal_with_retry(result.rev);

        if (!journal)
            return;  // TODO: what to do in this case? throw exception ?

        result = ::read_updates(journal, result.rev, on_entry, buffers);
    }

    // TODO: trace debug info (elapsed time, count, bytes)
}

void nplex::journal_updates_persister_t::write_updates(const std::span<update_entry_t> &entries, std::size_t &num_writes)
{
    num_writes = 0;

    if (!m_journal)
        return;

    if (entries.empty())
        return;

    // hack: we use the fact than update_entry_t mimics ldb_entry_t
    static_assert(sizeof(ldb_entry_t) == sizeof(update_entry_t));
    static_assert(alignof(ldb_entry_t) == alignof(update_entry_t));
    ldb_entry_t *entries_data = reinterpret_cast<ldb_entry_t *>(entries.data());

    int rc = m_journal->append(entries_data, entries.size(), &num_writes);
    if (rc != LDB_OK)
        throw nplex_exception("failed to append journal entries: {}", ldb_strerror(rc));
}

nplex::journal_ptr nplex::journal_updates_persister_t::open_journal_file(const std::filesystem::path &path, const std::string &name, int flags)
{
    auto journal = std::make_shared<ldb::journal_t>(path, name, flags);

    ::check_journal_metadata(journal);

    return journal;
}

nplex::journal_ptr nplex::journal_updates_persister_t::open_journal_file(const std::filesystem::path &filepath, int flags)
{
    auto path  = filepath.parent_path();
    auto name = filepath.stem().string();

    return open_journal_file(path, name, flags);
}

void nplex::journal_updates_persister_t::maintenance()
{
    std::lock_guard lock(m_catalog_mutex);
    auto now = std::chrono::system_clock::now().time_since_epoch();

    for (auto &item : m_catalog)
    {
        // Release idle journals
        if (item.second.journal && now > item.second.last_read_time + MAX_READ_IDLE_TIME) {
            item.second.last_read_time = millis_t::zero();
            item.second.journal = nullptr;
        }
    }
}

bool nplex::journal_updates_persister_t::update_catalog(bool force)
{
    std::lock_guard lock(m_catalog_mutex);
    auto now = std::chrono::system_clock::now().time_since_epoch();
    std::set<std::string> seen_files;

    // Skip catalog update if it was recently updated
    if (!force && m_last_catalog_update != millis_t::zero() && now < m_last_catalog_update + MIN_CATALOG_UPDATE_INTERVAL)
        return false;

    // Check current catalog entries
    for (auto it = m_catalog.begin(); it != m_catalog.end(); )
    {
        auto filepath = m_path / it->second.file_name;

        // Remove catalog entry if the file no longer exists
        if (!std::filesystem::exists(filepath)) {
            LOG_DEBUG("Journal file {} no longer exists, removing from catalog", filepath.string());
            it = m_catalog.erase(it);
            continue;
        }

        auto file_size = std::filesystem::file_size(filepath);
        auto last_write_time = std::filesystem::last_write_time(filepath).time_since_epoch();
        auto last_write_time_ms = std::chrono::duration_cast<millis_t>(last_write_time);

        // Remove catalog entry if the file size or last write time has changed
        if (file_size != it->second.file_size || last_write_time_ms != it->second.last_write_time) {
            LOG_DEBUG("Journal file {} has changed, removing from catalog", filepath.string());
            it = m_catalog.erase(it);
            continue;
        }

        seen_files.insert(it->second.file_name);

        ++it;
    }

    // Scan the directory for new journal files
    for (const auto &entry : fs::directory_iterator(m_path))
    {
        if (!entry.is_regular_file())
            continue;

        const std::string file_name = entry.path().filename().string();

        if (!file_name.starts_with(FILENAME_PREFIX))
            continue;

        if (!file_name.ends_with(FILENAME_EXTENSION))
            continue;

        if (seen_files.find(file_name) != seen_files.end())
            continue;

        auto segment_opt = get_journal_segment(entry.path());
        if (!segment_opt)
            continue;

        auto it = m_catalog.find(segment_opt->range.second);
        if (it != m_catalog.end()) {
            LOG_WARN("Journal {} overlaps with {}", file_name, it->second.file_name);
            continue;
        }

        m_catalog[segment_opt->range.second] = *segment_opt;
        LOG_DEBUG("Journal {} added to catalog", segment_opt->file_name);
    }

    auto catalog_range = normalize_catalog();

    LOG_DEBUG("Catalog update complete: range {}-{}", catalog_range.first, catalog_range.second);
    m_last_catalog_update = std::chrono::duration_cast<millis_t>(now);

    return true;
}

std::pair<nplex::rev_t, nplex::rev_t> nplex::journal_updates_persister_t::normalize_catalog()
{
    std::pair<rev_t, rev_t> catalog_range = {0, 0};
    std::pair<rev_t, rev_t> prev_range = {0, 0};

    // Check continuity in the catalog and keep only the latest contiguous chain.
    for (auto it = m_catalog.begin(); it != m_catalog.end(); ++it)
    {
        auto curr_range = it->second.range;
        if (prev_range.second == 0) {
            catalog_range = curr_range;
            prev_range = curr_range;
            continue;
        }

        if (prev_range.second + 1 < curr_range.first) {
            LOG_WARN("Journal gap: {}-{}", prev_range.second + 1, curr_range.first - 1);
            catalog_range = curr_range;
            prev_range = curr_range;
            while (it != m_catalog.begin())
                it = m_catalog.erase(std::prev(it));
            continue;
        }

        if (prev_range.second + 1 > curr_range.first) {
            LOG_WARN("Journal overlap: {}-{}", curr_range.first, prev_range.second);
            catalog_range = curr_range;
            prev_range = curr_range;
            while (it != m_catalog.begin())
                it = m_catalog.erase(std::prev(it));
            continue;
        }

        prev_range = curr_range;
        catalog_range.second = curr_range.second;
    }

    rev_t min_rev_in_current_segment = m_journal->range().first;

    if (catalog_range.second != 0 && min_rev_in_current_segment != 0 && 
        catalog_range.second + 1 != min_rev_in_current_segment)
    {
        LOG_WARN("Journal gap: {}-{}", catalog_range.second + 1, min_rev_in_current_segment - 1);
        catalog_range = {0, 0};
        m_catalog.clear();
    }

    return catalog_range;
}

nplex::journal_ptr nplex::journal_updates_persister_t::get_journal_with_retry(rev_t rev)
{
    auto journal = get_journal(rev);

    if (journal)
        return journal;

    if (!update_catalog())
        return nullptr;

    return get_journal(rev);
}

nplex::journal_ptr nplex::journal_updates_persister_t::get_journal(rev_t rev)
{
    std::lock_guard lock(m_catalog_mutex);

    auto it = m_catalog.lower_bound(rev);

    if (it == m_catalog.end())
    {
        if (m_journal->contains(rev))
            return m_journal;

        return nullptr;
    }

    assert(it->first == it->second.range.second);
    assert(rev <= it->second.range.second);

    if (rev < it->second.range.first)
        return nullptr;

    if (!it->second.journal)
    {
        try
        {
            it->second.journal = open_journal_file(m_path / it->second.file_name, LDB_READONLY | LDB_CHECK);
        }
        catch(const std::exception &e)
        {
            LOG_WARN("Failed to open journal file {}: {}", it->second.file_name, e.what());
            return nullptr;
        }
    }

    auto now = std::chrono::system_clock::now().time_since_epoch();
    it->second.last_read_time = std::chrono::duration_cast<millis_t>(now);

    return it->second.journal;
}

std::optional<nplex::journal_updates_persister_t::journal_segment_t> nplex::journal_updates_persister_t::get_journal_segment(const std::filesystem::path &filepath)
{
    journal_segment_t segment;

    try
    {
        segment.journal = open_journal_file(filepath, LDB_READONLY | LDB_CHECK);
    }
    catch(const std::exception &e)
    {
        LOG_WARN("Failed to open journal file {}: {}", filepath.filename().string(), e.what());
        return std::nullopt;
    }

    auto now = std::chrono::system_clock::now().time_since_epoch();

    segment.file_name = filepath.filename().string();
    segment.file_size = std::filesystem::file_size(filepath);
    auto last_write_time = std::filesystem::last_write_time(filepath).time_since_epoch();
    segment.last_write_time = std::chrono::duration_cast<millis_t>(last_write_time);
    segment.last_read_time = std::chrono::duration_cast<millis_t>(now);
    segment.range = segment.journal->range();

    return segment;
}

std::pair<nplex::rev_t, nplex::rev_t> nplex::journal_updates_persister_t::get_catalog_range() const
{
    std::lock_guard lock(m_catalog_mutex);

    if (m_catalog.empty())
        return {0, 0};

    return {m_catalog.begin()->second.range.first, m_catalog.rbegin()->second.range.second};
}
