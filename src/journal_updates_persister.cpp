#include "journal_updates_persister.hpp"
#include "exception.hpp"
#include "logger.hpp"
#include "utils.hpp"
#include "journal.h"
#include <algorithm>
#include <vector>
#include <set>

namespace fs = std::filesystem;

// ==========================================================
// Internal (static) functions
// ==========================================================

namespace {

constexpr std::string CURRENT_SEGMENT = "journal";
constexpr std::string FILENAME_PREFIX = "journal-";
constexpr std::string MAGIC_BYTES = "NPLEXLOG";
constexpr std::size_t READ_BATCH_ENTRIES = 1024;
constexpr std::size_t READ_BATCH_BYTES = 1 * 1024 * 1024;
constexpr nplex::millis_t MAX_READ_IDLE_TIME = std::chrono::seconds(30);
constexpr nplex::millis_t MIN_CATALOG_UPDATE_INTERVAL = std::chrono::minutes(5);

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

bool is_empty(const std::pair<nplex::rev_t, nplex::rev_t> &range)
{
    return (range.second == 0);
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
            throw nplex::nplex_exception("Failed to read journal entries from seqnum {}: {}", seqnum, ldb_strerror(rc));

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
            // Case: reached end of this journal
            if (entries[num_reads].seqnum == 0)
                break;

            // Case: buffer too short
            size_t need = entries[num_reads].data_len + 32;

            if (need > arena.size())
                arena.resize(next_pow2(need), 0);
        }
    }

    LOG_TRACE("read_updates(): range=[{}-{}], bytes={}", from_rev, seqnum - 1, nplex::bytes_to_string(total_bytes));

    return read_result_t{seqnum, num_entries, total_bytes, false};
}

bool remove_file_if_exists(const fs::path &file_path)
{
    if (!fs::exists(file_path))
        return true;

    std::error_code ec;

    fs::remove(file_path, ec);
    if (ec) {
        LOG_ERROR("Failed to remove file {}: {}", file_path.string(), ec.message());
        return false;
    }

    return true;
}

} // unnamed namespace

// ==========================================================
// journal_updates_persister_t methods
// ==========================================================

nplex::journal_updates_persister_t::journal_updates_persister_t(const fs::path &path, const storage_params_t &params) : m_path(path)
{
    std::error_code ec;
    const auto status = fs::status(m_path, ec);

    if (ec || !fs::exists(status) || !fs::is_directory(status))
        throw nplex_exception("Journal directory does not exist or is not a directory: {}", m_path.string());

    const auto perms = status.permissions();

    if ((perms & fs::perms::owner_read) == fs::perms::none &&
        (perms & fs::perms::group_read) == fs::perms::none &&
        (perms & fs::perms::others_read) == fs::perms::none)
    {
        throw nplex_exception("Journal directory is not readable: {}", m_path.string());
    }

    if ((perms & fs::perms::owner_write) == fs::perms::none &&
        (perms & fs::perms::group_write) == fs::perms::none &&
        (perms & fs::perms::others_write) == fs::perms::none)
    {
        throw nplex_exception("Journal directory is not writable: {}", m_path.string());
    }

    m_fsync = params.fsync;
    m_rotate_max_bytes = params.rotate_journal_max_bytes;
    m_rotate_max_entries = params.rotate_journal_max_entries;

    m_journal = open_journal_file(m_path, CURRENT_SEGMENT, LDB_CREATE | LDB_RECOVER | LDB_CHECK | (m_fsync ? LDB_FSYNC : 0));

    update_catalog();
}

std::pair<nplex::rev_t, nplex::rev_t> nplex::journal_updates_persister_t::get_updates_range()
{
    auto catalog_range = get_catalog_range();
    auto journal_range = get_journal_range();

    if (is_empty(journal_range))
        return catalog_range;

    if (is_empty(catalog_range))
        return journal_range;

    assert(journal_range.first == 0 || catalog_range.second + 1 == journal_range.first);

    return {catalog_range.first, journal_range.second};
}

void nplex::journal_updates_persister_t::read_updates(rev_t from_rev, const read_callback_f &on_entry)
{
    read_buffers_t buffers;
    read_result_t result{from_rev, 0, 0, false};

    // Read entries from catalog
    while (!result.stopped)
    {
        journal_ptr journal = get_journal_from_catalog(result.rev);

        if (!journal)
            break;

        result = ::read_updates(journal, result.rev, on_entry, buffers);
    }

    // Read entries from the current journal
    if (!result.stopped)
    {
        result = ::read_updates(m_journal, result.rev, on_entry, buffers);
    }
}

bool nplex::journal_updates_persister_t::rotate_at(rev_t rev)
{
    auto journal_range = get_journal_range();

    if (rev < journal_range.second)
        return false;

    m_rotate_rev = rev;

    return true;
}

std::pair<std::span<nplex::updates_persister_t::update_entry_t>, bool> nplex::journal_updates_persister_t::select_write_batch(std::span<update_entry_t> entries)
{
    if (m_rotate_rev == 0 && m_rotate_max_bytes == 0 && m_rotate_max_entries == 0)
        return {entries, false};

    if (entries.empty())
        return {entries, false};

    std::size_t rotate_rev = m_rotate_rev;
    std::pair<rev_t, rev_t> journal_range = {0, 0};
    std::size_t journal_bytes = 0;

    if (m_rotate_max_entries || m_rotate_max_bytes)
    {
        std::lock_guard lock(m_journal_mutex);
        journal_range = m_journal->range();
        journal_bytes = m_journal->bytes();
    }

    auto journal_entries = (journal_range.second == 0 ? 0 : journal_range.second - journal_range.first + 1);

    for (size_t i = 0; i < entries.size(); ++i)
    {
        if (rotate_rev != 0 && entries[i].seqnum >= rotate_rev) {
            m_rotate_rev = 0;
            return {entries.subspan(0, i + 1), true};
        }

        journal_entries++;

        if (m_rotate_max_entries != 0 && journal_entries >= m_rotate_max_entries)
            return {entries.subspan(0, i + 1), true};

        journal_bytes += entries[i].data_len + 16;

        if (m_rotate_max_bytes != 0 && journal_bytes >= m_rotate_max_bytes)
            return {entries.subspan(0, i + 1), true};
    }

    return {entries, false};
}

std::size_t nplex::journal_updates_persister_t::write_batch(std::span<update_entry_t> entries, std::size_t &num_writes)
{
    std::lock_guard lock(m_journal_mutex);
    ldb_entry_t *batch_data = reinterpret_cast<ldb_entry_t *>(entries.data());
    std::size_t batch_num = entries.size();
    std::size_t batch_num_writes = 0;

    int rc = m_journal->append(batch_data, batch_num, &batch_num_writes);

    num_writes += batch_num_writes;

    if (rc != LDB_OK)
        throw nplex_exception("Journal write error: {}", ldb_strerror(rc));

    return batch_num_writes;
}

void nplex::journal_updates_persister_t::write_updates(const std::span<update_entry_t> &entries, std::size_t &num_writes)
{
    auto pending_entries = entries;
    
    num_writes = 0;

    while (!pending_entries.empty())
    {
        auto [batch_entries, rotation_required] = select_write_batch(pending_entries);

        if (batch_entries.empty())
            throw nplex_exception("No entries selected for writing");

        std::size_t batch_num_writes = write_batch(batch_entries, num_writes);

        if (batch_num_writes == 0)
            throw nplex_exception("Failed to write batch to journal");

        pending_entries = pending_entries.subspan(batch_num_writes);

        if (rotation_required)
            rotate();
    }
}

nplex::journal_ptr nplex::journal_updates_persister_t::open_journal_file(const fs::path &path, const std::string &name, int flags)
{
    auto journal = std::make_shared<ldb::journal_t>(path, name, flags);
    ::check_journal_metadata(journal);
    LOG_TRACE("Opened journal file: name={}", name + LDB_EXT_DAT);
    return journal;
}

nplex::journal_ptr nplex::journal_updates_persister_t::open_journal_file(const fs::path &filepath, int flags)
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
            LOG_TRACE("Released idle journal: name={}", item.second.file_name);
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
        if (!fs::exists(filepath)) {
            LOG_DEBUG("Journal file {} no longer exists, removing from catalog", filepath.string());
            it = m_catalog.erase(it);
            continue;
        }

        auto file_size = fs::file_size(filepath);
        auto last_write_time = fs::last_write_time(filepath).time_since_epoch();
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

        if (!file_name.ends_with(LDB_EXT_DAT))
            continue;

        if (seen_files.find(file_name) != seen_files.end())
            continue;

        auto segment_opt = get_journal_segment(entry.path());
        if (!segment_opt)
            continue;

        if (is_empty(segment_opt->range))
            continue;

        auto it = m_catalog.find(segment_opt->range.second);
        if (it != m_catalog.end()) {
            LOG_WARN("Journal {} overlaps with {}", file_name, it->second.file_name);
            continue;
        }

        m_catalog[segment_opt->range.second] = *segment_opt;
        LOG_DEBUG("Journal {} added to catalog, range=[{}-{}]", 
            segment_opt->file_name, segment_opt->range.first, segment_opt->range.second);
    }

    auto catalog_range = normalize_catalog();
    UNUSED(catalog_range);

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
        if (is_empty(prev_range)) {
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

    auto journal_range = get_journal_range();

    if (catalog_range.second != 0 && journal_range.first != 0 && 
        catalog_range.second + 1 != journal_range.first)
    {
        LOG_WARN("Journal gap: {}-{}", catalog_range.second + 1, journal_range.first - 1);
        catalog_range = {0, 0};
        m_catalog.clear();
    }

    return catalog_range;
}

nplex::journal_ptr nplex::journal_updates_persister_t::get_journal_from_catalog_with_retry(rev_t rev)
{
    auto journal = get_journal_from_catalog(rev);

    if (journal)
        return journal;

    if (!update_catalog())
        return nullptr;

    journal = get_journal_from_catalog(rev);

    if (!journal)
        throw nplex_exception("Failed to get journal for rev {}", rev);

    return journal;
}

nplex::journal_ptr nplex::journal_updates_persister_t::get_journal_from_catalog(rev_t rev)
{
    std::lock_guard lock1(m_catalog_mutex);

    auto it = m_catalog.lower_bound(rev);

    if (it == m_catalog.end())
        return nullptr;

    assert(it->first == it->second.range.second);
    assert(rev <= it->second.range.second);

    if (rev < it->second.range.first)
        return nullptr;

    if (!it->second.journal)
    {
        try
        {
            it->second.journal = open_journal_file(m_path / it->second.file_name, LDB_READONLY | LDB_RECOVER | LDB_CHECK);
        }
        catch(const std::exception &e)
        {
            LOG_WARN("Failed to open journal file {}: {}", it->second.file_name, e.what());
            return nullptr;
        }
    }

    auto now = std::chrono::system_clock::now().time_since_epoch();
    it->second.last_read_time = std::chrono::duration_cast<millis_t>(now);

    LOG_TRACE("get_journal(r{}): name={}, range=[{}-{}]", 
        rev, it->second.file_name, it->second.range.first, it->second.range.second);
    
    return it->second.journal;
}

std::optional<nplex::journal_updates_persister_t::journal_segment_t> nplex::journal_updates_persister_t::get_journal_segment(const fs::path &filepath)
{
    journal_segment_t segment;

    try
    {
        segment.journal = open_journal_file(filepath, LDB_READONLY | LDB_RECOVER | LDB_CHECK);
    }
    catch(const std::exception &e)
    {
        LOG_WARN("Failed to open journal file {}: {}", filepath.filename().string(), e.what());
        return std::nullopt;
    }

    auto now = std::chrono::system_clock::now().time_since_epoch();

    segment.file_name = filepath.filename().string();
    segment.file_size = fs::file_size(filepath);
    auto last_write_time = fs::last_write_time(filepath).time_since_epoch();
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

std::pair<nplex::rev_t, nplex::rev_t> nplex::journal_updates_persister_t::get_journal_range() const
{
    std::lock_guard lock(m_journal_mutex);
    assert(m_journal);
    return m_journal->range();
}

bool nplex::journal_updates_persister_t::rotate(const std::string &name)
{
    std::lock_guard lock2(m_catalog_mutex);
    std::lock_guard lock1(m_journal_mutex);
    std::pair<rev_t, rev_t> journal_range = m_journal->range();
    std::string segment_name = std::string{FILENAME_PREFIX} + name;

    if (is_empty(journal_range))
        return false;

    if (name.empty()) {
        rev_t rev = m_journal->range().second;
        segment_name += std::to_string(rev);
    }

    auto segment_path_dat = m_path / (segment_name + std::string{LDB_EXT_DAT});
    auto segment_path_idx = m_path / (segment_name + std::string{LDB_EXT_IDX});

    // Grants that destination journal files does not exist
    if (!remove_file_if_exists(segment_path_dat))
        return false;
    if (!remove_file_if_exists(segment_path_idx))
        return false;

    // Close the current journal
    assert(m_journal);
    m_journal = nullptr;
    LOG_TRACE("Closed current journal: name={}", CURRENT_SEGMENT + std::string{LDB_EXT_DAT});

    // Rename the current journal file
    auto rc = ldb::rename(m_path, CURRENT_SEGMENT, segment_name);
    if (rc != LDB_OK)
    {
        LOG_ERROR("Failed to rename journal to {}: {}", segment_name, ldb_strerror(rc));
        m_journal = open_journal_file(m_path / CURRENT_SEGMENT, LDB_CREATE | LDB_RECOVER | LDB_CHECK | (m_fsync ? LDB_FSYNC : 0));
        return false;
    }

    LOG_INFO("Journal rotated at r{}: {}", journal_range.second, segment_name);

    // Add the rotated segment to the catalog
    if (!is_empty(journal_range))
    {
        auto segment_opt = get_journal_segment(segment_path_dat);

        if (segment_opt) {
            assert(segment_opt->range.first == journal_range.first);
            assert(segment_opt->range.second == journal_range.second);
            m_catalog[segment_opt->range.second] = *segment_opt;
        }
    }

    // Open a new journal file for writing
    m_journal = open_journal_file(m_path, CURRENT_SEGMENT, LDB_CREATE | LDB_RECOVER | LDB_CHECK | (m_fsync ? LDB_FSYNC : 0));

    return true;
}
