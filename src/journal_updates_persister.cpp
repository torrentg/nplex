#include "journal_updates_persister.hpp"
#include "journal_catalog.hpp"
#include "journal_base.hpp"
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

constexpr std::string JOURNAL_CURRENT_NAME = "journal";
constexpr std::string JOURNAL_SEGMENT_PREFIX = "journal-";
constexpr std::size_t READ_BATCH_ENTRIES = 10000;
constexpr std::size_t READ_BATCH_BYTES = 20 * 1024 * 1024;
constexpr nplex::millis_t MAX_READ_IDLE_TIME = std::chrono::seconds(30);
constexpr nplex::millis_t MIN_CATALOG_UPDATE_INTERVAL = std::chrono::minutes(5);

bool is_empty(const std::pair<nplex::rev_t, nplex::rev_t> &range)
{
    return (range.second == 0);
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

nplex::journal_updates_persister_t::~journal_updates_persister_t() {
    // Defined out-of-line because journal_file_t is forward-declared
    // in the header and must be a complete type at the destructor definition.
}

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
    m_rotate_max_bytes = params.journal_max_bytes;
    m_rotate_max_entries = params.journal_max_entries;

    m_journal = open_journal_file(m_path, JOURNAL_CURRENT_NAME, LDB_CREATE | LDB_RECOVER | LDB_CHECK | (m_fsync ? LDB_FSYNC : 0));

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

    assert(catalog_range.second + 1 >= journal_range.first);
    assert(catalog_range.second + 1 <= journal_range.second);

    return {catalog_range.first, journal_range.second};
}

void nplex::journal_updates_persister_t::read_updates(rev_t from_rev, const read_callback_f &on_entry)
{
    // Checks that update_entry_t and ldb_entry_t are the same
    static_assert(sizeof(update_entry_t) == sizeof(ldb_entry_t));
    static_assert(alignof(update_entry_t) == alignof(ldb_entry_t));
    static_assert(std::is_standard_layout_v<update_entry_t>);
    static_assert(std::is_standard_layout_v<ldb_entry_t>);

    // Define read objects
    read_journal_buffers_t buffers(READ_BATCH_ENTRIES, READ_BATCH_BYTES);
    read_journal_result_t result{from_rev, 0, 0, false};
    read_journal_callback_f callback = [&on_entry](const ldb_entry_t &e) {
        return on_entry(reinterpret_cast<const update_entry_t &>(e));
    };

    // Read entries from catalog
    while (!result.stopped)
    {
        journal_ptr journal = get_journal_from_catalog_with_retry(result.rev);

        if (!journal)
            break;

        result = read_journal(journal, result.rev, callback, buffers);
    }

    // Read entries from the current journal
    if (!result.stopped)
    {
        LOG_TRACE("Reading updates from current journal starting at rev {}", result.rev);
        result = read_journal(m_journal, result.rev, callback, buffers);
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

void nplex::journal_updates_persister_t::maintenance()
{
    std::lock_guard lock(m_catalog_mutex);
    auto now = std::chrono::duration_cast<millis_t>(std::chrono::system_clock::now().time_since_epoch());

    for (auto &item : m_catalog)
    {
        // Release idle journals
        if (item.second.journal && now > item.second.last_read + MAX_READ_IDLE_TIME) {
            item.second.last_read = millis_t::zero();
            item.second.journal = nullptr;
            LOG_TRACE("Released idle journal: name={}", item.second.file_name);
        }
    }
}

bool nplex::journal_updates_persister_t::update_catalog(bool force)
{
    std::lock_guard lock(m_catalog_mutex);
    auto now = std::chrono::duration_cast<millis_t>(std::chrono::system_clock::now().time_since_epoch());

    // Skip catalog update if it was recently updated
    if (!force && m_last_catalog_update != millis_t::zero() && now < m_last_catalog_update + MIN_CATALOG_UPDATE_INTERVAL)
        return false;

    journal_catalog_builder_t builder;

    builder.with_path(m_path)
           .with_files_prefix(JOURNAL_SEGMENT_PREFIX)
           .with_files_extension(LDB_EXT_DAT)
           .with_overlap()
           .with_range(m_journal->range());

    m_catalog = builder.build(m_catalog);
    m_last_catalog_update = now;

    [[maybe_unused]] rev_t min_rev = (m_catalog.empty() ? 0 : m_catalog.begin()->second.min_rev);
    [[maybe_unused]] rev_t max_rev = (m_catalog.empty() ? 0 : m_catalog.rbegin()->second.max_rev);

    LOG_DEBUG("Journal catalog update complete: range {}-{}, entries={}", min_rev, max_rev, m_catalog.size());

    return true;
}

nplex::journal_ptr nplex::journal_updates_persister_t::get_journal_from_catalog_with_retry(rev_t rev)
{
    auto journal = get_journal_from_catalog(rev);

    if (journal)
        return journal;

    if (!update_catalog())
        return nullptr;

    journal = get_journal_from_catalog(rev);

    if (!journal) {
        LOG_WARN("No journal found in catalog for rev {}", rev);
        return nullptr;
    }

    return journal;
}

nplex::journal_ptr nplex::journal_updates_persister_t::get_journal_from_catalog(rev_t rev)
{
    std::lock_guard lock1(m_catalog_mutex);

    auto it = m_catalog.lower_bound(rev);

    if (it == m_catalog.end())
        return nullptr;

    assert(it->first == it->second.max_rev);
    assert(rev <= it->second.max_rev);

    auto &segment = it->second;

    if (rev < segment.min_rev)
        return nullptr;

    if (!segment.journal)
    {
        try
        {
            segment.journal = open_journal_file(m_path / segment.file_name, LDB_READONLY | LDB_RECOVER | LDB_CHECK);
        }
        catch(const std::exception &e)
        {
            LOG_WARN("Failed to open journal file {}: {}", segment.file_name, e.what());
            return nullptr;
        }
    }

    auto now = std::chrono::duration_cast<millis_t>(std::chrono::system_clock::now().time_since_epoch());
    segment.last_read = now;

    LOG_TRACE("get_journal(r{}): name={}, range=[{}-{}]", 
        rev, segment.file_name, segment.min_rev, segment.max_rev);

    return segment.journal;
}

std::pair<nplex::rev_t, nplex::rev_t> nplex::journal_updates_persister_t::get_catalog_range() const
{
    std::lock_guard lock(m_catalog_mutex);

    if (m_catalog.empty())
        return {0, 0};

    return {m_catalog.begin()->second.min_rev, m_catalog.rbegin()->second.max_rev};
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
    std::string segment_name = std::string{JOURNAL_SEGMENT_PREFIX} + name;

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
    LOG_TRACE("Closed current journal: name={}", JOURNAL_CURRENT_NAME + std::string{LDB_EXT_DAT});

    // Rename the current journal file
    auto rc = ldb::rename(m_path, JOURNAL_CURRENT_NAME, segment_name);
    if (rc != LDB_OK)
    {
        LOG_ERROR("Failed to rename journal to {}: {}", segment_name, ldb_strerror(rc));
        m_journal = open_journal_file(m_path / JOURNAL_CURRENT_NAME, LDB_CREATE | LDB_RECOVER | LDB_CHECK | (m_fsync ? LDB_FSYNC : 0));
        return false;
    }

    LOG_INFO("Journal rotated at r{}: {}", journal_range.second, segment_name);

    // Add the rotated segment to the catalog
    if (!is_empty(journal_range))
    {
        auto segment = journal_file_t(segment_path_dat);
        m_catalog[segment.max_rev] = segment;
    }

    // Open a new journal file for writing
    m_journal = open_journal_file(m_path, JOURNAL_CURRENT_NAME, LDB_CREATE | LDB_RECOVER | LDB_CHECK | (m_fsync ? LDB_FSYNC : 0));

    return true;
}
