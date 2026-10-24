#include "journal_updates_persister.hpp"
#include "exception.hpp"
#include "journal.h"
#include <algorithm>
#include <vector>

namespace fs = std::filesystem;

// ==========================================================
// Internal (static) functions
// ==========================================================

namespace {

constexpr std::string_view FILENAME_PREFIX = "journal";
constexpr std::string_view MAGIC_BYTES = "NPLEXLOG";
constexpr std::size_t READ_BATCH_ENTRIES = 1024;
constexpr std::size_t READ_BATCH_BYTES   = 1 * 1024 * 1024;
constexpr std::size_t READ_BATCH_FACTOR  = 2;

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

    bool is_empty = (journal->get_range().first == 0);

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

} // unnamed namespace

// ==========================================================
// journal_updates_persister_t methods
// ==========================================================

nplex::journal_updates_persister_t::journal_updates_persister_t(const fs::path &path) : m_path(path)
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

    m_journal = open_journal_file(m_path, std::string{FILENAME_PREFIX}, LDB_OPEN_CREATE);
}

std::pair<nplex::rev_t, nplex::rev_t> nplex::journal_updates_persister_t::get_updates_range()
{
    if (!m_journal)
        return {0, 0};

    return m_journal->get_range();
}

void nplex::journal_updates_persister_t::read_updates(rev_t from_rev, const read_callback_f &on_entry, std::size_t max_entries)
{
    if (!m_journal || max_entries == 0)
        return;

    std::vector<ldb_entry_t> entries(READ_BATCH_ENTRIES, ldb_entry_t{0, 0, nullptr});
    std::vector<char> arena(READ_BATCH_BYTES, 0);
    std::uint64_t seqnum = from_rev;
    std::size_t num_entries = 0;

    while (num_entries < max_entries)
    {
        std::size_t len = std::min(READ_BATCH_ENTRIES, max_entries - num_entries);
        std::size_t num_reads = 0;

        int rc = m_journal->read(seqnum, entries.data(), len, arena.data(), arena.size(), &num_reads);

        if (rc != LDB_OK && rc != LDB_ERR_NOT_FOUND)
            throw nplex_exception("failed to read journal entries from seqnum {}: {}", seqnum, ldb_strerror(rc));

        if (num_reads == 0)
            break;

        for (std::size_t i = 0; i < num_reads; ++i)
        {
            update_entry_t raw = {
                .seqnum = entries[i].seqnum,
                .data_len = entries[i].data_len,
                .data = entries[i].data
            };

            num_entries++;
            seqnum = entries[i].seqnum + 1;

            if (!on_entry(raw))
                return;
        }

        if (num_reads < len)
        {
            // case: reached end of this journal
            if (entries[num_reads].seqnum == 0)
                return;

            // case: buffer too short
            size_t need = entries[num_reads].data_len + 32;
            size_t new_size = arena.size();

            while (new_size < need)
                new_size *= READ_BATCH_FACTOR;

            arena.resize(new_size, 0);
        }
    }
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

nplex::journal_updates_persister_t::journal_ptr nplex::journal_updates_persister_t::open_journal_file(const std::filesystem::path &path, const std::string &name, int flags)
{
    auto journal = std::make_shared<ldb::journal_t>(path, name, flags);

    ::check_journal_metadata(journal);

    return journal;
}

nplex::journal_updates_persister_t::journal_ptr nplex::journal_updates_persister_t::open_journal_file(const std::filesystem::path &filepath, int flags)
{
    auto path  = filepath.parent_path();
    auto name = filepath.stem().string();

    return open_journal_file(path, name, flags);
}
