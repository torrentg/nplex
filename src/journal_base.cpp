#include "journal_base.hpp"
#include "exception.hpp"
#include "schema.hpp"
#include "logger.hpp"
#include "utils.hpp"
#include "journal.h"
#include <cstring>

namespace fs = std::filesystem;

// ==========================================================
// Internal (static) functions
// ==========================================================

namespace {

constexpr std::string MAGIC_BYTES = "NPLEXLOG";

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

const nplex::msgs::Update * get_update(const std::string &content)
{
    auto verifier = fb::Verifier(reinterpret_cast<const std::uint8_t *>(content.data()), content.size());

    if (!verifier.VerifyBuffer<nplex::msgs::Update>(nullptr))
        throw nplex::nplex_exception("invalid journal entry");

    return fb::GetRoot<nplex::msgs::Update>(content.c_str());
}

nplex::millis_t get_timestamp(const nplex::journal_ptr &journal, nplex::rev_t rev)
{
    if (!journal || rev == 0)
        return nplex::millis_t{0};

    nplex::read_journal_buffers_t buffers(1);
    std::string content;

    auto on_entry = [&content](const ldb_entry_t &entry) {
        const char *ptr = static_cast<const char *>(entry.data);
        content.assign(ptr, entry.data_len);
        return false;
    };

    auto result = nplex::read_journal(journal, rev, on_entry, buffers);

    if (!result.count)
        return nplex::millis_t{0};

    auto update = ::get_update(content);

    return nplex::millis_t{update->timestamp()};
}

} // unnamed namespace

// ==========================================================
// journal_file_t methods
// ==========================================================

nplex::journal_file_t::journal_file_t(const fs::path &filepath)
{
    file_name = filepath.filename().string();
    file_size = fs::file_size(filepath);
    last_write = fs::last_write_time(filepath);

    journal = open_journal_file(filepath, LDB_READONLY | LDB_RECOVER | LDB_CHECK);

    auto now = std::chrono::duration_cast<millis_t>(std::chrono::system_clock::now().time_since_epoch());
    last_read = now;

    auto range = journal->range();
    min_rev = range.first;
    max_rev = range.second;

    min_timestamp = ::get_timestamp(journal, min_rev);
    max_timestamp = ::get_timestamp(journal, max_rev);
}

// ==========================================================
// nplex journal functions
// ==========================================================

nplex::journal_ptr nplex::open_journal_file(const fs::path &path, const std::string &name, int flags)
{
    auto journal = std::make_shared<ldb::journal_t>(path, name, flags);

    ::check_journal_metadata(journal);

    LOG_TRACE("Opened journal file: name={}, range=[{}-{}]", 
        name + LDB_EXT_DAT, journal->range().first, journal->range().second);

    return journal;
}

nplex::journal_ptr nplex::open_journal_file(const fs::path &filepath, int flags)
{
    auto path  = filepath.parent_path();
    auto name = filepath.stem().string();

    return open_journal_file(path, name, flags);
}

nplex::read_journal_buffers_t::read_journal_buffers_t(std::size_t max_batch_entries_, std::size_t initial_batch_bytes_) :
    max_batch_entries(max_batch_entries_), 
    batch_entries(std::make_unique<ldb_entry_t[]>(max_batch_entries_)), 
    arena(initial_batch_bytes_)
{
    // Nothing to do
}

nplex::read_journal_result_t nplex::read_journal(const journal_ptr &journal, rev_t from_rev, const read_journal_callback_f &on_entry, read_journal_buffers_t &buffers)
{
    if (!journal)
        return read_journal_result_t{from_rev, 0, 0, true};

    std::uint64_t seqnum = from_rev;
    std::size_t total_entries = 0;
    std::size_t total_bytes = 0;

    auto num_entries = buffers.max_batch_entries;
    auto &entries = buffers.batch_entries;
    auto &arena = buffers.arena;

    while (true)
    {
        std::size_t num_reads = 0;

        int rc = journal->read(seqnum, entries.get(), num_entries, arena.data(), arena.size(), &num_reads);

        if (rc != LDB_OK && rc != LDB_ERR_NOT_FOUND)
            throw nplex::nplex_exception("Failed to read journal entries from seqnum {}: {}", seqnum, ldb_strerror(rc));

        for (std::size_t i = 0; i < num_reads; ++i)
        {
            if (entries[i].seqnum != seqnum)
                throw nplex::nplex_exception("Journal corruption at rev {}", seqnum);

            seqnum++;
            total_entries++;
            total_bytes += entries[i].data_len;

            if (!on_entry(entries[i]))
                return read_journal_result_t{seqnum, total_entries, total_bytes, true};
        }

        if (num_reads < num_entries)
        {
            // Case: reached end of this journal
            if (entries[num_reads].seqnum == 0)
                break;

            // Case: buffer too short
            size_t need = entries[num_reads].data_len + 32;

            if (need > arena.size())
                arena.resize(::next_pow2(need), 0);
        }
    }

    LOG_TRACE("read_updates(): range=[{}-{}], bytes={}", 
        from_rev, (seqnum > from_rev ? seqnum - 1 : from_rev), nplex::bytes_to_string(total_bytes));

    return read_journal_result_t{seqnum, total_entries, total_bytes, false};
}
