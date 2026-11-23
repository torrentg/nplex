#pragma once

#include "common.hpp"
#include <functional>
#include <filesystem>

// Forward declarations
struct ldb_entry_t;
namespace ldb {
    class journal_t;
}

namespace nplex {

using journal_ptr = std::shared_ptr<ldb::journal_t>;

struct journal_file_t
{
    std::string file_name;                          // Name of the journal file.
    size_t file_size = 0;                           // Size of the journal file in bytes (used to detect file changes).
    std::filesystem::file_time_type last_write;     // Last write time of the journal file (used to detect file changes).
    millis_t last_read = millis_t{0};               // Last read time of the journal file (used to release journal_ptr).
    journal_ptr journal;                            // Pointer to the loaded journal object.
    rev_t min_rev = 0;                              // Minimum revision covered by this journal file.
    rev_t max_rev = 0;                              // Maximum revision covered by this journal file.
    millis_t min_timestamp = millis_t{0};           // Minimum timestamp covered by this journal file.
    millis_t max_timestamp = millis_t{0};           // Maximum timestamp covered by this journal file.

    journal_file_t() = default;
    journal_file_t(const std::filesystem::path &filepath);
    bool is_empty() const { return (min_rev == 0 || max_rev == 0); }
};

/**
 * Opens a journal file with the specified path, name, and flags.
 * 
 * @param[in] path The directory path of the journal file.
 * @param[in] name The name of the journal file (ex: "journal", with no extension).
 * @param[in] flags Flags used for opening the journal file (see ldb_open() flags).
 * 
 * @return A shared pointer to the opened journal object.
 * @exception std::exception If the journal file cannot be opened.
 */
journal_ptr open_journal_file(const std::filesystem::path &path, const std::string &name, int flags);

/**
 * Opens a journal file with the specified path, name, and flags.
 * 
 * @param[in] filepath The full path of the journal file (ex: "/tmp/journal.dat").
 * @param[in] flags Flags used for opening the journal file (see ldb_open() flags).
 * 
 * @return A shared pointer to the opened journal object.
 * @exception std::exception If the journal file cannot be opened.
 */
journal_ptr open_journal_file(const std::filesystem::path &filepath, int flags);

using read_journal_callback_f = std::function<bool(const ldb_entry_t &)>;

struct read_journal_buffers_t
{
    std::size_t max_batch_entries;
    std::unique_ptr<ldb_entry_t[]> batch_entries;
    std::vector<char> arena;

    read_journal_buffers_t(std::size_t max_batch_entries_ = 64, std::size_t initial_batch_bytes_ = 256 * 1024);
};

struct read_journal_result_t
{
    rev_t rev = 0;                                  // Next revision to read (i.e. last read revision + 1).
    std::size_t count = 0;                          // Number of entries read.
    std::size_t bytes = 0;                          // Total bytes read (entries data only).
    bool stopped = false;                           // Whether the callback requested to stop reading.
};

/**
 * Reads journal entries from the specified journal starting from the given revision.
 * 
 * This function hides the internal details of the journal reading process.
 * The reading process uses the provided buffers for temporary storage.
 * The provided callback is invoked for each entry read.
 * 
 * @param[in] journal The journal to read from.
 * @param[in] from_rev The revision to start reading from.
 * @param[in] on_entry Callback function invoked for each entry.
 * @param[in] buffers Buffers used for reading entries.
 * 
 * @return A read_journal_result_t structure containing the result of the read operation.
 * @exception std::exception If reading from the journal fails.
 */
read_journal_result_t read_journal(const journal_ptr &journal, rev_t from_rev, const read_journal_callback_f &on_entry, read_journal_buffers_t &buffers);

} // namespace nplex
