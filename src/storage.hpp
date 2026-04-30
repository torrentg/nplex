#pragma once

#include <map>
#include <mutex>
#include <string>
#include <memory>
#include <filesystem>
#include "journal.h"
#include "schema.hpp"
#include "common.hpp"
#include "store.hpp"
#include "user.hpp"

namespace nplex {

using journal_ptr = std::shared_ptr<ldb::journal_t>;

struct snapshot_item_t
{
    rev_t rev = 0;
    std::string filename{};
    bool checked = false;
};

struct journal_item_t
{
    rev_t from_rev = 0;
    rev_t to_rev = 0;
    std::string filename{};
    journal_ptr journal{};
    bool checked = false;
};

/** 
 * Persistent storage (disk).
 * 
 * All methods are blocking.
 * This object is accessed by multiple threads (tasks).
 * 
 * @see journal_writer
 */
class storage_t
{
  public:

    /**
     * Constructor.
     * 
     * @param[in] path Storage path.
     * @param[in] flags Journal flags.
     */
    storage_t(const std::filesystem::path &path, int flags);
    ~storage_t() = default;

    /**
     * Non-copyable class
     */
    storage_t(const storage_t &) = delete;
    storage_t & operator=(const storage_t &) = delete;

    /**
     * Get the journal object.
     * 
     * @return Reference to the journal.
     */
    ldb::journal_t & get_journal() { return *m_journal; }

    /**
     * Get the minimum revision constructible.
     * 
     * @return Minimum revision.
     */
    rev_t get_min_rev();

    /**
     * Get the maximum revision constructible.
     * 
     * @return Maximum revision.
     */
    rev_t get_max_rev();

    /**
     * Reads entries from rev until func returns false or last entry read.
     * 
     * This is a blocking method.
     * 
     * @param[in] rev Initial revision (included).
     * @param[in] func Function to execute on every entry.
     * @param[in] num Number of entries to read.
     * 
     * @return Number of entries read.
     * 
     * @exception nplex_exception Error reading entries.
     */
    std::size_t read_entries(rev_t rev, const std::function<bool(const msgs::Update *)> &func, std::size_t num = UINT64_MAX);

    /**
     * Reads a snapshot from disk.
     * 
     * Returns the latest snapshot available having revision less-than or 
     * equals-to the given revision.
     * 
     * This is a blocking method.
     * 
     * @param[in] rev Snapshot revision.
     * 
     * @return Snapshot binary data (empty if not available).
     * 
     * @exception nplex_exception If the file cannot be read or the content is invalid.
     */
    std::string read_snapshot(rev_t rev);

    /**
     * Writes a snapshot to disk.
     * 
     * If the snapshot already exists, it will be overwritten.
     * 
     * This is a blocking method.
     * 
     * @param[in] data Data to save to disk.
     * 
     * @return Revision of the persisted snapshot
     * 
     * @exception nplex_exception Invalid snapshot data, error writing file or invalid buffer.
     */
    rev_t write_snapshot(const std::string_view &data);

    /**
     * Recreates the store content at a given revision.
     * 
     * This method is blocking.
     * 
     * @param[in] rev Revision.
     * @param[in] user User used to filter store content (empty means no filter).
     * 
     * @return Store content adapted to user visibility.
     * 
     * @exception nplex_exception Error reading entries.
     */
    store_t get_store(rev_t rev, const user_ptr &user = nullptr);

  private:  // members

    mutable std::mutex m_mutex;                     // Protects m_snapshots and m_archives.
    const std::filesystem::path m_path;             // Storage path.
    journal_ptr m_journal;                          // Current journal.
    std::map<rev_t, snapshot_item_t> m_snapshots;   // Map of available snapshots.
    std::map<rev_t, journal_item_t> m_archives;     // Map of archived journals.
    bool m_check = false;                           // Whether to check snapshots and journals.

  private:  // methods

    std::string get_snapshot_filename(rev_t rev) const;
    void rebuild_map_snapshots();
    void rebuild_map_archives();
    journal_ptr get_archive(rev_t rev);
};

using storage_ptr = std::shared_ptr<storage_t>;

/**
 * Read and parse a snapshot file.
 * 
 * @param[in] file Snapshot file path (with dat extension).
 * @param[in] check Whether to check the snapshot content (validity and schema).
 * 
 * @return Snapshot binary data (a valid msgs::Snapshot)
 * 
 * @exception nplex_exception If the file cannot be read or the content is invalid.
 */
std::string read_snapshot(const std::filesystem::path &file, bool check = false);

/**
 * Open a journal file.
 * 
 * @param[in] file Journal file path (with dat or idx extension).
 * @param[in] flags Journal open flags (LDB_OPEN_*).
 * 
 * @return The opened journal object.
 * 
 * @exception nplex_exception If the journal cannot be opened or the metadata is invalid.
 */
journal_ptr open_journal(const std::filesystem::path &file, int flags);

} // namespace nplex
