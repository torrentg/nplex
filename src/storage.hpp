#pragma once

#include <mutex>
#include <atomic>
#include <thread>
#include <string>
#include <vector>
#include <memory>
#include <filesystem>
#include <condition_variable>
#include <flatbuffers/flatbuffers.h>
#include "common.hpp"
#include "params.hpp"
#include "cqueue.hpp"
#include "journal.h"
#include "repository.hpp"
#include "user.hpp"

namespace nplex {

/** 
 * Persistent storage (disk).
 * 
 * This object is accessed by multiple threads.
 */
class storage_t
{
  public:

    using callback_t = std::function<void(bool, const std::vector<update_t> &&)>;

    /**
     * Constructor.
     * 
     * Start a thread to write entries to disk.
     * 
     * @param[in] params Parameters.
     * 
     * @exception nplex_exception Error creating the storage object.
     */
    storage_t(const params_t &params);

    ~storage_t() { close(); }

    /**
     * Stops the writer thread.
     */
    void close();

    /**
     * Returns the data revision range.
     * 
     * @return Pair of revisions (min, max).
     */
    std::pair<rev_t, rev_t> get_range() const { return {min_rev, max_rev}; }

    /**
     * Sets the callback function invoked on each journal write.
     * 
     * First callback argument is the result (success/failure).
     * Second argument are the processed entries.
     * 
     * @param[in] callback Callback function.
     */
    void set_writer_callback(callback_t &&callback) { m_callback = std::move(callback); }

    /**
     * Write an entry into the journal.
     * 
     * This method is asynchronous and non-blocking.
     * On finish, the callback function is called with the result.
     * 
     * @param[in] upd Entry to write to disk.
     * 
     * @exception nplex_exception Error serializing update, or exceeded write-queue capacity.
     */
    void write_entry(update_t &&upd);

    /**
     * Reads num entries starting from rev (included).
     * 
     * Updates are filtered according to user visibility.
     * Even if an update has no visible content, it is still returned.
     * 
     * @param[in] rev Initial revision (included).
     * @param[in] num Number of entries to read.
     * @param[in] user User used to filter content (nullptr means no filter).
     * 
     * @return Array of entries (can be empty or have less than num records when no data).
     * 
     * @exception nplex_exception Error reading entries.
     */
    std::vector<update_t> read_entries(rev_t rev, std::size_t num, const user_ptr &user = nullptr);

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
     * @exception nplex_exception Error reading file or invalid content.
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
    rev_t write_snapshot(const std::string_view &data) const;

    /**
     * Recreates the repo content at a given revision.
     * 
     * This method is blocking.
     * 
     * @param[in] rev Revision.
     * @param[in] user User used to filter repo content (empty means no filter).
     * 
     * @return Repository content adapted to user visibility.
     * 
     * @exception nplex_exception Error reading entries.
     */
    repo_t get_repo(rev_t rev, const user_ptr &user = nullptr);

  private:

    struct cmd_t {
        update_t update;
        flatbuffers::DetachedBuffer buffer;
    };

    std::thread m_thread;
    std::filesystem::path m_path;
    callback_t m_callback;
    ldb::journal_t m_journal;
    gto::cqueue<cmd_t> m_queue;
    std::vector<ldb_entry_t> m_entries;
    std::condition_variable m_cond_not_empty{};
    std::mutex m_mutex;
    std::uint32_t m_queue_max_length;
    std::uint32_t m_queue_max_bytes;
    std::uint32_t m_flush_max_entries;
    std::uint32_t m_flush_max_bytes;
    std::uint32_t m_bytes_in_queue = 0;
    std::atomic<bool> m_running = false;
    rev_t min_rev = 0;
    rev_t max_rev = 0;

    void run_writer();
    void process_write_commands();
    rev_t get_snapshot_rev(rev_t rev, bool le = true) const;
};

using storage_ptr = std::shared_ptr<storage_t>;

} // namespace nplex
