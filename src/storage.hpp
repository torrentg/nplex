#include <mutex>
#include <atomic>
#include <thread>
#include <string>
#include <vector>
#include <filesystem>
#include <condition_variable>
#include <flatbuffers/flatbuffers.h>
#include "common.hpp"
#include "params.hpp"
#include "cqueue.hpp"
#include "journal.h"

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
     * @param[in] rev Initial revision.
     * @param[in] num Number of entries to read.
     * @param[out] entries Array of entries.
     * 
     * @exception nplex_exception Error reading entries.
     */
    void read_entries(rev_t rev, std::size_t num, std::vector<update_t> &entries);

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
     * @exception nplex_exception Error reading file or invalid buffer.
     */
    std::string read_snapshot(rev_t rev);

    /**
     * Writes a snapshot to disk.
     * 
     * If the snapshot already exists, it will be overwritten.
     * 
     * This is a blocking method.
     * 
     * @param[in] rev Snapshot revision.
     * @param[in] data Data to save to disk.
     * 
     * @exception nplex_exception Error writing file or invalid buffer.
     */
    void write_snapshot(rev_t rev, const std::string_view &data) const;

    // TODO: implement
    //repo_t get_repo(rev_t rev, const user_ptr &user);

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

    void run_writer();
    void process_write_commands();
};

} // namespace nplex
