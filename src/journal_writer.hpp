#pragma once

#include <mutex>
#include <atomic>
#include <thread>
#include <vector>
#include <functional>
#include <condition_variable>
#include <flatbuffers/flatbuffers.h>
#include "cqueue.hpp"
#include "common.hpp"
#include "params.hpp"

// Forward references
namespace ldb {
    class journal_t;
}

namespace nplex {

/**
 * Background single-threaded journal writer.
 */
class journal_writer
{
  public:

    /**
     * Callback type invoked when a journal write completes.
     *
     * If a batch write partially succeeds, the callback is invoked twice:
     * first with success=true for the successfully written updates, and then
     * with success=false for the updates that failed to be written.
     * 
     * The callback is invoked from the journal writer thread context. Callbacks
     * should avoid blocking or long-running operations and must not throw exceptions.
     * 
     * Implementations must handle any necessary synchronization if they access
     * shared state, as the callback may run concurrently with producer threads.
     * 
     * @param[in] success True if the write to the journal succeeded, false on failure.
     * @param[in] updates An rvalue reference to a vector containing the updates that 
     *             were flushed (or attempted). Ownership of the vector and its contents 
     *             is transferred to the callback; the writer moves this vector when 
     *             invoking the callback.
     */
    using result_callback_t = std::function<void(bool success, std::vector<update_t> &&updates)>;

    /**
     * Error callback type invoked when an exception occurs in the journal writer.
     *
     * @param[in] e The exception that was thrown.
     */
    using error_callback_t = std::function<void(std::exception_ptr)>;

    journal_writer(ldb::journal_t &journal, const journal_params_t &params);
    ~journal_writer();

    journal_writer(const journal_writer&) = delete;
    journal_writer& operator=(const journal_writer&) = delete;

    /**
     * Starts the journal writer thread.
     * 
     * If the thread is already started, does nothing.
     * 
     * @param[in] result_cb The callback function that will be called on every write completion.
     * @param[in] error_cb The callback function that will be called when an exception occurs.
     */
    void start(result_callback_t &&result_cb, error_callback_t &&error_cb);

    /**
     * Enqueues an update to be written to the journal.
     * 
     * This method is asynchronous and non-blocking.
     * If the thread is not running, does nothing.
     * 
     * After processing, the callback is invoked with the result.
     * 
     * @param[in] upd Entry to write to disk.
     * 
     * @exception nplex_exception Exceeded write-queue capacity.
     */
    void write(update_t &&upd);

    /**
     * Stops the journal writer thread.
     * 
     * This method is synchronous and blocking.
     * If the thread is not running, does nothing.
     *
     * Notifies the thread to finish and waits until all pending enqueued 
     * writes have been processed (flushed to the journal) before returning; 
     * it does not drop pending updates.
     */
    void stop();

  private:

    struct cmd_t {
        update_t update;                            // Update to write to the journal
        flatbuffers::DetachedBuffer buffer;         // Serialized update
    };

    ldb::journal_t &m_journal;                      // Reference to the journal object
    journal_params_t m_params;                      // Journal parameters

    gto::cqueue<cmd_t> m_queue;                     // Queue of pending write commands
    std::size_t m_bytes_in_queue = 0;               // Total bytes in the queue

    std::thread m_thread;                           // Writer thread
    std::atomic<bool> m_running{false};             // Running flag
    std::condition_variable m_cond;                 // Used by producer to notify consumer that m_queue is not empty
    std::mutex m_mutex;                             // Protects m_queue and m_bytes_in_queue

    result_callback_t m_result_cb;                  // Callback invoked on each write completion
    error_callback_t m_error_cb;                    // Callback invoked on exception

    /**
     * Main function executed by the writer thread.
     * 
     * This function continuously processes the write queue, flushing updates to the journal
     * in batches based on the configured maximum number of entries or bytes. It waits for
     * new updates to be enqueued if the queue is empty.
     */
    void run();
};

} // namespace nplex
