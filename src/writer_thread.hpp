#pragma once

#include "persister.hpp"
#include "cqueue.hpp"
#include <mutex>
#include <atomic>
#include <thread>
#include <condition_variable>
#include <functional>
#include <span>

namespace nplex {

/**
 * Background thread used for writing updates asynchronously.
 */
class writer_thread_t
{
  public:

    using result_callback_f = std::function<void(bool success, const std::span<update_t> &updates)>;
    using error_callback_f = std::function<void(std::exception_ptr)>;

    writer_thread_t() = default;
    ~writer_thread_t();

    writer_thread_t(const writer_thread_t &) = delete;
    writer_thread_t & operator=(const writer_thread_t &) = delete;

    void set_persister(const updates_persister_ptr &persister);
    void set_queue_limits(std::uint32_t max_entries, std::uint32_t max_bytes);
    void set_flush_limits(std::uint32_t max_entries, std::uint32_t max_bytes);
    void set_result_callback(result_callback_f &&result_cb);
    void set_error_callback(error_callback_f &&error_cb);

    /**
     * Starts the writer thread.
     * 
     * If the thread is already started, does nothing.
     * 
     * @note The persister must be set using set_persister() before starting the thread.
     * @note The result callback must be set using set_result_callback() before starting the thread.
     * @note The error callback must be set using set_error_callback() before starting the thread.
     */
    void start();

    /**
     * Checks if the writer queue is full.
     * 
     * @return true = writer queue can accept writes, 
     *         false = writer queue cannot accept writes.
     */
    bool can_accept_writes() const;

    /**
     * Enqueues an update to be written to the journal.
     * 
     * This method is asynchronous and non-blocking.
     * If the thread is not running, returns false.
     * 
     * After processing, the callback is invoked with the result.
     * 
     * @param[in] upd Entry to write to disk.
     * 
     * @return true if the update was successfully enqueued, false otherwise.
     */
    bool write(update_t &&upd);

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

    gto::cqueue<update_t> m_queue;                  // Queue of pending writes
    std::atomic<std::size_t> m_queue_size = 0;      // Number of entries in the queue
    std::atomic<std::size_t> m_queue_bytes = 0;     // Total bytes in the queue

    std::thread m_thread;                           // Writer thread
    std::atomic<bool> m_running{false};             // Running flag
    std::condition_variable m_cond;                 // Used to notify that m_queue is not empty
    mutable std::mutex m_mutex;                     // Protects m_queue

    updates_persister_ptr m_updates_persister;      // Updates persister instance
    result_callback_f m_result_cb;                  // Callback invoked on each write completion
    error_callback_f m_error_cb;                    // Callback invoked on exception

    std::uint32_t m_write_queue_max_entries = UINT32_MAX;
    std::uint32_t m_write_queue_max_bytes = UINT32_MAX;
    std::uint32_t m_flush_max_entries = UINT32_MAX;
    std::uint32_t m_flush_max_bytes = UINT32_MAX;

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
