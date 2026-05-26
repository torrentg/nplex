#include "journal_writer.hpp"
#include "exception.hpp"
#include "messaging.hpp"
#include "common.hpp"
#include "utils.hpp"
#include "journal.h"
#include <spdlog/spdlog.h>
#include <cassert>

// ==========================================================
// Internal (static) variables and functions
// ==========================================================

static inline std::uint32_t get_param(std::uint32_t value) {
    return (value == 0 ? UINT32_MAX : value);
}

// ==========================================================
// journal_writer methods
// ==========================================================

nplex::journal_writer::journal_writer(ldb::journal_t &journal, const journal_params_t &params) : m_journal(journal), m_params(params)
{
    assert(m_params.write_queue_max_entries > 0);
    assert(m_params.write_queue_max_bytes > 0);
    assert(m_params.flush_max_entries > 0);
    assert(m_params.flush_max_bytes > 0);
}

nplex::journal_writer::~journal_writer()
{
    stop();
}

bool nplex::journal_writer::is_blocked() const
{
    std::lock_guard<std::mutex> lock(m_mutex);

    return (m_queue.size() >= m_params.write_queue_max_entries) || 
           ((m_bytes_in_queue + 1) > m_params.write_queue_max_bytes);
}

void nplex::journal_writer::start(result_callback_t &&result_cb, error_callback_t &&error_cb)
{
    m_result_cb = std::move(result_cb);
    m_error_cb = std::move(error_cb);

    bool expected = false;
    if (m_running.compare_exchange_strong(expected, true)) {
        m_thread = std::thread([this]{
            try {
                run();
            } catch (...) {
                if (m_error_cb) {
                    m_error_cb(std::current_exception());
                }
                m_running.store(false);
            }
        });
    }
}

void nplex::journal_writer::stop()
{
    bool expected = true;

    if (m_running.compare_exchange_strong(expected, false))
    {
        {
            std::lock_guard<std::mutex> lk(m_mutex);
            // notify run loop to exit
        }

        m_cond.notify_all();
    }

    if (m_thread.joinable()) 
        m_thread.join();
}

void nplex::journal_writer::write(update_t &&upd)
{
    if (!m_running.load()) {
        SPDLOG_TRACE("Trying to write to journal_writer while not running");
        return;
    }

    bool was_empty = false;

    {
        std::lock_guard<std::mutex> lock(m_mutex);

        was_empty = m_queue.empty();

        m_bytes_in_queue += estimate_bytes(upd);

        m_queue.push(std::move(upd));

        SPDLOG_TRACE("journal entry queued. queue-length={}, queue-bytes={}", 
            m_queue.size(), bytes_to_string(m_bytes_in_queue));
    }

    if (was_empty)
        m_cond.notify_one();
}

void nplex::journal_writer::run()
{
    std::size_t num_writes = 0;
    std::vector<std::pair<update_t, flatbuffers::DetachedBuffer>> items_batch;
    std::vector<ldb_entry_t> entries_batch;
    std::size_t bytes_batch = 0;
    std::uint64_t start_time = 0;
    std::vector<update_t> updates;
    int ldb_rc = LDB_OK;

    items_batch.reserve(32);
    entries_batch.reserve(32);
    updates.reserve(32);

    SPDLOG_DEBUG("journal_writer thread started");

    while (m_running.load() || !m_queue.empty())
    {
        // Step1: Get data from queue
        {
            std::unique_lock<std::mutex> lock(m_mutex);

            m_cond.wait(lock, [this] {
                return (!m_running.load() || !m_queue.empty());
            });

            if (m_queue.empty() && !m_running.load())
                break;

            items_batch.clear();

            while (!m_queue.empty())
            {
                size_t bytes = estimate_bytes(m_queue.front());

                if (!items_batch.empty() && bytes_batch + bytes > m_params.flush_max_bytes)
                    break;

                if (!items_batch.empty() && items_batch.size() >= m_params.flush_max_entries)
                    break;

                items_batch.push_back({std::move(m_queue.pop()), flatbuffers::DetachedBuffer()});

                bytes_batch += bytes;
            }
        }

        if (items_batch.empty())
            continue;

        // Step2: Prepare batch of entries to write to journal
        entries_batch.clear();

        for (auto &[upd, buffer] : items_batch)
        {
            buffer = serialize_update(upd);

            ldb_entry_t entry = {
                .seqnum = upd.meta->rev,
                .data_len = static_cast<std::uint32_t>(buffer.size()),
                .data = reinterpret_cast<char *>(buffer.data())
            };

            entries_batch.push_back(entry);
        }

        // Step3: Write batch to journal
        start_time = uv_hrtime();

        ldb_rc = m_journal.append(entries_batch.data(), entries_batch.size(), &num_writes);

        SPDLOG_TRACE("journal_writer wrote {}/{} entries ({} bytes) in {}μs", 
            num_writes, entries_batch.size(), bytes_to_string(bytes_batch), (uv_hrtime() - start_time) / 1000);

        if (ldb_rc != LDB_OK)
            SPDLOG_ERROR("journal_writer append failed: {}", ldb_strerror(ldb_rc));

        // Step4: Reports back results via callback and remove entries from m_queue
        // Written entries
        updates.clear();
        for (std::size_t i = 0; i < num_writes; ++i)
            updates.emplace_back(std::move(items_batch[i].first));

        if (!updates.empty() && m_result_cb)
            m_result_cb(true, std::move(updates));

        // Remaining staged entries (not written)
        updates.clear();
        for (std::size_t i = num_writes; i < entries_batch.size(); ++i)
            updates.emplace_back(std::move(items_batch[i].first));

        if (!updates.empty() && m_result_cb)
            m_result_cb(false, std::move(updates));

        // Notify error if write failed
        if (ldb_rc != LDB_OK && m_error_cb)
            m_error_cb(std::make_exception_ptr(nplex_exception(ldb_strerror(ldb_rc))));

        // Update state
        assert(m_bytes_in_queue >= bytes_batch);
        m_bytes_in_queue -= bytes_batch;

        // Clear batches for next iteration
        bytes_batch = 0;
        entries_batch.clear();
        items_batch.clear();
        updates.clear();
    }

    SPDLOG_DEBUG("journal_writer thread stopped");

    return;
}
