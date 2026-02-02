#include <cassert>
#include <spdlog/spdlog.h>
#include "journal.h"
#include "exception.hpp"
#include "messaging.hpp"
#include "messages.hpp"
#include "common.hpp"
#include "utils.hpp"
#include "journal_writer.hpp"

// ==========================================================
// Internal (static) variables and functions
// ==========================================================

static inline std::uint32_t get_param(std::uint32_t value) {
    return (value == 0 ? UINT32_MAX : value);
}

// ==========================================================
// journal_writer methods
// ==========================================================

nplex::journal_writer::journal_writer(ldb::journal_t &journal, const params_t &params) : m_journal(journal)
{
    m_queue_max_length  = get_param(params.write_queue_max_length);
    m_queue_max_bytes   = get_param(params.write_queue_max_bytes);
    m_flush_max_entries = get_param(params.flush_max_entries);
    m_flush_max_bytes   = get_param(params.flush_max_bytes);
}

nplex::journal_writer::~journal_writer()
{
    stop();
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

    auto buffer = serialize_update(upd);
    auto entry_size = buffer.size();

    {
        std::lock_guard<std::mutex> lock(m_mutex);

        assert(!m_queue.empty() || m_bytes_in_queue == 0);

        if (m_queue.size() >= m_queue_max_length)
            throw nplex_exception("journal_writer queue full (entries)");

        if ((m_bytes_in_queue + entry_size) > m_queue_max_bytes)
            throw nplex_exception("journal_writer queue full (bytes)");

        m_queue.push(cmd_t{std::move(upd), std::move(buffer)});
        m_bytes_in_queue += entry_size;

        SPDLOG_TRACE("journal entry queued. queue-length={}, queue-bytes={}", 
            m_queue.size(), bytes_to_string(m_bytes_in_queue));
    }

    m_cond.notify_one();
}

void nplex::journal_writer::run()
{
    std::size_t num_writes = 0;
    std::vector<ldb_entry_t> batch;
    std::vector<update_t> updates;
    std::size_t bytes_batch = 0;
    std::uint64_t start_time = 0;
    int ldb_rc = LDB_OK;

    batch.reserve(32);
    updates.reserve(32);

    SPDLOG_DEBUG("journal_writer thread started");

    while (m_running.load() || !m_queue.empty())
    {
        // Step1: Prepare batch of entries to write to journal
        {
            std::unique_lock<std::mutex> lock(m_mutex);

            m_cond.wait(lock, [this] {
                return (!m_running.load() || !m_queue.empty());
            });

            if (m_queue.empty() && !m_running.load())
                break;

            bytes_batch = 0;
            batch.clear();

            for (auto it = m_queue.begin(); it != m_queue.end(); ++it)
            {
                auto bytes = it->buffer.size();

                if (!batch.empty() && batch.size() >= m_flush_max_entries)
                    break;

                if (!batch.empty() && bytes_batch + bytes > m_flush_max_bytes)
                    break;

                ldb_entry_t entry = {
                    .seqnum = it->update.meta->rev,
                    .timestamp = static_cast<std::uint64_t>(it->update.meta->timestamp.count()),
                    .data_len = static_cast<std::uint32_t>(it->buffer.size()),
                    .data = reinterpret_cast<char *>(it->buffer.data())
                };

                batch.push_back(entry);
                bytes_batch += bytes;
            }

            if (batch.empty())
                continue;
        }

        // Step2: Write batch to journal
        start_time = uv_hrtime();

        ldb_rc = m_journal.append(batch.data(), batch.size(), &num_writes);

        SPDLOG_TRACE("journal_writer wrote {}/{} entries ({} bytes) in {}μs", 
            num_writes, batch.size(), bytes_to_string(bytes_batch), (uv_hrtime() - start_time) / 1000);

        if (ldb_rc != LDB_OK) {
            SPDLOG_DEBUG("journal_writer append failed: {}", ldb_strerror(ldb_rc));
        }

        // Step3: Reports back results via callback and remove entries from m_queue
        {
            std::lock_guard<std::mutex> lock(m_mutex);

            // Written entries
            updates.clear();
            for (std::size_t i = 0; i < num_writes; ++i)
            {
                assert(!m_queue.empty());
                auto cmd = m_queue.pop_front();

                assert(cmd.buffer.size() <= m_bytes_in_queue);
                m_bytes_in_queue -= cmd.buffer.size();

                updates.emplace_back(std::move(cmd.update));
            }

            if (!updates.empty() && m_result_cb)
                m_result_cb(true, std::move(updates));

            // Remaining staged entries (not written)
            updates.clear();
            for (std::size_t i = num_writes; i < batch.size(); ++i)
            {
                assert(!m_queue.empty());
                auto cmd = m_queue.pop_front();

                assert(cmd.buffer.size() <= m_bytes_in_queue);
                m_bytes_in_queue -= cmd.buffer.size();

                updates.emplace_back(std::move(cmd.update));
            }

            if (!updates.empty() && m_result_cb)
                m_result_cb(false, std::move(updates));

            if (ldb_rc != LDB_OK && m_error_cb)
                m_error_cb(std::make_exception_ptr(ldb_strerror(ldb_rc)));

            assert(!m_queue.empty() || m_bytes_in_queue == 0);
        }

        batch.clear();
        updates.clear();
    }

    SPDLOG_DEBUG("journal_writer thread stopped");

    return;
}
