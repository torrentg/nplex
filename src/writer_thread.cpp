#include "writer_thread.hpp"
#include "exception.hpp"
#include "messaging.hpp"
#include "common.hpp"
#include "logger.hpp"
#include "utils.hpp"
#include <fmt/core.h>
#include <cassert>

// ==========================================================
// writer_thread_t methods
// ==========================================================

nplex::writer_thread_t::~writer_thread_t()
{
    stop();
}

void nplex::writer_thread_t::set_persister(const updates_persister_ptr &persister)
{
    m_updates_persister = persister;
    assert(m_updates_persister);
}

void nplex::writer_thread_t::set_result_callback(result_callback_f &&result_cb)
{
    m_result_cb = std::move(result_cb);
    assert(m_result_cb);
}

void nplex::writer_thread_t::set_error_callback(error_callback_f &&error_cb)
{
    m_error_cb = std::move(error_cb);
    assert(m_error_cb);
}

void nplex::writer_thread_t::set_queue_limits(std::uint32_t max_entries, std::uint32_t max_bytes)
{
    m_write_queue_max_entries = (max_entries == 0 ? UINT32_MAX : max_entries);
    m_write_queue_max_bytes = (max_bytes == 0 ? UINT32_MAX : max_bytes);
}

void nplex::writer_thread_t::set_flush_limits(std::uint32_t max_entries, std::uint32_t max_bytes)
{
    m_flush_max_entries = (max_entries == 0 ? UINT32_MAX : max_entries);
    m_flush_max_bytes = (max_bytes == 0 ? UINT32_MAX : max_bytes);
}

bool nplex::writer_thread_t::can_accept_writes() const
{
    if (m_queue_size.load() >= m_write_queue_max_entries)
        return false;

    if (m_queue_bytes.load() + 1 > m_write_queue_max_bytes)
        return false;

    return true;
}

void nplex::writer_thread_t::start()
{
    bool expected = false;

    assert(m_updates_persister);
    assert(m_result_cb);
    assert(m_error_cb);

    if (m_running.compare_exchange_strong(expected, true))
    {
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

void nplex::writer_thread_t::stop()
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

bool nplex::writer_thread_t::write(update_t &&upd)
{
    if (!m_running.load()) {
        LOG_TRACE("Write requested while writer_thread is not running");
        return false;
    }

    bool was_empty = false;

    {
        std::lock_guard<std::mutex> lock(m_mutex);

        was_empty = m_queue.empty();
        m_queue_bytes += estimate_bytes(upd);
        m_queue_size += 1;

        m_queue.push(std::move(upd));
    }

    LOG_TRACE("Write entry queued. queue-length={}, queue-bytes={}", 
        m_queue_size.load(), bytes_to_string(m_queue_bytes.load()));

    if (was_empty)
        m_cond.notify_one();

    return true;
}

void nplex::writer_thread_t::run()
{
    using entry_t = updates_persister_t::update_entry_t;

    std::size_t num_writes = 0;
    std::vector<update_t> updates_batch;
    std::vector<flatbuffers::DetachedBuffer> buffers_batch;
    std::vector<entry_t> entries_batch;
    std::size_t bytes_batch = 0;
    std::uint64_t start_time = 0;
    std::exception_ptr excp_ptr;

    updates_batch.reserve(32);
    buffers_batch.reserve(32);
    entries_batch.reserve(32);

    LOG_DEBUG("writer_thread started");

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

            bytes_batch = 0;
            updates_batch.clear();

            while (!m_queue.empty())
            {
                size_t bytes = estimate_bytes(m_queue.front());

                if (!updates_batch.empty() && bytes_batch + bytes > m_flush_max_bytes)
                    break;

                if (!updates_batch.empty() && updates_batch.size() >= m_flush_max_entries)
                    break;

                updates_batch.emplace_back(m_queue.pop());

                bytes_batch += bytes;
            }
        }

        if (updates_batch.empty())
            continue;

        // Step2: Prepare batch of entries to write to journal

        buffers_batch.clear();
        entries_batch.clear();

        buffers_batch.reserve(updates_batch.size());
        entries_batch.reserve(updates_batch.size());

        for (const auto &upd : updates_batch)
        {
            auto tmp = serialize_update(upd);
            buffers_batch.emplace_back(std::move(tmp));

            auto &buffer = buffers_batch.back();

            entry_t entry = {
                .seqnum = upd.meta->rev,
                .data_len = static_cast<std::uint32_t>(buffer.size()),
                .data = buffer.data()
            };

            entries_batch.push_back(entry);
        }

        assert(updates_batch.size() == buffers_batch.size());
        assert(updates_batch.size() == entries_batch.size());

        // Step3: Write batch to journal
        start_time = uv_hrtime();
        excp_ptr = nullptr;
        num_writes = 0;

        try
        {
            m_updates_persister->write_updates(entries_batch, num_writes);
        }
        catch (const std::exception &e)
        {
            LOG_ERROR("writer_thread failed: {}", e.what());
            excp_ptr = std::make_exception_ptr(e);
        }

        LOG_TRACE("writer_thread wrote {}/{} entries ({} bytes) in {}μs", 
            num_writes, entries_batch.size(), bytes_to_string(bytes_batch), (uv_hrtime() - start_time) / 1000);

        UNUSED(start_time);
        assert(num_writes <= entries_batch.size());

        // Step4: Reports back results via callback

        // Report successful writes 
        if (num_writes > 0 && m_result_cb)
            m_result_cb(true, {updates_batch.data(), num_writes});

        // Report unsuccessful writes
        if (num_writes < entries_batch.size() && m_result_cb)
            m_result_cb(false, {updates_batch.data() + num_writes, updates_batch.size() - num_writes});

        // Notify error if write failed
        if (excp_ptr && m_error_cb)
            m_error_cb(excp_ptr);

        // Update state
        assert(m_queue_bytes >= bytes_batch);
        assert(m_queue_size >= updates_batch.size());
        m_queue_bytes -= bytes_batch;
        m_queue_size -= updates_batch.size();

        // Clear batches for next iteration
        updates_batch.clear();
        buffers_batch.clear();
        entries_batch.clear();
    }

    LOG_DEBUG("writer_thread stopped");

    return;
}
