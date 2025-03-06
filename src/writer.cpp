#include <cassert>
#include <spdlog/spdlog.h>
#include "exception.hpp"
#include "messaging.hpp"
#include "writer.hpp"

// ==========================================================
// m_writer methods
// ==========================================================

nplex::writer_t::writer_t(const params_t &params, const journal_ptr &journal, callback_t callback)
{
    assert(journal);

    m_queue_max_length = params.write_queue_max_length;
    m_queue_max_bytes = params.write_queue_max_bytes;
    m_flush_max_entries = params.flush_max_entries;
    m_flush_max_bytes = params.flush_max_bytes;

    m_callback = std::move(callback);
    m_journal = journal;
}

void nplex::writer_t::write(update_t &&upd)
{ 
    auto buffer = serialize_update(upd);

    std::lock_guard<std::mutex> lock(m_mutex);

    if (!m_running)
        return;

    if (!m_queue.empty() && std::holds_alternative<stop_writer_cmd_t>(m_queue.back()))
        return;

    if (m_queue.size() >= m_queue_max_length)
        throw nplex_exception("exceeded write-queue capacity (length)");

    if (m_bytes_in_queue + buffer.size() > m_queue_max_bytes)
        throw nplex_exception("exceeded write-queue capacity (bytes)");

    m_bytes_in_queue += static_cast<uint32_t>(buffer.size());

    m_queue.push(write_entry_cmd_t{std::move(upd), std::move(buffer)});
    m_cond_not_empty.notify_one();
}

void nplex::writer_t::stop()
{ 
    std::lock_guard<std::mutex> lock(m_mutex);

    if (!m_running)
        return;

    if (!m_queue.empty() && std::holds_alternative<stop_writer_cmd_t>(m_queue.back()))
        return;

    m_queue.push(stop_writer_cmd_t{});
    m_cond_not_empty.notify_one();
}

void nplex::writer_t::process_messages()
{
    std::unique_lock<std::mutex> lock(m_mutex);
    std::vector<update_t> updates;
    std::uint64_t bytes = 0;
    size_t num_writes = 0;
    int ldb_rc = LDB_OK;

    assert(!m_queue.empty());

    if (std::holds_alternative<stop_writer_cmd_t>(m_queue.front())) {
        m_queue.clear();
        m_running = false;
        return;
    }

    m_entries.clear();

    for (int i = static_cast<int>(m_queue.size()) - 1; i >= 0; i--)
    {
        auto &cmd = m_queue[static_cast<uint32_t>(i)];

        if (!std::holds_alternative<write_entry_cmd_t>(cmd))
            break;

        auto &write_cmd = std::get<write_entry_cmd_t>(cmd);

        if (!m_entries.empty() && (m_entries.size() >= m_flush_max_entries || bytes + write_cmd.buffer.size() > m_flush_max_bytes))
            break;

        ldb_entry_t entry = {
            .seqnum = write_cmd.update.meta->rev,
            .timestamp = static_cast<std::uint64_t>(write_cmd.update.meta->timestamp.count()),
            .metadata_len = 0,
            .data_len = static_cast<std::uint32_t>(write_cmd.buffer.size()),
            .metadata = nullptr,
            .data = reinterpret_cast<char *>(write_cmd.buffer.data())
        };

        m_entries.push_back(entry);
        bytes += write_cmd.buffer.size();
    }

    lock.unlock();

    assert(!m_entries.empty());
    SPDLOG_DEBUG("Writing {}/{} entries to journal ...", m_entries.size(), m_queue.size());

    ldb_rc = m_journal->append(m_entries.data(), m_entries.size(), &num_writes);
    SPDLOG_DEBUG("Journal write completed, writes = {}, result = {}", num_writes, ldb_strerror(ldb_rc));

    lock.lock();

    updates.reserve(num_writes);

    for (size_t i = 0; i < m_entries.size(); i++)
    {
        auto aux = m_queue.pop();
        write_entry_cmd_t &cmd = std::get<write_entry_cmd_t>(aux);

        assert(cmd.buffer.size() <= m_bytes_in_queue);
        m_bytes_in_queue -= static_cast<uint32_t>(cmd.buffer.size());

        if (i < num_writes)
            updates.push_back(std::move(cmd.update));
    }

    lock.unlock();

    m_callback(ldb_rc == LDB_OK, std::move(updates));

    m_entries.clear();
    m_running = (ldb_rc == LDB_OK);
}

void nplex::writer_t::run()
{
    std::unique_lock<std::mutex> lock(m_mutex);

    if (m_running)
        return;

    m_running = true;

    SPDLOG_DEBUG("Writer thread started");

    while (m_running)
    {
        m_cond_not_empty.wait(lock, [this]() { return !m_queue.empty(); });
        lock.unlock();
        process_messages();
    }

    SPDLOG_DEBUG("Writer thread stopped");
}
