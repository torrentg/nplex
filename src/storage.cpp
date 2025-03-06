#include <regex>
#include <fstream>
#include <charconv>
#include <spdlog/spdlog.h>
#include "exception.hpp"
#include "messaging.hpp"
#include "storage.hpp"

namespace fs = std::filesystem;

// ==========================================================
// Internal (static) variables and functions
// ==========================================================

static const std::regex snapshot_pattern(R"(.*/snapshot-\d+\.dat)");

static nplex::rev_t parse_rev(const char* filename)
{
    std::string_view str(filename);

    assert(str.ends_with(".dat"));
    str.remove_suffix(std::string(".dat").length());

    auto pos = str.find_last_of('-');
    assert(pos != std::string::npos);

    str = str.substr(pos + 1);
    assert(!str.empty());

    nplex::rev_t num = 0;
    auto [ptr, ec] = std::from_chars(str.begin(), str.end(), num);
    if (ec != std::errc())
        throw nplex::nplex_exception(fmt::format("Invalid snapshot filename ({})", filename));

    return num;
}

// ==========================================================
// snapshots_t methods
// ==========================================================

nplex::storage_t::storage_t(const nplex::params_t &params) : m_path{params.datadir}
{
    if (!fs::exists(m_path))
        throw nplex::nplex_exception(fmt::format("Storage directory does not exist ({})", m_path.string()));

    if (!fs::is_directory(m_path))
        throw nplex::nplex_exception(fmt::format("Invalid storage directory ({})", m_path.string()));

    m_queue_max_length = params.write_queue_max_length;
    m_queue_max_bytes = params.write_queue_max_bytes;
    m_flush_max_entries = params.flush_max_entries;
    m_flush_max_bytes = params.flush_max_bytes;

    m_journal = ldb::journal_t(m_path, "entries");
    m_journal.set_fsync(!params.disable_fsync);

    m_thread = std::thread(&nplex::storage_t::run_writer, this);
}

std::string nplex::storage_t::read_snapshot(rev_t rev)
{
    rev_t rev_available = 0;

    for (auto const& dir_entry : std::filesystem::directory_iterator(m_path)) 
    {
        if (!std::regex_match(dir_entry.path().c_str(), snapshot_pattern))
        continue;

        rev_t rnum = parse_rev(dir_entry.path().c_str());

        if (rnum <= rev && rnum > rev_available)
            rev_available = rnum;
    }

    if (rev_available == 0)
        return {};

    fs::path path = m_path / fmt::format(SNAPSHOT_FILENAME, rev_available);
    std::ifstream ifs(path, std::ios::binary | std::ios::ate);

    if (!ifs)
        throw nplex::nplex_exception(fmt::format("Failed to open snapshot file ({})", path.string()));

    auto file_size = ifs.tellg();
    ifs.seekg(0, std::ios::beg);

    std::string content(static_cast<std::size_t>(file_size), '\0');
    ifs.read(&content[0], static_cast<std::streamsize>(file_size));

    return content;
}

void nplex::storage_t::write_snapshot(rev_t rev, const std::string_view &data) const
{
    fs::path path = m_path / fmt::format(SNAPSHOT_FILENAME, rev);
    std::ofstream ofs(path, std::ios::binary | std::ios::trunc);

    if (!ofs)
        throw nplex::nplex_exception(fmt::format("Failed to open snapshot file ({})", path.string()));

    ofs.write(data.data(), static_cast<std::streamsize>(data.size()));
    if (!ofs)
        throw nplex::nplex_exception(fmt::format("Failed to write to snapshot file ({})", path.string()));
}

void nplex::storage_t::write_entry(update_t &&upd)
{ 
    auto buffer = serialize_update(upd);

    std::lock_guard<std::mutex> lock(m_mutex);

    if (!m_running)
        return;

    if (m_queue.size() >= m_queue_max_length)
        throw nplex_exception("exceeded write-queue capacity (length)");

    if (m_bytes_in_queue + buffer.size() > m_queue_max_bytes)
        throw nplex_exception("exceeded write-queue capacity (bytes)");

    assert(!m_queue.empty() || m_bytes_in_queue == 0);

    m_bytes_in_queue += static_cast<std::uint32_t>(buffer.size());

    m_queue.push(cmd_t{std::move(upd), std::move(buffer)});
    m_cond_not_empty.notify_one();
}

void nplex::storage_t::close()
{ 
    if (!m_running)
        return;

    m_running = false;
    m_cond_not_empty.notify_one();

    if (m_thread.joinable())
        m_thread.join();
}

void nplex::storage_t::run_writer()
{
    assert(!m_running);

    std::unique_lock<std::mutex> lock(m_mutex);

    SPDLOG_DEBUG("Writer thread started");

    m_running = true;

    while (m_running)
    {
        m_cond_not_empty.wait(lock, [this]() { 
            return (!m_queue.empty() || !m_running);
        });

        if (m_running && !m_queue.empty()) {
            lock.unlock();
            process_write_commands();
        }
    }

    if (m_callback && !m_queue.empty())
    {
        std::vector<update_t> updates;

        while (!m_queue.empty())
        {
            auto cmd = m_queue.pop();
            updates.push_back(std::move(cmd.update));
        }

        m_callback(false, std::move(updates));
    }

    m_entries.clear();
    m_queue.clear();

    SPDLOG_DEBUG("Writer thread stopped");
}

void nplex::storage_t::process_write_commands()
{
    std::unique_lock<std::mutex> lock(m_mutex);
    std::vector<update_t> updates;
    auto it = m_queue.rbegin();
    std::uint64_t bytes = 0;
    size_t num_writes = 0;
    int ldb_rc = LDB_OK;

    assert(!m_queue.empty());

    m_entries.clear();

    while (it != m_queue.rend())
    {
        if (!m_entries.empty()) // flush controls don't apply to the first entry
            if (m_entries.size() >= m_flush_max_entries || bytes + it->buffer.size() > m_flush_max_bytes)
                break;

        ldb_entry_t entry = {
            .seqnum = it->update.meta->rev,
            .timestamp = static_cast<std::uint64_t>(it->update.meta->timestamp.count()),
            .metadata_len = 0,
            .data_len = static_cast<std::uint32_t>(it->buffer.size()),
            .metadata = nullptr,
            .data = reinterpret_cast<char *>(it->buffer.data())
        };

        m_entries.push_back(entry);
        bytes += it->buffer.size();

        it++;
    }

    lock.unlock();

    assert(!m_entries.empty());
    SPDLOG_DEBUG("Writing {}/{} entries to journal ...", m_entries.size(), m_queue.size());

    ldb_rc = m_journal.append(m_entries.data(), m_entries.size(), &num_writes);
    SPDLOG_DEBUG("Journal write completed, writes = {}, result = {}", num_writes, ldb_strerror(ldb_rc));

    lock.lock();

    updates.clear();

    for (size_t i = 0; i < num_writes; i++)
    {
        auto cmd = m_queue.pop();

        assert(cmd.buffer.size() <= m_bytes_in_queue);
        m_bytes_in_queue -= static_cast<std::uint32_t>(cmd.buffer.size());

        updates.push_back(std::move(cmd.update));
    }

    if (!updates.empty() && m_callback)
        m_callback(true, std::move(updates));

    updates.clear();

    for (size_t i = num_writes; i < m_entries.size(); i++)
    {
        auto cmd = m_queue.pop();

        assert(cmd.buffer.size() <= m_bytes_in_queue);
        m_bytes_in_queue -= static_cast<std::uint32_t>(cmd.buffer.size());

        updates.push_back(std::move(cmd.update));
    }

    if (!updates.empty() && m_callback)
        m_callback(false, std::move(updates));

    lock.unlock();

    m_entries.clear();
}
