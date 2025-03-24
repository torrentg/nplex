#include <regex>
#include <limits>
#include <fstream>
#include <charconv>
#include <algorithm>
#include <experimental/scope>
#include <spdlog/spdlog.h>
#include "exception.hpp"
#include "messaging.hpp"
#include "utils.hpp"
#include "storage.hpp"

#define READ_BATCH_ENTRIES 100ul
#define READ_BATCH_BYTES (5 * 1024 * 1024)

namespace fs = std::filesystem;

// ==========================================================
// Internal (static) variables and functions
// ==========================================================

static const std::regex snapshot_pattern(R"(.*/snapshot-\d+\.dat)");

template <typename T>
static const std::uint8_t * as_uint8_ptr(T *ptr) {
    return reinterpret_cast<const std::uint8_t *>(ptr);
}

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

    m_journal = ldb::journal_t(m_path, JOURNAL_NAME, params.check_journal);
    m_journal.set_fsync(!params.disable_fsync);

    int rc = 0;
    ldb_stats_t stats = {};
    if ((rc = m_journal.stats(0, UINT64_MAX, &stats)) != LDB_OK)
        throw nplex::nplex_exception(fmt::format("Failed to get journal stats ({})", ldb_strerror(rc)));

    min_rev = stats.min_seqnum;
    max_rev = stats.max_seqnum;
    SPDLOG_INFO("Journal range: [r{}, r{}]", min_rev, max_rev);

    rev_t min_snp = get_snapshot_rev(0, false);
    rev_t max_snp = get_snapshot_rev(std::numeric_limits<rev_t>::max(), true);
    SPDLOG_INFO("Snapshot range: [r{}, r{}]", min_snp, max_snp);

    rev_t rev0 = get_snapshot_rev(min_rev, false);

    if (rev0 == 0 && min_rev > 1)
        throw nplex_exception("No snapshot available for the first revision (r{})", min_rev);

    min_rev = rev0;

    SPDLOG_INFO("Data range: [r{}, r{}]", min_rev, max_rev);

    m_thread = std::thread(&nplex::storage_t::run_writer, this);
}

// Returns the latest snapshot revision available that is less-eq/great-eq than rev
nplex::rev_t nplex::storage_t::get_snapshot_rev(rev_t rev, bool le) const
{
    nplex::rev_t rev_available = 0;

    for (auto const &dir_entry : std::filesystem::directory_iterator(m_path)) 
    {
        if (!std::regex_match(dir_entry.path().c_str(), snapshot_pattern))
            continue;

        rev_t rnum = parse_rev(dir_entry.path().c_str());

        if (le)
        {
            if (rnum <= rev && rnum > rev_available)
                rev_available = rnum;
        }
        else
        {
            if (rev <= rnum && (rev_available == 0 || rnum < rev_available))
                rev_available = rnum;
        }
    }

    return rev_available;
}

std::string nplex::storage_t::read_snapshot(rev_t rev)
{
    rev_t rev_available = get_snapshot_rev(rev);

    if (rev_available == 0)
        return {};

    std::string filename = fmt::format(SNAPSHOT_FILENAME, rev_available);
    std::ifstream ifs(m_path / filename, std::ios::binary | std::ios::ate);

    if (!ifs)
        throw nplex::nplex_exception(fmt::format("Failed to open snapshot file ({})", filename));

    auto file_size = ifs.tellg();
    ifs.seekg(0, std::ios::beg);

    std::string content(static_cast<std::size_t>(file_size), '\0');
    ifs.read(&content[0], static_cast<std::streamsize>(file_size));

    auto verifier = flatbuffers::Verifier(as_uint8_ptr(content.c_str()), content.length());

    if (!verifier.VerifyBuffer<nplex::msgs::Snapshot>(nullptr))
        throw nplex_exception("Invalid snapshot file ({})", filename);

    return content;
}

nplex::rev_t nplex::storage_t::write_snapshot(const std::string_view &data) const
{
    auto verifier = flatbuffers::Verifier(as_uint8_ptr(data.data()), data.length());

    if (!verifier.VerifyBuffer<nplex::msgs::Snapshot>(nullptr))
        throw nplex_exception("Invalid snapshot data");

    auto snapshot = flatbuffers::GetRoot<nplex::msgs::Snapshot>(data.data());
    rev_t rev = snapshot->rev();

    std::string filename = fmt::format(SNAPSHOT_FILENAME, rev);
    std::ofstream ofs(m_path / filename, std::ios::binary | std::ios::trunc);

    if (!ofs)
        throw nplex::nplex_exception(fmt::format("Failed to open snapshot file ({})", filename));

    ofs.write(data.data(), static_cast<std::streamsize>(data.size()));
    if (!ofs)
        throw nplex::nplex_exception(fmt::format("Failed to write to snapshot file ({})", filename));

    return rev;
}

std::size_t nplex::storage_t::read_entries(rev_t rev, const std::function<bool(const msgs::Update *)> &func, std::size_t num)
{
    if (rev == 0 || num == 0)
        return 0;

    int rc = 0;
    std::size_t count = 0;
    std::size_t bytes = 0;
    auto start_time = uv_hrtime();
    ldb_entry_t entries[READ_BATCH_ENTRIES] = {};
    ldb_stats_t stats = {};
    std::vector<char> buf;

    if ((rc = m_journal.stats(rev, rev + std::min(num, READ_BATCH_ENTRIES) - 1, &stats)) != LDB_OK)
        throw nplex_exception("{}", ldb_strerror(rc));

    buf.resize(std::min(stats.data_size, static_cast<size_t>(READ_BATCH_BYTES)), 0);

    while (count < num)
    {
        std::size_t num_reads = 0;
        std::size_t len = std::min(READ_BATCH_ENTRIES, num - count);

        rc = m_journal.read(rev, entries, len, buf.data(), buf.size(), &num_reads);

        SPDLOG_TRACE("Read journal entries, range = [r{}, r{}], rc = {}",  
            rev, rev + num_reads - (num_reads ? 1 : 0), ldb_strerror(rc));

        if (rc != LDB_OK && rc != LDB_ERR_NOT_FOUND)
            throw nplex_exception("{}", ldb_strerror(rc));

        for (std::size_t i = 0; i < num_reads; i++)
        {
            auto verifier = flatbuffers::Verifier(as_uint8_ptr(entries[i].data), entries[i].data_len);

            if (!verifier.VerifyBuffer<nplex::msgs::Update>(nullptr))
                throw nplex_exception("Invalid journal entry (r{})", rev);

            auto update = flatbuffers::GetRoot<nplex::msgs::Update>(entries[i].data);

            assert(update->rev() == rev);

            bytes += entries[i].data_len;
            count++;
            rev++;

            if (!func(update))
                goto READ_END;
        }

        if (num_reads < len)
        {
            // case: reached end of journal
            if (entries[num_reads].seqnum == 0)
                break;

            // case: buffer too short
            if (buf.size() < 128 + entries[num_reads].data_len)
                buf.resize(128 + entries[num_reads].data_len, 0);
        }
    }

READ_END:

    uint64_t duration_us = (uv_hrtime() - start_time) / 1000;

    SPDLOG_TRACE("Journal read completed, revs={}, bytes={}, elapsed={}μs", 
        count, bytes_to_string(bytes), duration_us);

    return count;
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

    SPDLOG_TRACE("Writer thread started");

    m_running = true;

    while (m_running)
    {
        m_cond_not_empty.wait(lock, [this]() { 
            return (!m_queue.empty() || !m_running);
        });

        if (m_running && !m_queue.empty()) {
            lock.unlock();
            process_write_commands();
            lock.lock();
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

    SPDLOG_TRACE("Writer thread stopped");
}

void nplex::storage_t::process_write_commands()
{
    auto start_time = uv_hrtime();
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
            .data_len = static_cast<std::uint32_t>(it->buffer.size()),
            .data = reinterpret_cast<char *>(it->buffer.data())
        };

        m_entries.push_back(entry);
        bytes += it->buffer.size();

        it++;
    }

    lock.unlock();

    assert(!m_entries.empty());
    SPDLOG_TRACE("Writing {}/{} entries to journal ...", m_entries.size(), m_queue.size());

    ldb_rc = m_journal.append(m_entries.data(), m_entries.size(), &num_writes);

    if (ldb_rc != LDB_OK)
        throw nplex_exception("{}", ldb_strerror(ldb_rc));

    uint64_t duration_us = (uv_hrtime() - start_time) / 1000;

    SPDLOG_TRACE("Journal write completed, revs={}, bytes={}, elapsed={}μs", 
        num_writes, bytes_to_string(bytes), duration_us);

    lock.lock();

    updates.clear();

    for (size_t i = 0; i < num_writes; i++)
    {
        auto cmd = m_queue.pop();

        assert(cmd.buffer.size() <= m_bytes_in_queue);
        m_bytes_in_queue -= static_cast<std::uint32_t>(cmd.buffer.size());

        max_rev = cmd.update.meta->rev;
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

nplex::repo_t nplex::storage_t::get_repo(rev_t rev, const user_ptr &user)
{
    repo_t repo;

    std::string str = read_snapshot(rev);
    if (!str.empty())
    {
        auto snapshot = flatbuffers::GetRoot<nplex::msgs::Snapshot>(str.c_str());
        repo.load(snapshot, user);
        str = std::string{};
    }

    if (repo.rev() == rev)
        return repo;

    auto func = [&repo, rev, user](const nplex::msgs::Update *update) -> bool {
        repo.update(update, user);
        return (repo.rev() < rev);
    };

    read_entries(repo.rev() + 1, func);

    if (repo.rev() != rev)
        throw nplex_exception("Failed to load repository at revision {}", rev);

    return repo;
}
