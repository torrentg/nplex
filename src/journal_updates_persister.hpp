#pragma once

#include "persister.hpp"
#include <filesystem>
#include <optional>
#include <mutex>
#include <map>

// Forward declarations
namespace ldb {
    class journal_t;
}

namespace nplex {

using journal_ptr = std::shared_ptr<ldb::journal_t>;

/**
 * Persister that stores updates in a journal.
 * 
 * @see https://github.com/torrentg/journal
 *
 * A journal is composed of multiple sequential segments.
 *
 * New entries are always appended to the current (latest) segment.
 * When a segment reaches its size or entry limit, it is closed and a
 * new segment is created.
 *
 * Closed segments are immutable and are only accessed in read-only mode.
 * At any given time, only the latest segment is writable.
 *
 * A segment catalog is built at startup and indexed by the last
 * sequence number of each segment.
 *
 * Read workflow:
 * - Locate the segment containing the requested sequence number.
 * - If no segment is found, rescan the directory and retry.
 * - Open the segment on demand if it is not already open.
 * - If the file cannot be opened, rescan the directory and retry.
 * - Update the segment last-access time after each read.
 *
 * Maintenance:
 * - Periodically close segments that have been idle for a configurable
 *   amount of time.
 *
 * Segment metadata is kept in memory, while file descriptors are opened
 * on demand and released when no longer needed.
 */
class journal_updates_persister_t : public updates_persister_t
{
  public:

    journal_updates_persister_t(const std::filesystem::path &path, bool fsync);
    ~journal_updates_persister_t() override = default;

    journal_updates_persister_t(const journal_updates_persister_t &) = delete;
    journal_updates_persister_t &operator=(const journal_updates_persister_t &) = delete;
    journal_updates_persister_t(journal_updates_persister_t &&) = delete;
    journal_updates_persister_t &operator=(journal_updates_persister_t &&) = delete;

    std::pair<nplex::rev_t, nplex::rev_t> get_updates_range() override;
    void read_updates(rev_t from_rev, const read_callback_f &on_entry) override;
    void write_updates(const std::span<update_entry_t> &entries, std::size_t &num_writes) override;

    void maintenance();

    static journal_ptr open_journal_file(const std::filesystem::path &path, const std::string &name, int flags);
    static journal_ptr open_journal_file(const std::filesystem::path &filepath, int flags);

  private:

    struct journal_segment_t {
        std::string file_name;
        size_t file_size = 0;
        millis_t last_write_time = std::chrono::milliseconds(0);
        millis_t last_read_time = std::chrono::milliseconds(0);
        std::pair<rev_t, rev_t> range = {0, 0};
        journal_ptr journal;
    };

    std::filesystem::path m_path;
    journal_ptr m_journal;  // Current segment
    std::map<rev_t, journal_segment_t> m_catalog;
    mutable std::mutex m_catalog_mutex;
    millis_t m_last_catalog_update = millis_t::zero();
    static constexpr millis_t MAX_READ_IDLE_TIME = std::chrono::seconds(30);
    static constexpr millis_t MIN_CATALOG_UPDATE_INTERVAL = std::chrono::minutes(5);

    journal_ptr get_journal(rev_t rev);
    journal_ptr get_journal_with_retry(rev_t rev);
    std::optional<journal_segment_t> get_journal_segment(const std::filesystem::path &filepath);
    std::pair<rev_t, rev_t> get_catalog_range() const;
    std::pair<rev_t, rev_t> normalize_catalog();
    bool update_catalog(bool force = false);
};

} // namespace nplex
