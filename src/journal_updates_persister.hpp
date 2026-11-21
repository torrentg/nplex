#pragma once

#include "persister.hpp"
#include "params.hpp"
#include <filesystem>
#include <atomic>
#include <mutex>
#include <map>

// Forward declarations
namespace ldb {
    class journal_t;
}

namespace nplex {

struct journal_segment_t;
using journal_ptr = std::shared_ptr<ldb::journal_t>;
using journal_catalog_t = std::map<rev_t, journal_segment_t>;

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

    journal_updates_persister_t(const std::filesystem::path &path, const storage_params_t &params);
    ~journal_updates_persister_t() override;

    journal_updates_persister_t(const journal_updates_persister_t &) = delete;
    journal_updates_persister_t &operator=(const journal_updates_persister_t &) = delete;
    journal_updates_persister_t(journal_updates_persister_t &&) = delete;
    journal_updates_persister_t &operator=(journal_updates_persister_t &&) = delete;

    std::pair<nplex::rev_t, nplex::rev_t> get_updates_range() override;
    void read_updates(rev_t from_rev, const read_callback_f &on_entry) override;
    void write_updates(const std::span<update_entry_t> &entries, std::size_t &num_writes) override;

    void maintenance();
    bool rotate_at(rev_t rev);

  private:

    std::filesystem::path m_path;
    journal_ptr m_journal;
    journal_catalog_t m_catalog;
    millis_t m_last_catalog_update = millis_t::zero();
    mutable std::mutex m_catalog_mutex;
    mutable std::mutex m_journal_mutex;
    std::size_t m_rotate_max_entries = 0;
    std::size_t m_rotate_max_bytes = 0;
    std::atomic<rev_t> m_rotate_rev = 0;
    bool m_fsync = false;

    journal_ptr get_journal_from_catalog(rev_t rev);
    journal_ptr get_journal_from_catalog_with_retry(rev_t rev);
    std::pair<rev_t, rev_t> get_catalog_range() const;
    std::pair<rev_t, rev_t> get_journal_range() const;
    std::pair<std::span<update_entry_t>, bool> select_write_batch(std::span<update_entry_t> entries);
    std::size_t write_batch(std::span<update_entry_t> entries, std::size_t &num_writes);
    bool update_catalog(bool force = false);
    bool rotate(const std::string &name = {});
};

} // namespace nplex
