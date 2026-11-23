#pragma once

#include "persister.hpp"
#include <filesystem>
#include <mutex>
#include <map>

namespace nplex {

struct snapshot_file_t;
using snapshot_catalog_t = std::map<rev_t, snapshot_file_t>;

/**
 * Persister that stores snapshots in a filesystem directory.
 *
 * Snapshot files are named using a revision-based pattern and include a small
 * header that identifies the file format, payload type and checksum.
 */
class filesystem_snapshot_persister_t : public snapshot_persister_t
{
  public:

    filesystem_snapshot_persister_t(const std::filesystem::path &path);
    ~filesystem_snapshot_persister_t() override;

    filesystem_snapshot_persister_t(const filesystem_snapshot_persister_t &) = delete;
    filesystem_snapshot_persister_t &operator=(const filesystem_snapshot_persister_t &) = delete;
    filesystem_snapshot_persister_t(filesystem_snapshot_persister_t &&) = delete;
    filesystem_snapshot_persister_t &operator=(filesystem_snapshot_persister_t &&) = delete;

    std::pair<rev_t, rev_t> get_snapshots_range() override;
    std::tuple<rev_t, millis_t, std::string> read_snapshot(rev_t rev) override;
    void write_snapshot(rev_t rev, millis_t timestamp, std::string_view data) override;

    void maintenance();
    bool update_catalog(bool force = false);

  private:

    std::filesystem::path m_path;
    snapshot_catalog_t m_catalog;
    mutable std::mutex m_catalog_mutex;
    millis_t m_last_catalog_update = millis_t::zero();

    std::tuple<rev_t, millis_t, std::string> read_snapshot_from_catalog(rev_t rev);
};

} // namespace nplex
