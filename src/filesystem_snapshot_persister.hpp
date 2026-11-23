#pragma once

#include "persister.hpp"
#include <filesystem>

namespace nplex {

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
    ~filesystem_snapshot_persister_t() override = default;

    filesystem_snapshot_persister_t(const filesystem_snapshot_persister_t &) = delete;
    filesystem_snapshot_persister_t &operator=(const filesystem_snapshot_persister_t &) = delete;
    filesystem_snapshot_persister_t(filesystem_snapshot_persister_t &&) = delete;
    filesystem_snapshot_persister_t &operator=(filesystem_snapshot_persister_t &&) = delete;

    std::pair<rev_t, rev_t> get_snapshots_range() override;
    std::tuple<rev_t, millis_t, std::string> read_snapshot(rev_t rev) override;
    void write_snapshot(rev_t rev, millis_t timestamp, std::string_view data) override;

  private:

    std::filesystem::path m_path;
};

} // namespace nplex
