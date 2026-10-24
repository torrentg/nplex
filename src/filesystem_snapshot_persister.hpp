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

    std::pair<nplex::rev_t, nplex::rev_t> get_snapshots_range() override;
    std::string read_snapshot(rev_t rev) override;
    void write_snapshot(rev_t rev, const std::string_view &data) override;

    static std::string read_snapshot_file(const std::filesystem::path &path);
    static void write_snapshot_file(const std::filesystem::path &path, const std::string_view &data);

  private:

    std::filesystem::path m_path;
};

} // namespace nplex
