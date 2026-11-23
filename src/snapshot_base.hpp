#pragma once

#include "common.hpp"
#include <filesystem>
#include <tuple>

namespace nplex {

struct snapshot_file_t
{
    snapshot_file_t() = default;
    snapshot_file_t(const std::filesystem::path &filepath);

    std::string file_name;                          // Name of the snapshot file.
    size_t file_size = 0;                           // Size of the snapshot file in bytes (used to detect file changes).
    std::filesystem::file_time_type last_write;     // Last write time of the snapshot file (used to detect file changes).
    rev_t rev = 0;                                  // Snapshot revision.
    millis_t timestamp = millis_t{0};               // Snapshot timestamp.
};

/**
 * Reads a snapshot file from the specified path.
 * 
 * This function hides the internal details of the snapshot reading process.
 * 
 * @param[in] path The path to the snapshot file.
 * 
 * @return A tuple containing the revision, timestamp, and snapshot data.
 * @exception std::exception If reading from the snapshot file fails.
 */
std::tuple<rev_t, millis_t, std::string> read_snapshot_file(const std::filesystem::path &path);

/**
 * Writes a snapshot file to the specified path.
 * 
 * This function hides the internal details of the snapshot writing process.
 * 
 * @param[in] rev The revision of the snapshot.
 * @param[in] timestamp The timestamp of the snapshot.
 * @param[in] path The path to the snapshot file.
 * @param[in] data The snapshot data to write.
 * 
 * @exception std::exception If writing to the snapshot file fails.
 */
void write_snapshot_file(rev_t rev, millis_t timestamp, const std::filesystem::path &path, std::string_view data);

} // namespace nplex
