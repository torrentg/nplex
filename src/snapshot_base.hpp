#pragma once

#include "common.hpp"
#include <filesystem>
#include <tuple>

namespace nplex {

struct snapshot_file_header_t
{
    std::uint8_t magic[8] = {0};
    std::uint32_t schema2_hash = 0;
    std::uint32_t checksum = 0;
    std::uint64_t rev = 0;
    std::uint64_t timestamp = 0;
};

/**
 * Reads the header of a snapshot file.
 * 
 * Used to get the metadata of a snapshot file without reading the entire content.
 * 
 * @param[in] path The path to the snapshot file.
 * 
 * @return The snapshot file header.
 * @exception std::exception If reading the snapshot file header fails.
 */
snapshot_file_header_t read_snapshot_file_header(const std::filesystem::path &path);

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
