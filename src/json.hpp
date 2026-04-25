#pragma once

#include <string>
#include <cstdint>

namespace nplex {

/**
 * JSON functions (for debugging/info purposes).
 * 
 * We don't use the flatbuffers json features because we need:
 *   - custom formatting options
 *   - ad-hoc binary data printing
 *   - hide password fields
 */

/**
 * Prints an update in JSON format.
 * 
 * @param[in] data Pointer to the update data (msgs::Update).
 * @param[in] len Length of the update data.
 * @param[in] mode JSON output mode ('c' for compact, 'i' for indented).
 * 
 * @return A string containing the JSON representation of the update.
 */
std::string update_to_json(const char *data, size_t len, char mode);

/**
 * Prints a snapshot in JSON format.
 * 
 * @param[in] data Pointer to the snapshot data (msgs::Snapshot).
 * @param[in] len Length of the snapshot data.
 * @param[in] mode JSON output mode ('c' for compact, 'i' for indented).
 *
 * @return A string containing the JSON representation of the snapshot.
 */
std::string snapshot_to_json(const char *data, size_t len, char mode);

} // namespace nplex
