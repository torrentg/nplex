#pragma once

#include "common.hpp"
#include "snapshot_base.hpp"
#include <filesystem>
#include <vector>
#include <map>

namespace nplex {

/**
 * List of snapshot files indexed by their revision.
 */
using snapshot_catalog_t = std::map<rev_t, snapshot_file_t>;

/**
* Builds a snapshot catalog from the snapshot files in a directory.
* 
* The builder selects the snapshots in the given revision range.
*
* A previously built catalog can be provided to reuse discovered metadata
* and speed up the catalog construction process.
*/
class snapshot_catalog_builder_t
{
  public:

    // Fluent api methods
    snapshot_catalog_builder_t& with_path(const std::filesystem::path &path);
    snapshot_catalog_builder_t& with_files_prefix(const std::string &prefix);
    snapshot_catalog_builder_t& with_files_extension(const std::string &ext);
    snapshot_catalog_builder_t& with_range(rev_t first_rev, rev_t last_rev);
    snapshot_catalog_builder_t& with_range(std::pair<rev_t, rev_t> range);

    /**
     * Build the snapshot catalog based on the current configuration and the previous catalog.
     * 
     * If range not set or equals {0, 0} then returns the catalog having the top update found.
     * 
     * @param[in] prev_catalog The previous catalog to consider for incremental updates.
     * 
     * @return The newly built snapshot catalog.
     * @exception nplex_exception If the catalog cannot be built.
     */
    snapshot_catalog_t build(const snapshot_catalog_t &prev_catalog = {});

  protected:

    std::filesystem::path m_path = ".";
    std::string m_files_prefix = "snapshot-";
    std::string m_files_extension = ".dat";
    std::pair<rev_t, rev_t> m_range = {0, 0};
};

} // namespace nplex
