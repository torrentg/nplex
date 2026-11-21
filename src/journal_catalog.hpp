#pragma once

#include "common.hpp"
#include "journal_base.hpp"
#include <filesystem>
#include <vector>
#include <map>

namespace nplex {

/**
 * Chain of read-only journal segments indexed by their maximum revision.
 *
 * The catalog does not contain the current writable journal (HEAD) file.
 * Enables efficient lookup of the segment containing a given revision.
 * File descriptors are automatically closed after an idle period.
 */
using journal_catalog_t = std::map<rev_t, journal_segment_t>;

/**
* Builds a journal catalog from the journal files in a directory.
* 
* The builder selects the shortest valid chain of journal segments connected
* to the current writable journal (head), maximizing revision coverage
* while minimizing overlaps between segments.
*
* A previously built catalog can be provided to reuse discovered metadata
* and speed up the catalog construction process.
*/
class journal_catalog_builder_t
{
  public:

    struct file_t;
    using file_ptr = std::shared_ptr<file_t>;
    using journal_ptr = std::shared_ptr<ldb::journal_t>;

    enum class status_e : std::uint8_t {
        UNASSIGNED,                                 //!< Status not yet determined.
        ORPHAN,                                     //!< Not reachable from the current journal chain.
        ACTIVE,                                     //!< Reachable from the current journal chain.
        UNUSED,                                     //!< All entries are superseded by other active segments.
        DUPLICATED,                                 //!< Exact copy of another journal under a different name.
        EMPTY,                                      //!< Journal segment with no entries.
        INVALID,                                    //!< Invalid or corrupted journal segment.
        HEAD,                                       //!< Current writable journal segment.
    };

    struct link_t
    {
        file_ptr file;                              // Upstream file (upstream.rmax + 1 <= rmin && upstream.rmax < rmax).
        std::uint32_t num_segments = 0;             // Number of segments in the chain (including this one).
        std::uint32_t num_overlaps = 0;             // Number of overlapping updates in the chain.
        rev_t min_rev = 0;                          // Minimum revision covered by the chain.
    };

    struct file_t : public journal_segment_t
    {
        file_t() = default;
        file_t(const journal_segment_t &p) : journal_segment_t(p) {}
        file_t(const std::filesystem::path &filepath) : journal_segment_t(filepath) {}
        file_ptr upstream() const { return (links.empty() ? nullptr : links[0].file); }
        rev_t chain_min_rev() const { return (links.empty() ? min_rev : links[0].min_rev); }
        std::size_t chain_num_segments() const { return 1 + (links.empty() ? 0 : links[0].num_segments); }
        std::size_t chain_num_overlaps() const { return (links.empty() ? 0 : links[0].num_overlaps); }

        std::vector<link_t> links;                  // Sorted list of upstream (lower is better).
        status_e status = status_e::UNASSIGNED;     // Status of the journal file.
    };

    using files_map_t = std::multimap<rev_t, file_ptr>;

    // Fluent api methods
    journal_catalog_builder_t& with_path(const std::filesystem::path &path);
    journal_catalog_builder_t& with_files_prefix(const std::string &prefix);
    journal_catalog_builder_t& with_files_extension(const std::string &ext);
    journal_catalog_builder_t& with_range(rev_t first_rev, rev_t last_rev);
    journal_catalog_builder_t& with_range(std::pair<rev_t, rev_t> range);
    journal_catalog_builder_t& with_overlap(bool overlap = true);

    /**
     * Build the journal catalog based on the current configuration and the previous catalog.
     * 
     * If range not set or equals {0, 0} then returns the catalog having the top update found.
     * 
     * @param[in] prev_catalog The previous catalog to consider for incremental updates.
     * 
     * @return The newly built journal catalog.
     * @exception nplex_exception If the catalog cannot be built.
     */
    journal_catalog_t build(const journal_catalog_t &prev_catalog = {});

    /**
     * Get the list of files considered during the catalog building process.
     * 
     * @return A reference to the map of files.
     */
    const files_map_t & files() const { return m_files; };

  protected:

    std::filesystem::path m_path = ".";
    std::string m_files_prefix = "journal-";
    std::string m_files_extension = ".dat";
    std::pair<rev_t, rev_t> m_range = {0, 0};
    bool m_overlap_allowed = false;
    files_map_t m_files;

    void mark_head();
    void create_links();
    void mark_empties();
    void mark_duplicates();
    void mark_orphans_and_unuseds(rev_t rev);
    void populate_files(const journal_catalog_t &prev_catalog);
    journal_catalog_t create_catalog();
};

} // namespace nplex
