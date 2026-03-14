#pragma once

#include <string>
#include <memory>
#include <filesystem>
#include "messages.hpp"
#include "common.hpp"
#include "store.hpp"
#include "user.hpp"

// Forward references
namespace ldb {
    class journal_t;
}

namespace nplex {

/** 
 * Persistent storage (disk).
 * 
 * All methods are blocking.
 * This object is accessed by multiple threads (tasks).
 * 
 * @see journal_writer
 */
class storage_t
{
  public:

    /**
     * Constructor.
     * 
     * @param[in] journal Journal object.
     * @param[in] path Storage path.
     */
    storage_t(ldb::journal_t &journal, const std::filesystem::path &path) : m_path{path}, m_journal{journal} {}
    ~storage_t() = default;

    /**
     * Returns the revision range of constructible stores.
     * 
     * @return Pair of revisions (min, max).
     * 
     * @exception nplex_exception Data dir not found, inconsistent data.
     */
    std::pair<rev_t, rev_t> get_revs_range();

    /**
     * Reads entries from rev until func returns false or last entry read.
     * 
     * This is a blocking method.
     * 
     * @param[in] rev Initial revision (included).
     * @param[in] func Function to execute on every entry.
     * @param[in] num Number of entries to read.
     * 
     * @return Number of entries read.
     * 
     * @exception nplex_exception Error reading entries.
     */
    std::size_t read_entries(rev_t rev, const std::function<bool(const msgs::Update *)> &func, std::size_t num = UINT64_MAX);

    /**
     * Reads a snapshot from disk.
     * 
     * Returns the latest snapshot available having revision less-than or 
     * equals-to the given revision.
     * 
     * This is a blocking method.
     * 
     * @param[in] rev Snapshot revision.
     * 
     * @return Snapshot binary data (empty if not available).
     * 
     * @exception nplex_exception If the file cannot be read or the content is invalid.
     */
    std::string read_snapshot(rev_t rev);

    /**
     * Writes a snapshot to disk.
     * 
     * If the snapshot already exists, it will be overwritten.
     * 
     * This is a blocking method.
     * 
     * @param[in] data Data to save to disk.
     * 
     * @return Revision of the persisted snapshot
     * 
     * @exception nplex_exception Invalid snapshot data, error writing file or invalid buffer.
     */
    rev_t write_snapshot(const std::string_view &data) const;

    /**
     * Recreates the store content at a given revision.
     * 
     * This method is blocking.
     * 
     * @param[in] rev Revision.
     * @param[in] user User used to filter store content (empty means no filter).
     * 
     * @return Store content adapted to user visibility.
     * 
     * @exception nplex_exception Error reading entries.
     */
    store_t get_store(rev_t rev, const user_ptr &user = nullptr);

  private:

    const std::filesystem::path m_path;   // Storage path.
    ldb::journal_t &m_journal;            // Journal object.
};

using storage_ptr = std::shared_ptr<storage_t>;

} // namespace nplex
