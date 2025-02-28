#pragma once

#include <map>
#include <vector>
#include <memory>
#include <limits>
#include "cqueue.hpp"
#include "common.hpp"
#include "messages.hpp"
#include "user.hpp"

namespace nplex {

struct update_t {
    meta_ptr meta;
    std::vector<std::pair<key_t, value_ptr>> upserts;
    std::vector<key_t> deletes;
};

/**
 * In-memory database content.
 * 
 * @note This class is not thread-safe.
 */
struct cache_t
{
    rev_t m_rev = 0;
    std::map<rev_t, meta_ptr> m_metas;
    std::map<key_t, value_ptr, gto::cstring_compare> m_data;
    std::map<gto::cstring, std::uint32_t, gto::cstring_compare> m_users; // value=number of updates
    gto::cqueue<key_t> m_removed_keys;   // can contain duplicates and reinserted keys

    /**
     * Load the database content from a snapshot.
     * 
     * On exception, the database is left in an inconsistent state.
     * 
     * @param[in] snapshot Content to load (nullptr reset cache).
     * 
     * @exception nplex_exception Invalid snapshot.
     */
    void load(const msgs::Snapshot *snapshot = nullptr);

    /**
     * Apply an update to the database (comming from disk or another server).
     * 
     * This method alters the cache content on success increasing the revision.
     * On exception, the database is left in an inconsistent state.
     * 
     * @param[in] msg Update to apply.
     * 
     * @return true = update done, false = no update.
     * 
     * @exception nplex_exception Invalid update (ex: update.rev < cache.rev, or invalid-key).
     */
    bool update(const msgs::Update *msg);

    /**
     * Try to commit a transaction (coming from a client).
     * 
     * This method alters the cache content on success increasing the revision.
     * On invalid or rejected request data is not modified.
     * On exception, the database is left in an inconsistent state.
     * 
     * @param[in] user User submitting the request.
     * @param[in] msg Submit request.
     * @param[out] update Update to apply (only set on accepted).
     * 
     * @return Result code.
     * 
     * @exception nplex_exception Unrecoverable error (no-memory, etc).
     */
    msgs::SubmitCode try_commit(const user_t &user, const msgs::SubmitRequest *msg, update_t &update);

    /**
     * Purge removed entries with timestamp less-than a fixed value.
     * 
     * @param[in] timestamp Timestamp (in milliseconds).
     * 
     * @return Number of entries purged.
     */
    std::uint32_t purge(millis_t timestamp = std::numeric_limits<millis_t>::max());

  private:

    /**
     * Creates a transaction metadata object.
     * Using current time and the given params.
     * Aggregating the user to user list if required.
     * 
     * @param[in] rev Revision.
     * @param[in] username User name.
     * @param[in] type Transaction type (user-defined value).
     * 
     * @return The inserted metadata.
     */
    meta_ptr create_meta(const rev_t rev, const char *username, std::uint32_t type);

    /**
     * Release a metadata decreasing the ref counters and removing 
     * entries in metas and users if required.
     * 
     * @param[in] meta Metadata to release.
     */
    void release_meta(const meta_ptr &meta);

    /**
     * Upsert an entry updating content accordingly.
     * 
     * @param[in] key Key to insert or update.
     * @param[in] value Value to set.
     * 
     * @return true = change done, false = no change.
     */
    bool upsert_entry(const char *key, const value_ptr &value);
    bool upsert_entry(const key_t &key, const value_ptr &value);

    /**
     * Delete an entry updating content accordingly.
     * 
     * @param[in] key Key to delete.
     * 
     * @return true = entry delete, false = otherwise.
     */
    bool delete_entry(const char *key);
    bool mark_as_removed(const key_t &key, const meta_ptr &meta);

    /**
     * Internal function.
     * 
     * Pre-conditions: update is empty.
     * Post-conditions: update has a meta that must be released on error.
     */
    msgs::SubmitCode try_commit_inner(const user_t &user, const msgs::SubmitRequest *msg, update_t &update);

};

using cache_ptr = std::shared_ptr<cache_t>;

} // namespace nplex
