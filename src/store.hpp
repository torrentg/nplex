#pragma once

#include "common.hpp"
#include "schema.hpp"
#include "params.hpp"
#include "user.hpp"
#include "cqueue.hpp"
#include <vector>
#include <map>
#include <set>

namespace nplex {

/**
 * In-memory database content.
 * 
 * @note This class is not thread-safe.
 */
class store_t
{
  public: // types

    struct stats_t {
        std::size_t count = 0;        // Number of updates from last snapshot
        std::size_t bytes = 0;        // Total accumulated size of updates (approx)
    };

  public: // methods

    /**
     * Last applied update revision.
     * 
     * @return The revision number.
     */
    rev_t rev() const noexcept { return m_rev; }

    /**
     * Last applied update timestamp.
     * 
     * @return The timestamp in milliseconds since Unix epoch (UTC).
     */
    millis_t timestamp() const noexcept { return m_timestamp; }

    /**
     * Configure tombstone parameters.
     * 
     * @param[in] params Tombstone parameters.
     */
    void config(const store_params_t &params) noexcept;

    /**
     * Get info about accumulated updates since last snapshot.
     * 
     * @return The store stats.
     */
    const stats_t & stats() const noexcept { return m_stats; }

    /**
     * Reset store. statistics.
     */
    void reset_stats() noexcept { m_stats = {}; }

    /**
     * Load the database content from a snapshot.
     * 
     * On exception, the database is left in an inconsistent state.
     * 
     * @param[in] snapshot Content to load (nullptr reset content).
     * @param[in] user User whose permissions are considered during loading (nullptr means no filter).
     * 
     * @exception nplex_exception Invalid snapshot.
     */
    void load(const msgs::Snapshot *snapshot = nullptr, const user_ptr &user = nullptr);

    /**
     * Apply an update to the database (coming from disk or another server).
     * 
     * This method alters the store. content. 
     * On success, the content is updated and revision increased.
     * On exception, the content is left in an inconsistent state.
     * 
     * @param[in] msg Update to apply.
     * @param[in] user User whose permissions are considered during loading (nullptr means no filter).
     * 
     * @return The update_t object applied to the store.
     * 
     * @exception nplex_exception Invalid update (ex: update.rev < store.rev, or invalid-key).
     */
    update_t update(const msgs::Update *msg, const user_ptr &user = nullptr);

    /**
     * Try to commit a transaction (coming from a client).
     * 
     * This method alters the store. content. 
     * On success, the content is updated and revision increased.
     * On invalid or rejected request, content is not modified.
     * On exception, the content is left in an inconsistent state.
     * 
     * @param[in] user User submitting the request (mandatory).
     * @param[in] msg Submit request.
     * @param[out] upd Update to apply (only set on accepted).
     * 
     * @return Result code.
     * 
     * @exception nplex_exception Unrecoverable error (no-memory, etc).
     */
    msgs::SubmitCode try_commit(const user_ptr &user, const msgs::SubmitRequest *msg, update_t &upd);

    /**
     * Get a snapshot of the current database content filtered by the user's permissions.
     * 
     * @param[in] user User whose permissions are considered during snapshot creation (nullptr means no filter).
     * 
     * @return Snapshot of the current database content.
     */
    snapshot_t snapshot(const user_ptr &user = nullptr) const;

  private: // types

    struct tombstone_t {
        key_t key;                              // Removed key.
        rev_t rev = 0;                          // Revision when removed.
    };

    using data_map_t = std::map<key_t, stored_value_ptr, gto::cstring_compare>;   // Key-value data store
    using username_set_t = std::set<username_t, gto::cstring_compare>;            // Set of usernames
    using tombstone_queue_t = gto::cqueue<tombstone_t>;                           // Queue of removed keys

  private:  // members

    rev_t m_rev = 0;                            // Current revision.
    millis_t m_timestamp = millis_t{0};         // Revision timestamp (milliseconds since Unix epoch, UTC).
    data_map_t m_data;                          // Key-value data store.
    username_set_t m_usernames;                 // Usernames list (used to unificate user names references).
    tombstone_queue_t m_tombstones;             // List of removed keys (can contain duplicates and reinserted keys).

    rev_t m_min_rev = 0;                        // Minimum rev with guaranteed tombstone info.
    store_params_t m_params;                    // Store parameters.
    stats_t m_stats;                            // Store statistics.

  private: // methods

    /**
     * Get a unified username.
     * 
     * @param[in] username User name to unify.
     * 
     * @return Unified username.
     */
    username_t get_unified_username(std::string_view username);

    /**
     * Creates an update metadata object.
     * Aggregating the user to user list if required.
     * 
     * @param[in] rev_ Revision.
     * @param[in] username User name.
     * @param[in] type Transaction type (user-defined value).
     * @param[in] ts Timestamp of the transaction.
     * 
     * @return The inserted metadata.
     */
    meta_ptr create_meta(rev_t rev_, std::string_view username, std::uint32_t type, millis_t ts);
    
    /**
     * Mark entries as deleted.
     * 
     * Marks the specified keys as removed in the store.
     * Updates the tombstone queue.
     * 
     * @param[in] keys Keys to delete.
     * @param[in] meta Metadata of the transaction removing the keys.
     */
    void mark_as_removed(const std::vector<key_t> &keys, const meta_ptr &meta);

    /**
     * Validate an update without applying it.
     * Converts the update into an internal update structure.
     * 
     * @param[in] msg Update message.
     * @param[in] user User used to filter keys (nullptr means no filter).
     * 
     * @return The internal update structure (with meta = nullptr if no visible modifications).
     * 
     * @exception nplex_exception Invalid update (ex: update.rev < store.rev, or invalid-key).
     */
    update_t validate_update(const msgs::Update *msg, const user_ptr &user = nullptr);

    /**
     * Validate a commit request without applying it.
     * Converts the request into an internal update structure.
     * 
     * @param[in] user User submitting the request.
     * @param[in] msg Submit request.
     * @param[out] upd Update to apply.
     * 
     * Pre-conditions: update is empty.
     * Post-conditions: update has a meta that must be released on error.
     */
    msgs::SubmitCode validate_commit(const user_ptr &user, const msgs::SubmitRequest *msg, update_t &upd);

    /**
     * Internal function to apply a validated update.
     * 
     * @param[in] upd Update to apply.
     */
    void apply_update(const update_t &upd);

    /**
     * Purge removed entries based on revision age and tombstone limits.
     * 
     * Uses tombstone_min_age and tombstone_max_entries to decide which
     * tombstones can be safely discarded. Stale entries are always
     * removed. tombstone_min_age has precedence over tombstone_max_entries.
     * 
     * @return Number of tombstones removed.
     */
    std::uint32_t purge_tombstones();

};

using store_ptr = std::shared_ptr<store_t>;

} // namespace nplex
