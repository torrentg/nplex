#pragma once

#include "common.hpp"
#include "schema.hpp"
#include "params.hpp"
#include "user.hpp"
#include "cqueue.hpp"
#include <map>

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
     * @param[in] user User submitting the request.
     * @param[in] msg Submit request.
     * @param[out] upd Update to apply (only set on accepted).
     * 
     * @return Result code.
     * 
     * @exception nplex_exception Unrecoverable error (no-memory, etc).
     */
    msgs::SubmitCode try_commit(const user_t &user, const msgs::SubmitRequest *msg, update_t &upd);

    /**
     * Serialize the database content to a snapshot.
     * 
     * The content is filtered according to the user's permissions.
     * 
     * @param[in] builder FlatBufferBuilder to use.
     * @param[in] user User whose permissions are considered during serialization (nullptr means no filter).
     * 
     * @return Serialized snapshot content.
     */
    fb::Offset<msgs::Snapshot> serialize(FlatBufferBuilder &builder, const user_ptr &user = nullptr) const;

  private: // types

    enum struct meta_e : std::uint8_t {
      APPEND,
      SUBTRACT
    };

    struct removed_key_t {
        key_t key;                          // Removed key.
        rev_t rev = 0;                      // Revision when removed.
    };

    using user_map_t = std::map<gto::cstring, std::uint32_t, gto::cstring_compare>;
    using data_map_t = std::map<key_t, value_ptr, gto::cstring_compare>;
    using meta_map_t = std::map<rev_t, meta_ptr>;
    using rm_queue_t = gto::cqueue<removed_key_t>;

  private:  // members

    rev_t m_rev = 0;                             // Current revision.
    millis_t m_timestamp = millis_t{0};          // Revision timestamp (milliseconds since Unix epoch, UTC).
    meta_map_t m_metas;                          // Metas indexed by revision.
    data_map_t m_data;                           // Key-value data store.
    user_map_t m_users;                          // Users list (with number of references)
    rm_queue_t m_removed_keys;                   // List of removed keys (can contain duplicates and reinserted keys)
    store_params_t m_params;                     // Store parameters.
    stats_t m_stats;                             // Store statistics.
    rev_t m_min_rev = 0;                         // Minimum rev with guaranteed tombstone info.

  private: // methods

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
    meta_ptr create_meta(rev_t rev_, const std::string_view &username, std::uint32_t type, millis_t ts);

    /**
     * Update a metadata object.
     * 
     * On append mode, adds key as new reference.
     * On subtract mode, removes key and if empty removes the meta itself.
     * 
     * @param[in] meta Metadata to update.
     * @param[in] key Key to add/remove as reference.
     * @param[in] mode Update mode.
     */
    void update_meta(const meta_ptr &meta, const key_t &key, meta_e mode);

    /**
     * Upsert an entry updating content accordingly.
     * 
     * @param[in] key Key to insert or update.
     * @param[in] value Value to set.
     * 
     * @return true = change done, false = no change.
     */
    bool upsert_entry(const key_t &key, const value_ptr &value);

    /**
     * Mark an entry as deleted.
     * 
     * Append the key to the removed keys set.
     * 
     * @param[in] key Key to delete.
     * @param[in] meta Metadata of the transaction removing the key.
     * 
     * @return true = entry delete, false = otherwise.
     */
    bool mark_as_removed(const key_t &key, const meta_ptr &meta);

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
    msgs::SubmitCode validate_commit(const user_t &user, const msgs::SubmitRequest *msg, update_t &upd);

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
    std::uint32_t purge();

};

using store_ptr = std::shared_ptr<store_t>;

} // namespace nplex
