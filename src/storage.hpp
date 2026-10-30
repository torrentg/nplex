#pragma once

#include "common.hpp"
#include "schema.hpp"
#include "params.hpp"
#include "store.hpp"
#include "user.hpp"
#include "persister.hpp"
#include "writer_thread.hpp"

namespace nplex {

/**
 * Methods used by the nplex server to interact with the storage backend.
 */
class storage_t
{
  public:

    using result_callback_f = writer_thread_t::result_callback_f;
    using error_callback_f = writer_thread_t::error_callback_f;

    storage_t(const storage_params_t &params);
    ~storage_t() = default;

    void set_updates_persister(const std::shared_ptr<updates_persister_t> &persister);
    void set_snapshot_persister(const std::shared_ptr<snapshot_persister_t> &persister);

    void start(result_callback_f &&result_cb, error_callback_f &&error_cb);
    void stop();

    /**
     * Returns the minimum revision that can be reconstructed.
     *
     * Execution model:
     * - Synchronous
     * - Non-blocking (expected to return cached metadata)
     */
    rev_t min_rev();

    /**
     * Returns the maximum revision currently available.
     *
     * Execution model:
     * - Synchronous
     * - Non-blocking (expected to return cached metadata)
     */
    rev_t max_rev();

    /**
     * Reads updates starting at the given revision and invokes a callback per entry.
     *
     * The callback should return true to continue and false to stop early.
     *
     * Execution model:
     * - Synchronous
     * - Blocking (may perform storage I/O)
     *
     * @param[in] from_rev First revision to read (included).
     * @param[in] on_entry Callback invoked for each read update.
     * @param[in] max_entries Maximum number of entries to read.
     */
    void read_updates(
        rev_t from_rev,
        const std::function<bool(const msgs::Update *)> &on_entry,
        std::size_t max_entries = UINT64_MAX);

    /**
     * Reconstructs the store at a given revision.
     *
     * If a user is provided, the result should be filtered by user visibility.
     *
     * Execution model:
     * - Synchronous
     * - Blocking (may perform storage I/O and replay)
     *
     * @param[in] rev Target revision.
     * @param[in] user Optional user filter.
     *
     * @return Store content at the requested revision.
     * 
     * @throws nplex_exception if the store cannot be reconstructed at the requested revision.
     */
    store_t rebuild_store_at(rev_t rev, const user_ptr &user = nullptr);

    /**
     * Reports whether the writer can currently accept more updates.
     *
     * When false, callers should apply backpressure and avoid submitting more writes.
     *
     * Execution model:
     * - Synchronous
     * - Non-blocking
     */
    bool can_accept_writes() const;

    /**
     * Submits one update for asynchronous persistence.
     *
     * Completion (success/failure) is reported through the result callback.
     *
     * Execution model:
     * - Asynchronous
     * - Non-blocking
     *
     * @param[in] upd Update to persist (ownership is transferred).
     * 
     * @return True if the update was successfully queued for persistence, false otherwise.
     */
    bool write_update_async(update_t &&upd);

    /**
     * Writes a snapshot payload.
     *
     * If a snapshot for the same revision already exists, implementations may overwrite it.
     *
     * Execution model:
     * - Synchronous
     * - Blocking (may perform storage I/O)
     *
     * @param[in] rev Snapshot revision.
     * @param[in] data Serialized snapshot payload.
     */
    void write_snapshot(rev_t rev, const std::string_view &data);

  private:

    std::shared_ptr<updates_persister_t> m_updates_persister;
    std::shared_ptr<snapshot_persister_t> m_snapshot_persister;
    writer_thread_t m_writer_thread;
};

using storage_ptr = std::shared_ptr<storage_t>;

} // namespace nplex
