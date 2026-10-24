#pragma once

#include "common.hpp"
#include <cstddef>
#include <cstdint>
#include <exception>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <span>

namespace nplex {

/**
 * Methods to be implemented by the updates persister.
 *
 * Execution model:
 *   - Synchronous
 *   - Blocking
 */
class updates_persister_t
{
  public:

    typedef struct update_entry_t
    {
        std::uint64_t seqnum;               // Sequence number.
        std::uint32_t data_len;             // Length of data (in bytes).
        void *data;                         // Pointer to data.
    } update_entry_t;

    using read_callback_f = std::function<bool(const update_entry_t &)>;

    virtual ~updates_persister_t() = default;

    /**
     * Returns the range of available updates.
     * 
     * @throw std::exception on error.
     */
    virtual std::pair<nplex::rev_t, nplex::rev_t> get_updates_range() = 0;

    /**
     * Reads updates starting at the given revision and invokes a callback per entry.
     *
     * The callback should return true to continue and false to stop early.
     *
     * @param[in] from_rev First revision to read (included).
     * @param[in] on_entry Callback invoked for each read update.
     * @param[in] max_entries Maximum number of entries to read.
     *
     * @throw std::exception on read error.
     */
    virtual void read_updates(rev_t from_rev, const read_callback_f &on_entry, std::size_t max_entries = UINT64_MAX) = 0;

    /**
     * Writes a batch of updates.
     *
     * Fails if there is a duplicate or a gap in the sequence numbers.
     * 
     * @param[in] entries Updates to write.
     * @param[out] num_writes Number of updates successfully written.
     *
     * @throw std::exception on write error.
     */
    virtual void write_updates(const std::span<update_entry_t> &entries, std::size_t &num_writes) = 0;
};

/**
 * Methods to be implemented by the snapshot persister.
 *
 * Execution model:
 *   - Synchronous
 *   - Blocking
 */
class snapshot_persister_t
{
  public:

    virtual ~snapshot_persister_t() = default;

    /**
     * Returns the range of available snapshots.
     * 
     * @throw std::exception on error.
     */
    virtual std::pair<nplex::rev_t, nplex::rev_t> get_snapshots_range() = 0;

    /**
     * Reads the latest snapshot with revision less-than-or-equal to the requested revision.
     *
     * Returns an empty string when no snapshot is available.
     *
     * @param[in] rev Target revision.
     *
     * @return Serialized snapshot payload.
     * 
     * @throw std::exception on read error.
     */
    virtual std::string read_snapshot(rev_t rev) = 0;

    /**
     * Writes a snapshot for the given revision.
     * 
     * If snapshot for the given revision already exists, it may be overwritten.
     *
     * @param[in] rev Target revision.
     * @param[in] data Serialized snapshot payload.
     *
     * @throw std::exception on write error.
     */
    virtual void write_snapshot(const rev_t rev, const std::string_view &data) = 0;
};

using updates_persister_ptr = std::shared_ptr<updates_persister_t>;
using snapshot_persister_ptr = std::shared_ptr<snapshot_persister_t>;

} // namespace nplex
