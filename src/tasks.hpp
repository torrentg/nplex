#pragma once

#include "common.hpp"
#include "storage.hpp"
#include "messaging.hpp"
#include "store.hpp"
#include <uv.h>
#include <functional>
#include <exception>

namespace nplex {

// Forward declarations
class session_t;
using session_ptr = std::shared_ptr<session_t>;

/**
 * Wrapper for a uv_work_t to be executed in the event loop.
 * 
 * This object contains:
 *   - uv_work_t object required by libuv
 *   - exception (if an exception is thrown)
 *   - callback function to be called after task completion
 *   - task data (implemented by subclasses)
 *   - run() method required by uv_queue_work (implemented by subclasses)
 * 
 * This class is intended to be subclassed.
 */
struct task_t
{
    using callback_f = std::function<void(task_t *)>;

    uv_work_t work;
    std::exception_ptr excpt;
    uint64_t start_time = 0;
    callback_f callback;

    task_t() { work.data = this; }
    virtual ~task_t() = default;
    //! Task name.
    virtual const char * name() const = 0;
    //! Function executed in the thread-pool.
    virtual void run() = 0;
};

/**
 * Builds the snapshot at rev for a given user.
 *   - Retrieve nearest snapshot from disk (filtered by user)
 *   - Apply updates until the given rev (filtered by user)
 *   - Creates the SnapshotResponse message
 *   - Sends the message over the network
 */
struct build_snapshot_task_t : public task_t
{
    rev_t rev = 0;                          // Snapshot revision
    user_ptr user;                          // User (used to filter content)
    storage_ptr storage;                    // Storage object (data access)
    snapshot_response_builder_t builder;    // Builder for the response message

    build_snapshot_task_t(nplex::rev_t rev_, user_ptr user_, storage_ptr storage_);

    const char * name() const override { return "build_snapshot_task"; }
    void run() override;
};

/**
 * Read updates from rev for a given user.
 *   - Read updates from disk
 *   - Save non-empty user updates in a list
 *   - Sends these updates over the network
 */
struct read_updates_task_t : public task_t
{
    rev_t rev = 0;                          // First revision to read
    user_ptr user;                          // User (used to filter content)
    storage_ptr storage;                    // Storage object (data access)
    std::uint32_t max_msg_bytes = 0;        // Max bytes per message
    std::uint32_t max_total_bytes = 0;      // Max cumulated bytes to be sent
    std::size_t total_bytes = 0;            // Cumulated bytes (filtered by user)
    rev_t last_rev = 0;                     // Last revision read
    std::vector<updates_push_builder_t> msgs;

    read_updates_task_t(rev_t rev_, user_ptr user_, storage_ptr storage_, std::uint32_t max_msg_bytes_ = 0, std::uint32_t max_total_bytes_ = 0);

    const char * name() const override { return "read_updates_task"; }
    void run() override;
};

/**
 * Save a snapshot to disk.
 */
struct save_snapshot_task_t : public task_t
{
    rev_t rev;                              // Snapshot revision
    millis_t timestamp;                     // Snapshot timestamp
    DetachedBuffer buf;                     // Serialized snapshot buffer
    storage_ptr storage;                    // Storage object

    save_snapshot_task_t(rev_t rev_, millis_t timestamp_, DetachedBuffer &&buf_, storage_ptr storage_);

    const char * name() const override { return "save_snapshot_task"; }
    void run() override;
};

} // namespace nplex
