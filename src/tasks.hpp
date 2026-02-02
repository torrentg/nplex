#pragma once

#include <exception>
#include <functional>
#include <uv.h>
#include "common.hpp"
#include "storage.hpp"
#include "messaging.hpp"
#include "repository.hpp"

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
 *   - task data (implemented by subclasses)
 *   - run() method required by uv_queue_work (implemented by subclasses)
 *   - after() method required by uv_queue_work() (implemented by subclasses)
 * 
 * This class is intended to be subclassed.
 */
struct task_t
{
    uv_work_t work;
    std::exception_ptr excpt;
    uint64_t start_time = 0;

    task_t() { work.data = this; }
    virtual ~task_t() = default;
    //! Task name.
    virtual const char * name() const = 0;
    //! Function executed in the thread-pool.
    virtual void run() = 0;
    //! Function executed in the event-loop thread.
    virtual void after() = 0;
};

/**
 * Create a snapshot and sends a response to the session.
 *   - Retrieve nearest snapshot from disk (filtered by user)
 *   - Apply updates until the given rev (filtered by user)
 *   - Creates the SnapshotResponse message
 *   - Sends the message over the network
 */
struct snapshot_task_t : public task_t
{
    rev_t m_rev;                        // snapshot revision
    std::size_t m_cid;                  // correlation id
    session_ptr m_session;              // session used to send messages
    nplex::repo_t m_repo;               // users repo at m_rev (result)

    snapshot_task_t(const session_ptr &session, nplex::rev_t rev, std::size_t cid)
        : m_rev(rev), m_cid(cid), m_session(session) {}

    const char * name() const override { return "snapshot_task"; }
    void run() override;
    void after() override;
};

/**
 * Synchronize the session with the repo.
 *   - Read journal entries from disk
 *   - Save non-empty user updates in a list
 *   - Sends these updates over the network
 */
struct sync_task_t : public task_t
{
    rev_t m_rev;                            // last revision sent to the session
    std::size_t m_cid;                      // correlation id
    session_ptr m_session;                  // users session
    std::uint32_t m_max_items = UINT32_MAX; // max number of updates
    std::uint32_t m_max_bytes = UINT32_MAX; // max bytes of retrieved updates
    std::vector<update_dto_t> m_updates;    // non-empty updates read (filtered by user)
    std::size_t m_bytes = 0;                // cumulated bytes (filtered by user)
    rev_t m_last_rev = 0;                   // last revision read

    sync_task_t(const session_ptr &session, nplex::rev_t rev, std::size_t cid, std::uint32_t max_items, std::uint32_t max_bytes)
        : m_rev(rev), m_cid(cid), m_session(session), m_max_items(max_items), m_max_bytes(max_bytes) {}

    const char * name() const override { return "sync_task"; }
    void run() override;
    void after() override;
};

/**
 * Write a snapshot to disk.
 */
struct write_snapshot_task_t : public task_t
{
    rev_t m_rev;                            // snapshot revision
    flatbuffers::DetachedBuffer m_buf;      // serialized snapshot buffer
    storage_ptr m_storage;                  // storage object

    write_snapshot_task_t(nplex::rev_t rev, flatbuffers::DetachedBuffer &&buf, const storage_ptr &storage)
        : m_rev(rev), m_buf(std::move(buf)), m_storage(storage) {}

    const char * name() const override { return "write_snapshot_task"; }
    void run() override;
    void after() override {}
};

} // namespace nplex
