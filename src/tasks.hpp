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
struct session_t;
class server_t;

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

    // Never calls this method from run() because it is executed in the thread-pool.
    // It is safe to call it in after() because it is executed in the event-loop thread.
    server_t * get_server() const { return static_cast<server_t *>(work.loop->data); }
};

/**
 * Create and sends a snapshot.
 *   - Retrieve nearest snapshot from disk
 *   - Apply updates until the given rev
 *   - Creates the LoadResponse message
 *   - Sends the message over the network
 */
struct repo_task_t : public task_t
{
    storage_ptr m_storage;              // storage object
    session_t *m_session;               // session used to send messages
    std::size_t m_cid;                  // correlation id
    rev_t m_rev;                        // snapshot revision
    flatbuffers::DetachedBuffer m_buf;  // serialized message

    repo_task_t(storage_ptr &storage, session_t *session, nplex::rev_t rev, std::size_t cid)
        : m_storage(storage), m_session(session), m_cid(cid), m_rev(rev) {}

    const char * name() const override { return "repo_task"; }
    void run() override;
    void after() override;
};

/**
 * Synchronize the session with the repo.
 *   - Read journal entries from disk
 *   - Create some ChangesPush messages
 *   - Sends the messages over the network
 */
struct sync_task_t : public task_t
{
    storage_ptr m_storage;              // storage object
    session_t *m_session;               // session used to send messages
    std::size_t m_cid;                  // correlation id
    rev_t m_rev;                        // last revision
    std::uint32_t m_changes_max_revs;   // max number of revisions in a single Changes message
    std::uint32_t m_changes_max_bytes;  // max bytes in a single Changes message
    std::uint32_t m_max_msgs;           // max number of Changes messages
    std::uint32_t m_max_bytes;          // max bytes of generated Changes messages

    changes_builder_t m_builder;        // Changes messages builder
    std::vector<flatbuffers::DetachedBuffer> m_buffers;  // Changes messages to send

    const char * name() const override { return "sync_task"; }
    void run() override;
    void after() override;

    // Functor used in storage_t::read_entries()
    bool operator()(const nplex::msgs::Update *update);
};

} // namespace nplex
