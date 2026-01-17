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
 * Create and sends a snapshot.
 *   - Retrieve nearest snapshot from disk
 *   - Apply updates until the given rev
 *   - Creates the LoadResponse message
 *   - Sends the message over the network
 */
struct repo_task_t : public task_t
{
    storage_ptr m_storage;              // storage object
    session_ptr m_session;              // session used to send messages
    std::size_t m_cid;                  // correlation id
    rev_t m_rev;                        // snapshot revision
    flatbuffers::DetachedBuffer m_buf;  // serialized message

    repo_task_t(const storage_ptr &storage, const session_ptr &session, nplex::rev_t rev, std::size_t cid)
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
    session_ptr m_session;              // session used to send messages
    rev_t m_rev;                        // last revision sent to the session
    std::uint32_t m_max_msgs;           // max number of Changes messages
    std::uint32_t m_max_bytes;          // max bytes of generated Changes messages

    changes_builder_t m_builder;        // Changes messages builder
    std::vector<flatbuffers::DetachedBuffer> m_buffers;  // Changes messages to send
    std::size_t m_bytes = 0;            // bytes of generated Changes messages

    sync_task_t(const storage_ptr &storage, const session_ptr &session, nplex::rev_t rev, std::uint32_t max_msgs, std::uint32_t max_bytes)
        : m_storage(storage), m_session(session), m_rev(rev), m_max_msgs(max_msgs), m_max_bytes(max_bytes) {}

    const char * name() const override { return "sync_task"; }
    void run() override;
    void after() override;

    void config_builder(std::size_t cid, std::uint32_t changes_max_revs, std::uint32_t changes_max_bytes);
    bool append_update(const nplex::msgs::Update *update);
};

} // namespace nplex
