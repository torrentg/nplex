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
    virtual const char * name() const = 0;
    virtual void run() = 0;
    virtual void after() = 0;

    // Never calls this method from run() because it is executed in the thread-pool.
    // It is safe to call it in after() because it is executed in the event-loop thread.
    server_t * get_server() const { return static_cast<server_t *>(work.loop->data); }
};

/**
 * Retrieve the repo at rev and sends it to the session as load response.
 */
struct repo_task_t : public task_t
{
    storage_ptr &m_storage;
    session_t *m_session;
    rev_t m_rev;
    load_builder_t m_builder;
    flatbuffers::Offset<nplex::msgs::Snapshot>  m_offset;

    repo_task_t(storage_ptr &storage, session_t *session, nplex::rev_t rev, std::size_t cid)
        : m_storage(storage), m_session(session), m_rev(rev), m_builder(cid) {}

    const char * name() const override { return "repo_task"; }
    void run() override;
    void after() override;
};

} // namespace nplex
