#pragma once

#include <exception>
#include <functional>
#include <uv.h>
#include "common.hpp"
#include "storage.hpp"
#include "repository.hpp"

namespace nplex {

// Forward declarations
struct session_t;

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

    task_t() { work.data = this; }
    virtual ~task_t() = default;
    virtual void run() = 0;
    virtual void after() = 0;
};

/**
 * Retrieve the repo at rev and sends it to the session as load response.
 */
struct repo_task_t : public task_t
{
    storage_ptr &m_storage;
    session_t *m_session;
    nplex::rev_t m_rev;
    std::size_t m_cid;
    repo_t m_repo;

    repo_task_t(storage_ptr &storage, session_t *session, nplex::rev_t rev, std::size_t cid)
        : m_storage(storage), m_session(session), m_rev(rev), m_cid(cid) {}

    void run() override;
    void after() override;
};

} // namespace nplex
