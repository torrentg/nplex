#pragma once

#include <map>
#include <set>
#include <memory>
#include <string>
#include <uv.h>
#include "repository.hpp"
#include "session.hpp"
#include "storage.hpp"
#include "utils.hpp"
#include "user.hpp"

namespace nplex {

// Forward declarations.
struct task_t;

struct context_t : public std::enable_shared_from_this<context_t>
{
    uv_loop_t *loop;                            // Event loop (owns the thread pool)
    std::map<std::string, user_ptr> users;      // User data
    storage_ptr storage;                        // Storage object (owns the write-thread)
    repo_t repo;                                // Repository object (the key-value map)
    std::set<session_ptr, shared_ptr_compare<session_t>> sessions; // Current sessions
    std::uint32_t m_max_sessions = 0;           // Maximum number of sessions (0 = unlimited)
    std::uint32_t m_num_running_tasks = 0;
    bool m_running = false;

    context_t(uv_loop_t *loop_, const params_t &params);
    ~context_t() = default;

    rev_t rev() const { return repo.rev(); }

    void submit_task(task_t *task);
    void release_task(task_t *task);
    void append_session(uv_stream_t *stream);
    void release_session(session_t *session);
    void publish(const std::span<update_t> &updates);
};

using context_ptr = std::shared_ptr<context_t>;

} // namespace nplex
