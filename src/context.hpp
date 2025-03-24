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
#include "tasks.hpp"
#include "user.hpp"

namespace nplex {

struct context_t
{
    uv_loop_t *loop;                            // Event loop (owns the thread pool)
    std::map<std::string, user_ptr> users;      // User data
    storage_ptr storage;                        // Storage object (owns the write-thread)
    repo_t repo;                                // Repository object (the key-value map)
    std::set<session_ptr, shared_ptr_compare<session_t>> sessions; // Current sessions
    std::uint32_t m_num_running_tasks = 0;

    context_t(uv_loop_t *loop_, const params_t &params);
    ~context_t() = default;

    void submit_task(task_t *task);
};

using context_ptr = std::shared_ptr<context_t>;

} // namespace nplex
