#pragma once

#include <map>
#include <set>
#include <vector>
#include <memory>
#include <mutex>
#include <string>
#include <uv.h>
#include "repository.hpp"
#include "session.hpp"
#include "utils.hpp"

// Forward declarations.
namespace ldb {
  class journal_t;
}

namespace nplex {

// Forward declarations.
struct task_t;
struct user_t;
class storage_t;
struct params_t;
class journal_writer;
using user_ptr = std::shared_ptr<user_t>;
using storage_ptr = std::shared_ptr<storage_t>;

struct context_t : public std::enable_shared_from_this<context_t>
{
    uv_loop_t *loop;                            // Event loop (owns the thread pool)
    std::map<std::string, user_ptr> users;      // Users in config file (pwd, permissions, etc)
    storage_ptr storage;                        // Storage functions (journal, snapshots, etc)
    repo_t repo;                                // Repository object (the key-value map)
    std::set<session_ptr, shared_ptr_compare<session_t>> sessions; // Current sessions
    std::uint32_t m_max_sessions = 0;           // Maximum number of sessions (0 = unlimited)
    std::uint32_t m_num_running_tasks = 0;
    bool m_running = false;

    context_t(uv_loop_t *loop_, const params_t &params);
    ~context_t();

    rev_t minimum_rev() const { return m_rev_0; }
    rev_t last_persisted_rev() const { return m_rev_w; }

    void stop();
    void close();
    bool has_active_tasks_or_sessions() const { return (m_num_running_tasks != 0 || !sessions.empty()); }
    void persist(update_t &&upd);
    void submit_task(task_t *task);
    void release_task(task_t *task);
    void append_session(uv_stream_t *stream);
    void release_session(session_t *session);
    void on_updates_written_1(bool success, std::vector<update_t> &&updates);
    void on_updates_written_2();

  private:

    rev_t m_rev_0 = 0;                              // Minimum revision available
    rev_t m_rev_w = 0;                              // Last revision written to storage 

    std::unique_ptr<ldb::journal_t> m_journal;
    std::unique_ptr<journal_writer> m_journal_writer;

    // pending updates published by storage writer thread
    std::vector<update_t> m_pending_publish;
    std::mutex m_pending_publish_mutex;
    std::unique_ptr<uv_async_t> m_async_updates_written;
    std::unique_ptr<uv_async_t> m_async_stop_loop;

    void publish(const std::span<update_t> &updates);
};

using context_ptr = std::shared_ptr<context_t>;

} // namespace nplex
