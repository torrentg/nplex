#pragma once

#include <map>
#include <set>
#include <vector>
#include <memory>
#include <mutex>
#include <string>
#include <uv.h>
#include "cqueue.hpp"
#include "repository.hpp"
#include "session.hpp"
#include "params.hpp"
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
struct config_t;
class journal_writer;
using user_ptr = std::shared_ptr<user_t>;
using storage_ptr = std::shared_ptr<storage_t>;

struct context_t : public std::enable_shared_from_this<context_t>
{
    repo_t m_repo;                              // Repository object (the key-value map)

    context_t(uv_loop_t *loop, const config_t &config);
    ~context_t();

    rev_t minimum_rev() const { return m_rev_0; }
    rev_t last_persisted_rev() const { return m_rev_w; }
    user_ptr get_user(const std::string &name) const;
    storage_ptr storage() const { return m_storage; }
    const repo_t & repo() const { return m_repo; }
    bool has_active_tasks_or_sessions() const { return (m_num_running_tasks != 0 || !m_sessions.empty()); }

    void open();
    void close();
    void persist(update_t &&upd);
    void submit_task(task_t *task);
    void release_task(task_t *task);
    void append_session(uv_stream_t *stream);
    void release_session(session_t *session);
    void on_updates_written_1(bool success, std::vector<update_t> &&updates);
    void on_updates_written_2();
    std::tuple<msgs::SubmitCode, rev_t> try_commit(const msgs::SubmitRequest *msg, const user_t &user);
    std::span<const update_t> get_cached_updates(rev_t from_rev, std::size_t max_bytes) const;

  private: // types

    using user_map_t = std::map<std::string, user_ptr>;
    using session_set_t = std::set<session_ptr, shared_ptr_compare<session_t>>;

  private: // members

    uv_loop_t *m_loop;                                  // Reference to the event loop
    storage_ptr m_storage;                              // Storage functions (journal, snapshots, etc)
    std::unique_ptr<ldb::journal_t> m_journal;          // Journal object
    std::unique_ptr<journal_writer> m_journal_writer;   // Journal writer thread
    std::vector<update_t> m_pending_publish;            // Updates waiting to be published
    std::mutex m_pending_publish_mutex;                 // Mutex for pending publish updates
    std::unique_ptr<uv_async_t> m_async_updates_written;// Async handle for updates written notification
    std::unique_ptr<uv_async_t> m_async_stop_loop;      // Async handle to stop the loop
    context_params_t m_params;                          // Context parameters (network + snapshot)
    session_set_t m_sessions;                           // Set of current sessions
    user_map_t m_users;                                 // Users in config file (pwd, permissions, etc)
    std::uint32_t m_num_running_tasks = 0;              // Number of running tasks
    bool m_running = false;                             // Event loop is running and nplex is operational
    rev_t m_rev_0 = 0;                                  // Minimum revision available
    rev_t m_rev_w = 0;                                  // Last revision written to storage 
    gto::cqueue<update_t> m_cache;                      // Cached updates in revision order
    std::size_t m_cache_bytes = 0;                      // Approximate serialized size of cached updates

  private: // methods

    void stop();                                        // Stops the event loop
    void publish(const std::span<update_t> &updates);   // Publishes updates to all sessions
    void check_for_snapshot();                          // Checks if a new snapshot is needed
    void update_cache(const std::span<update_t> &);     // Update in-memory cache with new commits
};

using context_ptr = std::shared_ptr<context_t>;

} // namespace nplex
