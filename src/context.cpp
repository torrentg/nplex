#include <spdlog/spdlog.h>
#include <fmt/core.h>
#include <fmt/ranges.h>
#include "journal.h"
#include "journal_writer.hpp"
#include "exception.hpp"
#include "storage.hpp"
#include "params.hpp"
#include "tasks.hpp"
#include "user.hpp"
#include "context.hpp"

// ==========================================================
// Internal (static) functions
// ==========================================================

static bool is_valid_user(const nplex::user_t &user)
{
    if (!user.active)
        return false;

    if (user.password.empty()) {
        SPDLOG_WARN("User {} discarded (no password)", user.name);
        return false;
    }

    if (user.timeout_factor != 0.0f && user.timeout_factor <= 1.0f) {
        SPDLOG_WARN("User {} discarded (invalid timeout factor)", user.name);
        return false;
    }

    int sum = 0;

    for (const auto &perm : user.permissions)
        sum += perm.mode;

    if (sum == 0) {
        SPDLOG_WARN("User {} discarded (no acls)", user.name);
        return false;
    }

    return true;
}

static std::map<std::string, nplex::user_ptr> create_users(const nplex::params_t &params)
{
    using namespace nplex;

    std::map<std::string, nplex::user_ptr> ret;
    std::vector<std::string> valid_users;

    for (const auto &user : params.users)
    {
        if (!::is_valid_user(user))
            continue;

        ret[user.name] = std::make_shared<user_t>(user);
        valid_users.push_back(user.name);
    }

    if (ret.empty())
        throw nplex_exception("No valid users found");

    SPDLOG_INFO("Users: [{}]", fmt::join(valid_users, ", "));

    return ret;
}

static void cb_task_run(uv_work_t *req)
{
    assert(req->data);
    auto task = static_cast<nplex::task_t *>(req->data);

    try {
        task->run();
    }
    catch (const std::exception &e) {
        SPDLOG_ERROR("Task {} error: {}", task->name(), e.what());
        task->excpt = std::current_exception();
    }
}

static void cb_task_after(uv_work_t *req, int status)
{
    assert(req->data);
    auto task = static_cast<nplex::task_t *>(req->data);

    assert(req->loop->data);
    auto context = static_cast<nplex::context_t *>(req->loop->data);

    uint64_t duration_us = (uv_hrtime() - task->start_time) / 1000;

    SPDLOG_TRACE("Task {}: duration = {} μs", task->name(), duration_us);

    if (status == UV_ECANCELED) {
        SPDLOG_DEBUG("Task {} cancelled", task->name());
        goto END;
    }

    if (status != 0) {
        SPDLOG_ERROR("Task {} error: {}", task->name(), uv_strerror(status));
        goto ERR;
    }

    if (task->excpt) {
        // Error was already traced in cb_task_run()
        goto ERR;
    }

    try {
        task->after();
        goto END;
    }
    catch (const std::exception &e) {
        SPDLOG_ERROR("Task {} exception: {}", task->name(), e.what());
        goto ERR;
    }

ERR:
    uv_stop(req->loop);
END:
    context->release_task(task);
    delete task;
}

static void cb_async_stop_loop(uv_async_t *handle)
{
    uv_stop(handle->loop);
}

static void cb_async_updates_result(uv_async_t *handle)
{
    assert(handle->data);
    auto context = static_cast<nplex::context_t *>(handle->data);
    context->on_updates_written_2();
}

// ==========================================================
// context_t methods
// ==========================================================

nplex::context_t::context_t(uv_loop_t *loop, const params_t &params) : m_loop(loop)
{
    m_users = ::create_users(params);

    m_max_sessions = (params.max_connections ? params.max_connections : UINT32_MAX);
    m_max_updates_between_snapshots = (params.max_updates_between_snapshots ? params.max_updates_between_snapshots : UINT32_MAX);
    m_max_bytes_between_snapshots = (params.max_bytes_between_snapshots ? params.max_bytes_between_snapshots : UINT32_MAX);

    auto path = params.datadir;

    if (!fs::exists(path))
        throw nplex::nplex_exception(fmt::format("Storage directory does not exist ({})", path.string()));

    if (!fs::is_directory(path))
        throw nplex::nplex_exception(fmt::format("Invalid storage directory ({})", path.string()));

    m_journal = std::make_unique<ldb::journal_t>(path, JOURNAL_NAME, params.check_journal);
    m_journal->set_fsync(!params.disable_fsync);

    m_storage = std::make_shared<storage_t>(*m_journal, path);

    m_journal_writer = std::make_unique<journal_writer>(*m_journal, params);

    std::tie(m_rev_0, m_rev_w) = m_storage->get_revs_range();
    SPDLOG_INFO("Data range: [r{}, r{}]", m_rev_0, m_rev_w);

    m_repo = m_storage->get_repo(m_rev_w);

    // async handle to stop the loop
    m_async_stop_loop = std::make_unique<uv_async_t>();
    m_async_stop_loop->data = this;
    uv_async_init(m_loop, m_async_stop_loop.get(), cb_async_stop_loop);

    // async handle to process updates written by storage thread
    m_async_updates_written = std::make_unique<uv_async_t>();
    m_async_updates_written->data = this;
    uv_async_init(m_loop, m_async_updates_written.get(), cb_async_updates_result);

    // start journal writing thread
    m_journal_writer->start(
        [this](bool success, std::vector<update_t> &&updates) {
            on_updates_written_1(success, std::move(updates));
        },
        [this](const std::exception_ptr &ex) {
            try {
                if (ex) std::rethrow_exception(ex);
            } catch (const std::exception &e) {
                SPDLOG_ERROR("Exception in journal writer: {}", e.what());
            }
            stop();
        }
    );
}

nplex::context_t::~context_t()
{
    // this method is defined in the compilation unit to avoid incomplete type errors
    // nothing to do explicitly
}

void nplex::context_t::open()
{
    m_running = true;
}

void nplex::context_t::close()
{
    m_running = false;

    // closing all sessions
    for (auto &session : m_sessions)
        session->disconnect(ERR_CLOSED_BY_LOCAL);

    // stopping the journal writing thread
    if (m_journal_writer)
        m_journal_writer->stop();

    // release libuv resources
    if (m_async_updates_written)
        uv_close(reinterpret_cast<uv_handle_t *>(m_async_updates_written.get()), nullptr);

    if (m_async_stop_loop)
        uv_close(reinterpret_cast<uv_handle_t *>(m_async_stop_loop.get()), nullptr);
}

void nplex::context_t::stop()
{
    if (m_async_stop_loop)
        uv_async_send(m_async_stop_loop.get());
}

void nplex::context_t::persist(update_t &&upd)
{
    if (!m_running)
        return;

    if (m_journal_writer)
        m_journal_writer->write(std::move(upd));
}

void nplex::context_t::submit_task(task_t *task)
{
    assert(task);

    if (!m_running) {
        delete task;
        return;
    }

    task->start_time = uv_hrtime();
    m_num_running_tasks++;

    int rc = 0;
    if ((rc = uv_queue_work(m_loop, &task->work, ::cb_task_run, ::cb_task_after)) != 0)
        throw std::runtime_error(uv_strerror(rc));

}

void nplex::context_t::release_task(task_t *)
{
    assert(m_num_running_tasks);
    m_num_running_tasks--;

    if (!m_running && !m_num_running_tasks)
        uv_stop(m_loop);
}

void nplex::context_t::append_session(uv_stream_t *stream)
{
    assert(stream);

    if (!m_running)
        return;

    if (m_sessions.size() >= m_max_sessions) {
        SPDLOG_WARN("Incomming connection rejected (max {} clients reached)", m_max_sessions);
        return;
    }

    auto session = std::make_shared<session_t>(shared_from_this(), stream);
    SPDLOG_DEBUG("New connection: {}", session->id());

    m_sessions.insert(session);
}

void nplex::context_t::release_session(session_t *session)
{
    if (!session)
        return;

    auto it = m_sessions.find(session);

    if (it == m_sessions.end())
        return;

    if (session->user())
        SPDLOG_INFO("Session closed: {} ({})", session->id(), session->strerror());
    else
        SPDLOG_DEBUG("Connection closed: {} ({})", session->id(), session->strerror());

    if (session->user() && session->user()->num_connections > 0)
        session->user()->num_connections--;

    m_sessions.erase(it);

    if (!m_running && m_sessions.empty())
        uv_stop(m_loop);
}

void nplex::context_t::on_updates_written_1(bool success, std::vector<update_t> &&updates)
{
    if (!success) {
        auto min_rev = updates.empty() ? 0 : updates.front().meta->rev;
        auto max_rev = updates.empty() ? 0 : updates.back().meta->rev;
        SPDLOG_ERROR("Unable to write updates r{}-r{} to journal", min_rev, max_rev);
        stop();
        return;
    }

    {
        std::lock_guard<std::mutex> lock(m_pending_publish_mutex);

        m_pending_publish.insert(m_pending_publish.end(),
            std::make_move_iterator(updates.begin()),
            std::make_move_iterator(updates.end()));
    }

    if (m_async_updates_written)
        uv_async_send(m_async_updates_written.get());
}

void nplex::context_t::on_updates_written_2()
{
    std::vector<update_t> updates;

    {
        std::lock_guard<std::mutex> lock(m_pending_publish_mutex);
        updates.swap(m_pending_publish);
    }

    if (updates.empty())
        return;

    assert(m_rev_w < updates.front().meta->rev);
    m_rev_w = updates.back().meta->rev;

    publish(updates);
}

void nplex::context_t::publish(const std::span<update_t> &updates)
{
    for (auto &session : m_sessions)
        session->push_changes(updates);
}

nplex::user_ptr nplex::context_t::get_user(const std::string &name) const
{
    auto it = m_users.find(name);

    if (it == m_users.end())
        return nullptr;

    return it->second;
}

std::tuple<nplex::msgs::SubmitCode, nplex::rev_t> nplex::context_t::try_commit(const msgs::SubmitRequest *msg, const user_t &user)
{
    update_t update;

    auto rc = m_repo.try_commit(user, msg, update);
    if (rc != msgs::SubmitCode::ACCEPTED) {
        SPDLOG_DEBUG("Commit rejected: user={}, crev={}, rc={}", user.name, msg->crev(), static_cast<int>(rc));  
        return { rc, 0 };
    }

    assert(m_repo.rev() == update.meta->rev);
    SPDLOG_DEBUG("Commit accepted: rev={}, user={}, type={}", update.meta->rev, update.meta->user.c_str(), update.meta->type);

    auto erev = update.meta->rev;
    persist(std::move(update));

    // TODO: check for snapshot
    // TODO: repo cleanup (purge)

    return { rc, erev };
}
