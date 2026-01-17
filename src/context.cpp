#include <spdlog/spdlog.h>
#include <fmt/core.h>
#include <fmt/ranges.h>
#include "exception.hpp"
#include "context.hpp"
#include "tasks.hpp"

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

    SPDLOG_DEBUG("Task {}: duration = {} μs", task->name(), duration_us);

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

static void cb_async_updates_written(uv_async_t *handle)
{
    assert(handle->data);
    auto context = static_cast<nplex::context_t *>(handle->data);
    context->on_updates_written_2();
}

// ==========================================================
// context_t methods
// ==========================================================

nplex::context_t::context_t(uv_loop_t *loop_, const params_t &params) : loop(loop_)
{
    users = ::create_users(params);
    m_max_sessions = params.max_connections;
    storage = std::make_shared<storage_t>(params);
    auto [min_rev, max_rev] = storage->get_range();
    repo = storage->get_repo(max_rev);

    // async handle to process updates written by storage thread
    m_async_updates_written = (uv_async_t *) malloc(sizeof(uv_async_t));
    m_async_updates_written->data = this;
    uv_async_init(loop, m_async_updates_written, cb_async_updates_written);
    // will be deleted on loop close

    storage->set_writer_callback([this](bool success, std::vector<update_t> &&updates) {
        on_updates_written_1(success, std::move(updates));
    });
}

nplex::context_t::~context_t()
{
    storage->set_writer_callback(nullptr);

    if (m_async_updates_written) {
        uv_close(reinterpret_cast<uv_handle_t *>(m_async_updates_written), (uv_close_cb) free);
        m_async_updates_written = nullptr;
    }
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
    if ((rc = uv_queue_work(loop, &task->work, ::cb_task_run, ::cb_task_after)) != 0)
        throw std::runtime_error(uv_strerror(rc));

}

void nplex::context_t::release_task(task_t *)
{
    assert(m_num_running_tasks);
    m_num_running_tasks--;

    if (!m_running && !m_num_running_tasks)
        uv_stop(loop);
}

void nplex::context_t::append_session(uv_stream_t *stream)
{
    assert(stream);

    if (!m_running)
        return;

    if (m_max_sessions && sessions.size() >= m_max_sessions) {
        SPDLOG_WARN("Incomming connection rejected (max {} clients reached)", m_max_sessions);
        return;
    }

    auto session = std::make_shared<session_t>(shared_from_this(), stream);
    SPDLOG_DEBUG("New connection: {}", session->id());

    sessions.insert(session);
}

void nplex::context_t::release_session(session_t *session)
{
    if (!session)
        return;

    auto it = sessions.find(session);

    if (it == sessions.end())
        return;

    if (session->user())
        SPDLOG_INFO("Session closed: {} ({})", session->id(), session->strerror());
    else
        SPDLOG_DEBUG("Connection closed: {} ({})", session->id(), session->strerror());

    if (session->user() && session->user()->num_connections > 0)
        session->user()->num_connections--;

    sessions.erase(it);

    if (!m_running && sessions.empty())
        uv_stop(loop);
}

void nplex::context_t::on_updates_written_1(bool success, std::vector<update_t> &&updates)
{
    if (!success) {
        SPDLOG_ERROR("Error writing to journal");
        // TODO: stop the event loop
        return;
    }

    {
        std::lock_guard<std::mutex> lock(m_pending_publish_mutex);

        m_pending_publish.insert(m_pending_publish.end(),
            std::make_move_iterator(updates.begin()),
            std::make_move_iterator(updates.end()));
    }

    if (m_async_updates_written)
        uv_async_send(m_async_updates_written);
}

void nplex::context_t::on_updates_written_2()
{
    std::vector<update_t> updates;

    {
        std::lock_guard<std::mutex> lock(m_pending_publish_mutex);
        updates.swap(m_pending_publish);
    }

    if (!updates.empty())
        publish(updates);
}

void nplex::context_t::publish(const std::span<update_t> &updates)
{
    for (auto &session : sessions)
    {
        if (session->state() != session_t::state_e::SYNCED)
            continue;

        session->push_changes(updates);
    }
}
