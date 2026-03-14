#include <spdlog/spdlog.h>
#include <fmt/core.h>
#include <fmt/ranges.h>
#include "journal.h"
#include "journal_writer.hpp"
#include "exception.hpp"
#include "storage.hpp"
#include "config.hpp"
#include "tasks.hpp"
#include "user.hpp"
#include "context.hpp"

// ==========================================================
// Internal (static) functions
// ==========================================================

static bool is_valid_user(const nplex::user_t &user)
{
    if (!user.params.active)
        return false;

    if (user.password.empty()) {
        SPDLOG_WARN("User {} discarded (no password)", user.name);
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

static std::map<std::string, nplex::user_ptr> create_users(const std::vector<nplex::user_t> &users)
{
    using namespace nplex;

    std::map<std::string, nplex::user_ptr> ret;
    std::vector<std::string> valid_users;

    for (const auto &user : users)
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

nplex::context_t::context_t(uv_loop_t *loop, const config_t &config) : m_loop(loop)
{
    m_users = ::create_users(config.users);

    m_params = config.context;
    assert(m_params.max_sessions);
    assert(m_params.snapshot_max_entries);
    assert(m_params.snapshot_max_bytes);
    assert(m_params.cache_max_entries);
    assert(m_params.cache_max_bytes);

    auto path = std::filesystem::current_path();

    m_journal = std::make_unique<ldb::journal_t>(path, JOURNAL_NAME, config.journal.check);
    m_journal->set_fsync(config.journal.fsync);

    m_storage = std::make_shared<storage_t>(*m_journal, path);

    m_journal_writer = std::make_unique<journal_writer>(*m_journal, config.journal);

    std::tie(m_rev_0, m_rev_w) = m_storage->get_revs_range();
    SPDLOG_INFO("Data range: [r{}, r{}]", m_rev_0, m_rev_w);

    // set store content
    m_store = m_storage->get_store(m_rev_w);
    m_store.config(config.store);

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

    if (m_sessions.size() >= m_params.max_sessions) {
        SPDLOG_WARN("Incomming connection rejected (max {} clients reached)", m_params.max_sessions);
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

    if (m_rev_0 == 0)
        m_rev_0 = updates.front().meta->rev;

    m_rev_w = updates.back().meta->rev;

    publish(updates);
    update_cache(updates);
}

void nplex::context_t::publish(std::span<const update_t> updates)
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
    if (m_journal_writer->is_blocked())
        return { msgs::SubmitCode::TRY_LATER, 0 };

    update_t update;

    auto rc = m_store.try_commit(user, msg, update);
    if (rc != msgs::SubmitCode::ACCEPTED) {
        SPDLOG_DEBUG("Commit rejected: user={}, crev={}, rc={}", user.name, msg->crev(), static_cast<int>(rc));
        return { rc, 0 };
    }

    assert(m_store.rev() == update.meta->rev);
    SPDLOG_DEBUG("Commit accepted: rev={}, user={}, type={}", update.meta->rev, update.meta->user.c_str(), update.meta->type);

    auto erev = update.meta->rev;
    persist(std::move(update));

    check_for_snapshot();

    return { rc, erev };
}

void nplex::context_t::check_for_snapshot()
{
    const auto &stats = m_store.stats();

    if (stats.count < m_params.snapshot_max_entries && stats.bytes < m_params.snapshot_max_bytes)
        return;

    auto rev = m_store.rev();
    SPDLOG_INFO("Creating snapshot at r{}", rev);

    flatbuffers::FlatBufferBuilder builder;
    auto snapshot = m_store.serialize(builder);
    builder.Finish(snapshot);

    auto buf = builder.Release();

    auto task = new write_snapshot_task_t(rev, std::move(buf), m_storage);
    submit_task(task);

    m_store.reset_stats();
}

void nplex::context_t::update_cache(std::vector<update_t> &updates)
{
    if (updates.empty())
        return;

    // Append new updates to cache
    assert(m_cache.empty() || m_cache.back().meta->rev + 1 == updates.front().meta->rev);

    for (auto &upd : updates)
    {
        m_cache_bytes += estimate_bytes(upd);
        m_cache.push_back(std::move(upd));
    }

    // Purge old updates if cache limits are exceeded
    while (!m_cache.empty())
    {
        if (m_cache.size() <= m_params.cache_max_entries && m_cache_bytes <= m_params.cache_max_bytes)
            break;

        const auto upd = m_cache.pop();
        auto bytes = estimate_bytes(upd);

        assert(m_cache_bytes >= bytes);
        m_cache_bytes -= bytes;
    }

    assert(!m_cache.empty() || m_cache_bytes == 0);
}

std::span<const nplex::update_t> nplex::context_t::get_cached_updates(rev_t from_rev, size_t max_bytes) const
{
    if (m_cache.empty() || from_rev < m_cache.front().meta->rev || from_rev > m_cache.back().meta->rev)
        return {};

    size_t pos = from_rev - m_cache.front().meta->rev;
    assert(pos < m_cache.size());

    // we need to return a contiguous span of updates starting at pos, 
    // but cqueue does not guarantee that the internal buffer is contiguous, 
    // so we need to find the largest contiguous range starting at pos

    auto ptr0 = &m_cache[pos];
    size_t bytes = estimate_bytes(*ptr0);

    for (size_t i = pos + 1; i < m_cache.size(); ++i)
    {
        bytes += estimate_bytes(m_cache[i]);

        if (&m_cache[i] < ptr0 || max_bytes < bytes)
            return std::span<const update_t>{ptr0, i - pos};
    }

    return std::span<const update_t>{ptr0, m_cache.size() - pos};
}
