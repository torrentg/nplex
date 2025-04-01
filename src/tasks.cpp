#include <cassert>
#include <cstddef>
#include <spdlog/spdlog.h>
#include "exception.hpp"
#include "messaging.hpp"
#include "session.hpp"
#include "server.hpp"
#include "tasks.hpp"

// ==========================================================
// repo_task_t methods
// ==========================================================

void nplex::repo_task_t::run()
{
    load_builder_t builder(m_cid);
    auto m_repo = m_storage->get_repo(m_rev, m_session->user());
    SPDLOG_TRACE("repo_task completed: r{}", m_repo.rev());
    builder.set_snapshot(m_repo);
    m_buf = builder.finish(0, true);
}

void nplex::repo_task_t::after()
{
    m_session->send(std::move(m_buf));
    m_session->do_sync();
}

// ==========================================================
// sync_task_t methods
// ==========================================================

void nplex::sync_task_t::run()
{
    m_storage->read_entries(m_rev + 1, [this](const msgs::Update *update) -> bool {
        return append_update(update);
    });
}

void nplex::sync_task_t::after()
{
    if (!m_builder.empty()) {
        auto buf = m_builder.finish(m_rev, true);
        m_bytes += buf.size();
        m_buffers.push_back(std::move(buf));
    }

    for (auto &buf : m_buffers)
        m_session->send(std::move(buf));

    SPDLOG_INFO("sync_task completed: r{}, {} msgs, {} bytes", m_rev, m_buffers.size(), m_bytes);
    m_session->sync_task_complete();
}

void nplex::sync_task_t::config_builder(std::size_t cid, std::uint32_t changes_max_revs, std::uint32_t changes_max_bytes)
{
    m_builder = changes_builder_t(cid, m_session->user(), changes_max_revs, changes_max_bytes);
}

bool nplex::sync_task_t::append_update(const nplex::msgs::Update *update)
{
    auto rc = m_builder.append_update(update);

    if (!rc || m_bytes + m_builder.m_builder.GetSize() >= m_max_bytes)
    {
        auto buf = m_builder.finish(m_rev, true);
        m_bytes += buf.size();
        m_buffers.push_back(std::move(buf));
    }

    return (m_bytes < m_max_bytes && m_buffers.size() < m_max_msgs);
}