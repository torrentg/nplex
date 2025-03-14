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
    auto m_repo = m_storage->get_repo(m_rev, m_session->m_user);
    SPDLOG_TRACE("repo_task completed: r{}", m_repo.rev());
    m_builder.set_snapshot(m_repo);
}

void nplex::repo_task_t::after()
{
    const auto *server = get_server();
    auto buf = m_builder.finish(server->rev(), true);
    m_session->send(std::move(buf));
    m_session->do_step2();
}
