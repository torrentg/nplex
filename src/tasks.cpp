#include <cassert>
#include <cstddef>
#include <spdlog/spdlog.h>
#include "exception.hpp"
#include "messaging.hpp"
#include "session.hpp"
#include "server.hpp"
#include "tasks.hpp"

// ==========================================================
// Internal (static) functions
// ==========================================================

static nplex::server_t * get_server(uv_work_t *req)
{
    assert(req);
    assert(req->loop);
    return static_cast<nplex::server_t *>(req->loop->data);
}

// ==========================================================
// repo_task_t methods
// ==========================================================

void nplex::repo_task_t::run()
{
    auto m_repo = m_storage->get_repo(m_rev, m_session->m_user);
    SPDLOG_TRACE("repo_task completed: r{}", m_repo.rev());
    m_offset = m_repo.serialize(m_builder);
}

void nplex::repo_task_t::after()
{
    const auto *server = get_server(&work);

    auto msg = msgs::CreateMessage(m_builder, 
        msgs::MsgContent::LOAD_RESPONSE,
        msgs::CreateLoadResponse(m_builder, 
            m_cid,
            server->rev(),
            true,
            m_offset
        ).Union()
    );

    m_builder.Finish(msg);

    m_session->send(m_builder.Release());
    m_session->do_step2();
}
