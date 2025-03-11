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

void nplex::repo_task_t::run() {
    m_repo = m_storage->get_repo(m_rev, m_session->m_user);
    SPDLOG_TRACE("repo_task completed: r{}", m_repo.rev());
    // TODO: create DettachedBuffer in run
    // TODO: set crev in after() using use SetField() and send msg
}

void nplex::repo_task_t::after()
{
    using namespace msgs;
    using namespace flatbuffers;

    const auto *server = get_server(&work);
    FlatBufferBuilder builder;

    auto msg = CreateMessage(builder, 
        msgs::MsgContent::LOAD_RESPONSE,
        CreateLoadResponse(builder, 
            m_cid,
            server->rev(),
            true,
            m_repo.serialize(builder)
        ).Union() 
    );

    builder.Finish(msg);

    m_session->send(builder.Release());
    m_session->do_step2();
}
