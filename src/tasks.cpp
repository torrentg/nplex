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

static nplex::task_t * get_task(uv_work_t *req)
{
    assert(req);
    assert(req->data);
    return static_cast<nplex::task_t *>(req->data);
}

static nplex::server_t * get_server(uv_work_t *req)
{
    assert(req);
    assert(req->loop);
    return static_cast<nplex::server_t *>(req->loop->data);
}

// ==========================================================
// Public functions
// ==========================================================

void nplex::cb_task_run(uv_work_t *req)
{
    auto *task = get_task(req);

    try {
        task->run();
    }
    catch (const std::exception &e) {
        SPDLOG_ERROR("Task error: {}", e.what());
        task->excpt = std::current_exception();
    }
}

void nplex::cb_task_after(uv_work_t *req, int status)
{
    auto *task = get_task(req);
    auto *server = get_server(req);

    if (status != 0)
    {
        if (status != UV_ECANCELED)
            SPDLOG_ERROR("Task error: {}", uv_strerror(status));

        task->excpt = std::make_exception_ptr(nplex_exception(uv_strerror(status)));
    }

    if (!task->excpt)
    {
        try {
            task->after();
        }
        catch (const std::exception &e) {
            SPDLOG_ERROR("Task error: {}", e.what());
            task->excpt = std::current_exception();
        }
    }

    if (task->excpt && status != UV_ECANCELED) {
        uv_stop(req->loop);
    }

    server->release_task(task);

    delete task;
}

// ==========================================================
// repo_task_t methods
// ==========================================================

void nplex::repo_task_t::run() {
    m_repo = m_storage->get_repo(m_rev, m_session->m_user);
    SPDLOG_DEBUG("repo_task completed: r{}", m_repo.rev());
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
