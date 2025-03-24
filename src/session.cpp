#include <cassert>
#include <arpa/inet.h>
#include <spdlog/spdlog.h>
#include "exception.hpp"
#include "messaging.hpp"
#include "server.hpp"
#include "utils.hpp"
#include "user.hpp"
#include "context.hpp"
#include "session.hpp"

// maximum message size (in bytes) for a non logged user
#define MAX_MSG_BYTES_FROM_NO_USER 1024
// maximum time (in millis) between login and load messages reception
#define TIMEOUT_NO_USER 5000

// ==========================================================
// session_t methods
// ==========================================================

nplex::session_t::session_t(const context_ptr &context, uv_stream_t *stream) : 
    m_context{context}, 
    m_con{this, stream}
{
    assert(stream);
    assert(stream->loop);
    assert(stream->loop->data);

    m_con.config(MAX_MSG_BYTES_FROM_NO_USER, 0, 0);
    m_con.set_connection_lost(TIMEOUT_NO_USER);
    m_id = m_con.addr().str();
}

void nplex::session_t::send(flatbuffers::DetachedBuffer &&buf)
{
    auto msg = flatbuffers::GetRoot<nplex::msgs::Message>(buf.data());

    if (msg->content_type() == msgs::MsgContent::KEEPALIVE_PUSH)
        SPDLOG_TRACE("Sent {} to {}", msgs::EnumNameMsgContent(msg->content_type()), m_id);
    else
        SPDLOG_DEBUG("Sent {} to {}", msgs::EnumNameMsgContent(msg->content_type()), m_id);

    m_con.send(std::move(buf));
}

void nplex::session_t::set_user(const user_ptr &user)
{
    assert(user);

    m_user = user;
    m_id = fmt::format("{}@{}", user->name, m_con.addr().str());
    m_con.set_keepalive(user->keepalive_millis);
    m_con.state(conn_state_e::LOGGED);
}

void nplex::session_t::do_sync()
{
    switch (m_con.state())
    {
        case conn_state_e::LOGGED: {
            auto keepalive_millis = static_cast<double>(m_user->keepalive_millis);
            auto millis = static_cast<std::uint32_t>(keepalive_millis * m_user->timeout_factor);
            m_con.set_connection_lost(millis);
            break;
        }
        case conn_state_e::SYNCING:
            break;

        case conn_state_e::SYNCED: {
            auto rev = m_context->repo.rev();
            if (m_lrev == rev)
                return;
            if (m_lrev < rev)
                break;
            [[fallthrough]];
        }
        default:
            assert(false);
            return;
    }

    m_con.state(conn_state_e::SYNCING);
}

const char * nplex::session_t::strerror() const
{
    auto error = m_con.error();

    if (error < 0)
        return uv_strerror(error);

    switch (error)
    {
        case ERR_CLOSED_BY_LOCAL: return "closed by server";
        case ERR_CLOSED_BY_PEER: return "closed by peer";
        case ERR_MSG_ERROR: return "invalid message";
        case ERR_MSG_UNEXPECTED: return "unexpected message";
        case ERR_MSG_SIZE: return "message too large";
        case ERR_USR_NOT_FOUND: return "user not found";
        case ERR_USR_INVL_PWD: return "invalid password";
        case ERR_USR_MAX_CONN: return "maximum usr connections reached";
        case ERR_MAX_CONN: return "maximum total connections reached";
        case ERR_API_VERSION: return "unsupported API version";
        case ERR_TIMEOUT_STEP_1: return "login request timeout";
        case ERR_TIMEOUT_STEP_2: return "load request timeout";
        case ERR_CONNECTION_LOST: return "connection lost";
        case ERR_QUEUE_LENGTH: return "max queue length exceeded";
        case ERR_QUEUE_BYTES: return "max queue bytes exceeded";
        default: return "unknow error";
    }
}
