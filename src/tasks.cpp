#include "tasks.hpp"
#include "exception.hpp"
#include "messaging.hpp"
#include "session.hpp"
#include "context.hpp"
#include "logger.hpp"
#include "utils.hpp"
#include <cassert>

// ==========================================================
// snapshot_task_t methods
// ==========================================================

void nplex::snapshot_task_t::run()
{
    auto storage = m_session->context()->storage();

    m_store = storage->rebuild_store_at(m_rev, m_session->user());
}

void nplex::snapshot_task_t::after()
{
    if (excpt) {
        m_session->disconnect(ERR_CLOSED_BY_LOCAL);
        return;
    }

    m_session->send_snapshot(m_cid, m_store);
}

// ==========================================================
// sync_task_t methods
// ==========================================================

nplex::sync_task_t::sync_task_t(const session_ptr &session, nplex::rev_t rev, std::size_t cid, std::uint32_t max_items, std::uint32_t max_bytes)
    : m_rev(rev), m_cid(cid), m_session(session), m_max_items(max_items), m_max_bytes(max_bytes)
{
    LOG_TRACE("Launching sync_task: rev={}, max_bytes={}", m_rev, bytes_to_string(max_bytes));
}

void nplex::sync_task_t::run()
{
    auto storage = m_session->context()->storage();

    // TODO: catch exceptions from read_updates (disconnect session)

    storage->read_updates(m_rev + 1, [this](const msgs::Update *update) -> bool {

        auto upd = deserialize_update(update, m_session->user());
        auto bytes = estimate_bytes(upd);

        m_last_rev = upd.rev;

        if (bytes == 0)
            return true;

        if (!m_updates.empty() && m_updates.size() + 1 > m_max_items)
            return false;

        if (!m_updates.empty() && m_bytes + bytes > m_max_bytes)
            return false;

        m_updates.push_back(std::move(upd));
        m_bytes += bytes;

        return (m_bytes < m_max_bytes && m_updates.size() < m_max_items);
    });
}

void nplex::sync_task_t::after()
{
    LOG_TRACE("sync_task completed: range=[{}-{}], items={}, bytes={}", 
        m_rev, m_last_rev, m_updates.size(), bytes_to_string(m_bytes));
    m_session->send_updates(m_cid, m_rev + 1, m_last_rev, m_updates);
    m_session->do_sync(m_cid);
}

// ==========================================================
// read_updates_task_t methods
// ==========================================================

nplex::read_updates_task_t::read_updates_task_t(const session_ptr &session, rev_t rev, std::size_t cid, std::uint32_t max_msg_bytes, std::uint32_t max_total_bytes)
    : m_rev(rev), m_cid(cid), m_session(session), m_max_msg_bytes(max_msg_bytes), m_max_total_bytes(max_total_bytes)
{
    LOG_TRACE("Launching read_updates_task: rev={}, max_msg_bytes={}, max_total_bytes={}", m_rev, bytes_to_string(max_msg_bytes), bytes_to_string(max_total_bytes));
}

void nplex::read_updates_task_t::run()
{
    auto storage = m_session->context()->storage();

    m_msgs.reserve(8);
    m_msgs.emplace_back(m_max_msg_bytes * 1.1);

    storage->read_updates(m_rev, [this](const msgs::Update *update) -> bool {

        assert(update);
        assert(m_last_rev < update->rev());

        if (!update)
            return false;

        m_last_rev = update->rev();

        auto &builder = m_msgs.back();

        auto rc = builder.append(update, m_session->user());

        if (rc)
        {
            auto msg_bytes = builder.bytes();

            if (msg_bytes >= m_max_msg_bytes) {
                m_msgs.emplace_back(m_max_msg_bytes * 1.1);
                m_total_bytes += msg_bytes;
                msg_bytes = 0;
            }

            if (m_total_bytes + msg_bytes >= m_max_total_bytes)
                return false;
        }

        return true;
    });
}

void nplex::read_updates_task_t::after()
{
    if (excpt) {
        m_session->disconnect(ERR_CLOSED_BY_LOCAL);
        return;
    }

    m_session->send_updates(this);
}

// ==========================================================
// write_snapshot_task_t methods
// ==========================================================
void nplex::write_snapshot_task_t::run()
{
    m_storage->write_snapshot(m_rev, std::string_view{
        reinterpret_cast<const char *>(m_buf.data()), m_buf.size() });
}
