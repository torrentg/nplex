#include <cassert>
#include <cstddef>
#include <spdlog/spdlog.h>
#include "exception.hpp"
#include "messaging.hpp"
#include "session.hpp"
#include "context.hpp"
#include "utils.hpp"
#include "tasks.hpp"

// ==========================================================
// snapshot_task_t methods
// ==========================================================

void nplex::snapshot_task_t::run()
{
    auto storage = m_session->context()->storage();

    m_store = storage->get_store(m_rev, m_session->user());
}

void nplex::snapshot_task_t::after()
{
    m_session->send_snapshot(m_cid, m_store);
}

// ==========================================================
// sync_task_t methods
// ==========================================================

void nplex::sync_task_t::run()
{
    auto storage = m_session->context()->storage();

    storage->read_entries(m_rev + 1, [this](const msgs::Update *update) -> bool {

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
    m_session->send_updates(m_cid, m_rev + 1, m_last_rev, m_updates);
    m_session->do_sync(m_cid);
}

// ==========================================================
// write_snapshot_task_t methods
// ==========================================================
void nplex::write_snapshot_task_t::run()
{
    m_storage->write_snapshot(std::string_view{
        reinterpret_cast<const char *>(m_buf.data()), m_buf.size() });
}
