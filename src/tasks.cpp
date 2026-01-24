#include <cassert>
#include <cstddef>
#include <spdlog/spdlog.h>
#include "exception.hpp"
#include "messaging.hpp"
#include "session.hpp"
#include "context.hpp"
#include "tasks.hpp"

// ==========================================================
// Internal (static) functions
// ==========================================================

static std::size_t estimate_bytes(const nplex::update_dto_t &update)
{
    using namespace nplex;

    if (update.deletes.empty() && update.upserts.empty())
        return 0;

    std::size_t bytes = 0;

    bytes += sizeof(std::uint64_t);             // rev
    bytes += sizeof(std::uint64_t);             // timestamp
    bytes += sizeof(std::uint32_t);             // type
    bytes += sizeof(std::uint32_t);             // num upserts
    bytes += sizeof(std::uint32_t);             // num deletes
    bytes += sizeof(std::uint32_t);             // user length
    bytes += update.user.size();                // user
    bytes += sizeof(std::uint32_t);             // num upserts
    bytes += sizeof(std::uint32_t);             // num deletes

    for (const auto &pair : update.upserts)
    {
        bytes += sizeof(std::uint32_t);         // key length
        bytes += pair.first.size();             // key data
        bytes += sizeof(std::uint32_t);         // value length
        bytes += pair.second.size();            // value data
    }

    for (const auto &key : update.deletes)
    {
        bytes += sizeof(std::uint32_t);         // key length
        bytes += key.size();                    // key data
    }

    return bytes;
}

// ==========================================================
// snapshot_task_t methods
// ==========================================================

void nplex::snapshot_task_t::run()
{
    auto &storage = m_session->context()->storage;

    m_repo = storage->get_repo(m_rev, m_session->user());
}

void nplex::snapshot_task_t::after()
{
    m_session->send_snapshot(m_cid, m_repo);
}

// ==========================================================
// sync_task_t methods
// ==========================================================

void nplex::sync_task_t::run()
{
    auto &storage = m_session->context()->storage;

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
    m_session->do_sync();
}
