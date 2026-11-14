#include "tasks.hpp"
#include "exception.hpp"
#include "messaging.hpp"
#include "session.hpp"
#include "context.hpp"
#include "logger.hpp"
#include "utils.hpp"
#include <cassert>

// ==========================================================
// build_snapshot_task_t methods
// ==========================================================

nplex::build_snapshot_task_t::build_snapshot_task_t(nplex::rev_t rev_, user_ptr user_, storage_ptr storage_)
    : task_t(), rev(rev_), user(std::move(user_)), storage(std::move(storage_))
{
    LOG_TRACE("Launching build_snapshot_task: rev={}, user={}", rev, user->name);
}

void nplex::build_snapshot_task_t::run()
{
    auto store = storage->rebuild_store_at(rev, user);

    builder.set_snapshot(store);
}

// ==========================================================
// read_updates_task_t methods
// ==========================================================

nplex::read_updates_task_t::read_updates_task_t(rev_t rev_, user_ptr user_, storage_ptr storage_, std::uint32_t max_msg_bytes_, std::uint32_t max_total_bytes_)
    : task_t(), rev(rev_), user(std::move(user_)), storage(std::move(storage_)), max_msg_bytes(max_msg_bytes_), max_total_bytes(max_total_bytes_)
{
    LOG_TRACE("Launching read_updates_task: rev={}, user={}, max_msg_bytes={}, max_total_bytes={}", 
        rev, user->name, bytes_to_string(max_msg_bytes_), bytes_to_string(max_total_bytes_));
}

void nplex::read_updates_task_t::run()
{
    msgs.reserve(8);
    msgs.emplace_back(user, max_msg_bytes * 1.1);

    storage->read_updates(rev, [this](const msgs::Update *update) -> bool {

        assert(update);
        if (!update)
            return false;

        assert(last_rev < update->rev());
        last_rev = update->rev();

        auto &builder = msgs.back();

        auto rc = builder.append(update);

        if (rc)
        {
            auto msg_bytes = builder.bytes();

            if (max_msg_bytes && msg_bytes >= max_msg_bytes) {
                msgs.emplace_back(user, max_msg_bytes * 1.1);
                total_bytes += msg_bytes;
                msg_bytes = 0;
            }

            if (total_bytes + msg_bytes >= max_total_bytes)
                return false;
        }

        return true;
    });
}

// ==========================================================
// save_snapshot_task_t methods
// ==========================================================

nplex::save_snapshot_task_t::save_snapshot_task_t(rev_t rev_, DetachedBuffer &&buf_, storage_ptr storage_)
    : task_t(), rev(rev_), buf(std::move(buf_)), storage(std::move(storage_))
{
    LOG_TRACE("Launching save_snapshot_task: rev={}, bytes={}", rev, buf.size());
}

void nplex::save_snapshot_task_t::run()
{
    storage->write_snapshot(rev, std::string_view{
        reinterpret_cast<const char *>(buf.data()), buf.size() });
}
