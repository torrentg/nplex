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

nplex::read_updates_task_t::read_updates_task_t(rev_t rev_, user_ptr user_, storage_ptr storage_, std::uint32_t msg_max_bytes_, std::uint32_t max_total_bytes_)
    : task_t(), rev(rev_), user(std::move(user_)), storage(std::move(storage_)), msg_max_bytes(msg_max_bytes_), max_total_bytes(max_total_bytes_)
{
    LOG_TRACE("Launching read_updates_task: rev={}, user={}, msg_max_bytes={}, max_total_bytes={}", 
        rev, user->name, bytes_to_string(msg_max_bytes_), bytes_to_string(max_total_bytes_));
}

void nplex::read_updates_task_t::run()
{
    msgs.reserve(8);
    msgs.emplace_back(user, msg_max_bytes);

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

            if (msg_max_bytes && msg_bytes >= msg_max_bytes) {
                msgs.emplace_back(user, msg_max_bytes);
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

nplex::save_snapshot_task_t::save_snapshot_task_t(rev_t rev_, millis_t timestamp_, DetachedBuffer &&buf_, storage_ptr storage_)
    : task_t(), rev(rev_), timestamp(timestamp_), buf(std::move(buf_)), storage(std::move(storage_))
{
    LOG_TRACE("Launching save_snapshot_task: rev={}, timestamp={}, bytes={}", rev, to_iso8601(timestamp), buf.size());
}

void nplex::save_snapshot_task_t::run()
{
    storage->write_snapshot(rev, timestamp, std::string_view{
        reinterpret_cast<const char *>(buf.data()), buf.size() });
}
