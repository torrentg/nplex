#include "storage.hpp"
#include "exception.hpp"
#include "logger.hpp"
#include <cassert>

// ==========================================================
// storage_t methods
// ==========================================================

nplex::storage_t::storage_t(const storage_params_t &params)
{
    m_writer_thread.set_queue_limits(params.write_queue_max_entries, params.write_queue_max_bytes);
    m_writer_thread.set_flush_limits(params.flush_max_entries, params.flush_max_bytes);
}

void nplex::storage_t::set_updates_persister(const std::shared_ptr<updates_persister_t> &persister)
{
    m_updates_persister = persister;

    m_writer_thread.set_persister(m_updates_persister);
}

void nplex::storage_t::set_snapshot_persister(const std::shared_ptr<snapshot_persister_t> &persister)
{
    m_snapshot_persister = persister;
}

void nplex::storage_t::start(result_callback_f &&result_cb, error_callback_f &&error_cb)
{
    assert(m_updates_persister);
    assert(m_snapshot_persister);

    m_writer_thread.set_result_callback(std::move(result_cb));
    m_writer_thread.set_error_callback(std::move(error_cb));

    m_writer_thread.start();
}

void nplex::storage_t::stop()
{
    m_writer_thread.stop();
}

nplex::rev_t nplex::storage_t::min_rev()
{
    auto [min_upd, max_upd] = m_updates_persister->get_updates_range();
    auto [min_snp, max_snp] = m_snapshot_persister->get_snapshots_range();

    assert(min_upd <= max_upd);
    assert(min_snp <= max_snp);

    // TODO: Case when there are no updates and no snapshots
    // TODO: Case when there are no updates but snapshots exist
    // TODO: Case when there are no snapshots but updates exist
    // TODO: Case when snapshots are lower or upper than updates range

    return (min_upd <= 1 ? min_upd : min_snp);
}

nplex::rev_t nplex::storage_t::max_rev()
{
    return m_updates_persister->get_updates_range().second;
}

void nplex::storage_t::read_updates(rev_t from_rev, const std::function<bool(const msgs::Update *)> &callback)
{
    auto on_raw_entry = [&callback](const updates_persister_t::update_entry_t &raw_entry) -> bool {
        auto verifier = fb::Verifier(
            reinterpret_cast<const std::uint8_t *>(raw_entry.data),
            raw_entry.data_len);

        if (!verifier.VerifyBuffer<msgs::Update>(nullptr))
            throw nplex_exception("invalid update payload");

        auto update = fb::GetRoot<msgs::Update>(raw_entry.data);

        return callback(update);
    };

    m_updates_persister->read_updates(from_rev, on_raw_entry);
}

void nplex::storage_t::read_snapshot(rev_t rev, const std::function<void(const msgs::Snapshot *)> &callback)
{
    auto [srev, sts, sdata] = m_snapshot_persister->read_snapshot(rev);

    if (!srev || sdata.empty()) {
        LOG_TRACE("No snapshot available at rev={}", rev);
        callback(nullptr);
        return;
    }

    LOG_TRACE("Loading snapshot at rev={}", srev);
    auto snapshot = fb::GetRoot<msgs::Snapshot>(sdata.c_str());

    auto verifier = fb::Verifier(reinterpret_cast<const std::uint8_t *>(sdata.c_str()), sdata.size());
    if (!verifier.VerifyBuffer<msgs::Snapshot>(nullptr))
        throw nplex_exception("invalid snapshot");

    assert(snapshot->rev() == srev);

    callback(snapshot);
}

nplex::store_t nplex::storage_t::rebuild_store_at(rev_t rev, const user_ptr &user)
{
    store_t store;

    LOG_TRACE("Rebuilding store at rev={}, user={}", rev, (user ? user->name : "<none>"));

    // Step1: retrieve nearest snapshot (less-eq than rev)
    read_snapshot(rev, [&](const msgs::Snapshot *snapshot) {
        if (snapshot)
            store.load(snapshot, user);
    });

    if (store.rev() == rev)
        return store;

    // Step2: define a function to update the repo
    auto func = [&store, rev, user](const msgs::Update *update) -> bool {
        store.update(update, user);
        return (store.rev() < rev);
    };

    // Step3: apply updates until m_rev
    LOG_TRACE("Loading updates from rev={} to rev={}", store.rev() + 1, rev);
    read_updates(store.rev() + 1, func);

    if (store.rev() != rev)
        throw nplex_exception("Failed to load store at revision {}", rev);

    return store;
}

bool nplex::storage_t::can_accept_writes() const
{
    return m_writer_thread.can_accept_writes();
}

bool nplex::storage_t::write_update_async(update_t &&upd)
{
    return m_writer_thread.write(std::move(upd));
}

void nplex::storage_t::write_snapshot(rev_t rev, millis_t timestamp, const std::string_view &data)
{
    m_snapshot_persister->write_snapshot(rev, timestamp, data);
}
