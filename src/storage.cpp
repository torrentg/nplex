#include "storage.hpp"
#include "exception.hpp"
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
    auto [min_rev, max_rev] = m_updates_persister->get_updates_range();
    auto [min_snp, max_snp] = m_snapshot_persister->get_snapshots_range();

    assert(!max_snp || max_snp <= max_rev);
    assert( min_snp || min_rev <= 1);

    return (min_rev <= 1 ? min_rev : min_snp);
}

nplex::rev_t nplex::storage_t::max_rev()
{
    return m_updates_persister->get_updates_range().second;
}

void nplex::storage_t::read_updates(
    rev_t from_rev,
    const std::function<bool(const msgs::Update *)> &on_entry,
    std::size_t max_entries)
{
    auto on_raw_entry = [&on_entry](const updates_persister_t::update_entry_t &raw_entry) -> bool {
        auto verifier = flatbuffers::Verifier(
            reinterpret_cast<const std::uint8_t *>(raw_entry.data),
            raw_entry.data_len);

        if (!verifier.VerifyBuffer<msgs::Update>(nullptr))
            throw nplex_exception("invalid update payload");

        auto update = flatbuffers::GetRoot<msgs::Update>(raw_entry.data);

        return on_entry(update);
    };

    m_updates_persister->read_updates(from_rev, on_raw_entry, max_entries);
}

nplex::store_t nplex::storage_t::rebuild_store_at(rev_t rev, const user_ptr &user)
{
    store_t store;

    // step1: retrieve nearest snapshot (less-eq than rev)
    std::string str = m_snapshot_persister->read_snapshot(rev);
    if (!str.empty())
    {
        auto snapshot = flatbuffers::GetRoot<msgs::Snapshot>(str.c_str());
        store.load(snapshot, user);
        std::string{}.swap(str);
    }

    if (store.rev() == rev)
        return store;

    // step2: define a function to update the repo
    auto func = [&store, rev, user](const msgs::Update *update) -> bool {
        store.update(update, user);
        return (store.rev() < rev);
    };

    // step3: apply updates until m_rev
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

void nplex::storage_t::write_snapshot(rev_t rev, const std::string_view &data)
{
    m_snapshot_persister->write_snapshot(rev, data);
}
