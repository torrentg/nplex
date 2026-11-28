#include "store.hpp"
#include "utils.hpp"
#include "exception.hpp"
#include "match.h"
#include <algorithm>
#include <cassert>
#include <chrono>

// ==========================================================
// Internal to compilation unit
// ==========================================================

static const gto::cstring REMOVED_VALUE = "<REMOVED>";

struct pending_upsert_t {
    nplex::key_t key;
    gto::cstring data;
};

static gto::cstring create_cstring(const fb::Vector<std::uint8_t> *value)
{
    if (!value)
        return {};

    return gto::cstring{reinterpret_cast<const char *>(value->data()), static_cast<std::size_t>(value->size())};
}

static const void * address_of(const gto::cstring &str)
{
    return static_cast<const void *>(str.c_str());
}

static bool is_removed(const nplex::value_ptr &value)
{
    return (value && ::address_of(value->data()) == ::address_of(REMOVED_VALUE));
}

// ==========================================================
// store methods
// ==========================================================

void nplex::store_t::config(const store_params_t &params) noexcept 
{
    assert(params.tombstone_min_age <= params.tombstone_max_age);
    assert(params.tombstone_max_entries > 0);

    m_params = params;
}

nplex::meta_ptr nplex::store_t::create_meta(rev_t rev_, std::string_view username, std::uint32_t type, millis_t ts)
{
    gto::cstring user;
    auto it = m_users.find(username);

    if (it == m_users.end())
    {
        user = key_t{username};
        m_users.emplace(user);
    }
    else
    {
        user = *it;
    }

    return gto::make_counted<const meta_t>(meta_t{rev_, std::move(user), ts, type});
}

void nplex::store_t::mark_as_removed(const std::vector<key_t> &keys, const meta_ptr &meta)
{
    if (keys.empty())
        return;

    auto num_tombstones = m_tombstones.size();
    auto removed_value = gto::make_counted<const value_t>(REMOVED_VALUE, meta);
    auto rev_ = meta->rev;

    for (const auto &key : keys)
    {
        auto it = m_data.find(key);
    
        if (it == m_data.end())
            continue;

        it->second = removed_value;
        m_tombstones.push({key, rev_});
    }
    
    if (m_tombstones.size() > num_tombstones)
        purge_tombstones();
}

void nplex::store_t::load(const msgs::Snapshot *snapshot, const user_ptr &user)
{
    m_rev = 0;
    m_timestamp = millis_t{0};
    m_data.clear();
    m_users.clear();
    m_tombstones.clear();
    m_min_rev = 0;
    m_stats = {};

    if (!snapshot)
        return;

    auto updates = snapshot->updates();
    rev_t srev = snapshot->rev();

    if (updates)
    {
        for (auto upd : *updates)
        {
            update(upd, user);

            if (m_rev > srev)
                throw nplex_exception("Invalid snapshot");
        }
    }

    m_stats = {};
    m_rev = srev;
    m_timestamp = millis_t{snapshot->timestamp()};
    m_min_rev = m_rev;
}

nplex::update_t nplex::store_t::update(const msgs::Update *msg, const user_ptr &user)
{
    if (!msg)
        throw nplex_exception("Null update message");

    auto update = validate_update(msg, user);

    if (!update.meta)
    {
        // No visible modifications for this user: keep revision in sync only
        rev_t urev = msg->rev();

        if (urev > m_rev) {
            m_rev = urev;
            m_timestamp = millis_t{msg->timestamp()};
        }

        return update;
    }

    apply_update(update);

    return update;
}

nplex::update_t nplex::store_t::validate_update(const msgs::Update *msg, const user_ptr &user)
{
    std::vector<pending_upsert_t> pending_upserts;
    std::vector<key_t> pending_deletes;
    auto upserts = msg->upserts();
    auto deletes = msg->deletes();
    rev_t urev = msg->rev();

    if (urev <= m_rev)
        throw nplex_exception("Update out of order (r{})", urev);

    if (!msg->user() || msg->user()->size() == 0)
        throw nplex_exception("Malformed update message (r{})", urev);

    if (upserts)
    {
        for (auto keyval : *upserts)
        {
            if (!keyval || !keyval->key() || !keyval->value())
                throw nplex_exception("Malformed update message (r{})", urev);

            auto key = keyval->key()->c_str();

            if (user && !user->is_authorized(CRUD_READ, key))
                continue;

            gto::cstring data = ::create_cstring(keyval->value());

            // Reuse existing key storage when possible
            auto it = m_data.find(key);
            key_t ckey = (it != m_data.end() ? it->first : key_t{key});

            pending_upserts.push_back(pending_upsert_t{std::move(ckey), std::move(data)});
        }
    }

    if (deletes)
    {
        for (auto key : *deletes)
        {
            if (!key || !key->c_str())
                throw nplex_exception("Malformed update message (r{})", urev);

            if (user && !user->is_authorized(CRUD_READ, key->c_str()))
                continue;

            auto it = m_data.find(key->c_str());

            if (it == m_data.end())
                continue;

            pending_deletes.emplace_back(it->first);
        }
    }

    update_t ret;

    // No visible modifications for this user -> return empty update
    // We do this to avoid create meta info (user, etc)
    if (pending_upserts.empty() && pending_deletes.empty())
        return ret;

    auto meta = create_meta(urev, msg->user()->string_view(), msg->tx_type(), millis_t{msg->timestamp()});
    ret.meta = meta;

    ret.upserts.reserve(pending_upserts.size());

    for (auto &pu : pending_upserts)
    {
        auto value = gto::make_counted<const value_t>(std::move(pu.data), meta);
        ret.upserts.emplace_back(std::move(pu.key), std::move(value));
    }

    ret.deletes = std::move(pending_deletes);

    return ret;
}

nplex::msgs::SubmitCode nplex::store_t::try_commit(const user_ptr &user, const msgs::SubmitRequest *msg, update_t &upd)
{
    if (!user || !user->params.active)
        throw nplex_exception("Invalid user");

    if (!msg)
        return msgs::SubmitCode::INVALID_MESSAGE;

    rev_t crev = msg->crev();

    if (crev > m_rev)
        return msgs::SubmitCode::ERROR_INVALID_REVISION;

    if (crev < m_min_rev)
        return msgs::SubmitCode::REJECTED_OLD_REVISION;

    upd.meta = nullptr;
    upd.upserts.clear();
    upd.deletes.clear();

    auto rc = validate_commit(user, msg, upd);
    if (rc != msgs::SubmitCode::ACCEPTED)
        return rc;

    apply_update(upd);
    return rc;
}

nplex::msgs::SubmitCode nplex::store_t::validate_commit(const user_ptr &user, const msgs::SubmitRequest *msg, update_t &upd)
{
    bool forced = (user->params.can_force && msg->force());
    std::set<key_t, gto::cstring_compare> keys;
    std::vector<pending_upsert_t> pending_upserts;
    std::vector<key_t> pending_deletes;

    auto upserts = msg->upserts();
    auto deletes = msg->deletes();
    auto ensures = msg->ensures();
    rev_t crev = msg->crev();

    if (upserts)
    {
        for (auto keyval : *upserts)
        {
            if (!keyval || !keyval->value())
                return msgs::SubmitCode::INVALID_MESSAGE;

            if (!keyval->key() || !is_valid_key(keyval->key()->string_view()))
                return msgs::SubmitCode::ERROR_INVALID_KEY;

            if (keys.contains(keyval->key()->string_view()))
                return msgs::SubmitCode::ERROR_DUPLICATE_KEY;

            auto it = m_data.find(keyval->key()->string_view());

            if (it == m_data.end())
            {
                if (!user->is_authorized(CRUD_CREATE, keyval->key()->c_str()))
                    return msgs::SubmitCode::REJECTED_PERMISSION;
            }
            else if (::is_removed(it->second))
            {
                if (!user->is_authorized(CRUD_CREATE, keyval->key()->c_str()))
                    return msgs::SubmitCode::REJECTED_PERMISSION;

                if (!forced && it->second->rev() > crev)
                    return msgs::SubmitCode::REJECTED_INTEGRITY;
            }
            else
            {
                if (!user->is_authorized(CRUD_UPDATE, keyval->key()->c_str()))
                    return msgs::SubmitCode::REJECTED_PERMISSION;

                if (!forced && it->second->rev() > crev)
                    return msgs::SubmitCode::REJECTED_INTEGRITY;
            }

            key_t key = (it != m_data.end() ? it->first : key_t{keyval->key()->string_view()});
            auto data = ::create_cstring(keyval->value());

            pending_upserts.push_back(pending_upsert_t{key, std::move(data)});
            keys.insert(std::move(key));
        }
    }

    if (deletes)
    {
        for (auto key : *deletes)
        {
            if (!key || !is_valid_key(key->string_view()))
                return msgs::SubmitCode::ERROR_INVALID_KEY;

            if (!user->is_authorized(CRUD_DELETE, key->c_str()))
                return msgs::SubmitCode::REJECTED_PERMISSION;

            if (keys.contains(key->string_view()))
                return msgs::SubmitCode::ERROR_DUPLICATE_KEY;

            auto it = m_data.find(key->string_view());

            if (it == m_data.end())
                continue;

            if (!forced && it->second->rev() > crev)
                return msgs::SubmitCode::REJECTED_INTEGRITY;

            pending_deletes.emplace_back(it->first);
            keys.insert(it->first);
        }
    }

    if (pending_upserts.empty() && pending_deletes.empty() && !forced)
        return msgs::SubmitCode::NO_MODIFICATIONS;

    if (ensures)
    {
        for (auto ensure : *ensures)
        {
            if (!ensure || ensure->size() == 0)
                return msgs::SubmitCode::INVALID_MESSAGE;

            const char *pattern = ensure->c_str();
            std::string_view prefix{pattern, strcspn(pattern, "*?")};

            auto it = (prefix.empty() ? m_data.begin() : m_data.lower_bound(prefix));
            auto end = m_data.end();

            while (it != end)
            {
                if (!it->first.starts_with(prefix))
                    break;

                if (!glob_match(it->first.data(), pattern)) {
                    ++it;
                    continue;
                }

                if (!user->is_authorized(CRUD_READ, it->first.c_str())) {
                    ++it;
                    continue;
                }

                if (it->second->rev() > crev)
                    return msgs::SubmitCode::REJECTED_ENSURE;

                ++it;
            }
        }
    }

    // Data validated - filling update
    using namespace std::chrono;
    millis_t now = duration_cast<millis_t>(system_clock::now().time_since_epoch());
    auto meta = create_meta(m_rev + 1, user->name, msg->tx_type(), now);

    upd.meta = meta;
    upd.upserts.clear();
    upd.upserts.reserve(pending_upserts.size());

    for (auto &pu : pending_upserts)
    {
        auto value = gto::make_counted<const value_t>(std::move(pu.data), meta);
        upd.upserts.emplace_back(std::move(pu.key), std::move(value));
    }

    upd.deletes = std::move(pending_deletes);

    return msgs::SubmitCode::ACCEPTED;
}

void nplex::store_t::apply_update(const update_t &upd)
{
    assert(upd.meta);

    m_rev = upd.meta->rev;
    m_timestamp = upd.meta->timestamp;
    m_stats.bytes += estimate_bytes(upd);
    m_stats.count++;

    for (const auto &[key, value] : upd.upserts)
        m_data.insert_or_assign(key, value);

    mark_as_removed(upd.deletes, upd.meta);
}

std::uint32_t nplex::store_t::purge_tombstones()
{
    std::uint32_t count = 0;
    rev_t last_purged_rev = 0;

    auto purge_entry = [&](auto it) {
        last_purged_rev = it->second->rev();
        m_data.erase(it);
        m_tombstones.pop();
        count++;
    };

    while (!m_tombstones.empty())
    {
        auto [rkey, rrev] = m_tombstones.front();
        auto it = m_data.find(rkey);

        // Security check
        if (it == m_data.end()) {
            m_tombstones.pop();
            count++;
            continue;
        }

        // Case: useless entry (modified after deletion)
        if (rrev != it->second->rev()) {
            assert(rrev < it->second->rev());
            m_tombstones.pop();
            count++;
            continue;
        }

        // Invariants
        assert(it->first == rkey);
        assert(it->second->rev() == rrev);
        assert(::is_removed(it->second));

        // Case: entry at a distance greater than configured
        if (m_params.tombstone_max_age > 0 && rrev + m_params.tombstone_max_age < m_rev) {
            purge_entry(it);
            continue;
        }

        // Case: revision removed from tombstones
        if (last_purged_rev && rrev == last_purged_rev) {
            purge_entry(it);
            continue;
        }

        // Case: max tombstones limit reached (hard)
        // 0 means unlimited tombstones.
        if (m_params.tombstone_max_entries == 0 || m_tombstones.size() <= m_params.tombstone_max_entries)
            break;

        // Granting a minimum number of revisions before enforcing max_tombstones
        if (m_params.tombstone_min_age > 0 && m_rev <= rrev + m_params.tombstone_min_age)
            break;

        // Case: max_tombstones exceeded
        purge_entry(it);
    }

    if (last_purged_rev > 0) {
        assert(last_purged_rev < m_rev);
        m_min_rev = last_purged_rev + 1;
    }

    return count;
}

nplex::snapshot_t nplex::store_t::snapshot(const user_ptr &user) const
{
    snapshot_t snap;

    snap.rev = m_rev;
    snap.timestamp = m_timestamp;

    for (const auto &[key, value] : m_data)
    {
        if (user && !user->is_authorized(CRUD_READ, key))
            continue;

        if (::is_removed(value))
            continue;

        snap.data[key] = value;
    }

    return snap;
}
