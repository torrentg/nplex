#include <set>
#include <chrono>
#include <cassert>
#include <algorithm>
#include "match.h"
#include "utils.hpp"
#include "exception.hpp"
#include "store.hpp"

// ==========================================================
// Internal to compilation unit
// ==========================================================

struct pending_upsert_t {
    nplex::key_t key;
    gto::cstring data;
};

static gto::cstring create_cstring(const flatbuffers::Vector<std::uint8_t> *value)
{
    if (!value)
        return {};

    return gto::cstring{reinterpret_cast<const char *>(value->data()), static_cast<std::size_t>(value->size())};
}

// ==========================================================
// store methods
// ==========================================================

void nplex::store_t::config(const store_params_t &params) noexcept 
{
    assert(params.retention_min <= params.retention_max);
    assert(params.max_tombstones > 0);

    m_params = params;
}

nplex::meta_ptr nplex::store_t::create_meta(rev_t rev, const char *username, std::uint32_t type, millis_t timestamp)
{
    assert(username);

    gto::cstring user;
    auto user_it = m_users.find(username);

    if (user_it == m_users.end())
    {
        user = key_t{username};
        m_users.emplace(user, 1);
    }
    else
    {
        user = user_it->first;
        user_it->second++;
    }

    return std::make_shared<meta_t>(meta_t{rev, user, timestamp, type, {}});
}

void nplex::store_t::update_meta(const meta_ptr &meta, const key_t &key, meta_e mode)
{
    assert(meta);

    if (mode == meta_e::APPEND) {
        meta->refs.insert(key);
        return;
    }

    meta->refs.erase(key);

    if (!meta->refs.empty())
        return;

    auto it = m_users.find(meta->user);

    if (it != m_users.end())
    {
        if (it->second > 1)
            it->second--;
        else
            m_users.erase(it);
    }

    m_metas.erase(meta->rev);
}

bool nplex::store_t::upsert_entry(const char *key, const value_ptr &value)
{
    assert(key);
    assert(value);
    assert(value->m_meta);

    if (!is_valid_key(key))
        throw nplex_exception("Trying to upsert an invalid key: {}", key);

    key_t ckey{};
    auto it = m_data.find(key);

    if (it != m_data.end())
    {
        ckey = it->first;
        update_meta(it->second->m_meta, ckey, meta_e::SUBTRACT);
        it->second = value;
    }
    else
    {
        ckey = key;
        m_data[ckey] = value;
    }

    update_meta(value->m_meta, ckey, meta_e::APPEND);

    return true;
}

bool nplex::store_t::upsert_entry(const key_t &key, const value_ptr &value)
{
    auto it = m_data.find(key);

    if (it != m_data.end())
    {
        update_meta(it->second->m_meta, key, meta_e::SUBTRACT);
        it->second = value;
    }
    else
    {
        m_data[key] = value;
    }

    update_meta(value->m_meta, key, meta_e::APPEND);

    return true;
}

bool nplex::store_t::delete_entry(const char *key)
{
    assert(key);

    auto it = m_data.find(key);

    if (it == m_data.end())
        return false;

    update_meta(it->second->m_meta, it->first, meta_e::SUBTRACT);

    m_data.erase(it);

    return true;
}

bool nplex::store_t::mark_as_removed(const key_t &key, const meta_ptr &meta)
{
    auto it = m_data.find(key);

    if (it == m_data.end())
        return false;

    if (it->second->m_meta != meta) {
        update_meta(it->second->m_meta, key, meta_e::SUBTRACT);
        update_meta(meta, key, meta_e::APPEND);
    }

    it->second->set_removed();
    it->second->m_meta = meta;
    m_removed_keys.push({key, meta->rev});

    return true;
}

void nplex::store_t::load(const msgs::Snapshot *snapshot, const user_ptr &user)
{
    m_rev = 0;
    m_data.clear();
    m_metas.clear();
    m_users.clear();
    m_removed_keys.clear();
    m_stats = {};
    m_min_rev = 0;

    if (!snapshot)
        return;

    auto updates = snapshot->updates();
    rev_t srev = snapshot->rev();

    if (updates)
    {
        for (flatbuffers::uoffset_t i = 0; i < updates->size(); i++)
        {
            update(updates->Get(i), user);

            if (m_rev > srev)
                throw nplex_exception("Invalid snapshot");
        }
    }

    m_stats = {};
    m_rev = srev;
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
        if (urev > m_rev)
            m_rev = urev;
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
        for (flatbuffers::uoffset_t i = 0; i < upserts->size(); i++)
        {
            auto keyval = upserts->Get(i);

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
        for (flatbuffers::uoffset_t i = 0; i < deletes->size(); i++)
        {
            auto key = deletes->Get(i);

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
    if (pending_upserts.empty() && pending_deletes.empty())
        return ret;

    auto meta = create_meta(urev, msg->user()->c_str(), msg->tx_type(), millis_t{msg->timestamp()});
    ret.meta = meta;

    ret.upserts.reserve(pending_upserts.size());

    for (auto &pu : pending_upserts)
    {
        auto value = std::make_shared<value_t>(pu.data, meta);
        ret.upserts.emplace_back(pu.key, std::move(value));
    }

    ret.deletes = std::move(pending_deletes);

    return ret;
}

nplex::msgs::SubmitCode nplex::store_t::try_commit(const user_t &user, const msgs::SubmitRequest *msg, update_t &update)
{
    assert(user.params.active);

    if (!msg)
        return msgs::SubmitCode::INVALID_MESSAGE;

    rev_t crev = msg->crev();

    if (crev > m_rev)
        return msgs::SubmitCode::ERROR_INVALID_REVISION;

    if (crev < m_min_rev)
        return msgs::SubmitCode::REJECTED_OLD_REVISION;

    update.meta = nullptr;
    update.upserts.clear();
    update.deletes.clear();

    auto rc = validate_commit(user, msg, update);
    if (rc != msgs::SubmitCode::ACCEPTED)
        return rc;

    apply_update(update);
    return rc;
}

nplex::msgs::SubmitCode nplex::store_t::validate_commit(const user_t &user, const msgs::SubmitRequest *msg, update_t &update)
{
    bool forced = (user.params.can_force && msg->force());
    std::set<key_t, gto::cstring_compare> keys;
    std::vector<pending_upsert_t> pending_upserts;
    std::vector<key_t> pending_deletes;

    auto upserts = msg->upserts();
    auto deletes = msg->deletes();
    auto ensures = msg->ensures();
    rev_t crev = msg->crev();

    if (upserts)
    {
        for (flatbuffers::uoffset_t i = 0; i < upserts->size(); i++)
        {
            auto keyval = upserts->Get(i);

            if (!keyval || !keyval->value())
                return msgs::SubmitCode::INVALID_MESSAGE;

            if (!keyval->key() || !is_valid_key(keyval->key()->c_str()))
                return msgs::SubmitCode::ERROR_INVALID_KEY;

            if (keys.contains(keyval->key()->c_str()))
                return msgs::SubmitCode::ERROR_DUPLICATE_KEY;

            auto it = m_data.find(keyval->key()->c_str());

            if (it == m_data.end())
            {
                if (!user.is_authorized(CRUD_CREATE, keyval->key()->c_str()))
                    return msgs::SubmitCode::REJECTED_PERMISSION;
            }
            else if (it->second->is_removed())
            {
                if (!user.is_authorized(CRUD_CREATE, keyval->key()->c_str()))
                    return msgs::SubmitCode::REJECTED_PERMISSION;

                if (!forced && it->second->rev() > crev)
                    return msgs::SubmitCode::REJECTED_INTEGRITY;
            }
            else
            {
                if (!user.is_authorized(CRUD_UPDATE, keyval->key()->c_str()))
                    return msgs::SubmitCode::REJECTED_PERMISSION;

                if (!forced && it->second->rev() > crev)
                    return msgs::SubmitCode::REJECTED_INTEGRITY;
            }

            key_t key = (it != m_data.end() ? it->first : key_t{keyval->key()->c_str()});
            auto data = ::create_cstring(keyval->value());

            pending_upserts.push_back(pending_upsert_t{key, std::move(data)});
            keys.insert(key);
        }
    }

    if (deletes)
    {
        for (flatbuffers::uoffset_t i = 0; i < deletes->size(); i++)
        {
            auto key = deletes->Get(i);

            if (!key || !is_valid_key(key->c_str()))
                return msgs::SubmitCode::ERROR_INVALID_KEY;

            if (!user.is_authorized(CRUD_DELETE, key->c_str()))
                return msgs::SubmitCode::REJECTED_PERMISSION;

            if (keys.contains(key->c_str()))
                return msgs::SubmitCode::ERROR_DUPLICATE_KEY;

            auto it = m_data.find(key->c_str());

            if (it == m_data.end())
                continue;

            if (!forced && it->second->m_meta->rev > crev)
                return msgs::SubmitCode::REJECTED_INTEGRITY;

            pending_deletes.emplace_back(it->first);
            keys.insert(it->first);
        }
    }

    if (pending_upserts.empty() && pending_deletes.empty() && !forced)
        return msgs::SubmitCode::NO_MODIFICATIONS;

    if (ensures)
    {
        for (flatbuffers::uoffset_t i = 0; i < ensures->size(); i++)
        {
            auto ensure = ensures->Get(i);

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

                if (!user.is_authorized(CRUD_READ, it->first.c_str())) {
                    ++it;
                    continue;
                }

                if (it->second->m_meta->rev > crev)
                    return msgs::SubmitCode::REJECTED_ENSURE;

                ++it;
            }
        }
    }

    // Data validated - filling update
    using namespace std::chrono;
    millis_t timestamp = duration_cast<milliseconds>(system_clock::now().time_since_epoch());
    auto meta = create_meta(m_rev + 1, user.name.c_str(), msg->tx_type(), timestamp);

    update.meta = meta;
    update.upserts.clear();
    update.upserts.reserve(pending_upserts.size());

    for (auto &pu : pending_upserts)
    {
        auto value = std::make_shared<value_t>(pu.data, meta);
        update.upserts.emplace_back(pu.key, std::move(value));
    }

    update.deletes = std::move(pending_deletes);

    return msgs::SubmitCode::ACCEPTED;
}

void nplex::store_t::apply_update(const update_t &update)
{
    assert(update.meta);

    m_rev = update.meta->rev;
    m_metas[m_rev] = update.meta;
    m_stats.bytes += estimate_bytes(update);
    m_stats.count++;

    for (const auto &[key, value] : update.upserts)
        upsert_entry(key, value);

    auto num_removed_keys = m_removed_keys.size();

    for (const auto &key : update.deletes)
        mark_as_removed(key, update.meta);

    if (m_removed_keys.size() > num_removed_keys)
        purge();
}

std::uint32_t nplex::store_t::purge()
{
    std::uint32_t count = 0;
    rev_t last_purged_rev = 0;

    auto purge_entry = [&](auto it) {
        last_purged_rev = it->second->rev();
        update_meta(it->second->m_meta, it->first, meta_e::SUBTRACT);
        m_data.erase(it);
        m_removed_keys.pop();
        count++;
    };

    while (!m_removed_keys.empty())
    {
        auto [key, rev] = m_removed_keys.front();
        auto it = m_data.find(key);

        // Security check
        if (it == m_data.end()) {
            m_removed_keys.pop();
            count++;
            continue;
        }

        // Case: useless entry (modified after deletion)
        if (rev != it->second->rev()) {
            assert(rev < it->second->rev());
            m_removed_keys.pop();
            count++;
            continue;
        }

        // Invariants
        assert(it->first == key);
        assert(it->second->rev() == rev);
        assert(it->second->is_removed());

        // Case: entry at a distance greater than configured max retention
        if (m_params.retention_max > 0 && rev + m_params.retention_max < m_rev) {
            purge_entry(it);
            continue;
        }

        // Case: revision removed from tombstones
        if (last_purged_rev && rev == last_purged_rev) {
            purge_entry(it);
            continue;
        }

        // Case: max tombstones limit reached (hard)
        // 0 means unlimited tombstones.
        if (m_params.max_tombstones == 0 || m_removed_keys.size() <= m_params.max_tombstones)
            break;

        // Granting a minimum number of revisions before enforcing max_tombstones
        if (m_params.retention_min > 0 && m_rev <= rev + m_params.retention_min)
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

flatbuffers::Offset<nplex::msgs::Snapshot> nplex::store_t::serialize(flatbuffers::FlatBufferBuilder &builder, const user_ptr &user) const
{
    std::vector<flatbuffers::Offset<msgs::Update>> updates;
    std::vector<flatbuffers::Offset<msgs::KeyValue>> upserts;

    for (const auto &[mrev, meta] : m_metas)
    {
        assert(mrev == meta->rev);

        upserts.clear();

        for (auto &key : meta->refs)
        {
            if (user && !user->is_authorized(CRUD_READ, key))
                continue;

            auto it = m_data.find(key);

            if (it == m_data.end()) {
                assert(false);
                continue;
            }

            if (it->second->is_removed())
                continue;

            auto kv = msgs::CreateKeyValue(
                builder, 
                builder.CreateString(key), 
                builder.CreateVector(
                    reinterpret_cast<const uint8_t *>(it->second->data().c_str()), 
                    it->second->data().size()
                )
            );

            upserts.push_back(kv);
        }

        if (upserts.empty())
            continue;

        auto upd = msgs::CreateUpdate(
            builder,
            static_cast<std::uint64_t>(mrev),
            builder.CreateString(meta->user),
            static_cast<std::uint64_t>(meta->timestamp.count()),
            static_cast<std::uint32_t>(meta->tx_type),
            builder.CreateVector(upserts)
        );

        updates.push_back(upd);
    }

    auto snapshot = msgs::CreateSnapshot(
        builder, 
        static_cast<std::uint64_t>(m_rev), 
        builder.CreateVector(updates)
    );

    return snapshot;
}
