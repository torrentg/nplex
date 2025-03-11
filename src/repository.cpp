#include <set>
#include <chrono>
#include <cstring>
#include <cassert>
#include <algorithm>
#include "match.h"
#include "utils.hpp"
#include "exception.hpp"
#include "repository.hpp"

// ==========================================================
// Internal (static) functions
// ==========================================================

static gto::cstring create_cstring(const flatbuffers::Vector<std::uint8_t> *value) {
    return gto::cstring{reinterpret_cast<const char *>(value->data()), static_cast<std::size_t>(value->size())};
}

// ==========================================================
// repo_t methods
// ==========================================================

nplex::meta_ptr nplex::repo_t::create_meta(rev_t rev, const char *username, std::uint32_t type)
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

    auto now = std::chrono::system_clock::now();
    millis_t timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch());

    return std::make_shared<meta_t>(meta_t{rev, user, timestamp, type, {}});
}

void nplex::repo_t::update_meta(const meta_ptr &meta, const key_t &key, meta_e mode)
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

bool nplex::repo_t::upsert_entry(const char *key, const value_ptr &value)
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

bool nplex::repo_t::upsert_entry(const key_t &key, const value_ptr &value)
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

bool nplex::repo_t::delete_entry(const char *key)
{
    assert(key);

    auto it = m_data.find(key);

    if (it == m_data.end())
        return false;

    update_meta(it->second->m_meta, it->first, meta_e::SUBTRACT);

    m_data.erase(it);

    return true;
}

bool nplex::repo_t::mark_as_removed(const key_t &key, const meta_ptr &meta)
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
    m_removed_keys.push(key);

    return true;
}

void nplex::repo_t::load(const msgs::Snapshot *snapshot, const user_ptr &user)
{
    m_rev = 0;
    m_data.clear();
    m_metas.clear();
    m_users.clear();

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

    m_rev = srev;
}

bool nplex::repo_t::update(const msgs::Update *msg, const user_ptr &user)
{
    if (!msg) {
        assert(false);
        return false;
    }

    auto upserts = msg->upserts();
    auto deletes = msg->deletes();
    rev_t urev = msg->rev();

    if (urev <= m_rev)
        throw nplex_exception("Update out of order (r{})", urev);

    if (!msg->user() || msg->user()->size() == 0)
        throw nplex_exception("Malformed update message (r{})", urev);

    auto meta = create_meta(urev, msg->user()->c_str(), msg->type());

    m_rev = urev;

    if (upserts)
    {
        for (flatbuffers::uoffset_t i = 0; i < upserts->size(); i++)
        {
            auto keyval = upserts->Get(i);

            if (!keyval || !keyval->key() || !keyval->value())
                throw nplex_exception("Malformed update message (r{})", urev);

            auto key = keyval->key()->c_str();

            if (user && !user->is_authorized(NPLEX_READ, key))
                continue;

            gto::cstring data = ::create_cstring(keyval->value());
            auto value = std::make_shared<value_t>(data, meta);

            upsert_entry(key, value);
        }
    }

    if (deletes)
    {
        for (flatbuffers::uoffset_t i = 0; i < deletes->size(); i++)
        {
            auto key = deletes->Get(i);

            if (!key || !key->c_str())
                throw nplex_exception("Malformed update message (r{})", urev);

            if (user && !user->is_authorized(NPLEX_READ, key->c_str()))
                continue;

            delete_entry(key->c_str());
        }
    }

    if (meta->refs.empty()) {
        update_meta(meta, key_t{}, meta_e::SUBTRACT);
        return false;
    }

    m_metas[urev] = meta;
    return true;
}

nplex::msgs::SubmitCode nplex::repo_t::try_commit(const user_t &user, const msgs::SubmitRequest *msg, update_t &update)
{
    assert(user.active);

    if (!msg)
        return msgs::SubmitCode::ERROR_MESSAGE;

    if (msg->crev() > m_rev)
        return msgs::SubmitCode::ERROR_INVALID_REVISION;

    update.meta = nullptr;
    update.upserts.clear();
    update.deletes.clear();

    auto ret = try_commit_inner(user, msg, update);

    if (ret != msgs::SubmitCode::ACCEPTED)
    {
        if (update.meta)
            update_meta(update.meta, key_t{}, meta_e::SUBTRACT);

        update.meta = nullptr;
        update.upserts.clear();
        update.deletes.clear();
    }

    return ret;
}

nplex::msgs::SubmitCode nplex::repo_t::try_commit_inner(const user_t &user, const msgs::SubmitRequest *msg, update_t &update)
{
    bool forced = (user.can_force && msg->force());
    auto meta = create_meta(m_rev + 1, user.name.c_str(), msg->type());
    std::set<key_t, gto::cstring_compare> keys;
    auto upserts = msg->upserts();
    auto deletes = msg->deletes();
    auto ensures = msg->ensures();
    rev_t crev = msg->crev();

    update.meta = meta;

    if (upserts)
    {
        for (flatbuffers::uoffset_t i = 0; i < upserts->size(); i++)
        {
            auto keyval = upserts->Get(i);

            if (!keyval || !keyval->value())
                return msgs::SubmitCode::ERROR_MESSAGE;

            if (!keyval->key() || !is_valid_key(keyval->key()->c_str()))
                return msgs::SubmitCode::ERROR_INVALID_KEY;

            if (keys.contains(keyval->key()->c_str()))
                return msgs::SubmitCode::ERROR_DUPLICATE_KEY;

            auto it = m_data.find(keyval->key()->c_str());

            if (it == m_data.end())
            {
                if (!user.is_authorized(NPLEX_CREATE, keyval->key()->c_str()))
                    return msgs::SubmitCode::REJECTED_PERMISSION;
            }
            else if (it->second->is_removed())
            {
                if (!user.is_authorized(NPLEX_CREATE, keyval->key()->c_str()))
                    return msgs::SubmitCode::REJECTED_PERMISSION;

                if (!forced && it->second->rev() > crev)
                    return msgs::SubmitCode::REJECTED_INTEGRITY;
            }
            else
            {
                if (!user.is_authorized(NPLEX_UPDATE, keyval->key()->c_str()))
                    return msgs::SubmitCode::REJECTED_PERMISSION;
            }

            key_t key = (it != m_data.end() ? it->first : key_t{keyval->key()->c_str()});
            auto value = std::make_shared<value_t>(::create_cstring(keyval->value()), meta);
            update.upserts.emplace_back(key, value);
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

            if (!user.is_authorized(NPLEX_DELETE, key->c_str()))
                return msgs::SubmitCode::REJECTED_PERMISSION;

            if (keys.contains(key->c_str()))
                return msgs::SubmitCode::ERROR_DUPLICATE_KEY;

            auto it = m_data.find(key->c_str());

            if (it == m_data.end())
            {
                // No drama if trying to delete a non-existing key
                continue;
            }
            else if (!forced)
            {
                if (it->second->m_meta->rev > crev)
                    return msgs::SubmitCode::REJECTED_INTEGRITY;
            }

            update.deletes.emplace_back(it->first);
            keys.insert(it->first);
        }
    }

    if (update.upserts.empty() && update.deletes.empty() && !forced)
        return msgs::SubmitCode::NO_MODIFICATIONS;

    if (ensures)
    {
        for (flatbuffers::uoffset_t i = 0; i < ensures->size(); i++)
        {
            auto ensure = ensures->Get(i);

            if (!ensure || ensure->size() == 0)
                return msgs::SubmitCode::ERROR_MESSAGE;

            const char *pattern = ensure->c_str();
            std::string_view prefix = std::string_view{pattern, strcspn(pattern, "*?")};

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

                if (!user.is_authorized(NPLEX_READ, it->first.c_str())) {
                    ++it;
                    continue;
                }

                if (it->second->m_meta->rev > crev)
                    return msgs::SubmitCode::REJECTED_ENSURE;

                ++it;
            }
        }
    }

    // Finally, we update the database

    m_rev = meta->rev;
    m_metas[m_rev] = meta;

    for (const auto &[key, value] : update.upserts)
        upsert_entry(key, value);

    for (const auto &key : update.deletes)
        mark_as_removed(key, meta);

    return msgs::SubmitCode::ACCEPTED;
}

std::uint32_t nplex::repo_t::purge(millis_t timestamp)
{
    std::uint32_t count = 0;

    while (!m_removed_keys.empty())
    {
        auto &key = m_removed_keys.front();
        auto it = m_data.find(key);

        // Case: removed key not found
        if (it == m_data.end()) {
            m_removed_keys.pop();
            count++;
            continue;
        }

        // Case: reinserted after deletion
        if (!it->second->is_removed()) {
            m_removed_keys.pop();
            count++;
            continue;
        }

        // Case: deletion is older than the timestamp
        if (it->second->timestamp() < timestamp) {
            update_meta(it->second->m_meta, key_t{}, meta_e::SUBTRACT);
            m_data.erase(it);
            m_removed_keys.pop();
            count++;
            continue;
        }

        // Case: deleted key is fresh
        break;
    }

    return count;
}

flatbuffers::Offset<nplex::msgs::Snapshot> nplex::repo_t::serialize(flatbuffers::FlatBufferBuilder &builder, const user_ptr &user) const
{
    std::vector<flatbuffers::Offset<msgs::Update>> updates;
    std::vector<flatbuffers::Offset<msgs::KeyValue>> upserts;

    for (const auto &[mrev, meta] : m_metas)
    {
        assert(mrev == meta->rev);

        upserts.clear();

        for (auto &key : meta->refs)
        {
            if (user && !user->is_authorized(NPLEX_READ, key))
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
                builder.CreateString(key.c_str()), 
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
            builder.CreateString(meta->user.c_str()),
            static_cast<std::uint64_t>(meta->timestamp.count()),
            static_cast<std::uint32_t>(meta->type),
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
