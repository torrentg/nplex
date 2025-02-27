#include <set>
#include <chrono>
#include <cstring>
#include <cassert>
#include <algorithm>
#include "match.h"
#include "exception.hpp"
#include "cache.hpp"

namespace {

using namespace nplex;

gto::cstring create_cstring(const flatbuffers::Vector<std::uint8_t> *value) {
    return gto::cstring{reinterpret_cast<const char *>(value->data()), static_cast<std::size_t>(value->size())};
}

}; // unnamed namespace

nplex::meta_ptr nplex::cache_t::create_meta(const rev_t rev, const char *username, std::uint32_t type)
{
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

    return std::make_shared<meta_t>(meta_t{rev, user, timestamp, type, 0});
}

void nplex::cache_t::release_meta(const meta_ptr &meta)
{
    if (meta->nrefs > 0)
        meta->nrefs--;

    if (meta->nrefs == 0)
    {
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
}

bool nplex::cache_t::upsert_entry(const char *key, const value_ptr &value)
{
    assert(value && value->m_meta);

    if (!is_valid_key(key))
        throw nplex_exception("Trying to upsert an invalid key: {}", key);

    auto it = m_data.find(key);

    if (it != m_data.end())
    {
        release_meta(it->second->m_meta);
        it->second = value;
    }
    else
    {
        nplex::key_t ckey = key;
        m_data[ckey] = value;
    }

    value->m_meta->nrefs++;

    return true;
}

bool nplex::cache_t::upsert_entry(const key_t &key, const value_ptr &value)
{
    auto it = m_data.find(key);

    if (it != m_data.end())
    {
        release_meta(it->second->m_meta);
        it->second = value;
    }
    else
    {
        m_data[key] = value;
    }

    value->m_meta->nrefs++;

    return true;
}

bool nplex::cache_t::delete_entry(const char *key)
{
    if (!is_valid_key(key))
        throw nplex_exception("Trying to delete an invalid key: {}", key);

    auto it = m_data.find(key);

    if (it == m_data.end())
        return false;

    release_meta(it->second->m_meta);

    m_data.erase(it);

    return true;
}

void nplex::cache_t::load(const msgs::Snapshot *snapshot)
{
    m_rev = 0;
    m_data.clear();
    m_metas.clear();
    m_users.clear();

    if (!snapshot)
        return;

    auto updates = snapshot->updates();

    if (updates)
    {
        for (flatbuffers::uoffset_t i = 0; i < updates->size(); i++)
        {
            update(updates->Get(i));

            if (m_rev > snapshot->rev())
                throw nplex_exception("Snapshot at r{} contains entries at r{}", snapshot->rev(), m_rev);
        }
    }

    m_rev = snapshot->rev();
}

bool nplex::cache_t::update(const msgs::Update *msg)
{
    if (!msg) {
        assert(false);
        return false;
    }

    auto upserts = msg->upserts();
    auto deletes = msg->deletes();
    rev_t rev = msg->rev();

    if (rev <= m_rev)
        throw nplex_exception("Received an update to r{} when cache is at r{}", rev, m_rev);

    if (!msg->user() || msg->user()->size() == 0)
        throw nplex_exception("Malformed update message at r{}", rev);

    auto meta = create_meta(rev, msg->user()->c_str(), msg->type());

    m_rev = rev;

    if (upserts)
    {
        for (flatbuffers::uoffset_t i = 0; i < upserts->size(); i++)
        {
            auto keyval = upserts->Get(i);

            if (!keyval || !keyval->key() || !keyval->value())
                throw nplex_exception("Malformed update message at r{}", rev);

            auto key = keyval->key()->c_str();
            gto::cstring data = create_cstring(keyval->value());
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
                throw nplex_exception("Malformed update message at r{}", rev);

            delete_entry(key->c_str());
        }
    }

    if (meta->nrefs)
        m_metas[rev] = meta;

    return true;
}

nplex::msgs::SubmitCode nplex::cache_t::try_commit(const user_t &user, const msgs::SubmitRequest *msg, update_t &update)
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
        if (update.meta) {
            update.meta->nrefs = 0;
            release_meta(update.meta);
        }

        update.meta = nullptr;
        update.upserts.clear();
        update.deletes.clear();
    }

    return ret;
}

nplex::msgs::SubmitCode nplex::cache_t::try_commit_inner(const user_t &user, const msgs::SubmitRequest *msg, update_t &update)
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
            else
            {
                if (!user.is_authorized(NPLEX_UPDATE, keyval->key()->c_str()))
                    return msgs::SubmitCode::REJECTED_PERMISSION;
            }

            key_t key = (it != m_data.end() ? it->first : key_t{keyval->key()->c_str()});
            auto value = std::make_shared<value_t>(create_cstring(keyval->value()), meta);
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
                if (forced)
                    continue;
                else
                    return msgs::SubmitCode::REJECTED_INTEGRITY;
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
            auto acl = ensures->Get(i);

            if (!acl || !acl->pattern() || acl->pattern()->size() == 0 || acl->mode() > 15)
                return msgs::SubmitCode::ERROR_MESSAGE;

            const char *pattern = acl->pattern()->c_str();
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
        delete_entry(key.c_str());

    return msgs::SubmitCode::ACCEPTED;
}
