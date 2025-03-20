#include "cppcrc.h"
#include "utils.hpp"
#include "exception.hpp"
#include "messaging.hpp"

using namespace nplex::msgs;
using namespace flatbuffers;

nplex::output_msg_t::output_msg_t(DetachedBuffer &&msg) : content(std::move(msg))
{
    len = (std::uint32_t)(content.size() + sizeof(len) + sizeof(metadata) + sizeof(checksum));
    len = htonl(len);

    metadata = htonl(0); // not-compressed

    checksum = CRC32::CRC32::calc(reinterpret_cast<const std::uint8_t *>(&len), sizeof(len));
    checksum = crc_utils::reverse(checksum ^ 0xFFFFFFFF);
    checksum = CRC32::CRC32::calc(reinterpret_cast<const std::uint8_t *>(&metadata), sizeof(metadata), checksum);
    checksum = crc_utils::reverse(checksum ^ 0xFFFFFFFF);
    checksum = CRC32::CRC32::calc(reinterpret_cast<const std::uint8_t *>(content.data()), content.size(), checksum);
    checksum = htonl(checksum);

    buf[0] = uv_buf_init(reinterpret_cast<char *>(&len), sizeof(len));
    buf[1] = uv_buf_init(reinterpret_cast<char *>(&metadata), sizeof(metadata));
    buf[2] = uv_buf_init(reinterpret_cast<char *>(content.data()), static_cast<unsigned int>(content.size()));
    buf[3] = uv_buf_init(reinterpret_cast<char *>(&checksum), sizeof(checksum));
}

const nplex::msgs::Message * nplex::parse_network_msg(const char *ptr, size_t len)
{
    if (len <= 3 * sizeof(std::uint32_t))
        return nullptr;

    if (len != ntohl_ptr(ptr))
        return nullptr;

    std::uint32_t metadata = ntohl_ptr(ptr + sizeof(std::uint32_t));
    UNUSED(metadata);

    std::uint32_t checksum = ntohl_ptr(ptr + len - sizeof(std::uint32_t));

    if (checksum != CRC32::CRC32::calc(reinterpret_cast<const std::uint8_t *>(ptr), len - sizeof(std::uint32_t)))
        return nullptr;

    ptr += 2 * sizeof(std::uint32_t);
    len -= 3 * sizeof(std::uint32_t);

    auto verifier = flatbuffers::Verifier(reinterpret_cast<const std::uint8_t *>(ptr), len);

    if (!verifier.VerifyBuffer<Message>(nullptr))
        return nullptr;

    return flatbuffers::GetRoot<Message>(ptr);
}

bool nplex::update_crev(flatbuffers::DetachedBuffer &buf, rev_t crev)
{
    using namespace msgs;
    using namespace flatbuffers;

    auto msg = GetRoot<Message>(buf.data());
    if (!msg)
        return false;

    auto content = const_cast<void *>(msg->content());
    if (!content)
        return false;

    auto table = static_cast<Table *>(content);

    switch(msg->content_type())
    {
        case msgs::MsgContent::PING_RESPONSE:
            table->SetField<uint64_t>(PingResponse::VT_CREV, crev);
            return true;
        case msgs::MsgContent::LOGIN_RESPONSE:  
            table->SetField<uint64_t>(LoginResponse::VT_CREV, crev);
            return true;
        case msgs::MsgContent::LOAD_RESPONSE:
            table->SetField<uint64_t>(LoadResponse::VT_CREV, crev);
            return true;
        case msgs::MsgContent::SUBMIT_RESPONSE: 
            table->SetField<uint64_t>(SubmitResponse::VT_CREV, crev);
            return true;
        case msgs::MsgContent::CHANGES_PUSH:
            table->SetField<uint64_t>(ChangesPush::VT_CREV, crev);
            return true;
        case msgs::MsgContent::KEEPALIVE_PUSH:
            table->SetField<uint64_t>(KeepAlivePush::VT_CREV, crev);
            return true;
        default:
            return false;
    }
}

DetachedBuffer nplex::create_login_msg(std::size_t cid, LoginCode code, rev_t rev0, rev_t crev, const user_t &user)
{
    FlatBufferBuilder builder;
    std::vector<Offset<Acl>> permissions;

    for (const auto &acl : user.permissions) {
        auto pattern = builder.CreateString(acl.pattern);
        permissions.push_back(CreateAcl(builder, pattern, acl.mode));
    }

    auto msg = CreateMessage(builder, 
        MsgContent::LOGIN_RESPONSE, 
        CreateLoginResponse(builder, 
            cid, 
            code,
            rev0,
            crev,
            user.can_force,
            user.keepalive_millis,
            builder.CreateVector(permissions)
        ).Union()
    );

    builder.Finish(msg);
    return builder.Release();
}

flatbuffers::DetachedBuffer nplex::create_keepalive_msg(rev_t crev)
{
    FlatBufferBuilder builder;

    auto msg = CreateMessage(builder, 
        MsgContent::KEEPALIVE_PUSH,
        CreateKeepAlivePush(builder, 
            crev
        ).Union()
    );

    builder.Finish(msg);
    return builder.Release();
}

flatbuffers::DetachedBuffer nplex::create_ping_msg(std::size_t cid, rev_t crev, const std::string &payload)
{
    FlatBufferBuilder builder;

    auto msg = CreateMessage(builder, 
        MsgContent::PING_RESPONSE,
        CreatePingResponse(builder,
            cid, 
            crev,
            builder.CreateString(payload)
        ).Union()
    );

    builder.Finish(msg);
    return builder.Release();
}

flatbuffers::DetachedBuffer nplex::create_submit_msg(std::size_t cid, rev_t crev, SubmitCode code, rev_t erev)
{
    FlatBufferBuilder builder;

    auto msg = CreateMessage(builder, 
        MsgContent::SUBMIT_RESPONSE,
        CreateSubmitResponse(builder,
            cid, 
            crev,
            code,
            erev
        ).Union()
    );

    builder.Finish(msg);
    return builder.Release();
}

flatbuffers::Offset<nplex::msgs::Update> nplex::serialize_update(flatbuffers::FlatBufferBuilder &builder, const update_t &update, const user_t *user, bool force)
{
    std::vector<Offset<KeyValue>> upserts;
    std::vector<Offset<String>> deletes;

    for (const auto &[key, value] : update.upserts)
    {
        if (user && !user->is_authorized(NPLEX_READ, key))
            continue;

        auto kv = CreateKeyValue(
            builder, 
            builder.CreateString(key),
            builder.CreateVector(
                reinterpret_cast<const uint8_t *>(value->data().c_str()), 
                value->data().size()
            )
        );

        upserts.push_back(kv);
    }

    for (const auto &key : update.deletes)
    {
        if (user && !user->is_authorized(NPLEX_READ, key))
            continue;

        deletes.push_back(builder.CreateString(key));
    }

    if (!force && upserts.empty() && deletes.empty())
        return 0;

    return CreateUpdate(
        builder,
        update.meta->rev,
        builder.CreateString(update.meta->user),
        static_cast<std::uint64_t>(update.meta->timestamp.count()),
        update.meta->type,
        builder.CreateVector(upserts),
        builder.CreateVector(deletes)
    );
}

flatbuffers::DetachedBuffer nplex::serialize_update(const update_t &update)
{
    FlatBufferBuilder builder;

    auto msg = serialize_update(builder, update, nullptr, true);

    builder.Finish(msg);
    return builder.Release();
}

nplex::update_t nplex::deserialize_update(const Update *msg, const user_ptr &user)
{
    update_t update;

    update.meta = std::make_shared<meta_t>(meta_t{
        msg->rev(),
        msg->user()->c_str(),
        millis_t(msg->timestamp()),
        msg->type(),
        {}
    });

    if (msg->upserts())
    {
        for (const auto &kv : *msg->upserts())
        {
            if (user && !user->is_authorized(NPLEX_READ, kv->key()->c_str()))
                continue;

            update.upserts.push_back({
                key_t{kv->key()->c_str()},
                std::make_shared<value_t>(
                    create_cstring(kv->value()),
                    update.meta
                )
            });
        }
    }

    if (msg->deletes())
    {
        for (const auto &key : *msg->deletes())
        {
            if (user && !user->is_authorized(NPLEX_READ, key->c_str()))
                continue;

            update.deletes.push_back(key->c_str());
        }
    }

    return update;
}

void nplex::load_builder_t::set_snapshot(const repo_t &repo, const user_ptr &user)
{ 
    m_offset_snapshot = repo.serialize(m_builder, user);
}

flatbuffers::DetachedBuffer nplex::load_builder_t::finish(rev_t crev, bool accepted)
{
    auto msg = CreateMessage(m_builder, 
        MsgContent::LOAD_RESPONSE,
        CreateLoadResponse(m_builder, 
            m_cid,
            crev,
            accepted,
            (!accepted ? 0 : m_offset_snapshot)
        ).Union()
    );

    m_builder.Finish(msg);
    return m_builder.Release();
}

bool nplex::changes_builder_t::append_updates(const std::span<update_t> &updates)
{
    for (const auto &update : updates)
    {
        if (m_num_revs >= m_max_revs || m_builder.GetSize() >= m_max_bytes)
            return false;

        auto off = serialize_update(m_builder, update, m_user.get(), false);

        m_last_meta.rev = update.meta->rev;
        m_last_meta.user = update.meta->user;
        m_last_meta.timestamp = static_cast<std::uint64_t>(update.meta->timestamp.count());
        m_last_meta.type = update.meta->type;
        m_num_revs++;

        if (!off.IsNull())
            m_updates.push_back(off);
    }

    return (m_num_revs < m_max_revs && m_builder.GetSize() < m_max_bytes);
}

/**
 * This method is equivalent to:
 * 
 *   auto upd = deserialize_update(buf, user)
 *   if (has_changes(upd)) {
 *       auto off = serialize_update(m_builder, upd)
 *       m_updates.push_back(off);
 *   }
 * 
 * The main difference is that this method avoids intermediary memory allocations.
 */
bool nplex::changes_builder_t::append_update(const Update *update)
{
    std::vector<Offset<KeyValue>> upserts;
    std::vector<Offset<String>> deletes;

    if (m_num_revs >= m_max_revs || m_builder.GetSize() >= m_max_bytes)
        return false;

    if (update->upserts())
    {
        for (const auto &kv : *update->upserts())
        {
            if (m_user && !m_user->is_authorized(NPLEX_READ, kv->key()->c_str()))
                continue;

            auto off = CreateKeyValue(
                m_builder, 
                m_builder.CreateString(kv->key()), 
                m_builder.CreateVector(
                    kv->value()->data(), 
                    kv->value()->size()
                )
            );

            upserts.push_back(off);
        }
    }

    if (update->deletes())
    {
        for (const auto &key : *update->deletes())
        {
            if (m_user && !m_user->is_authorized(NPLEX_READ, key->c_str()))
                continue;

            deletes.push_back(m_builder.CreateString(key));
        }
    }

    m_last_meta.rev = update->rev();
    m_last_meta.user = update->user()->c_str();
    m_last_meta.timestamp = update->timestamp();
    m_last_meta.type = update->type();
    m_num_revs++;

    if (!upserts.empty() || !deletes.empty())
    {
        auto off = CreateUpdate(
            m_builder,
            update->rev(),
            m_builder.CreateString(update->user()),
            update->timestamp(),
            update->type(),
            m_builder.CreateVector(upserts),
            m_builder.CreateVector(deletes)
        );

        m_updates.push_back(off);
    }

    return (m_num_revs < m_max_revs && m_builder.GetSize() < m_max_bytes);
}

flatbuffers::DetachedBuffer nplex::changes_builder_t::finish(rev_t crev, bool ending_meta)
{
    if (ending_meta && m_last_meta.rev != 0)
    {
        auto off = CreateUpdate(
            m_builder,
            m_last_meta.rev,
            m_builder.CreateString(m_last_meta.user),
            m_last_meta.timestamp,
            m_last_meta.type
        );

        m_updates.push_back(off);
    }

    auto msg = CreateMessage(m_builder, 
        MsgContent::CHANGES_PUSH,
        CreateChangesPush(m_builder, 
            m_cid,
            crev,
            (m_updates.empty() ? 0 : m_builder.CreateVector(m_updates))
        ).Union()
    );

    m_builder.Finish(msg);
    return m_builder.Release();
}
