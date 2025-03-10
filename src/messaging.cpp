#include "cppcrc.h"
#include "exception.hpp"
#include "messaging.hpp"

/**
 * Notes on this compilation unit:
 * 
 * When we use the libuv library we apply C conventions (instead of C++ ones):
 *   - When in Rome, do as the Romans do.
 *   - calloc/free are used instead of new/delete.
 *   - pointers to static functions.
 *   - C-style pointer casting.
 */

nplex::output_msg_t::output_msg_t(flatbuffers::DetachedBuffer &&content_)
{
    content = std::move(content_);

    len = (std::uint32_t)(content.size() + sizeof(len) + sizeof(metadata) + sizeof(checksum));
    len = htonl(len);

    metadata = htonl(0); // not-compressed

    checksum = CRC32::CRC32::calc(reinterpret_cast<const std::uint8_t *>(&len), sizeof(len));
    checksum = crc_utils::reverse(checksum ^ 0xFFFFFFFF);
    checksum = CRC32::CRC32::calc(reinterpret_cast<const std::uint8_t *>(&metadata), sizeof(metadata), checksum);
    checksum = crc_utils::reverse(checksum ^ 0xFFFFFFFF);
    checksum = CRC32::CRC32::calc(reinterpret_cast<const std::uint8_t *>(content.data()), content.size(), checksum);
    checksum = htonl(checksum);

    buf[0] = uv_buf_init((char *) &len, sizeof(len));
    buf[1] = uv_buf_init((char *) &metadata, sizeof(metadata));
    buf[2] = uv_buf_init((char *) content.data(), (unsigned int) content.size());
    buf[3] = uv_buf_init((char *) &checksum, sizeof(checksum));
}

flatbuffers::DetachedBuffer nplex::create_login_msg(std::size_t cid, msgs::LoginCode code, rev_t rev0, rev_t crev, const user_t &user)
{
    using namespace msgs;
    using namespace flatbuffers;

    FlatBufferBuilder builder;
    std::vector<flatbuffers::Offset<msgs::Acl>> permissions;

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
    using namespace msgs;
    using namespace flatbuffers;

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

const nplex::msgs::Message * nplex::parse_network_msg(const char *ptr, size_t len)
{
    using namespace nplex;
    using namespace nplex::msgs;
    using namespace flatbuffers;

    if (len <= 3 * sizeof(std::uint32_t))
        return nullptr;

    if (len != ntohl(*((const std::uint32_t *) ptr)))
        return nullptr;

    std::uint32_t metadata = ntohl(*((const std::uint32_t *) (ptr + sizeof(std::uint32_t))));
    // TODO: uncompress if (metadata & LZ4)
    UNUSED(metadata);

    std::uint32_t checksum = ntohl(*((const std::uint32_t *) (ptr + len - sizeof(std::uint32_t))));

    if (checksum != CRC32::CRC32::calc(reinterpret_cast<const std::uint8_t *>(ptr), len - sizeof(std::uint32_t)))
        return nullptr;

    ptr += 2 * sizeof(std::uint32_t);
    len -= 3 * sizeof(std::uint32_t);

    auto verifier = flatbuffers::Verifier((const std::uint8_t *) ptr, len);

    if (!verifier.VerifyBuffer<nplex::msgs::Message>(nullptr))
        return nullptr;

    return flatbuffers::GetRoot<nplex::msgs::Message>(ptr);
}

flatbuffers::DetachedBuffer nplex::create_ping_msg(std::size_t cid, rev_t crev, const std::string &payload)
{
    using namespace msgs;
    using namespace flatbuffers;

    FlatBufferBuilder builder;

    auto msg = CreateMessage(builder, 
        MsgContent::PING_RESPONSE,
        CreatePingResponse(builder,
            cid, 
            crev,
            builder.CreateString(payload.c_str())
        ).Union()
    );

    builder.Finish(msg);
    return builder.Release();
}

flatbuffers::DetachedBuffer nplex::create_load_err_msg(std::size_t cid, rev_t crev)
{
    using namespace msgs;
    using namespace flatbuffers;

    FlatBufferBuilder builder;

    auto msg = CreateMessage(builder, 
        MsgContent::LOAD_RESPONSE,
        CreateLoadResponse(builder,
            cid, 
            crev,
            false
        ).Union()
    );

    builder.Finish(msg);
    return builder.Release();
}

flatbuffers::DetachedBuffer nplex::create_submit_msg(std::size_t cid, rev_t crev, msgs::SubmitCode code, rev_t erev)
{
    using namespace msgs;
    using namespace flatbuffers;

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

flatbuffers::DetachedBuffer nplex::create_update_msg(flatbuffers::FlatBufferBuilder &builder, std::size_t cid, rev_t crev, flatbuffers::Offset<msgs::Update> upd)
{
    using namespace msgs;
    using namespace flatbuffers;

    auto msg = CreateMessage(builder, 
        MsgContent::UPDATE_PUSH,
        CreateUpdatePush(builder, 
            cid, 
            crev,
            upd
        ).Union()
    );

    builder.Finish(msg);
    return builder.Release();
}

flatbuffers::Offset<nplex::msgs::Update> nplex::serialize_update(flatbuffers::FlatBufferBuilder &builder, const update_t &update, const user_t *user, bool force)
{
    std::vector<flatbuffers::Offset<msgs::KeyValue>> upserts;
    std::vector<flatbuffers::Offset<flatbuffers::String>> deletes;

    for (const auto &[key, value] : update.upserts)
    {
        if (user && !user->is_authorized(NPLEX_READ, key))
            continue;

        auto kv = msgs::CreateKeyValue(
            builder, 
            builder.CreateString(key.c_str()), 
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

        deletes.push_back(builder.CreateString(key.c_str()));
    }

    if (!force && upserts.empty() && deletes.empty())
        return 0;

    return msgs::CreateUpdate(
        builder,
        update.meta->rev,
        builder.CreateString(update.meta->user.c_str()),
        static_cast<std::uint64_t>(update.meta->timestamp.count()),
        update.meta->type,
        builder.CreateVector(upserts),
        builder.CreateVector(deletes)
    );
}

flatbuffers::DetachedBuffer nplex::serialize_update(const update_t &update)
{
    using namespace msgs;
    using namespace flatbuffers;

    FlatBufferBuilder builder;

    auto msg = serialize_update(builder, update, nullptr, true);

    builder.Finish(msg);
    return builder.Release();
}

nplex::update_t nplex::deserialize_update(const msgs::Update *msg, const user_ptr &user)
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
                    gto::cstring{
                        reinterpret_cast<const char *>(kv->value()->data()), 
                        kv->value()->size()
                    },
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
