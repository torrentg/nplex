#include "messaging.hpp"
#include "user.hpp"
#include "utils.hpp"
#include "store.hpp"
#include "session.hpp"
#include "exception.hpp"
#include "journal.h"
#include <cassert>

using namespace nplex::msgs;
using namespace flatbuffers;

// ==========================================================
// Internal (static) functions
// ==========================================================

static std::string create_string(const flatbuffers::String *str)
{
    if (!str)
        return "";

    return std::string{str->c_str(), str->size()};
}

static std::string create_string(const flatbuffers::Vector<std::uint8_t> *value)
{
    if (!value)
        return "";

    return std::string{reinterpret_cast<const char *>(value->data()), static_cast<std::size_t>(value->size())};
}

static flatbuffers::Offset<nplex::msgs::Update> serialize_update(flatbuffers::FlatBufferBuilder &builder, const nplex::update_t &update, const nplex::user_ptr &user, bool force)
{
    std::vector<Offset<KeyValue>> upserts;
    std::vector<Offset<String>> deletes;

    for (const auto &[key, value] : update.upserts)
    {
        if (user && !user->is_authorized(CRUD_READ, key))
            continue;

        auto kv = CreateKeyValue(
            builder, 
            builder.CreateString(key.c_str(), key.size()),
            builder.CreateVector(
                reinterpret_cast<const uint8_t *>(value->data().c_str()), 
                value->data().size()
            )
        );

        upserts.push_back(kv);
    }

    for (const auto &key : update.deletes)
    {
        if (user && !user->is_authorized(CRUD_READ, key))
            continue;

        deletes.push_back(builder.CreateString(key));
    }

    if (!force && upserts.empty() && deletes.empty())
        return 0;

    return CreateUpdate(
        builder,
        update.meta->rev,
        builder.CreateString(update.meta->user.c_str(), update.meta->user.size()),
        static_cast<std::uint64_t>(update.meta->timestamp.count()),
        update.meta->tx_type,
        (upserts.empty() ? 0 : builder.CreateVector(upserts)),
        (deletes.empty() ? 0 : builder.CreateVector(deletes))
    );
}

static flatbuffers::Offset<nplex::msgs::Update> serialize_update(flatbuffers::FlatBufferBuilder &builder, const nplex::update_dto_t &update, const nplex::user_ptr &user, bool force)
{
    std::vector<Offset<KeyValue>> upserts;
    std::vector<Offset<String>> deletes;

    for (const auto &[key, value] : update.upserts)
    {
        if (user && !user->is_authorized(CRUD_READ, key.c_str()))
            continue;

        auto kv = CreateKeyValue(
            builder, 
            builder.CreateString(key),
            builder.CreateVector(
                reinterpret_cast<const uint8_t *>(value.data()), 
                value.size()
            )
        );

        upserts.push_back(kv);
    }

    for (const auto &key : update.deletes)
    {
        if (user && !user->is_authorized(CRUD_READ, key.c_str()))
            continue;

        deletes.push_back(builder.CreateString(key));
    }

    if (!force && upserts.empty() && deletes.empty())
        return 0;

    return CreateUpdate(
        builder,
        update.rev,
        builder.CreateString(update.user),
        static_cast<std::uint64_t>(update.timestamp),
        update.tx_type,
        (upserts.empty() ? 0 : builder.CreateVector(upserts)),
        (deletes.empty() ? 0 : builder.CreateVector(deletes))
    );
}

// ==========================================================
// output_chunk_t methods
// ==========================================================

nplex::output_chunk_t * nplex::output_chunk_t::create(std::span<flatbuffers::DetachedBuffer> msgs)
{
    if (msgs.empty() || msgs.size() > max_num_msgs())
        throw std::invalid_argument("invalid number of messages");

    size_t total_length = 0;
    size_t num_msgs = msgs.size();
    size_t allocation_size = sizeof(output_chunk_t) + num_msgs * (5 * sizeof(uv_buf_t) + sizeof(values_t));

    char *ptr = static_cast<char *>(::operator new(allocation_size));

    output_chunk_t *ret = new (ptr) output_chunk_t();
    uv_buf_t *buf_ptr = reinterpret_cast<uv_buf_t *>(ptr + sizeof(output_chunk_t));
    values_t *values_ptr = reinterpret_cast<values_t *>(ptr + sizeof(output_chunk_t) + num_msgs * 5 * sizeof(uv_buf_t));

    for (size_t i = 0; i < num_msgs; ++i)
    {
        auto &content = msgs[i];
        std::uint32_t checksum = 0;
        std::size_t content_len = content.size();
        auto msg_len = static_cast<std::uint32_t>(content_len + 4 * sizeof(std::uint32_t));

        assert(msg_len % sizeof(std::uint64_t) == 0); // aligned to 8 bytes
        values_ptr[i].len = htonl(msg_len);
        total_length += msg_len;

        values_ptr[i].metadata = htonl(0); // unused

        new (&values_ptr[i].content) flatbuffers::DetachedBuffer();
        values_ptr[i].content = std::move(content);

        checksum = crc32(reinterpret_cast<const char *>(&values_ptr[i].len), sizeof(std::uint32_t));
        checksum = crc32(reinterpret_cast<const char *>(&values_ptr[i].metadata), sizeof(std::uint32_t), checksum);
        checksum = crc32(reinterpret_cast<const char *>(values_ptr[i].content.data()), content_len, checksum);
        values_ptr[i].checksum = htonl(checksum);

        values_ptr[i].delimiter = htonl(0xFFFFFFFF);

        buf_ptr[5 * i + 0] = uv_buf_t{.base = reinterpret_cast<char *>(&values_ptr[i].len), .len = sizeof(uint32_t)};
        buf_ptr[5 * i + 1] = uv_buf_t{.base = reinterpret_cast<char *>(&values_ptr[i].metadata), .len = sizeof(uint32_t)};
        buf_ptr[5 * i + 2] = uv_buf_t{.base = reinterpret_cast<char *>(values_ptr[i].content.data()), .len = content_len};
        buf_ptr[5 * i + 3] = uv_buf_t{.base = reinterpret_cast<char *>(&values_ptr[i].checksum), .len = sizeof(uint32_t)};
        buf_ptr[5 * i + 4] = uv_buf_t{.base = reinterpret_cast<char *>(&values_ptr[i].delimiter), .len = sizeof(uint32_t)};
    }

    ret->num_msgs = static_cast<std::uint32_t>(num_msgs);

    if (total_length > 0xFFFFFFFF) {
        destroy(ret);
        throw std::overflow_error("Total message length exceeds maximum allowed size");
    }

    ret->total_length = static_cast<std::uint32_t>(total_length);

    return ret;
}

nplex::output_chunk_t::~output_chunk_t()
{
    if (num_msgs == 0)
        return;

    char *ptr = reinterpret_cast<char *>(this);
    uv_buf_t *buf_ptr = reinterpret_cast<uv_buf_t *>(ptr + sizeof(output_chunk_t));
    values_t *values_ptr = reinterpret_cast<values_t *>(ptr + sizeof(output_chunk_t) + num_msgs * 5 * sizeof(uv_buf_t));

    for (size_t i = 0; i < 5 * num_msgs; ++i)
        buf_ptr[i].~uv_buf_t();

    for (size_t i = 0; i < num_msgs; ++i)
        values_ptr[i].~values_t();
}

void nplex::output_chunk_t::destroy(output_chunk_t *obj)
{
    if (obj == nullptr)
        return;

    auto ptr = static_cast<void *>(obj);
    obj->~output_chunk_t();
    ::operator delete(ptr);
}

// ==========================================================
// nplex functions
// ==========================================================

const nplex::msgs::Message * nplex::parse_network_msg(const char *ptr, size_t len)
{
    if (len <= 4 * sizeof(std::uint32_t) || len % sizeof(std::uint64_t) != 0)
        return nullptr;

    if (len != ntohl_ptr(ptr))
        return nullptr;

    std::uint32_t metadata = ntohl_ptr(ptr + sizeof(std::uint32_t));
    UNUSED(metadata);

    std::uint32_t checksum = ntohl_ptr(ptr + len - 2 * sizeof(std::uint32_t));

    if (checksum != crc32(ptr, len - 2 * sizeof(std::uint32_t)))
        return nullptr;

    ptr += 2 * sizeof(std::uint32_t);
    len -= 4 * sizeof(std::uint32_t);

    auto verifier = flatbuffers::Verifier(reinterpret_cast<const std::uint8_t *>(ptr), len);

    if (!verifier.VerifyBuffer<Message>(nullptr))
        return nullptr;

    return flatbuffers::GetRoot<Message>(ptr);
}

DetachedBuffer nplex::create_login_msg(std::size_t cid, LoginCode code, rev_t rev0, rev_t crev, const user_ptr &user)
{
    FlatBufferBuilder builder(1024);
    std::vector<Offset<Acl>> permissions;

    if (user) {
        for (const auto &acl : user->permissions) {
            auto pattern = builder.CreateString(acl.pattern);
            permissions.push_back(CreateAcl(builder, pattern, acl.mode));
        }
    }

    auto msg = CreateMessage(builder, 
        MsgContent::LOGIN_RESPONSE, 
        CreateLoginResponse(builder, 
            cid, 
            code,
            rev0,
            crev,
            (user ? user->params.can_force : false),
            (user ? user->params.can_monitor : false),
            (user ? user->params.connection.keepalive_millis : 0),
            (user ? builder.CreateVector(permissions) : 0)
        ).Union()
    );

    builder.Finish(msg);
    assert(builder.GetSize() < 1024);
    return builder.Release();
}

flatbuffers::DetachedBuffer nplex::create_updates_msg(std::size_t cid, rev_t crev, rev_t rev0, bool accepted)
{
    FlatBufferBuilder builder(128);

    auto msg = CreateMessage(builder, 
        MsgContent::UPDATES_RESPONSE,
        CreateUpdatesResponse(builder, 
            cid,
            crev,
            rev0,
            accepted
        ).Union()
    );

    builder.Finish(msg);
    assert(builder.GetSize() < 128);
    return builder.Release();
}

flatbuffers::DetachedBuffer nplex::create_sessions_msg(std::size_t cid, rev_t crev, const session_ptr &session)
{
    assert(session->user());

    FlatBufferBuilder builder(512);
    ExitCode exit_code = ExitCode::CONNECTED;

    switch (session->error())
    {
        case 0:
            exit_code = ExitCode::CONNECTED;
            break;
        case ERR_CLOSED_BY_PEER:
            exit_code = ExitCode::CLOSED_BY_USER;
            break;
        case ERR_CLOSED_BY_LOCAL:
            exit_code = ExitCode::CLOSED_BY_SERVER;
            break;
        case ERR_CONNECTION_LOST:
            exit_code = ExitCode::CON_LOST;
            break;
        case ERR_UNACK:
            exit_code = ExitCode::EXCD_LIMITS;
            break;
        default:
            exit_code = ExitCode::COMM_ERROR;
            break;
    }

    auto off = CreateSession(
        builder,
        builder.CreateString(session->user()->name),
        builder.CreateString(session->addr().str()),
        exit_code,
        static_cast<uint64_t>(session->created_at().count()),
        static_cast<uint64_t>(session->disconnected_at().count())
    );

    auto msg = CreateMessage(builder, 
        MsgContent::SESSIONS_PUSH,
        CreateSessionsPush(builder, 
            cid,
            crev,
            off
        ).Union()
    );

    builder.Finish(msg);
    return builder.Release();
}

flatbuffers::DetachedBuffer nplex::create_keepalive_msg(rev_t crev)
{
    FlatBufferBuilder builder(128);

    auto msg = CreateMessage(builder, 
        MsgContent::KEEPALIVE_PUSH,
        CreateKeepAlivePush(builder, 
            crev
        ).Union()
    );

    builder.Finish(msg);
    assert(builder.GetSize() < 128);
    return builder.Release();
}

flatbuffers::DetachedBuffer nplex::create_ping_msg(std::size_t cid, rev_t crev, const std::string &payload)
{
    FlatBufferBuilder builder(128);

    auto msg = CreateMessage(builder, 
        MsgContent::PING_RESPONSE,
        CreatePingResponse(builder,
            cid, 
            crev,
            builder.CreateString(payload)
        ).Union()
    );

    builder.Finish(msg);
    assert(builder.GetSize() < 128);
    return builder.Release();
}

flatbuffers::DetachedBuffer nplex::create_submit_msg(std::size_t cid, rev_t crev, SubmitCode code, rev_t erev)
{
    FlatBufferBuilder builder(128);

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
    assert(builder.GetSize() < 128);
    return builder.Release();
}

flatbuffers::DetachedBuffer nplex::create_snapshot_msg(std::size_t cid, rev_t crev, rev_t rev0, bool accepted, const store_t &store, const user_ptr &user)
{
    FlatBufferBuilder builder(32 * 1024);

    auto snapshot_offset = (accepted ? store.serialize(builder, user) : 0);

    auto msg = CreateMessage(builder, 
        MsgContent::SNAPSHOT_RESPONSE,
        CreateSnapshotResponse(builder,
            cid, 
            crev,
            rev0,
            accepted,
            snapshot_offset
        ).Union()
    );

    builder.Finish(msg);
    return builder.Release();
}

flatbuffers::DetachedBuffer nplex::serialize_update(const update_t &update)
{
    FlatBufferBuilder builder;

    auto msg = ::serialize_update(builder, update, nullptr, true);

    builder.Finish(msg);
    return builder.Release();
}

nplex::update_dto_t nplex::deserialize_update(const Update *upd, const user_ptr &user)
{
    update_dto_t update;

    update.rev = upd->rev();
    update.user = ::create_string(upd->user());
    update.timestamp = upd->timestamp();
    update.tx_type = upd->tx_type();

    if (upd->upserts())
    {
        for (const auto &kv : *upd->upserts())
        {
            if (!kv || !kv->key() || !kv->key()->c_str() || !kv->value())
                continue;

            if (user && !user->is_authorized(CRUD_READ, kv->key()->c_str()))
                continue;

            update.upserts.push_back({
                ::create_string(kv->key()),
                ::create_string(kv->value())
            });
        }
    }

    if (upd->deletes())
    {
        for (const auto &key : *upd->deletes())
        {
            if (!key || !key->c_str())
                continue;

            if (user && !user->is_authorized(CRUD_READ, key->c_str()))
                continue;

            update.deletes.push_back(::create_string(key));
        }
    }

    return update;
}

// ==========================================================
// updates_builder_t methods
// ==========================================================

bool nplex::updates_builder_t::append(const update_t &update, const user_ptr &user, bool force)
{
    auto off = ::serialize_update(m_builder, update, user, force);

    if (!off.IsNull()) {
        m_updates.push_back(off);
        return true;
    }

    return false;
}

bool nplex::updates_builder_t::append(const update_dto_t &update, const user_ptr &user, bool force)
{
    auto off = ::serialize_update(m_builder, update, user, force);

    if (!off.IsNull()) {
        m_updates.push_back(off);
        return true;
    }

    return false;
}

flatbuffers::DetachedBuffer nplex::updates_builder_t::finish(uint64_t cid, rev_t crev)
{
    auto msg = CreateMessage(m_builder, 
        MsgContent::UPDATES_PUSH,
        CreateUpdatesPush(m_builder, 
            cid,
            crev,
            (m_updates.empty() ? 0 : m_builder.CreateVector(m_updates))
        ).Union()
    );

    m_builder.Finish(msg);
    auto buf = m_builder.Release();

    m_updates.clear();
    m_builder.Clear();

    return buf;
}

// ==========================================================
// sessions_builder_t methods
// ==========================================================

bool nplex::sessions_builder_t::append(const session_ptr &session)
{
    if (!session || !session->is_logged())
        return false;

    auto off = CreateSession(
        m_builder,
        m_builder.CreateString(session->user()->name),
        m_builder.CreateString(session->addr().str()),
        ExitCode::CONNECTED,
        static_cast<uint64_t>(session->created_at().count()),
        0
    );

    if (off.IsNull())
        return false;

    m_sessions.push_back(off);

    return true;
}

flatbuffers::DetachedBuffer nplex::sessions_builder_t::finish(uint64_t cid, rev_t crev)
{
    auto msg = CreateMessage(m_builder, 
        MsgContent::SESSIONS_RESPONSE,
        CreateSessionsResponse(m_builder, 
            cid,
            crev,
            (m_sessions.empty() ? 0 : m_builder.CreateVector(m_sessions))
        ).Union()
    );

    m_builder.Finish(msg);
    auto buf = m_builder.Release();

    m_sessions.clear();
    m_builder.Clear();

    return buf;
}
