#include "messaging.hpp"
#include "user.hpp"
#include "utils.hpp"
#include "store.hpp"
#include "session.hpp"
#include "exception.hpp"
#include <cassert>

using namespace nplex::msgs;
using namespace flatbuffers;

// ==========================================================
// Internal (static) functions
// ==========================================================

static constexpr std::size_t DEFAULT_BYTES_SNAPSHOT = 256 * 1024;
static constexpr std::size_t MIN_INITIAL_BYTES_UPDATES = 4 * 1024;
static constexpr std::size_t MAX_INITIAL_BYTES_UPDATES = 16 * 1024 * 1024;

[[nodiscard]]
static std::vector<nplex::update_t> get_updates(const nplex::snapshot_t &snapshot, const nplex::user_ptr &user = nullptr)
{
    using namespace nplex;

    std::map<rev_t, update_t> updates;

    for (const auto &[key, stored_value] : snapshot.data)
    {
        if (user && !user->is_authorized(CRUD_READ, key))
            continue;

        auto rev = stored_value->rev();
        auto it = updates.find(rev);

        if (it == updates.end())
        {
            auto [it_new, inserted] = updates.emplace(rev, update_t{
                .meta = stored_value->meta(), 
                .upserts = {}, 
                .deletes = {}
            });

            it = it_new;
        }

        auto &upserts = it->second.upserts;
        upserts.push_back({key, stored_value->value()});
    }

    std::vector<update_t> result;

    for (auto &[rev, update] : updates)
        result.push_back(std::move(update));

    return result;
}

[[nodiscard]] 
static fb::Offset<nplex::msgs::Update> serialize_update(fb::FlatBufferBuilder &builder, const nplex::update_t &update, const nplex::user_ptr &user = nullptr)
{
    std::vector<Offset<KeyValue>> upserts;
    std::vector<Offset<String>> deletes;

    for (const auto &[key, value] : update.upserts)
    {
        if (user && !user->is_authorized(CRUD_READ, key))
            continue;

        auto kv = CreateKeyValue(
            builder, 
            builder.CreateString(key),
            builder.CreateVector(
                reinterpret_cast<const uint8_t *>(value.c_str()), 
                value.size()
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

    if (user && upserts.empty() && deletes.empty())
        return 0;

    return CreateUpdate(
        builder,
        update.meta->rev,
        builder.CreateString(update.meta->username),
        static_cast<std::uint64_t>(update.meta->timestamp.count()),
        update.meta->tx_type,
        (upserts.empty() ? 0 : builder.CreateVector(upserts)),
        (deletes.empty() ? 0 : builder.CreateVector(deletes))
    );
}

[[nodiscard]]
static fb::Offset<nplex::msgs::Update> serialize_update(fb::FlatBufferBuilder &builder, const nplex::msgs::Update *update, const nplex::user_ptr &user = nullptr)
{
    std::vector<Offset<KeyValue>> upserts;
    std::vector<Offset<String>> deletes;

    if (update->upserts())
    {
        for (const auto kv : *update->upserts())
        {
            if (!kv || !kv->key() || !kv->value())
                continue;

            if (user && !user->is_authorized(CRUD_READ, kv->key()->c_str()))
                continue;

            auto offset = CreateKeyValue(
                builder, 
                builder.CreateString(kv->key()),
                builder.CreateVector(kv->value()->data(), kv->value()->size())
            );

            upserts.push_back(offset);
        }
    }

    if (update->deletes())
    {
        for (const auto key : *update->deletes())
        {
            if (!key)
                continue;

            if (user && !user->is_authorized(CRUD_READ, key->c_str()))
                continue;

            auto offset = builder.CreateString(key);

            deletes.push_back(offset);
        }
    }

    if (user && upserts.empty() && deletes.empty())
        return 0;

    return CreateUpdate(
        builder,
        update->rev(),
        builder.CreateString(update->username()),
        update->timestamp(),
        update->tx_type(),
        (upserts.empty() ? 0 : builder.CreateVector(upserts)),
        (deletes.empty() ? 0 : builder.CreateVector(deletes))
    );
}

[[nodiscard]]
static fb::Offset<nplex::msgs::Snapshot> serialize_snapshot(fb::FlatBufferBuilder &builder, const nplex::snapshot_t &snapshot, const nplex::user_ptr &user = nullptr)
{
    std::vector<fb::Offset<Update>> upserts;

    auto updates = get_updates(snapshot, user);

    for (const auto &update : updates) {
        auto offset = ::serialize_update(builder, update, user);
        upserts.push_back(offset);
    }

    return CreateSnapshot(
        builder, 
        static_cast<std::uint64_t>(snapshot.rev), 
        static_cast<std::uint64_t>(snapshot.timestamp.count()), 
        builder.CreateVector(upserts)
    );
}

// ==========================================================
// outgoing_network_frames_t methods
// ==========================================================

nplex::outgoing_network_frames_t * nplex::outgoing_network_frames_t::create(std::span<DetachedBuffer> msgs)
{
    if (msgs.empty() || msgs.size() > max_num_msgs())
        throw std::invalid_argument("invalid number of messages");

    size_t total_length = 0;
    size_t num_msgs = msgs.size();
    size_t allocation_size = sizeof(outgoing_network_frames_t) + num_msgs * (2 * sizeof(uv_buf_t) + sizeof(values_t));

    char *ptr = static_cast<char *>(::operator new(allocation_size));

    outgoing_network_frames_t *ret = new (ptr) outgoing_network_frames_t();
    uv_buf_t *buf_ptr = reinterpret_cast<uv_buf_t *>(ptr + sizeof(outgoing_network_frames_t));
    values_t *values_ptr = reinterpret_cast<values_t *>(ptr + sizeof(outgoing_network_frames_t) + num_msgs * 2 * sizeof(uv_buf_t));

    for (size_t i = 0; i < num_msgs; ++i)
    {
        auto &content = msgs[i];
        if (content.size() > UINT32_MAX) {
            destroy(ret);
            throw std::overflow_error("Message content exceeds maximum allowed size");
        }

        std::uint32_t content_len = static_cast<std::uint32_t>(content.size());
        std::uint32_t frame_len = content_len + 2 * sizeof(std::uint32_t);
        std::uint32_t checksum = 0;

        values_ptr[i].len = htonl(content_len);
        total_length += frame_len;

        new (&values_ptr[i].content) DetachedBuffer();
        values_ptr[i].content = std::move(content);

        checksum = crc32(reinterpret_cast<const char *>(values_ptr[i].content.data()), content_len);
        values_ptr[i].checksum = htonl(checksum);

        buf_ptr[2 * i + 0] = uv_buf_t{.base = reinterpret_cast<char *>(&values_ptr[i].len), .len = 2 * sizeof(std::uint32_t)};
        buf_ptr[2 * i + 1] = uv_buf_t{.base = reinterpret_cast<char *>(values_ptr[i].content.data()), .len = content_len};
    }

    ret->num_msgs = static_cast<std::uint32_t>(num_msgs);

    if (total_length > 0xFFFFFFFF) {
        destroy(ret);
        throw std::overflow_error("Total message length exceeds maximum allowed size");
    }

    ret->total_length = static_cast<std::uint32_t>(total_length);

    return ret;
}

nplex::outgoing_network_frames_t::~outgoing_network_frames_t()
{
    if (num_msgs == 0)
        return;

    char *ptr = reinterpret_cast<char *>(this);
    uv_buf_t *buf_ptr = reinterpret_cast<uv_buf_t *>(ptr + sizeof(outgoing_network_frames_t));
    values_t *values_ptr = reinterpret_cast<values_t *>(ptr + sizeof(outgoing_network_frames_t) + num_msgs * 2 * sizeof(uv_buf_t));

    for (size_t i = 0; i < 2 * num_msgs; ++i)
        buf_ptr[i].~uv_buf_t();

    for (size_t i = 0; i < num_msgs; ++i)
        values_ptr[i].~values_t();
}

void nplex::outgoing_network_frames_t::destroy(outgoing_network_frames_t *obj)
{
    if (obj == nullptr)
        return;

    auto ptr = static_cast<void *>(obj);
    obj->~outgoing_network_frames_t();
    ::operator delete(ptr);
}

// ==========================================================
// nplex functions
// ==========================================================

const nplex::msgs::Message * nplex::parse_network_frame(const char *ptr, size_t len)
{
    if (len <= 2 * sizeof(std::uint32_t))
        return nullptr;

    std::uint32_t content_len = ntohl_ptr(ptr);

    if (len != (2 * sizeof(std::uint32_t) + content_len))
        return nullptr;

    std::uint32_t checksum = ntohl_ptr(ptr + sizeof(std::uint32_t));

    ptr += 2 * sizeof(std::uint32_t);
    len -= 2 * sizeof(std::uint32_t);

    if (checksum != crc32(ptr, len))
        return nullptr;

    auto verifier = fb::Verifier(reinterpret_cast<const std::uint8_t *>(ptr), len);

    if (!verifier.VerifyBuffer<Message>(nullptr))
        return nullptr;

    return fb::GetRoot<Message>(ptr);
}

DetachedBuffer nplex::create_login_response(std::size_t cid, LoginCode code, rev_t rev0, rev_t crev, const user_ptr &user)
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

fb::DetachedBuffer nplex::create_updates_response(std::size_t cid, rev_t crev, rev_t rev0, bool accepted)
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

fb::DetachedBuffer nplex::create_sessions_push(std::size_t cid, rev_t crev, const session_ptr &session)
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
        case ERR_DATA_RETRIEVAL:
            exit_code = ExitCode::CLOSED_BY_SERVER;
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

fb::DetachedBuffer nplex::create_keepalive_push(rev_t crev)
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

fb::DetachedBuffer nplex::create_ping_response(std::size_t cid, rev_t crev, const std::string &payload)
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

fb::DetachedBuffer nplex::create_submit_response(std::size_t cid, rev_t crev, SubmitCode code, rev_t erev)
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

fb::DetachedBuffer nplex::create_snapshot_response_reject(std::size_t cid, rev_t crev, rev_t rev0)
{
    FlatBufferBuilder builder(128);

    auto msg = CreateMessage(builder, 
        MsgContent::SNAPSHOT_RESPONSE,
        CreateSnapshotResponse(builder,
            cid, 
            crev,
            rev0,
            false,
            0
        ).Union()
    );

    builder.Finish(msg);
    return builder.Release();
}

fb::DetachedBuffer nplex::serialize_update(const update_t &update)
{
    FlatBufferBuilder builder;

    auto msg = ::serialize_update(builder, update);

    builder.Finish(msg);
    return builder.Release();
}

fb::DetachedBuffer nplex::serialize_snapshot(const snapshot_t &snapshot)
{
    FlatBufferBuilder builder(DEFAULT_BYTES_SNAPSHOT);

    auto msg = ::serialize_snapshot(builder, snapshot);

    builder.Finish(msg);
    return builder.Release();
}

// ==========================================================
// snapshot_response_builder_t methods
// ==========================================================

nplex::snapshot_response_builder_t::snapshot_response_builder_t() : m_builder(DEFAULT_BYTES_SNAPSHOT)
{
    // Nothing to do
}

void nplex::snapshot_response_builder_t::set_snapshot(const snapshot_t &snapshot, const user_ptr &user)
{
    std::vector<fb::Offset<msgs::Update>> upserts;

    auto updates = get_updates(snapshot, user);

    for (const auto &update : updates) {
        auto offset = ::serialize_update(m_builder, update, user);
        upserts.push_back(offset);
    }

    m_snapshot_offset = msgs::CreateSnapshot(
        m_builder, 
        static_cast<std::uint64_t>(snapshot.rev), 
        static_cast<std::uint64_t>(snapshot.timestamp.count()), 
        m_builder.CreateVector(upserts)
    );
}

DetachedBuffer nplex::snapshot_response_builder_t::finish(uint64_t cid, rev_t crev, rev_t rev0)
{
    auto msg = CreateMessage(m_builder, 
        MsgContent::SNAPSHOT_RESPONSE,
        CreateSnapshotResponse(m_builder,
            cid, 
            crev,
            rev0,
            true,
            m_snapshot_offset
        ).Union()
    );

    m_builder.Finish(msg);
    auto buf = m_builder.Release();

    m_snapshot_offset = 0;
    m_builder.Clear();

    return buf;
}

// ==========================================================
// updates_push_builder_t methods
// ==========================================================

nplex::updates_push_builder_t::updates_push_builder_t(user_ptr usr, std::size_t initial_size) : m_user(std::move(usr))
{
    auto size = std::clamp<std::size_t>(initial_size, MIN_INITIAL_BYTES_UPDATES, MAX_INITIAL_BYTES_UPDATES);
    m_builder = FlatBufferBuilder(size);
}

// Try to aproximate the final size (in bytes) of the UpdatesPush message
std::size_t nplex::updates_push_builder_t::bytes() const
{
    std::size_t ret = 0;

    // Append the size of the current updates
    ret += m_builder.GetSize();

    // Append the updates array itself + length (to be done at finish)
    ret += m_updates.size() * sizeof(fb::Offset<msgs::Update>) + 4;

    // Others (cid, rev, msg_type, offsets, vtables, padding)
    ret += 64;

    return ret;
}

bool nplex::updates_push_builder_t::append(const update_t &update)
{
    assert(update.meta);
    rev_t rev = update.meta->rev;
    assert(m_last_rev < rev);

    // User commits are always notified, even when empty
    const auto &user = ((m_user && update.meta->username != m_user->name) ? m_user: nullptr);

    auto off = ::serialize_update(m_builder, update, user);

    m_last_rev = rev;

    if (off.IsNull())
        return false;

    m_updates.push_back(off);

    if (m_initial_rev == 0) m_initial_rev = rev;
    m_final_rev = rev;

    return true;
}

bool nplex::updates_push_builder_t::append(const msgs::Update *update)
{
    assert(update);
    rev_t rev = update->rev();
    assert(m_last_rev < rev);

    // User commits are always notified, even when empty
    const auto &user = ((m_user && update->username()->string_view() != m_user->name.view()) ? m_user : nullptr);

    auto off = ::serialize_update(m_builder, update, user);

    m_last_rev = rev;

    if (off.IsNull())
        return false;

    m_updates.push_back(off);

    if (m_initial_rev == 0) m_initial_rev = rev;
    m_final_rev = rev;

    return true;
}

fb::DetachedBuffer nplex::updates_push_builder_t::finish(uint64_t cid, rev_t crev, bool force)
{
    assert(m_last_rev <= crev);

    // Append an empty update to signal the end of sync
    if (force && m_final_rev < m_last_rev)
    {
        auto off = CreateUpdate(
            m_builder,
            m_last_rev,
            m_builder.CreateString("unknow"),
            0,
            0,
            0,
            0
        );

        m_updates.push_back(off);
        m_final_rev = m_last_rev;
    }

    // Create the message
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

    m_builder.Clear();
    m_updates.clear();
    m_initial_rev = 0;
    m_final_rev = 0;
    m_last_rev = 0;

    return buf;
}

// ==========================================================
// sessions_response_builder_t methods
// ==========================================================

bool nplex::sessions_response_builder_t::append(const session_ptr &session)
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

fb::DetachedBuffer nplex::sessions_response_builder_t::finish(uint64_t cid, rev_t crev)
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

    m_builder.Clear();
    m_sessions.clear();

    return buf;
}
