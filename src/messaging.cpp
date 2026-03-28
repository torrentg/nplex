#include <cassert>
#include "utf8.h"
#include "cppcrc.h"
#include "journal.h"
#include "user.hpp"
#include "utils.hpp"
#include "store.hpp"
#include "exception.hpp"
#include "messaging.hpp"

using namespace nplex::msgs;
using namespace flatbuffers;

// ==========================================================
// Internal (static) functions
// ==========================================================

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
        if (user && !user->is_authorized(CRUD_READ, key))
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
// nplex functions
// ==========================================================

nplex::output_msg_t::output_msg_t(DetachedBuffer &&msg) : content(std::move(msg))
{
    len = (std::uint32_t)(content.size() + sizeof(len) + sizeof(metadata) + sizeof(checksum));
    len = htonl(len);

    metadata = htonl(0); // not-compressed

    checksum = CRC32::CRC32::calc(reinterpret_cast<const std::uint8_t *>(&len), sizeof(len));
    checksum = CRC32::CRC32::calc(reinterpret_cast<const std::uint8_t *>(&metadata), sizeof(metadata), checksum);
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
        case msgs::MsgContent::SNAPSHOT_RESPONSE:
            table->SetField<uint64_t>(SnapshotResponse::VT_CREV, crev);
            return true;
        case msgs::MsgContent::UPDATES_RESPONSE:
            table->SetField<uint64_t>(UpdatesResponse::VT_CREV, crev);
            return true;
        [[likely]]
        case msgs::MsgContent::UPDATES_PUSH:
            table->SetField<uint64_t>(UpdatesPush::VT_CREV, crev);
            return true;
        case msgs::MsgContent::SUBMIT_RESPONSE: 
            table->SetField<uint64_t>(SubmitResponse::VT_CREV, crev);
            return true;
        [[likely]]
        case msgs::MsgContent::KEEPALIVE_PUSH:
            table->SetField<uint64_t>(KeepAlivePush::VT_CREV, crev);
            return true;
        default:
            return false;
    }
}

DetachedBuffer nplex::create_login_msg(std::size_t cid, LoginCode code, rev_t rev0, rev_t crev, const user_ptr &user)
{
    FlatBufferBuilder builder;
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
            (user ? user->params.connection.keepalive_millis : 0),
            (user ? builder.CreateVector(permissions) : 0)
        ).Union()
    );

    builder.Finish(msg);
    return builder.Release();
}

flatbuffers::DetachedBuffer nplex::create_updates_msg(std::size_t cid, rev_t crev, rev_t rev0, bool accepted)
{
    FlatBufferBuilder builder;

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

flatbuffers::DetachedBuffer nplex::create_snapshot_msg(std::size_t cid, rev_t crev, rev_t rev0, bool accepted, const store_t &store, const user_ptr &user)
{
    FlatBufferBuilder builder;

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

nplex::update_dto_t nplex::deserialize_update(const Update *msg, const user_ptr &user)
{
    update_dto_t update;

    update.rev = msg->rev();
    update.user = msg->user()->c_str();
    update.timestamp = msg->timestamp();
    update.tx_type = msg->tx_type();

    if (msg->upserts())
    {
        for (const auto &kv : *msg->upserts())
        {
            if (!kv || !kv->key() || !kv->key()->c_str() || !kv->value())
                continue;

            if (user && !user->is_authorized(CRUD_READ, kv->key()->c_str()))
                continue;

            update.upserts.push_back({
                kv->key()->c_str(),
                create_string(kv->value())
            });
        }
    }

    if (msg->deletes())
    {
        for (const auto &key : *msg->deletes())
        {
            if (!key || !key->c_str())
                continue;

            if (user && !user->is_authorized(CRUD_READ, key->c_str()))
                continue;

            update.deletes.push_back(key->c_str());
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
// JSON functions
// ==========================================================

static void json_append_text(std::string_view text, std::string &out)
{
    out.push_back('"');

    for (char c : text)
    {
        switch (c)
        {
            case '\"': out += "\\\""; break;
            case '\\': out += "\\\\"; break;
            case '\b': out += "\\b";  break;
            case '\f': out += "\\f";  break;
            case '\n': out += "\\n";  break;
            case '\r': out += "\\r";  break;
            case '\t': out += "\\t";  break;
            default:   out += c;      break;  // assume valid utf8
        }
    }

    out.push_back('"');
}

static void json_append_bytes(const flatbuffers::Vector<uint8_t> *bytes, std::string &out)
{
    const size_t max_preview = 12;

    if (bytes == nullptr) {
        out += "null";
        return;
    }

    const char *content = reinterpret_cast<const char *>(bytes->data());
    size_t length = bytes->size();
    bool is_utf8 = !(utf8nvalid(reinterpret_cast<const utf8_int8_t *>(content), length));

    if (is_utf8) {
        json_append_text({content, length}, out);
        return;
    }

    out += "\"<";

    for (size_t i = 0; i < std::min(length, max_preview); ++i)
        out += fmt::format("\\x{:02x}", static_cast<unsigned char>(content[i]));

    if (length > max_preview)
        out += "...";

    out += ">\"";
}

void nplex::to_json(const msgs::KeyValue *kv, json_params_t &params, std::string &out)
{
    assert(kv);

    std::string space = (params.mode == json_params_t::mode_e::INDENT ? " " : "");
    std::string indent1 = (params.mode == json_params_t::mode_e::INDENT ? std::string(params.indent_curr, ' ') : "");
    std::string indent2 = (params.mode == json_params_t::mode_e::INDENT ? std::string(params.indent_size, ' ') : "");
    std::string line_break = (params.mode == json_params_t::mode_e::INDENT ? "\n" : "");

    out += indent1;
    out += "{" + line_break;

    out += indent1 + indent2;
    json_append_text("key", out);
    out += ":" + space;
    json_append_text((kv->key() ? kv->key()->c_str() : "null"), out);
    out += "," + line_break;

    out += indent1 + indent2;
    json_append_text("value", out);
    out += ":" + space;
    json_append_bytes(kv->value(), out);
    out += line_break;

    out += indent1;
    out += "}";
}

void nplex::to_json(const msgs::Update *upd, json_params_t &params, std::string &out)
{
    assert(upd);

    std::string space = (params.mode == json_params_t::mode_e::INDENT ? " " : "");
    std::string indent1 = (params.mode == json_params_t::mode_e::INDENT ? std::string(params.indent_curr, ' ') : "");
    std::string indent2 = (params.mode == json_params_t::mode_e::INDENT ? std::string(params.indent_size, ' ') : "");
    std::string line_break = (params.mode == json_params_t::mode_e::INDENT ? "\n" : "");

    out += "{" + line_break;
    params.indent_curr += params.indent_size;

    out += indent1 + indent2;
    json_append_text("rev", out);
    out += ":" + space;
    json_append_text(std::to_string(upd->rev()), out);
    out += "," + line_break;

    out += indent1 + indent2;
    json_append_text("user", out);
    out += ":" + space;
    json_append_text(upd->user()->c_str(), out);
    out += "," + line_break;

    out += indent1 + indent2;
    json_append_text("timestamp", out);
    out += ":" + space;
    json_append_text(to_iso8601(millis_t{upd->timestamp()}), out);
    out += "," + line_break;

    out += indent1 + indent2;
    json_append_text("tx_type", out);
    out += ":" + space;
    json_append_text(std::to_string(upd->tx_type()), out);

    if (upd->upserts() && !upd->upserts()->empty())
    {
        out += "," + line_break;

        out += indent1 + indent2;
        json_append_text("upserts", out);
        out += ":" + space + "[" + line_break;

        size_t len = static_cast<size_t>(upd->upserts()->size());

        params.indent_curr += params.indent_size;

        for (size_t i = 0; i < len; ++i)
        {
            to_json(upd->upserts()->Get(i), params, out);

            if (i < len - 1)
                out += ",";

            out += line_break;
        }

        params.indent_curr -= params.indent_size;

        out += indent1 + indent2 + "]";
    }

    if (upd->deletes() && !upd->deletes()->empty())
    {
        out += "," + line_break;

        out += indent1 + indent2;
        json_append_text("deletes", out);
        out += ":" + space + "[" + line_break;

        params.indent_curr += params.indent_size;

        size_t len = static_cast<size_t>(upd->deletes()->size());

        for (size_t i = 0; i < len; ++i)
        {
            const auto &key = upd->deletes()->Get(i);

            out += indent1 + indent2 + indent2;

            json_append_text(key->c_str(), out);

            if (i < len - 1)
                out += ",";

            out += line_break;
        }

        params.indent_curr -= params.indent_size;

        out += indent1 + indent2 + "]";
    }

    out += line_break;
    params.indent_curr -= params.indent_size;
    out += indent1;
    out += "}";
}

void nplex::to_json(const msgs::Snapshot *snp, json_params_t &params, std::string &out)
{
    assert(snp);

    std::string space = (params.mode == json_params_t::mode_e::INDENT ? " " : "");
    std::string indent1 = (params.mode == json_params_t::mode_e::INDENT ? std::string(params.indent_curr, ' ') : "");
    std::string indent2 = (params.mode == json_params_t::mode_e::INDENT ? std::string(params.indent_size, ' ') : "");
    std::string line_break = (params.mode == json_params_t::mode_e::INDENT ? "\n" : "");

    out += "{" + line_break;
    params.indent_curr += params.indent_size;

    out += indent1 + indent2;
    json_append_text("rev", out);
    out += ":" + space;
    json_append_text(std::to_string(snp->rev()), out);

    if (snp->updates())
    {
        out += "," + line_break;

        out += indent1 + indent2;
        json_append_text("updates", out);
        out += ":" + space + "[" + line_break;

        params.indent_curr += params.indent_size;

        auto updates = snp->updates();
        auto len = updates->size();

        for (flatbuffers::uoffset_t i = 0; i < len; i++)
        {
            out += indent1 + indent2 + indent2;
            to_json(updates->Get(i), params, out);

            if (i < len - 1)
                out += ",";

            out += line_break;
        }

        params.indent_curr -= params.indent_size;

        out += indent1 + indent2 + "]";
    }

    out += line_break;
    params.indent_curr -= params.indent_size;
    out += indent1;
    out += "}";
}

/**
 * Function used by journalctl to print a journal entry.
 * 
 * @param out File stream to write the output to.
 * @param entry Journal entry to print.
 */
extern "C" void print_journal_entry(FILE *out, const ldb_entry_t *entry)
{
    auto verifier = flatbuffers::Verifier(reinterpret_cast<const std::uint8_t *>(entry->data), entry->data_len);

    if (!verifier.VerifyBuffer<nplex::msgs::Update>(nullptr)) {
        fprintf(out, "<invalid entry>\n");
        return;
    }

    auto update = flatbuffers::GetRoot<nplex::msgs::Update>(entry->data);

    std::string str;
    nplex::json_params_t json_params(nplex::json_params_t::mode_e::COMPACT);
    nplex::to_json(update, json_params, str);

    fprintf(out, "%s\n", str.c_str());
}

/**
 * Function used to print a snapshot.
 * 
 * @param out File stream to write the output to.
 * @param data Pointer to the snapshot data.
 * @param len Length of the snapshot data.
 * @param mode JSON output mode ('c' for compact, 'i' for indented).
 */
extern "C" void print_snapshot(FILE *out, const char *data, size_t len, char mode)
{
    auto verifier = flatbuffers::Verifier(reinterpret_cast<const std::uint8_t *>(data), len);

    if (!verifier.VerifyBuffer<nplex::msgs::Snapshot>(nullptr)) {
        fprintf(out, "<invalid snapshot>\n");
        return;
    }

    auto snapshot = flatbuffers::GetRoot<nplex::msgs::Snapshot>(data);

    std::string str;
    nplex::json_params_t json_params(mode == 'c' ? nplex::json_params_t::mode_e::COMPACT : nplex::json_params_t::mode_e::INDENT);
    nplex::to_json(snapshot, json_params, str);

    fprintf(out, "%s\n", str.c_str());
}
