#include <cassert>
#include <fmt/core.h>
#include "utf8.h"
#include "base64.hpp"
#include "utils.hpp"
#include "schema.hpp"
#include "json.hpp"

using namespace nplex::msgs;

// ==========================================================
// Internal (static) functions
// ==========================================================

template<typename T, std::enable_if_t<std::is_integral_v<T>, int> = 0>
static void json_append_number(T value, std::string &out)
{
    out += std::to_string(value);
}

static void json_append_text(std::string_view text, std::string &out)
{
    out.push_back('"');
    out += text;
    out.push_back('"');
}

static void json_append_escaped_text(std::string_view text, std::string &out)
{
    out.push_back('"');

    const char *p = text.data();
    const char *end = p + text.size();
    const char *start = p;

    while (p < end)
    {
        unsigned char c = static_cast<unsigned char>(*p);

        if (c >= 0x20 && c != '"' && c != '\\') {
            ++p;
            continue;
        }

        if (p > start)
            out.append(start, p);

        switch (c)
        {
            case '"':  out.append("\\\""); break;
            case '\\': out.append("\\\\"); break;
            case '\b': out.append("\\b");  break;
            case '\f': out.append("\\f");  break;
            case '\n': out.append("\\n");  break;
            case '\r': out.append("\\r");  break;
            case '\t': out.append("\\t");  break;
            default: {
                char buf[7];
                snprintf(buf, sizeof(buf), "\\u%04x", static_cast<int>(c));
                out.append(buf);
                break;
            }
        }

        start = ++p;
    }

    if (p > start)
        out.append(start, p);

    out.push_back('"');
}

// ==========================================================
// JSON functions
// ==========================================================

static void to_json(const KeyValue *kv, std::string &out)
{
    if (!kv) {
        out += "null";
        return;
    }

    const char *content = reinterpret_cast<const char *>(kv->value()->data());
    size_t length = kv->value()->size();
    bool is_utf8 = (content && !utf8nvalid(reinterpret_cast<const utf8_int8_t *>(content), length));

    out += "{";
    json_append_text("key", out);
    out += ":";
    json_append_escaped_text((kv->key() ? kv->key()->c_str() : "null"), out);
    out += ",";

    json_append_text("value", out);
    out += ":";

    if (content == nullptr)
    {
        out += "null";
    }
    else if (is_utf8)
    {
        json_append_escaped_text({content, length}, out);
        out += ",";
        json_append_text("encoding", out);
        out += ":";
        json_append_text("text", out);
    }
    else
    {
        json_append_text(base64::to_base64({content, length}), out);
        out += ",";
        json_append_text("encoding", out);
        out += ":";
        json_append_text("base64", out);
    }

    out += "}";
}

static void to_json(const Update *upd, std::string &out)
{
    assert(upd);

    out += "{";
    json_append_text("rev", out);
    out += ":";
    json_append_number(upd->rev(), out);
    out += ",";

    json_append_text("user", out);
    out += ":";
    json_append_escaped_text(upd->user()->c_str(), out);
    out += ",";

    json_append_text("timestamp", out);
    out += ":";
    json_append_text(nplex::to_iso8601(nplex::millis_t{upd->timestamp()}), out);
    out += ",";

    json_append_text("tx_type", out);
    out += ":";
    json_append_number(upd->tx_type(), out);

    if (upd->upserts() && !upd->upserts()->empty())
    {
        out += ",";
        json_append_text("upserts", out);
        out += ":";
        out += "[";

        size_t len = static_cast<size_t>(upd->upserts()->size());

        for (size_t i = 0; i < len; ++i)
        {
            to_json(upd->upserts()->Get(i), out);

            if (i < len - 1)
                out += ",";
        }

        out += "]";
    }

    if (upd->deletes() && !upd->deletes()->empty())
    {
        out += ",";
        json_append_text("deletes", out);
        out += ":";
        out += "[";

        size_t len = static_cast<size_t>(upd->deletes()->size());

        for (size_t i = 0; i < len; ++i)
        {
            const auto &key = upd->deletes()->Get(i);

            json_append_escaped_text({key->c_str(), key->size()}, out);

            if (i < len - 1)
                out += ",";
        }

        out += "]";
    }

    out += "}";
}

static void to_json(const Snapshot *snp, std::string &out)
{
    assert(snp);

    out += "{";
    json_append_text("rev", out);
    out += ":";
    json_append_number(snp->rev(), out);

    if (snp->updates())
    {
        out += ",";
        json_append_text("updates", out);
        out += ":";
        out += "[";

        auto updates = snp->updates();
        auto len = updates->size();

        for (flatbuffers::uoffset_t i = 0; i < len; i++)
        {
            to_json(updates->Get(i), out);

            if (i < len - 1)
                out += ",";
        }

        out += "]";
    }

    out += "}";
}

std::string nplex::update_to_json(const char *data, size_t len)
{
    assert(data);
    assert(len > 0);

    if (!data || len == 0)
        return "<invalid update>";

    auto verifier = flatbuffers::Verifier(reinterpret_cast<const std::uint8_t *>(data), len);

    if (!verifier.VerifyBuffer<nplex::msgs::Update>(nullptr))
        return "<invalid update>";

    auto update = flatbuffers::GetRoot<nplex::msgs::Update>(data);
    std::string str;

    str.reserve(len * 2);
    to_json(update, str);

    return str;
}

std::string nplex::snapshot_to_json(const char *data, size_t len)
{
    assert(data);
    assert(len > 0);

    if (!data || len == 0)
        return "<invalid snapshot>";

    auto verifier = flatbuffers::Verifier(reinterpret_cast<const std::uint8_t *>(data), len);

    if (!verifier.VerifyBuffer<nplex::msgs::Snapshot>(nullptr))
        return "<invalid snapshot>";

    auto snapshot = flatbuffers::GetRoot<nplex::msgs::Snapshot>(data);
    std::string str;

    str.reserve(len * 2);
    to_json(snapshot, str);

    return str;
}
