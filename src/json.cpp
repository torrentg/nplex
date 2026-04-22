#include <cassert>
#include <fmt/core.h>
#include "utf8.h"
#include "utils.hpp"
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

static void json_append_bytes(const flatbuffers::Vector<uint8_t> *bytes, std::string &out, size_t max_preview)
{
    if (bytes == nullptr) {
        out += "null";
        return;
    }

    const char *content = reinterpret_cast<const char *>(bytes->data());
    size_t length = bytes->size();
    bool is_utf8 = !(utf8nvalid(reinterpret_cast<const utf8_int8_t *>(content), length));

    if (is_utf8) {
        json_append_escaped_text({content, length}, out);
        return;
    }

    out += "\"<";

    for (size_t i = 0; i < std::min(length, max_preview); ++i)
        out += fmt::format("\\x{:02x}", static_cast<unsigned char>(content[i]));

    if (length > max_preview)
        out += "...";

    out += ">\"";
}

// ==========================================================
// JSON functions
// ==========================================================

void nplex::to_json(const msgs::KeyValue *kv, json_params_t &params, std::string &out)
{
    assert(kv);

    out += params.indent;
    out += "{";
    out += params.line_break;

    params.push_indent();

    out += params.indent;
    json_append_text("key", out);
    out += ":";
    out += params.space;
    json_append_escaped_text((kv->key() ? kv->key()->c_str() : "null"), out);
    out += ",";
    out += params.line_break;

    out += params.indent;
    json_append_text("value", out);
    out += ":";
    out += params.space;
    json_append_bytes(kv->value(), out, params.max_binary);
    out += params.line_break;

    params.pop_indent();

    out += params.indent;
    out += "}";
}

void nplex::to_json(const msgs::Update *upd, json_params_t &params, std::string &out)
{
    assert(upd);

    out += params.indent;
    out += "{";
    out += params.line_break;

    params.push_indent();

    out += params.indent;
    json_append_text("rev", out);
    out += ":";
    out += params.space;
    json_append_number(upd->rev(), out);
    out += ",";
    out += params.line_break;

    out += params.indent;
    json_append_text("user", out);
    out += ":";
    out += params.space;
    json_append_escaped_text(upd->user()->c_str(), out);
    out += ",";
    out += params.line_break;

    out += params.indent;
    json_append_text("timestamp", out);
    out += ":";
    out += params.space;
    json_append_text(to_iso8601(millis_t{upd->timestamp()}), out);
    out += ",";
    out += params.line_break;

    out += params.indent;
    json_append_text("tx_type", out);
    out += ":";
    out += params.space;
    json_append_number(upd->tx_type(), out);

    if (upd->upserts() && !upd->upserts()->empty())
    {
        out += ",";
        out += params.line_break;

        out += params.indent;
        json_append_text("upserts", out);
        out += ":";
        out += params.space;
        out += "[";
        out += params.line_break;

        params.push_indent();

        size_t len = static_cast<size_t>(upd->upserts()->size());

        for (size_t i = 0; i < len; ++i)
        {
            to_json(upd->upserts()->Get(i), params, out);

            if (i < len - 1)
                out += ",";

            out += params.line_break;
        }

        params.pop_indent();

        out += params.indent;
        out += "]";
    }

    if (upd->deletes() && !upd->deletes()->empty())
    {
        out += ",";
        out += params.line_break;

        out += params.indent;
        json_append_text("deletes", out);
        out += ":";
        out += params.space;
        out += "[";
        out += params.line_break;

        params.push_indent();

        size_t len = static_cast<size_t>(upd->deletes()->size());

        for (size_t i = 0; i < len; ++i)
        {
            const auto &key = upd->deletes()->Get(i);

            out += params.indent;
            json_append_escaped_text(key->c_str(), out);

            if (i < len - 1)
                out += ",";

            out += params.line_break;
        }

        params.pop_indent();

        out += params.indent;
        out += "]";
    }

    out += params.line_break;
    params.pop_indent();
    out += params.indent;
    out += "}";
}

void nplex::to_json(const msgs::Snapshot *snp, json_params_t &params, std::string &out)
{
    assert(snp);

    out += params.indent;
    out += "{";
    out += params.line_break;

    params.push_indent();

    out += params.indent;
    json_append_text("rev", out);
    out += ":";
    out += params.space;
    json_append_number(snp->rev(), out);

    if (snp->updates())
    {
        out += ",";
        out += params.line_break;

        out += params.indent;
        json_append_text("updates", out);
        out += ":";
        out += params.space;
        out += "[";
        out += params.line_break;

        params.push_indent();

        auto updates = snp->updates();
        auto len = updates->size();

        for (flatbuffers::uoffset_t i = 0; i < len; i++)
        {
            to_json(updates->Get(i), params, out);

            if (i < len - 1)
                out += ",";

            out += params.line_break;
        }

        params.pop_indent();

        out += params.indent;
        out += "]";
    }

    out += params.line_break;
    params.pop_indent();
    out += params.indent;
    out += "}";
}

void nplex::to_json(const msgs::Session *session, json_params_t &params, std::string &out)
{
    assert(session);

    out += params.indent;
    out += "{";
    out += params.line_break;

    params.push_indent();

    out += params.indent;
    json_append_text("user", out);
    out += ":";
    out += params.space;
    json_append_escaped_text(session->user() ? session->user()->c_str() : "null", out);
    out += ",";
    out += params.line_break;

    out += params.indent;
    json_append_text("ip", out);
    out += ":";
    out += params.space;
    json_append_escaped_text(session->ip() ? session->ip()->c_str() : "null", out);
    out += ",";
    out += params.line_break;

    out += params.indent;
    json_append_text("code", out);
    out += ":";
    out += params.space;
    json_append_text(EnumNameExitCode(session->code()), out);
    out += ",";
    out += params.line_break;

    out += params.indent;
    json_append_text("time0", out);
    out += ":";
    out += params.space;
    json_append_text(to_iso8601(millis_t{session->time0()}), out);

    if (session->time1() != 0)
    {
        out += ",";
        out += params.line_break;
        out += params.indent;
        json_append_text("time1", out);
        out += ":";
        out += params.space;
        json_append_text(to_iso8601(millis_t{session->time1()}), out);
    }

    out += params.line_break;

    params.pop_indent();
    out += params.indent;
    out += "}";
}

std::string nplex::update_to_json(const char *data, size_t len, char mode)
{
    assert(data);
    assert(len > 0);

    if (!data || len == 0)
        return "<invalid update>";

    auto verifier = flatbuffers::Verifier(reinterpret_cast<const std::uint8_t *>(data), len);

    if (!verifier.VerifyBuffer<nplex::msgs::Update>(nullptr))
        return "<invalid update>";

    nplex::json_params_t json_params(mode == 'c' ? json_params_t::mode_e::COMPACT : json_params_t::mode_e::INDENT);
    auto update = flatbuffers::GetRoot<nplex::msgs::Update>(data);
    std::string str;

    str.reserve(len * 2);
    nplex::to_json(update, json_params, str);

    return str;
}

std::string nplex::snapshot_to_json(const char *data, size_t len, char mode)
{
    assert(data);
    assert(len > 0);

    if (!data || len == 0)
        return "<invalid snapshot>";

    auto verifier = flatbuffers::Verifier(reinterpret_cast<const std::uint8_t *>(data), len);

    if (!verifier.VerifyBuffer<nplex::msgs::Snapshot>(nullptr))
        return "<invalid snapshot>";

    nplex::json_params_t json_params(mode == 'c' ? json_params_t::mode_e::COMPACT : json_params_t::mode_e::INDENT);
    auto snapshot = flatbuffers::GetRoot<nplex::msgs::Snapshot>(data);
    std::string str;

    str.reserve(len * 2);
    nplex::to_json(snapshot, json_params, str);

    return str;
}
