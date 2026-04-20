#include <fmt/core.h>
#include "utf8.h"
#include "utils.hpp"
#include "json.hpp"

#define MAX_BINARY_LENGTH 12

using namespace nplex::msgs;

// ==========================================================
// Internal (static) functions
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
    const size_t max_preview = MAX_BINARY_LENGTH;

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

// ==========================================================
// JSON functions
// ==========================================================

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

            out += indent1 + indent2;
            out += indent2;

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
            out += indent1 + indent2;
            out += indent2;

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

void nplex::to_json(const msgs::Session *session, json_params_t &params, std::string &out)
{
    assert(session);

    std::string space = (params.mode == json_params_t::mode_e::INDENT ? " " : "");
    std::string indent1 = (params.mode == json_params_t::mode_e::INDENT ? std::string(params.indent_curr, ' ') : "");
    std::string indent2 = (params.mode == json_params_t::mode_e::INDENT ? std::string(params.indent_size, ' ') : "");
    std::string line_break = (params.mode == json_params_t::mode_e::INDENT ? "\n" : "");

    out += "{" + line_break;
    params.indent_curr += params.indent_size;

    out += indent1 + indent2;
    json_append_text("user", out);
    out += ":" + space;
    json_append_text(session->user() ? session->user()->c_str() : "null", out);
    out += "," + line_break;

    out += indent1 + indent2;
    json_append_text("ip", out);
    out += ":" + space;
    json_append_text(session->ip() ? session->ip()->c_str() : "null", out);
    out += "," + line_break;

    out += indent1 + indent2;
    json_append_text("code", out);
    out += ":" + space;
    json_append_text(EnumNameExitCode(session->code()), out);
    out += "," + line_break;

    out += indent1 + indent2;
    json_append_text("time0", out);
    out += ":" + space;
    json_append_text(to_iso8601(millis_t{session->time0()}), out);

    if (session->time1() != 0)
    {
        out += "," + line_break;
        out += indent1 + indent2;
        json_append_text("time1", out);
        out += ":" + space;
        json_append_text(to_iso8601(millis_t{session->time1()}), out);
    }

    out += line_break;

    params.indent_curr -= params.indent_size;
    out += indent1;
    out += "}";
}
