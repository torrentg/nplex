#pragma once

#include <string>
#include <cstdint>
#include "schema.hpp"

namespace nplex {

/**
 * JSON functions (for debugging/info purposes).
 * 
 * We don't use the flatbuffers json features because we need:
 *   - custom formatting options
 *   - ad-hoc binary data printing
 *   - hide password fields
 */

struct json_params_t
{
    enum class mode_e : uint8_t {
        COMPACT = 0,
        INDENT = 1
    };

    mode_e mode = mode_e::COMPACT;
    uint32_t indent_size = 4;
    uint32_t max_binary = 12;     // max binary length
    std::string_view space;       // " " or ""
    std::string_view line_break;  // "\n" or ""
    std::string indent;           // cumulated indent

    json_params_t(mode_e m = mode_e::COMPACT, uint32_t size = 4)
        : mode(m)
        , indent_size(size)
        , max_binary(12)
        , space(m == mode_e::INDENT ? " " : "")
        , line_break(m == mode_e::INDENT ? "\n" : "")
    {}

    void push_indent() { if (mode == mode_e::INDENT) indent += std::string(indent_size, ' '); }
    void pop_indent()  { if (mode == mode_e::INDENT) indent.resize(indent.size() - indent_size); }
};

/**
 * Functions to serialize objects to JSON.
 */
void to_json(const msgs::KeyValue *kv, json_params_t &params, std::string &out);
void to_json(const msgs::Update *upd, json_params_t &params, std::string &out);
void to_json(const msgs::Snapshot *snp, json_params_t &params, std::string &out);
void to_json(const msgs::Session *session, json_params_t &params, std::string &out);

/**
 * Prints an update in JSON format.
 * 
 * @param[in] data Pointer to the update data (msgs::Update).
 * @param[in] len Length of the update data.
 * @param[in] mode JSON output mode ('c' for compact, 'i' for indented).
 * 
 * @return A string containing the JSON representation of the update.
 */
std::string update_to_json(const char *data, size_t len, char mode);

/**
 * Prints a snapshot in JSON format.
 * 
 * @param[in] data Pointer to the snapshot data (msgs::Snapshot).
 * @param[in] len Length of the snapshot data.
 * @param[in] mode JSON output mode ('c' for compact, 'i' for indented).
 *
 * @return A string containing the JSON representation of the snapshot.
 */
std::string snapshot_to_json(const char *data, size_t len, char mode);

} // namespace nplex
