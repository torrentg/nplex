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

    mode_e mode = mode_e::COMPACT;  // Output mode
    std::uint32_t indent_size = 4;  // Number of spaces for indentation (if required)
    std::uint32_t indent_curr = 0;  // Current indentation level (used internally for recursion)

    json_params_t(mode_e m = mode_e::COMPACT) : mode(m) {}
};

/**
 * Functions to serialize objects to JSON.
 */
void to_json(const msgs::KeyValue *kv, json_params_t &params, std::string &out);
void to_json(const msgs::Update *upd, json_params_t &params, std::string &out);
void to_json(const msgs::Snapshot *snp, json_params_t &params, std::string &out);
void to_json(const msgs::Session *session, json_params_t &params, std::string &out);

} // namespace nplex
