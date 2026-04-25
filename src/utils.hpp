#pragma once

#include <chrono>
#include "common.hpp"

namespace nplex {

// log_level_e related functions
log_level_e parse_log_level(const std::string_view &str);
std::string to_string(log_level_e level);

// Utility class (comparator)
template<typename T>
struct shared_ptr_compare
{
    using is_transparent = std::true_type;

    // std::shared_ptr<T> vs std::shared_ptr<T>
    bool operator()(const std::shared_ptr<T> &lhs, const std::shared_ptr<T> &rhs) const noexcept {
        return lhs < rhs;
    }

    // std::shared_ptr<T> vs T*
    bool operator()(const std::shared_ptr<T> &lhs, const T *rhs) const noexcept {
        return lhs.get() < rhs;
    }
    bool operator()(const T *lhs, const std::shared_ptr<T> &rhs) const noexcept {
        return lhs < rhs.get();
    }
};

// Convert bytes to a human readable string (ex. 13.34MB)
std::string bytes_to_string(std::size_t bytes);

// Estimate the number of bytes an update will take when serialized
std::size_t estimate_bytes(const update_dto_t &update);
std::size_t estimate_bytes(const update_t &update);

/**
 * Same as ntohl but receiving a pointer to uint32_t instead of a uint32_t.
 * 
 * @param[in] ptr Pointer to uint32_t in network byte order.
 * 
 * @return Value in host byte order.
 */
std::uint32_t ntohl_ptr(const char *ptr);

/**
 * Check if a key is valid.
 * 
 * A valid key:
 * - must not be empty, 
 * - must not start with a space,
 * - must not end with a space,
 * - must not contain "//",
 * - must not contain '\0' in the middle, 
 * - must not contain control characters,
 * - and must be valid UTF-8.
 * 
 * @param[in] key Key to check.
 * 
 * @return true or false.
 */
bool is_valid_key(const std::string_view &key);
inline bool is_valid_key(const char *key) { return (key && is_valid_key(std::string_view{key})); }
inline bool is_valid_key(const key_t &key) { return is_valid_key(key.view()); }

/**
 * Convert milliseconds since epoch to ISO 8601 format (e.g. "2024-06-30T12:34:56.789Z").
 * 
 * @param[in] ms_since_epoch Milliseconds since epoch.
 * 
 * @return ISO 8601 formatted string.
 */
std::string to_iso8601(std::chrono::milliseconds ms_since_epoch);

/**
 * Serialize an ACL mode to a string.
 * 
 * The return string is in the format "crud" where each character is 
 * 'c', 'r', 'u' or 'd' if the corresponding permission is present, 
 * or '-' if not.
 * 
 * Example: crud 0b1101 (create, read and delete permissions) will be serialized as "cr-d".
 * 
 * @param[in] crud ACL mode.
 * 
 * @return std::string Serialized ACL mode.
 */
std::string crud_to_string(std::uint8_t crud);

/**
 * Parse a string to an ACL mode.
 * 
 * The input string must be in the format "crud" where each character is 
 * 'c', 'r', 'u' or 'd' if the corresponding permission is present, 
 * or '-' if not.
 * 
 * Example: string "cr-d" will be parsed as mode 0b1101 (create, read and delete permissions).
 * 
 * @param[in] str String to parse.
 * 
 * @return std::uint8_t Parsed ACL mode.
 * 
 * @exception std::invalid_argument If the input string is not in the correct format.
 */
std::uint8_t parse_crud(const std::string_view &str);

} // namespace nplex
