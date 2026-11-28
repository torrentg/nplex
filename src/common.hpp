#pragma once

#include "buildinfo.hpp"
#include "cstring.hpp"
#include "counted_ptr.hpp"
#include <cstdint>
#include <string>
#include <chrono>
#include <memory>
#include <vector>
#include <map>

#define PROJECT_NAME            "nplex"
#define PROJECT_URL             "https://github.com/torrentg/nplex"

#define CONFIG_FILENAME         "nplex.ini"
#define LOG_FILENAME            "nplex.log"
#define README_FILENAME         "README.md"

#define CRUD_CREATE             0x01
#define CRUD_READ               0x02
#define CRUD_UPDATE             0x04
#define CRUD_DELETE             0x08

// Error codes (values less than 1000 are considered libuv errors)
#define ERR_CLOSED_BY_LOCAL     1000
#define ERR_CLOSED_BY_PEER      1001
#define ERR_EXCD_LIMITS         1002
#define ERR_MSG_ERROR           1003
#define ERR_MSG_UNEXPECTED      1004
#define ERR_MSG_SIZE            1005
#define ERR_USR_NOT_FOUND       1006
#define ERR_USR_INVL_PWD        1007
#define ERR_USR_MAX_CONN        1008
#define ERR_MAX_CONN            1009
#define ERR_SCHEMA              1010
#define ERR_CONNECTION_LOST     1011
#define ERR_UNACK               1012
#define ERR_DATA_RETRIEVAL      1013

#define UNUSED(x)               (void)(x)

namespace nplex {

//! Database revision (incremental number).
using rev_t = std::uint64_t;

//! Database key type (string).
using key_t = gto::cstring;

//! Database value type (binary content).
using value_t = gto::cstring;

//! Database timestamps (milliseconds from epoch time).
using millis_t = std::chrono::milliseconds;

//! User name (string),
using username_t = gto::cstring;

//! Transaction or update metadata.
struct meta_t
{
    rev_t rev = 0;                  //!< Revision at transaction creation.
    username_t username;            //!< Transaction creator.
    millis_t timestamp;             //!< Creation timestamp (milliseconds since Unix epoch, UTC).
    std::uint32_t tx_type = 0;      //!< Transaction type (user-defined).
};

//! Pointer to transaction metadata.
using meta_ptr = gto::counted_ptr<const meta_t>;

//! Database value.
class stored_value_t
{
  public:

    stored_value_t(const value_t &value, meta_ptr meta) : m_value{value}, m_meta{std::move(meta)} {}
    stored_value_t(value_t &&value, meta_ptr meta) : m_value{std::move(value)}, m_meta{std::move(meta)} {}

    // Getters
    const meta_ptr & meta() const { return m_meta; }
    const value_t & value() const { return m_value; }

    // Accessors
    rev_t rev() const { return (m_meta ? m_meta->rev : 0); }
    const username_t & username() const { return (m_meta ? m_meta->username : empty_username()); }
    millis_t timestamp() const { return (m_meta ? m_meta->timestamp : millis_t{0}); }
    std::uint32_t tx_type() const { return (m_meta ? m_meta->tx_type : 0); }

  private:

    value_t m_value;
    meta_ptr m_meta;

    static const username_t &empty_username()
    {
        static const username_t value = "";
        return value;
    }
};

//! Pointer to database value.
using stored_value_ptr = gto::counted_ptr<const stored_value_t>;

//! Database update (aka committed transaction).
struct update_t {
    meta_ptr meta;
    std::vector<std::pair<key_t, value_t>> upserts;
    std::vector<key_t> deletes;
};

//! Database snapshot.
struct snapshot_t
{
    rev_t rev = 0;
    millis_t timestamp = millis_t{0};
    std::map<key_t, stored_value_ptr> data;
};

//! Logging levels.
enum class log_level_e : std::uint8_t {
    TRACE,    // Developper messages
    DEBUG,    // Connection initiated/terminated, data update, sent/recv message (except KEEPALIVE_PUSH), tasks duration
    INFO,     // Users login/disconnection, snapshots
    WARN,     // Signal SIGINT, threshold exceeded, unexpected events
    ERROR,    // Critical errors
    NONE      // Disable logging
};

} // namespace nplex

/*
 * Flatbuffers library is used extensively in nplex
 * to reduce memory usage and improve serialization performance.
 * Some basic nplex types are flatbuffers types.
 */

// Forward declarations
namespace flatbuffers {
    class DetachedBuffer;
    template<bool>
    class FlatBufferBuilderImpl;
}

// Flatbuffers namespace alias for convenience.
namespace fb = flatbuffers;

// Flatbuffers type aliases for convenience.
using DetachedBuffer = flatbuffers::DetachedBuffer;
using FlatBufferBuilder = flatbuffers::FlatBufferBuilderImpl<false>;
