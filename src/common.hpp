#pragma once

#include <set>
#include <vector>
#include <memory>
#include <cstdint>
#include <string>
#include <chrono>
#include "cstring.hpp"
#include "buildinfo.hpp"

#define PROJECT_NAME            "nplex"
#define PROJECT_URL             "https://github.com/torrentg/nplex"

#define CONFIG_FILENAME         "nplex.ini"
#define LOG_FILENAME            "nplex.log"
#define JOURNAL_NAME            "journal"
#define SNAPSHOT_FILENAME       "snapshot-{}.dat"
#define JOURNAL_MAGIC           "NPLEXLOG"
#define SNAPSHOT_MAGIC          "NPLEXSNP"
#define MAGIC_LEN               8

#define CRUD_CREATE             0x01
#define CRUD_READ               0x02
#define CRUD_UPDATE             0x04
#define CRUD_DELETE             0x08

// Error codes (values less than 1000 are considered libuv errors)
#define ERR_CLOSED_BY_LOCAL     1000
#define ERR_CLOSED_BY_PEER      1001
#define ERR_MSG_ERROR           1002
#define ERR_MSG_UNEXPECTED      1003
#define ERR_MSG_SIZE            1004
#define ERR_USR_NOT_FOUND       1005
#define ERR_USR_INVL_PWD        1006
#define ERR_USR_MAX_CONN        1007
#define ERR_MAX_CONN            1008
#define ERR_SCHEMA              1009
#define ERR_CONNECTION_LOST     1010
#define ERR_UNACK               1011

#define UNUSED(x)               (void)(x)

namespace nplex {

//! Database revision number.
using rev_t = std::size_t;

//! Database key type.
using key_t = gto::cstring;

//! Database timestamps (milliseconds from epoch time).
using millis_t = std::chrono::milliseconds;

//! Transaction metadata.
struct meta_t
{
    rev_t rev;                      //!< Revision at transaction creation.
    gto::cstring user;              //!< Transaction creator.
    millis_t timestamp;             //!< Creation timestamp (milliseconds since Unix epoch, UTC).
    std::uint32_t tx_type;          //!< Transaction type (user-defined).
    std::set<key_t> refs;           //!< Set of keys modified by the transaction.
};

using meta_ptr = std::shared_ptr<meta_t>;

//! Database value.
class value_t
{
    friend class store_t;
    static const gto::cstring REMOVED;
    static const gto::cstring EMPTY;

  private:

    gto::cstring m_data;
    meta_ptr m_meta;

  public:

    value_t(const gto::cstring &data, std::shared_ptr<meta_t> meta) : m_data{data}, m_meta{std::move(meta)} {}

    const meta_ptr & meta() const { return m_meta; }
    const gto::cstring & data() const { return m_data; }

    rev_t rev() const { return (m_meta ? m_meta->rev : 0); }
    const gto::cstring & user() const { return (m_meta ? m_meta->user : EMPTY); }
    millis_t timestamp() const { return (m_meta ? m_meta->timestamp : millis_t{0}); }
    std::uint32_t tx_type() const { return (m_meta ? m_meta->tx_type : 0); }

    void set_removed() { m_data = REMOVED; }
    bool is_removed() const { return (m_data.c_str() == REMOVED.c_str()); }
};

using value_ptr = std::shared_ptr<value_t>;

struct update_t {
    meta_ptr meta;
    std::vector<std::pair<key_t, value_ptr>> upserts;
    std::vector<key_t> deletes;
};

struct update_dto_t {
    rev_t rev = 0;
    std::string user{};
    std::uint64_t timestamp = 0;
    std::uint32_t tx_type = 0;
    std::vector<std::pair<std::string, std::string>> upserts{};
    std::vector<std::string> deletes{};
};

enum log_level_e : std::uint8_t {
    TRACE,    // developper messages
    DEBUG,    // connection initiated/terminated, data update, sent/recv message (except KEEPALIVE_PUSH), tasks duration
    INFO,     // users login/disconnection, snapshots
    WARN,     // signal SIGINT, threshold exceeded, unexpected events
    ERROR,    // critical errors
    NONE      // disable logging
};

} // namespace nplex
