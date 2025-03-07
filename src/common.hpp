#pragma once

#include <set>
#include <vector>
#include <memory>
#include <cstdint>
#include <string>
#include <chrono>
#include "cstring.hpp"

#define PROJECT_NAME "nplex"
#define PROJECT_VERSION "0.1.0"
#define PROJECT_URL "https://github.com/torrentg/nplex"

#define API_VERSION 10

#define CONFIG_FILENAME "nplex.ini"
#define LOG_FILENAME "nplex.log"
#define DATA_FILENAME "entries.dat"
#define INDEX_FILENAME "entries.idx"
#define SNAPSHOT_FILENAME "snapshot-{}.dat"

#define DEFAULT_ADDR "localhost:14022"
#define MAX_CONNECTIONS 256

#define NPLEX_CREATE 1
#define NPLEX_READ   2
#define NPLEX_UPDATE 4
#define NPLEX_DELETE 8

// Error codes
#define ERR_CLOSED_BY_LOCAL 1000
#define ERR_CLOSED_BY_PEER 1001
#define ERR_MSG_ERROR 1002
#define ERR_MSG_UNEXPECTED 1003
#define ERR_MSG_SIZE 1004
#define ERR_USR_NOT_FOUND 1005
#define ERR_USR_INVL_PWD 1006
#define ERR_USR_MAX_CONN 1007
#define ERR_MAX_CONN 1008
#define ERR_API_VERSION 1009
#define ERR_TIMEOUT_STEP_1 1010
#define ERR_TIMEOUT_STEP_2 1011

#define UNUSED(x) (void)(x)

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
    millis_t timestamp;             //!< Timestamp at transaction creation.
    std::uint32_t type;             //!< Transaction type (user-defined).
    std::set<key_t> refs;           //!< Keys modified by the transaction.
};

using meta_ptr = std::shared_ptr<meta_t>;

//! Database value.
class value_t
{
    friend class repo_t;
    static const gto::cstring REMOVED;
    static const gto::cstring EMPTY;

  private:

    gto::cstring m_data;
    meta_ptr m_meta;

  public:

    value_t(const gto::cstring &data, std::shared_ptr<meta_t> meta) : m_data{data}, m_meta{meta} {}

    const meta_ptr & meta() const { return m_meta; }
    const gto::cstring & data() const { return m_data; }

    rev_t rev() const { return (m_meta ? m_meta->rev : 0); }
    const gto::cstring & user() const { return (m_meta ? m_meta->user : EMPTY); }
    millis_t timestamp() const { return (m_meta ? m_meta->timestamp : millis_t{0}); }
    std::uint32_t type() const { return (m_meta ? m_meta->type : 0); }

    void set_removed() { m_data = REMOVED; }
    bool is_removed() const { return (m_data.c_str() == REMOVED.c_str()); }
};

using value_ptr = std::shared_ptr<value_t>;

struct update_t {
    meta_ptr meta;
    std::vector<std::pair<key_t, value_ptr>> upserts;
    std::vector<key_t> deletes;
};

enum log_level_e : std::uint8_t {
    DEBUG,
    INFO,
    WARN,
    ERROR
};

inline log_level_e parse_log_level(const std::string_view &str)
{
    using namespace std::string_literals;

    if ("debug"s == str)
        return log_level_e::DEBUG;
    if ("info"s == str)
        return log_level_e::INFO;
    if ("warning"s == str)
        return log_level_e::WARN;
    if ("error"s == str)
        return log_level_e::ERROR;

    throw std::invalid_argument("Invalid log level");
}

inline std::string to_string(log_level_e level)
{
    switch (level)
    {
        case log_level_e::DEBUG: return "debug";
        case log_level_e::INFO: return "info";
        case log_level_e::WARN: return "warning";
        case log_level_e::ERROR: return "error";
        default: return "unknown";
    }
}

// Key support functions.
inline bool is_valid_key(const std::string_view &key) { return !key.empty(); }
inline bool is_valid_key(const char *key) { return (key && is_valid_key(std::string_view{key})); }
inline bool is_valid_key(const key_t &key) { return is_valid_key(key.view()); }

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

    // std::shared_ptr<T> vs std::unique_ptr<T>
    bool operator()(const std::shared_ptr<T> &lhs, const std::unique_ptr<T> &rhs) const noexcept {
        return lhs.get() < rhs.get();
    }
    bool operator()(const std::unique_ptr<T> &lhs, const std::shared_ptr<T> &rhs) const noexcept {
        return lhs.get() < rhs.get();
    }
};

} // namespace nplex
