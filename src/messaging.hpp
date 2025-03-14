#pragma once

#include <uv.h>
#include "common.hpp"
#include "messages.hpp"
#include "repository.hpp"
#include "user.hpp"

namespace nplex {

/**
 * Network message format:
 * 
 *  +--------------+--------------+-------------------+--------------+
 *  | length       | metadata     | content           | checksum     |
 *  +--------------+--------------+-------------------+--------------+
 *  | 4 bytes      | 4 bytes      | N bytes           | 4 bytes      |
 *  | big-endian   | big-endian   | flatbuffer        | big-endian   |
 *  +--------------+--------------+-------------------+--------------+
 *                                |<-- length - 12 -->|
 * 
 * - length: Total message size in bytes (including all 4 fields)
 * - metadata: Message properties (0=uncompressed, 1=lz4 compressed)
 * - content: FlatBuffer serialized data (content_length = msg_length - 12)
 * - checksum: CRC32 of [length + metadata + content]
 */
struct output_msg_t
{
    uv_write_t req;
    std::array<uv_buf_t, 4> buf;
    flatbuffers::DetachedBuffer content;
    std::uint32_t metadata;     // 0=none, 1=lz4 (big-endian)
    std::uint32_t checksum;     // CRC32 of len + metadata + content (big-endian)
    std::uint32_t len;          // Total message length (including len, metadata, content, checksum) (big-endian)

    output_msg_t(flatbuffers::DetachedBuffer &&msg);
    std::uint32_t length() const { return ntohl(len); }

    static inline std::size_t length(const flatbuffers::DetachedBuffer &buf) noexcept {
        return buf.size() + 3 * sizeof(std::uint32_t);
    }
};

struct load_builder_t
{
    uint64_t m_cid = 0;
    flatbuffers::FlatBufferBuilder m_builder;
    flatbuffers::Offset<msgs::Snapshot> m_offset_snapshot;

    load_builder_t(std::size_t cid) : m_cid(cid) {}
    void set_snapshot(const repo_t &repo, const user_ptr &user = nullptr);
    flatbuffers::DetachedBuffer finish(rev_t crev, bool accepted);
};

struct changes_builder_t
{
    uint64_t m_cid = 0;
    flatbuffers::FlatBufferBuilder m_builder;
    std::vector<flatbuffers::Offset<msgs::Update>> m_updates;
    struct { // meta info of the last update not set into updates due to user permissions
        std::uint64_t rev = 0;
        std::string user{};
        std::uint64_t timestamp = 0;
        std::uint32_t type = 0;
    } last_meta;

    changes_builder_t(std::size_t cid) : m_cid(cid) {} // TODO: append max-length, max-entries
    void append_updates(const std::span<update_t> &updates, const user_ptr &user = nullptr);
    void append_update(const msgs::Update *update, const user_ptr &user = nullptr);
    flatbuffers::DetachedBuffer finish(rev_t crev, bool ending_meta = true);
    bool empy() const { return m_updates.empty(); }
};

flatbuffers::DetachedBuffer create_ping_msg(std::size_t cid, rev_t crev, const std::string &payload);
flatbuffers::DetachedBuffer create_login_msg(std::size_t cid, msgs::LoginCode code, rev_t rev0 = 0, rev_t crev = 0, const user_t &user = {});
flatbuffers::DetachedBuffer create_keepalive_msg(rev_t crev);
flatbuffers::DetachedBuffer create_submit_msg(std::size_t cid, rev_t crev, msgs::SubmitCode code, rev_t erev = 0);

flatbuffers::DetachedBuffer serialize_update(const update_t &update);
flatbuffers::Offset<msgs::Update> serialize_update(flatbuffers::FlatBufferBuilder &builder, const update_t &update, const user_t *user = nullptr, bool force = false);
update_t deserialize_update(const msgs::Update *msg, const user_ptr &user = nullptr);

const nplex::msgs::Message * parse_network_msg(const char *ptr, size_t len);

} // namespace nplex
