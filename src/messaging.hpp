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
 * - metadata: Message properties (currently unused)
 * - content: FlatBuffer serialized data (content_length = msg_length - 12)
 * - checksum: CRC32 of [length + metadata + content]
 */
struct output_msg_t
{
    uv_write_t req;
    std::array<uv_buf_t, 4> buf;
    flatbuffers::DetachedBuffer content;
    std::uint32_t metadata;     // Message metadata (ex. compression alg) -currently unused- (big-endian)
    std::uint32_t checksum;     // CRC32 of len + metadata + content (big-endian)
    std::uint32_t len;          // Total message length (including len, metadata, content, checksum) (big-endian)

    output_msg_t(flatbuffers::DetachedBuffer &&msg);
    std::uint32_t length() const { return ntohl(len); }

    static inline std::size_t length(const flatbuffers::DetachedBuffer &buf) noexcept {
        return buf.size() + 3 * sizeof(std::uint32_t);
    }
};

/**
 * Parse a network message.
 * 
 * Checks the message integrity and returns the flatbuffer message.
 * 
 * @param[in] ptr Pointer to received data.
 * @param[in] len Length of received data.
 * 
 * @return The message or 
 *         nullptr on error.
 */
const msgs::Message * parse_network_msg(const char *ptr, size_t len);

/**
 * Set the crev value in a serialized message.
 * This is a hack to bypass the flatbuffers immutability.
 * 
 * @param[in] buf Flatbuffer serialized data.
 * @param[in] crev Current revision to set.
 */
bool update_crev(flatbuffers::DetachedBuffer &buf, rev_t crev);

/**
 * Builder to create a LoadResponse message.
 * 
 */
struct load_builder_t
{
    uint64_t m_cid = 0;
    flatbuffers::FlatBufferBuilder m_builder;
    flatbuffers::Offset<msgs::Snapshot> m_offset_snapshot;

    load_builder_t(std::size_t cid) : m_cid(cid) {}
    void set_snapshot(const repo_t &repo, const user_ptr &user = nullptr);
    flatbuffers::DetachedBuffer finish(rev_t crev, bool accepted);
};

/**
 * Builder to create a ChangesPush message.
 * 
 * max_bytes is the threshold used to stop appending updates when depassed.
 * Resulting message size can be greater than max_bytes.
 */
struct changes_builder_t
{
    uint64_t m_cid = 0;
    user_ptr m_user;
    flatbuffers::FlatBufferBuilder m_builder;
    std::vector<flatbuffers::Offset<msgs::Update>> m_updates;
    std::uint32_t m_num_revs = 0;           // Number of appended revisions
    std::uint32_t m_max_revs = 0;           // Maximum number of revisions in the message
    std::uint32_t m_max_bytes = 0;          // Maximum message size in bytes
    struct {                                // Metadata info of last update
        std::uint64_t rev = 0;
        std::string user{};
        std::uint64_t timestamp = 0;
        std::uint32_t type = 0;
        bool reported = false;
    } m_last_meta;

    changes_builder_t() = default;
    changes_builder_t(std::size_t cid, const user_ptr &user, std::uint32_t max_revs, std::uint32_t max_bytes) : 
        m_cid(cid), m_user(user), m_max_revs(max_revs), m_max_bytes(max_bytes) {}

    bool append_update(const msgs::Update *update);
    bool append_updates(const std::span<update_t> &updates);
    flatbuffers::DetachedBuffer finish(rev_t crev, bool ending_meta = true);
    rev_t last_rev() const { return rev_t{m_last_meta.rev}; }
    std::uint32_t num_revs() const { return m_num_revs; }
    bool empty() const { return m_updates.empty(); }
};

flatbuffers::DetachedBuffer create_ping_msg(std::size_t cid, rev_t crev, const std::string &payload);
flatbuffers::DetachedBuffer create_login_msg(std::size_t cid, msgs::LoginCode code, rev_t rev0 = 0, rev_t crev = 0, const user_t &user = {});
flatbuffers::DetachedBuffer create_keepalive_msg(rev_t crev);
flatbuffers::DetachedBuffer create_submit_msg(std::size_t cid, rev_t crev, msgs::SubmitCode code, rev_t erev = 0);

flatbuffers::DetachedBuffer serialize_update(const update_t &update);
flatbuffers::Offset<msgs::Update> serialize_update(flatbuffers::FlatBufferBuilder &builder, const update_t &update, const user_t *user = nullptr, bool force = false);
update_t deserialize_update(const msgs::Update *msg, const user_ptr &user = nullptr);

} // namespace nplex
