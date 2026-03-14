#pragma once

#include <uv.h>
#include "common.hpp"
#include "messages.hpp"
#include "store.hpp"
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
 * Builder to create a UpdatesPush message.
 */
struct updates_builder_t
{
    flatbuffers::FlatBufferBuilder m_builder;
    std::vector<flatbuffers::Offset<msgs::Update>> m_updates;

    std::size_t bytes() const { return m_builder.GetSize(); }
    std::size_t count() const { return m_updates.size(); }
    bool empty() const { return m_updates.empty(); }

    bool append(const update_t &update, const user_ptr &user = nullptr, bool force = false);
    bool append(const update_dto_t &update, const user_ptr &user = nullptr, bool force = false);
    flatbuffers::DetachedBuffer finish(uint64_t cid, rev_t crev);
};

flatbuffers::DetachedBuffer create_ping_msg(std::size_t cid, rev_t crev, const std::string &payload);
flatbuffers::DetachedBuffer create_login_msg(std::size_t cid, msgs::LoginCode code, rev_t rev0 = 0, rev_t crev = 0, const user_ptr &user = nullptr);
flatbuffers::DetachedBuffer create_keepalive_msg(rev_t crev);
flatbuffers::DetachedBuffer create_submit_msg(std::size_t cid, rev_t crev, msgs::SubmitCode code, rev_t erev = 0);
flatbuffers::DetachedBuffer create_snapshot_msg(std::size_t cid, rev_t crev, rev_t rev0, bool accepted, const store_t &store, const user_ptr &user = nullptr);
flatbuffers::DetachedBuffer create_updates_msg(std::size_t cid, rev_t crev, rev_t rev0, bool accepted);

flatbuffers::DetachedBuffer serialize_update(const update_t &update);
update_dto_t deserialize_update(const msgs::Update *msg, const user_ptr &user = nullptr);

} // namespace nplex
