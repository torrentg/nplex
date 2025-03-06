#pragma once

#include <uv.h>
#include "common.hpp"
#include "cache.hpp"
#include "user.hpp"
#include "messages.hpp"

namespace nplex {

struct output_msg_t
{
    uv_write_t req;
    std::array<uv_buf_t, 4> buf;
    flatbuffers::DetachedBuffer content;
    std::uint32_t metadata;     // 0=none, 1=lz4 (big-endian)
    std::uint32_t checksum;     // CRC32 of len + metadata + content (big-endian)
    std::uint32_t len;          // Total message length (including len, metadata, content, checksum) (big-endian)

    output_msg_t(flatbuffers::DetachedBuffer &&content_);
    std::uint32_t length() const { return ntohl(len); }
};

inline std::size_t get_msg_length(const flatbuffers::DetachedBuffer &buf) noexcept {
    return buf.size() + 3 * sizeof(std::uint32_t);
}

flatbuffers::DetachedBuffer create_ping_msg(std::size_t cid, rev_t crev, const std::string &payload);
flatbuffers::DetachedBuffer create_login_msg(std::size_t cid, msgs::LoginCode code, rev_t rev0 = 0, rev_t crev = 0, const user_t &user = {});
flatbuffers::DetachedBuffer create_keepalive_msg(rev_t crev);
flatbuffers::DetachedBuffer create_submit_msg(std::size_t cid, rev_t crev, msgs::SubmitCode code, rev_t erev = 0);
flatbuffers::DetachedBuffer create_update_msg(flatbuffers::FlatBufferBuilder &builder, std::size_t cid, rev_t crev, flatbuffers::Offset<msgs::Update> upd);

flatbuffers::DetachedBuffer serialize_update(const update_t &update);
flatbuffers::Offset<msgs::Update> serialize_update(flatbuffers::FlatBufferBuilder &builder, const update_t &update, const user_t *user = nullptr, bool force = false);
update_t deserialize_update(const msgs::Update *msg);

const nplex::msgs::Message * parse_network_msg(const char *ptr, size_t len);

} // namespace nplex
