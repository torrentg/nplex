#pragma once

#include "common.hpp"
#include "schema.hpp"
#include <uv.h>
#include <array>
#include <string>

namespace nplex {

// Forward declarations
class store_t;
class session_t;
struct user_t;
using user_ptr = std::shared_ptr<user_t>;
using session_ptr = std::shared_ptr<session_t>;

/**
 * Network message format:
 * 
 *  +--------------+--------------+-------------------+--------------+--------------+
 *  | length       | metadata     | content           | checksum     | delimiter    |
 *  +--------------+--------------+-------------------+--------------+--------------+
 *  | 4 bytes      | 4 bytes      | N bytes           | 4 bytes      | 4 bytes      |
 *  | big-endian   | big-endian   | flatbuffer        | big-endian   | 0xFFFFFFFF   |
 *  +--------------+--------------+-------------------+--------------+--------------+
 *                                |<-- length - 16 -->|
 * 
 * - length: Total message size in bytes (including all 5 fields)
 * - metadata: Message properties (currently unused)
 * - content: FlatBuffer serialized data (content_length = msg_length - 16)
 * - checksum: CRC32 of [length + metadata + content]
 * - delimiter: 0xFFFFFFFF (used to align next msg to 8-bytes)
 */

/**
 * This class is designed to minimize the number of memory allocations 
 * and copies when sending messages to the network. A single allocation covers:
 *   - uv_write_t object
 *   - array of uv_buf_t (required to pass multiple buffers to uv_write)
 *   - array of flatbuffers::DetachedBuffer (for message content)
 *   - array of lengths + metadatas and checksum (1 entry for message)
 */
class output_chunk_t
{
  public:

  // Factory methods
    static output_chunk_t * create(std::span<flatbuffers::DetachedBuffer> msgs);
    static output_chunk_t * create(flatbuffers::DetachedBuffer &&msg){
        flatbuffers::DetachedBuffer arr[1] = { std::move(msg) };
        return create(arr);
    }; 
    static void destroy(output_chunk_t *obj);

    // Non-copiable
    output_chunk_t(const output_chunk_t&) = delete;
    output_chunk_t& operator=(const output_chunk_t&) = delete;

    // Destructor
    ~output_chunk_t();

    constexpr static std::size_t max_num_msgs() { return 128; } // Max iovec supported by kernel is 1024, each msg is 5 iovecs
    uv_write_t *get_req() { return &write_req; }
    const uv_buf_t * get_bufs_ptr() const { return reinterpret_cast<const uv_buf_t *>(this + 1); }
    unsigned int get_bufs_len() const { return (num_msgs * 5); }
    size_t get_num_msgs() const { return num_msgs; }
    size_t get_total_length() const { return total_length; }
    const void * get_msg(size_t idx) const { return get_bufs_ptr()[idx * 5 + 2].base; }

  private:

    struct values_t {
        std::uint32_t len;                      // Total message length (including len, metadata, content, checksum and delimiter) (big-endian)
        std::uint32_t metadata;                 // Message metadata (ex. compression alg) -currently unused- (big-endian)
        flatbuffers::DetachedBuffer content;    // Message content (flatbuffer serialized data, length is multiple of 8)
        std::uint32_t checksum;                 // CRC32 of len + metadata + content (big-endian)
        std::uint32_t delimiter;                // Delimiter between messages (big-endian)
    };

    uv_write_t write_req;                       // Write request object
    std::uint32_t num_msgs = 0;                 // Number of messages in the batch
    std::uint32_t total_length = 0;             // Total length (sum of serialized data)
    // uv_buf_t[5 * num_msgs]                   // Memory allocated by create()
    // values_t[num_msgs]                       // Memory allocated by create()

    output_chunk_t() = default;
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

/**
 * Builder to create a SessionsResponse message.
 */
struct sessions_builder_t
{
    flatbuffers::FlatBufferBuilder m_builder;
    std::vector<flatbuffers::Offset<msgs::Session>> m_sessions;

    bool append(const session_ptr &session);
    flatbuffers::DetachedBuffer finish(uint64_t cid, rev_t crev);
};

/**
 * Helper functions to create messages.
 */
flatbuffers::DetachedBuffer create_ping_msg(std::size_t cid, rev_t crev, const std::string &payload);
flatbuffers::DetachedBuffer create_login_msg(std::size_t cid, msgs::LoginCode code, rev_t rev0 = 0, rev_t crev = 0, const user_ptr &user = nullptr);
flatbuffers::DetachedBuffer create_keepalive_msg(rev_t crev);
flatbuffers::DetachedBuffer create_submit_msg(std::size_t cid, rev_t crev, msgs::SubmitCode code, rev_t erev = 0);
flatbuffers::DetachedBuffer create_snapshot_msg(std::size_t cid, rev_t crev, rev_t rev0, bool accepted, const store_t &store, const user_ptr &user = nullptr);
flatbuffers::DetachedBuffer create_updates_msg(std::size_t cid, rev_t crev, rev_t rev0, bool accepted);
flatbuffers::DetachedBuffer create_sessions_msg(std::size_t cid, rev_t crev, const session_ptr &session);

/**
 * Functions used to write and read journal entries.
 */
flatbuffers::DetachedBuffer serialize_update(const update_t &update);
update_dto_t deserialize_update(const msgs::Update *upd, const user_ptr &user = nullptr);

} // namespace nplex
