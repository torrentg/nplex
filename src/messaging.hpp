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
 * Network frame:
 * 
 *  +--------------+--------------+-------------------+
 *  | length       | checksum     | content           |
 *  +--------------+--------------+-------------------+
 *  | 4 bytes      | 4 bytes      | N bytes           |
 *  | big-endian   | big-endian   | flatbuffer        |
 *  +--------------+--------------+-------------------+
 * 
 * - length: Content length in bytes
 * - checksum: CRC32 of content
 * - content: FlatBuffer Message
 */

/**
 * This class is designed to minimize the number of memory allocations 
 * and copies when sending messages to the network. A single allocation covers:
 *   - uv_write_t object
 *   - multiple uv_buf_t (required to pass multiple buffers to uv_write)
 *   - multiple flatbuffers::DetachedBuffer
 *   - multiple lengths and checksums
 */
class outgoing_network_frames_t
{
  public:

    // Factory methods
    static outgoing_network_frames_t * create(std::span<flatbuffers::DetachedBuffer> msgs);
    static outgoing_network_frames_t * create(flatbuffers::DetachedBuffer &&msg){
        flatbuffers::DetachedBuffer arr[1] = { std::move(msg) };
        return create(arr);
    }; 
    static void destroy(outgoing_network_frames_t *obj);

    // Non-copiable
    outgoing_network_frames_t(const outgoing_network_frames_t&) = delete;
    outgoing_network_frames_t& operator=(const outgoing_network_frames_t&) = delete;

    // Destructor
    ~outgoing_network_frames_t();

    constexpr static std::size_t max_num_msgs() { return 128; } // Max iovec supported by kernel is 1024, each msg is 2 iovecs
    uv_write_t *get_req() { return &write_req; }
    const uv_buf_t * get_bufs_ptr() const { return reinterpret_cast<const uv_buf_t *>(this + 1); }
    unsigned int get_bufs_len() const { return (num_msgs * 2); }
    size_t get_num_msgs() const { return num_msgs; }
    size_t get_total_length() const { return total_length; }
    const void * get_msg(size_t idx) const { return get_bufs_ptr()[idx * 2 + 1].base; }

  private:

    struct values_t {
        std::uint32_t len = 0;                  // FlatBuffer payload length in bytes (big-endian)
        std::uint32_t checksum = 0;             // CRC32 of payload bytes (big-endian)
        flatbuffers::DetachedBuffer content;    // Message content (flatbuffer serialized data)
    };

    uv_write_t write_req;                       // Write request object
    std::uint32_t num_msgs = 0;                 // Number of messages in the batch
    std::uint32_t total_length = 0;             // Total length (sum of serialized data)
    // uv_buf_t[2 * num_msgs]                   // Memory allocated by create()
    // values_t[num_msgs]                       // Memory allocated by create()

    outgoing_network_frames_t() = default;
};

/**
 * Parse a network frame.
 * 
 * Checks the frame integrity and returns the flatbuffer message.
 * 
 * @param[in] ptr Pointer to received data.
 * @param[in] len Length of received data.
 * 
 * @return The message or 
 *         nullptr on error.
 */
const msgs::Message * parse_network_frame(const char *ptr, size_t len);

/**
 * Builder to create a UpdatesPush message.
 */
struct updates_push_builder_t
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
struct sessions_response_builder_t
{
    flatbuffers::FlatBufferBuilder m_builder;
    std::vector<flatbuffers::Offset<msgs::Session>> m_sessions;

    bool append(const session_ptr &session);
    flatbuffers::DetachedBuffer finish(uint64_t cid, rev_t crev);
};

/**
 * Helper functions to create messages.
 */
flatbuffers::DetachedBuffer create_ping_response(std::size_t cid, rev_t crev, const std::string &payload);
flatbuffers::DetachedBuffer create_login_response(std::size_t cid, msgs::LoginCode code, rev_t rev0 = 0, rev_t crev = 0, const user_ptr &user = nullptr);
flatbuffers::DetachedBuffer create_keepalive_push(rev_t crev);
flatbuffers::DetachedBuffer create_submit_response(std::size_t cid, rev_t crev, msgs::SubmitCode code, rev_t erev = 0);
flatbuffers::DetachedBuffer create_snapshot_response(std::size_t cid, rev_t crev, rev_t rev0, bool accepted, const store_t &store, const user_ptr &user = nullptr);
flatbuffers::DetachedBuffer create_updates_response(std::size_t cid, rev_t crev, rev_t rev0, bool accepted);
flatbuffers::DetachedBuffer create_sessions_push(std::size_t cid, rev_t crev, const session_ptr &session);

/**
 * Functions used to write and read journal entries.
 */
flatbuffers::DetachedBuffer serialize_update(const update_t &update);
update_dto_t deserialize_update(const msgs::Update *upd, const user_ptr &user = nullptr);

} // namespace nplex
