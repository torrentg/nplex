#include "cppcrc.h"
#include "exception.hpp"
#include "messaging.hpp"

/**
 * Notes on this compilation unit:
 * 
 * When we use the libuv library we apply C conventions (instead of C++ ones):
 *   - When in Rome, do as the Romans do.
 *   - calloc/free are used instead of new/delete.
 *   - pointers to static functions.
 *   - C-style pointer casting.
 */

nplex::output_msg_t::output_msg_t(flatbuffers::DetachedBuffer &&content_)
{
    content = std::move(content_);

    len = (std::uint32_t)(content.size() + sizeof(len) + sizeof(metadata) + sizeof(checksum));
    len = htonl(len);

    metadata = htonl(0); // not-compressed

    checksum = CRC32::CRC32::calc(reinterpret_cast<const std::uint8_t *>(&len), sizeof(len));
    checksum = crc_utils::reverse(checksum ^ 0xFFFFFFFF);
    checksum = CRC32::CRC32::calc(reinterpret_cast<const std::uint8_t *>(&metadata), sizeof(metadata), checksum);
    checksum = crc_utils::reverse(checksum ^ 0xFFFFFFFF);
    checksum = CRC32::CRC32::calc(reinterpret_cast<const std::uint8_t *>(content.data()), content.size(), checksum);
    checksum = htonl(checksum);

    buf[0] = uv_buf_init((char *) &len, sizeof(len));
    buf[1] = uv_buf_init((char *) &metadata, sizeof(metadata));
    buf[2] = uv_buf_init((char *) content.data(), (unsigned int) content.size());
    buf[3] = uv_buf_init((char *) &checksum, sizeof(checksum));
}

flatbuffers::DetachedBuffer nplex::create_login_msg(std::size_t cid, msgs::LoginCode code, rev_t rev0, rev_t crev, bool can_force, std::uint32_t keepalive_millis)
{
    using namespace msgs;
    using namespace flatbuffers;

    FlatBufferBuilder builder;

    auto msg = CreateMessage(builder, 
        MsgContent::LOGIN_RESPONSE, 
        CreateLoginResponse(builder, 
            cid, 
            code,
            rev0,
            crev,
            can_force,
            keepalive_millis
        ).Union()
    );

    builder.Finish(msg);
    return builder.Release();
}

flatbuffers::DetachedBuffer nplex::create_load_msg(std::size_t cid, msgs::LoadMode mode, rev_t rev)
{
    using namespace msgs;
    using namespace flatbuffers;

    FlatBufferBuilder builder;

    auto msg = CreateMessage(builder, 
        MsgContent::LOAD_REQUEST,
        CreateLoadRequest(builder, 
            cid, 
            mode,
            rev
        ).Union()
    );

    builder.Finish(msg);
    return builder.Release();
}

flatbuffers::DetachedBuffer nplex::create_keepalive_msg(rev_t crev)
{
    using namespace msgs;
    using namespace flatbuffers;

    FlatBufferBuilder builder;

    auto msg = CreateMessage(builder, 
        MsgContent::LOAD_REQUEST,
        CreateKeepAlivePush(builder, 
            crev
        ).Union()
    );

    builder.Finish(msg);
    return builder.Release();
}

const nplex::msgs::Message * nplex::parse_network_msg(const char *ptr, size_t len)
{
    using namespace nplex;
    using namespace nplex::msgs;
    using namespace flatbuffers;

    if (len <= 3 * sizeof(std::uint32_t))
        return nullptr;

    if (len != ntohl(*((const std::uint32_t *) ptr)))
        return nullptr;

    std::uint32_t metadata = ntohl(*((const std::uint32_t *) (ptr + sizeof(std::uint32_t))));
    // TODO: uncompress if (metadata & LZ4)
    UNUSED(metadata);

    std::uint32_t checksum = ntohl(*((const std::uint32_t *) (ptr + len - sizeof(std::uint32_t))));

    if (checksum != CRC32::CRC32::calc(reinterpret_cast<const std::uint8_t *>(ptr), len - sizeof(std::uint32_t)))
        return nullptr;

    ptr += 2 * sizeof(std::uint32_t);
    len -= 3 * sizeof(std::uint32_t);

    auto verifier = flatbuffers::Verifier((const std::uint8_t *) ptr, len);
    if (!verifier.VerifyBuffer<nplex::msgs::Message>(nullptr))
        return nullptr;

    return flatbuffers::GetRoot<nplex::msgs::Message>(ptr);
}
