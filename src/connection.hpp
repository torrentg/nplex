#pragma once

#include <uv.h>
#include "addr.hpp"
#include "params.hpp"
#include "flatbuffers/flatbuffers.h"

namespace nplex {

/**
 * Internal class representing a connection to a server.
 * 
 * server_t is accessed via tcp.loop->data.
 */
struct connection_t
{
    enum class state_e : std::uint8_t {
        CONNECTED,
        LOGGED,
        SYNCING,
        SYNCED,
        CLOSED
    };

    uv_tcp_t tcp;
    addr_t addr;
    state_e state;
    char input_buffer[UINT16_MAX] = {0};
    std::string input_msg;
    int error = 0;

    struct {
        std::uint32_t max_unack_msgs = 0;
        std::uint32_t max_unack_bytes = 0;
        std::uint32_t max_msg_bytes = 0;
    } params;

    struct {
        std::uint32_t unack_msgs = 0;
        std::uint32_t unack_bytes = 0;
        std::size_t recv_msgs = 0;
        std::size_t recv_bytes = 0;
        std::size_t sent_msgs = 0;
        std::size_t sent_bytes = 0;
    } stats;

    connection_t(const addr_t &addr_, uv_loop_t *loop_, const user_params_t &params_);
    ~connection_t();

    void connect();
    void disconnect(int rc = 0);
    void send(flatbuffers::DetachedBuffer &&buf);
};

} // namespace nplex
