#pragma once

#include <cstdint>

namespace nplex {

    /**
     * Same as ntohl but receiving a pointer to uint32_t instead of a uint32_t.
     * 
     * @param ptr Pointer to uint32_t in network byte order.
     * 
     * @return Value in host byte order.
     */
    std::uint32_t ntohl_ptr(const char *ptr);

} // namespace nplex
