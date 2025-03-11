#include <arpa/inet.h>
#include "utils.hpp"

std::uint32_t nplex::ntohl_ptr(const char *ptr)
{
    return ntohl(*reinterpret_cast<const std::uint32_t *>(ptr));
}
