#pragma once

#include <string>
#include <cstdint>
#include <sys/socket.h>

namespace nplex {

/**
 * Server address.
 * This is an immutable class.
 */
class addr_t
{
  private:
    std::string m_host;         // ip4, ip6 without brackets, or hostname.
    std::uint16_t m_port;       // Port number.
    std::uint8_t m_family;      // AF_INET, AF_INET6, AF_UNSPEC (hostname).

  public:

    /**
     * Default constructor.
     * Localhost address with port 0.
     */
    addr_t() : m_host("localhost"), m_port(0), m_family(AF_UNSPEC) {}

    /**
     * Server address constructor.
     * 
     * Format is host:port, where:
     *   - host is an ip4 addres, or ip6 address with brackets, or a hostname.
     *   - port is a number in range [1, 65535].
     * 
     * Examples:
     *   - ip4: 127.0.0.1:1234, 79.153.75.78:25888
     *   - ip6: [2001:db8:85a3:8d3:1319:8a2e:370:7348]:1234, [fe80::1ff:fe23:4567:890a%eth2]:5542, [::1]:25888
     *   - hostname: localhost:1234, leviatan-1:25888, nplex.generacio.com:25888
     * 
     * @param[in] str Server address (host:port).
     * 
     * @exception nplex_exception Invalid address.
     */
    addr_t(const std::string &str);

    // getters
    const std::string & host() const { return m_host; };
    std::uint16_t port() const { return m_port; }
    std::uint8_t family() const { return m_family; }
    std::string str() const;
};

} // namespace nplex
