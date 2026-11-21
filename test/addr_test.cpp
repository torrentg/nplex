#include <doctest.h>
#include "addr.hpp"

using namespace std;
using namespace nplex;

namespace {

struct host_entry_t {
    std::uint8_t family;
    const std::string host;
};

const host_entry_t valid_hosts[] = {
    // ip4 address
    { AF_INET, "0.0.0.0" },
    { AF_INET, "1.2.3.4" },
    { AF_INET, "10.2.3.4" },
    { AF_INET, "1.20.3.4" },
    { AF_INET, "1.2.30.4" },
    { AF_INET, "1.2.3.40" },
    { AF_INET, "127.0.0.1" },
    { AF_INET, "79.153.84.109" },
    // ip6 address
    { AF_INET6, "[1:2:3:4:5:6:7:8]" },
    { AF_INET6, "[2001:db8:85a3:8d3:1319:8a2e:370:7348]" },
    { AF_INET6, "[fe80::1ff:fe23:4567:890a]" },
    { AF_INET6, "[FE80::1ff:fe23:4567:890a]" },
    { AF_INET6, "[fe80::1ff:fe23:4567:890a%eth2]" },
    { AF_INET6, "[fdda:5cc1:23:4::1f]" },
    { AF_INET6, "[1200:0000:AB00:1234:0000:2552:7777:1313]" },
    { AF_INET6, "[21DA:D3:0:2F3B:2AA:FF:FE28:9C5A]" },
    { AF_INET6, "[2001:db8:0:00:000:0000:370:7348]" },
    { AF_INET6, "[2001:db8::00:000:0000:370:7348]" },
    { AF_INET6, "[2001:db8::000:0:370:7348]" },
    { AF_INET6, "[2001:db8:000::0:370:7348]" },
    { AF_INET6, "[2001:db8::000:370:7348]" },
    { AF_INET6, "[2001:db8::370:7348]" },
    { AF_INET6, "[::]" },
    { AF_INET6, "[::1]" },
    { AF_INET6, "[::ffff:10.0.0.3]" },
    { AF_INET6, "[0064:ff9b:0000:0000:0000:0000:1234:5678]" },
    { AF_INET6, "[0064:ff9b:0001:1122:0033:4400:0000:0001]" },
    { AF_INET6, "[100::]" },
    { AF_INET6, "[2001:0000:6dcd:8c74:76cc:63bf:ac32:6a1]" },
    { AF_INET6, "[2001:1::1]" },
    { AF_INET6, "[2001:1::2]" },
    { AF_INET6, "[2001:0002:cd:65a:753::a1]" },
    { AF_INET6, "[2001:0003:cd:65a:753::a1]" },
    { AF_INET6, "[2001:4:112:cd:65a:753:0:a1]" },
    { AF_INET6, "[2001:5::]" },
    { AF_INET6, "[2001:21::3f4b:1aff:f7b2]" },
    { AF_INET6, "[2001:db8::a3]" },
    { AF_INET6, "[2002:6dcd:8c74:6501:fb2:61c:ac98:6be]" },
    { AF_INET6, "[2620:4f:8000::]" },
    { AF_INET6, "[fd07:a47c:3742:823e:3b02:76:982b:463]" },
    { AF_INET6, "[fea3:c65:43ee:54:e2a:2357:4ac4:732]" },
    { AF_INET6, "[::ffff:0:10.0.0.3]" },
    { AF_INET6, "[2001:0db8:1234:5678:00aa:aaaa:aaaa:aaaa]" },
    { AF_INET6, "[2001:0db8:0012:3456:0078:aaaa:aaaa:aaaa]" },
    { AF_INET6, "[2001:0db8:0000:1234:0056:78aa:aaaa:aaaa]" },
    { AF_INET6, "[2001:0db8:0000:0012:0034:5678:aaaa:aaaa]" },
    { AF_INET6, "[2001:0db8:0000:0000:0012:3456:78aa:aaaa]" },
    { AF_INET6, "[2001:0db8:0000:0000:0000:0000:1234:5678]" },
    // hostname
    { AF_UNSPEC, "example.com" },
    { AF_UNSPEC, "www.example.com" },
    { AF_UNSPEC, "leviatan-1" },
    { AF_UNSPEC, "localhost" },
    { AF_UNSPEC, "9gag.com" },
    { AF_UNSPEC, "00-001" },
    { AF_UNSPEC, "00-001.44-2" },
    { AF_UNSPEC, "xn--80ak6aa92e.com" }
};

const host_entry_t invalid_hosts[] = {
    // ip4 address
    { AF_INET, "1" },
    { AF_INET, "1.2" },
    { AF_INET, "1.2.3" },
    { AF_INET, "1.2.3.4.5" },
    { AF_INET, "257.1.1.1" },
    { AF_INET, "1.257.1.1" },
    { AF_INET, "1.1.257.1" },
    { AF_INET, "1.1.1.257" },
    // ip6 address
    { AF_INET6, "[]" },
    { AF_INET6, "[ ]" },
    { AF_INET6, "[[]" },
    { AF_INET6, "[]]" },
    { AF_INET6, "[[]]" },
    { AF_INET6, "[0064:ff9b:0000:0000:0000:0000:18.52.86.120]" },
    { AF_INET6, "[fe80:4:6c:8c74:0000:5efe:109.205.140.116]" },
    { AF_INET6, "[24a6:57:c:36cf:0000:5efe:109.205.140.116]" },
    { AF_INET6, "[2002:5654:ef3:c:0000:5efe:109.205.140.116]" },
    { AF_INET6, "[1]" },
    { AF_INET6, "[1:2]" },
    { AF_INET6, "[1:2:3]" },
    { AF_INET6, "[1:2:3:4]" },
    { AF_INET6, "[1:2:3:4:5]" },
    { AF_INET6, "[1:2:3:4:5:6]" },
    { AF_INET6, "[1:2:3:4:5:6:7]" },
    // invalid hexadecimal digits
    { AF_INET6, "[1:2:x:4:5:6:7:8]" },
    // simple : at begin or end
    { AF_INET6, "[:2:3:4:5:6:7:8]" },
    { AF_INET6, "[1:2:3:4:5:6:7:]" },
    // more than 8 groups
    { AF_INET6, "[2001:0db8:1234:5678:00aa:aaaa:aaaa:aaaa.1]" },
    // more than one '::'
    { AF_INET6, "[1::6::8]" },
    { AF_INET6, "[2001:db8:::370:7348]" },
    { AF_INET6, "[2001:db8::::370:7348]" },
    { AF_INET6, "[2001:db8::1::370:7348]" },
    { AF_INET6, "[::1:2:3::7:8]" },
    { AF_INET6, "[1:2:3::6::]" },
    // malformed ipv4
    { AF_INET6, "[0064:ff9b:0000:0000:0000:0000:18.52.86.257]" },
    { AF_INET6, "[0064:ff9b:0000:0000:0000:0000:18.52.86.a]" },
    { AF_INET6, "[0064:ff9b:0000:0000:0000:0000:18.52.86]" },
    { AF_INET6, "[0064:ff9b:0000:0000:0000:0000:18.52.86.]" },
    { AF_INET6, "[0064:ff9b:0000:0000:0000:0000:18.52.86.120:a]" },
    // zone not starting with fe80
    { AF_INET6, "[f123::1ff:fe23:4567:890a%eth2]" },
    // embeded ipv4 not starting with ffff or 0064:ff9b
    { AF_INET6, "[0666:ff9b:0000:0000:0000:0000:18.52.86.120]" },
    { AF_INET6, "[0666:ff9b:0000:0000:0000:0000:18.52.86.120]" },
    { AF_INET6, "[::aaaa:10.0.0.3]" },
    { AF_INET6, "[::aaaa:0:10.0.0.3]" },
    // hostname
    { AF_UNSPEC, "" },
    { AF_UNSPEC, "jdoe@example.com" },
    { AF_UNSPEC, "my_host.com" },
    { AF_UNSPEC, "my host.com" }
};

} // namespace unnamed

TEST_CASE("addr")
{
    addr_t addr;

    // default case
    CHECK(addr.host() == "localhost");
    CHECK(addr.port() == 0);
    CHECK(addr.family() == AF_UNSPEC);

    // valid hosts
    for (const auto &entry : valid_hosts)
    {
        string server = entry.host + ":12345";

        REQUIRE_NOTHROW(addr = addr_t{server});

        if (addr.family() == AF_INET6) 
            CHECK("[" + addr.host() + "]" == entry.host);
        else
            CHECK(addr.host() == entry.host);

        CHECK(addr.port() == 12345);
        CHECK(addr.family() == entry.family);
        CHECK(addr.str() == server);
    }

    // invalid hosts
    for (const auto &entry : invalid_hosts)
    {
        string server = string(entry.host) + ":12345";

        try {
            addr = addr_t{server};
            // some invalid IP4 hosts are valid hostnames (ex: 1, 1.2, etc)
            CHECK(addr.family() == AF_UNSPEC);
            CHECK(entry.family == AF_INET);
        } catch (const std::exception &ex) {
            CHECK(true);
        }
    }

    // invalid ports
    CHECK_THROWS_AS(addr_t{"localhost:"}, std::exception);
    CHECK_THROWS_AS(addr_t{"localhost:0"}, std::exception);
    CHECK_THROWS_AS(addr_t{"localhost:xx"}, std::exception);
    CHECK_THROWS_AS(addr_t{"localhost:+1"}, std::exception);
    CHECK_THROWS_AS(addr_t{"localhost:-1"}, std::exception);
    CHECK_THROWS_AS(addr_t{"localhost:99999"}, std::exception);
    CHECK_THROWS_AS(addr_t{"localhost:100000"}, std::exception);
}
