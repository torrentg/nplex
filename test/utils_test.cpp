#include <doctest.h>
#include "utils.hpp"
#include "user.hpp"
#include <chrono>

using namespace std;
using namespace nplex;

TEST_CASE("to_iso8601")
{
    using ms = std::chrono::milliseconds;

    // Unix epoch
    CHECK(to_iso8601(ms{0}) == "1970-01-01T00:00:00.000Z");

    // Known timestamp: 2024-03-15T11:34:56.789Z (UTC)
    // Precomputed: date -ud "2024-03-15T11:34:56Z" +%s -> 1710502496
    CHECK(to_iso8601(ms{1710502496789}) == "2024-03-15T11:34:56.789Z");

    // Milliseconds are zero
    CHECK(to_iso8601(ms{1710502496000}) == "2024-03-15T11:34:56.000Z");

    // End of a day / start of next
    // 1970-01-01T23:59:59.999Z -> 86399999 ms
    CHECK(to_iso8601(ms{86399999}) == "1970-01-01T23:59:59.999Z");
    CHECK(to_iso8601(ms{86400000}) == "1970-01-02T00:00:00.000Z");
}

TEST_CASE("crud_to_string")
{
    CHECK(crud_to_string(0) == "----");
    CHECK(crud_to_string(CRUD_CREATE) == "c---");
    CHECK(crud_to_string(CRUD_READ) == "-r--");
    CHECK(crud_to_string(CRUD_UPDATE) == "--u-");
    CHECK(crud_to_string(CRUD_DELETE) == "---d");
    CHECK(crud_to_string(CRUD_CREATE | CRUD_READ | CRUD_UPDATE | CRUD_DELETE) == "crud");
    CHECK(crud_to_string(CRUD_CREATE | CRUD_READ) == "cr--");
    CHECK(crud_to_string(CRUD_UPDATE | CRUD_DELETE) == "--ud");
}

TEST_CASE("parse_crud")
{
    // parse_crud: round-trip with crud_to_string
    for (std::uint8_t v = 0; v < 16; v++)
        CHECK(parse_crud(crud_to_string(v)) == v);

    // parse_crud: invalid inputs throw
    CHECK_THROWS(parse_crud(""));
    CHECK_THROWS(parse_crud("cru"));
    CHECK_THROWS(parse_crud("crudd"));
    CHECK_THROWS(parse_crud("xrud"));
    CHECK_THROWS(parse_crud("crux"));
}

TEST_CASE("checksum")
{
    const char data[] = "The quick brown fox jumps over the lazy dog";
    const std::uint32_t expected_crc = 0x22620404; // precomputed using https://crccalc.com/
    std::uint32_t crc = 0;

    // Single call
    crc = crc32(data, sizeof(data) - 1);
    CHECK(crc == expected_crc);

    // Incremental calculation
    crc = crc32(&data[0], 10);
    crc = crc32(&data[0] + 10, sizeof(data) - 1 - 10, crc);
    CHECK(crc == expected_crc);
}
