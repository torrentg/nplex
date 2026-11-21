#include <sstream>
#include <doctest.h>
#include "exception.hpp"

using namespace std;
using namespace nplex;

namespace {

    void function_throwing_exception() {
        throw nplex_exception("error in line {}.", 25);
    }

} // unnamed namespace

TEST_CASE("nplex_exception")
{
    SUBCASE("real case")
    {
        try {
            function_throwing_exception();
            CHECK(false);
        } catch (const std::exception &e) {
            CHECK(e.what() == string("error in line 25."));
        }
    }

    SUBCASE("Exception without parameters")
    {
        nplex_exception excp("message 1");
        CHECK(excp.what() == string("message 1"));
    }

    SUBCASE("Exception with 1 parameter")
    {
        // int value
        nplex_exception excp1("value1 = {}.", 4);
        CHECK(excp1.what() == string("value1 = 4."));

        // double value
        nplex_exception excp2("value1 = {}.", 2.5);
        CHECK(excp2.what() == string("value1 = 2.5."));

        // char array value
        nplex_exception excp3("value1 = {}.", "const char array");
        CHECK(excp3.what() == string("value1 = const char array."));

        // string value
        nplex_exception excp4("value1 = {}.", string("string"));
        CHECK(excp4.what() == string("value1 = string."));
    }

    SUBCASE("Exception with 2 parameters")
    {
        // int + double
        nplex_exception excp1("value1={}, value2={}.", 4, 2.5);
        CHECK(excp1.what() == string("value1=4, value2=2.5."));

        // string + const char array
        nplex_exception excp2("value1={}, value2={}.", string("str"), "char_array");
        CHECK(excp2.what() == string("value1=str, value2=char_array."));
    }

    SUBCASE("Exception with 3 parameters")
    {
        // int + double + string
        nplex_exception excp1("value1={}, value2={}, value3={}.", 4, 2.5, "str");
        CHECK(excp1.what() == string("value1=4, value2=2.5, value3=str."));
    }

    SUBCASE("Output stream")
    {
        std::ostringstream os;
        nplex_exception excp("message");
        os << excp;
        CHECK(os.str() == "message");
    }
}
