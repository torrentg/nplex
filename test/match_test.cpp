#include <doctest.h>
#include "match.h"

TEST_CASE("match")
{
    // pattern '*': anything except a / (lazy match)
    // ---------------------------------------------

    // * matches a, b but not x/a, x/y/b
    CHECK(glob_match("a", "*"));
    CHECK(glob_match("b", "*"));
    CHECK(!glob_match("x/a", "*"));
    CHECK(!glob_match("x/y/b", "*"));

    // a/*/b matches a/x/b, a/y/b but not a/b, a/x/y/b
    CHECK(glob_match("a/x/b", "a/*/b"));
    CHECK(glob_match("a/y/b", "a/*/b"));
    CHECK(!glob_match("a/b", "a/*/b"));
    CHECK(!glob_match("a/x/y/b", "a/*/b"));

    CHECK(glob_match("", "*"));
    CHECK(glob_match("jdoe", "*"));
    CHECK(glob_match("jdoe", "*doe"));
    CHECK(glob_match("jdoe", "jdo*"));
    CHECK(glob_match("jdoe", "*jdoe"));
    CHECK(glob_match("jdoe", "jdoe*"));
    CHECK(glob_match("jdoe", "*o*"));
    CHECK(glob_match("/", "/*"));
    CHECK(glob_match("/", "*/"));
    CHECK(glob_match("/", "*/*"));
    CHECK(glob_match("/system", "/*"));
    CHECK(glob_match("/system/", "/*/"));
    CHECK(glob_match("/system/users", "/system/*"));
    CHECK(glob_match("/system/users", "/*/users"));
    CHECK(glob_match("/system/users", "/*/users"));
    CHECK(glob_match("/system/users", "/*/use*"));
    CHECK(glob_match("/system/users", "/*/*ers"));
    CHECK(glob_match("/system/users", "/*/*ser*"));
    CHECK(glob_match("/system/users", "/*/*"));
    CHECK(glob_match("/system/users/", "/*/*/"));
    CHECK(glob_match("/system/users/", "/*/*/*"));
    CHECK(glob_match("/system/users/", "/*/users/"));
    CHECK(glob_match("/system/users/jdoe/password", "/system/*/*o*/*"));

    CHECK(!glob_match("abcd", "*o*"));
    CHECK(!glob_match("system/", "*"));
    CHECK(!glob_match("system/users", "*"));
    CHECK(!glob_match("/", "*"));
    CHECK(!glob_match("/system", "*"));
    CHECK(!glob_match("/system/", "/*"));
    CHECK(!glob_match("/system/users", "/*"));
    CHECK(!glob_match("/system/groups", "/system/users"));
    CHECK(!glob_match("/system/groups", "/system/users"));
    CHECK(!glob_match("/system/users/jdoe/password", "/system/*/*w*/*"));
    CHECK(!glob_match("a", "/*"));
    CHECK(!glob_match("a", "*/"));

    // pattern '**': anything, included / (greedy match)
    // -------------------------------------------------

    CHECK(glob_match("", "**"));
    CHECK(glob_match("a", "**"));
    CHECK(glob_match("abc", "**"));
    CHECK(glob_match("abc", "a**"));
    CHECK(glob_match("jdoe", "*o**"));
    CHECK(glob_match("jdoe", "**"));
    CHECK(glob_match("/system/users/jdoe", "/system/**"));
    CHECK(glob_match("/system/users/jdoe", "/system/**/jdoe"));
    CHECK(glob_match("./file.png", "*/*.png"));
    CHECK(glob_match("abc/avalon/door/xyz", "abc/a**/xyz"));
    CHECK(!glob_match("xyz", "a**"));

    // **/a matches a, x/a, x/y/a but not b, x/b
    CHECK(glob_match("x/a", "**/a"));
    CHECK(glob_match("x/y/a", "**/a"));
    CHECK(!glob_match("b", "**/a"));
    CHECK(!glob_match("x/b", "**/a"));

    // a/**/b matches a/b, a/x/b, a/x/y/b but not x/a/b, a/b/x
    CHECK(glob_match("a/b", "a/**/b"));
    CHECK(glob_match("a/x/b", "a/**/b"));
    CHECK(glob_match("a/x/y/b", "a/**/b"));
    CHECK(glob_match("a/x/y/z/b", "a/**/b"));
    CHECK(glob_match("a/x/y/z/", "a/**/"));
    CHECK(!glob_match("x/a/b", "a/**/b"));
    CHECK(!glob_match("a/b/x", "a/**/b"));

    // a/** matches a/x, a/y, a/x/y but not a, b/x
    CHECK(glob_match("a/x", "a/**"));
    CHECK(glob_match("a/y", "a/**"));
    CHECK(glob_match("a/x/y", "a/**"));
    CHECK(!glob_match("a", "a/**"));
    CHECK(!glob_match("b/x", "a/**"));

    // greedy match (** followed by something distinct than / fails)
    CHECK(!glob_match("abc", "**c"));
    CHECK(!glob_match("abc", "**b**"));

    // bizarre case (this case should be rejected)
    CHECK(glob_match("a", "**/a"));

    // pattern '?: any one character except a /
    // -----------------------------------

    CHECK(glob_match("*", "?"));
    CHECK(glob_match("?", "?"));
    CHECK(glob_match("a", "?"));
    CHECK(glob_match("[", "?"));
    CHECK(glob_match("]", "?"));
    CHECK(glob_match("ab", "a?"));
    CHECK(glob_match("a?c", "a?c"));
    CHECK(glob_match("ab", "??"));
    CHECK(glob_match("abcd", "a??d"));
    CHECK(glob_match("abcd", "ab??"));
    CHECK(glob_match("abcd", "a?c?"));
    CHECK(glob_match("/a", "/?"));

    CHECK(!glob_match("", "?"));
    CHECK(!glob_match("/", "?"));
    CHECK(!glob_match("/a", "?a"));

    // a?b matches axb, ayb but not a, b, ab, a/b
    CHECK(glob_match("axb", "a?b"));
    CHECK(glob_match("ayb", "a?b"));
    CHECK(!glob_match("a", "a?b"));
    CHECK(!glob_match("b", "a?b"));
    CHECK(!glob_match("ab", "a?b"));
    CHECK(!glob_match("a/b", "a?b"));

    // case [a-z]: one character in the selected range of characters
    // -----------------------------------

    // a[xy]b matches axb, ayb but not a, b, azb
    CHECK(glob_match("axb", "a[xy]b"));
    CHECK(glob_match("ayb", "a[xy]b"));
    CHECK(!glob_match("a", "a[xy]b"));
    CHECK(!glob_match("b", "a[xy]b"));
    CHECK(!glob_match("azb", "a[xy]b"));

    // a[a-z]b matches aab, abb, acb, azb but not a, b, a3b, aAb, aZb
    CHECK(glob_match("aab", "a[a-z]b"));
    CHECK(glob_match("abb", "a[a-z]b"));
    CHECK(glob_match("acb", "a[a-z]b"));
    CHECK(glob_match("azb", "a[a-z]b"));
    CHECK(!glob_match("a", "a[a-z]b"));
    CHECK(!glob_match("b", "a[a-z]b"));
    CHECK(!glob_match("a3b", "a[a-z]b"));
    CHECK(!glob_match("aAb", "a[a-z]b"));
    CHECK(!glob_match("aZb", "a[a-z]b"));

    // a[^xy]b matches aab, abb, acb, azb but not a, b, axb, ayb
    CHECK(glob_match("aab", "a[^xy]b"));
    CHECK(glob_match("abb", "a[^xy]b"));
    CHECK(glob_match("acb", "a[^xy]b"));
    CHECK(glob_match("azb", "a[^xy]b"));
    CHECK(!glob_match("a", "a[^xy]b"));
    CHECK(!glob_match("b", "a[^xy]b"));
    CHECK(!glob_match("axb", "a[^xy]b"));
    CHECK(!glob_match("ayb", "a[^xy]b"));

    // a[^a-z]b matches a3b, aAb, aZb but not a, b, aab, abb, acb, azb
    CHECK(glob_match("a3b", "a[^a-z]b"));
    CHECK(glob_match("aAb", "a[^a-z]b"));
    CHECK(glob_match("aZb", "a[^a-z]b"));
    CHECK(!glob_match("a", "a[^a-z]b"));
    CHECK(!glob_match("b", "a[^a-z]b"));
    CHECK(!glob_match("aab", "a[^a-z]b"));
    CHECK(!glob_match("abb", "a[^a-z]b"));
    CHECK(!glob_match("acb", "a[^a-z]b"));
    CHECK(!glob_match("azb", "a[^a-z]b"));

    // case \ (backslash): any character specified after the backslash
    // -----------------------------------

    // a\?b matches a?b but not a, b, ab, axb, a/b
    CHECK(glob_match("a?b", "a\\?b"));
    CHECK(!glob_match("a", "a\\?b"));
    CHECK(!glob_match("b", "a\\?b"));
    CHECK(!glob_match("ab", "a\\?b"));
    CHECK(!glob_match("axb", "a\\?b"));
    CHECK(!glob_match("a/b", "a\\?b"));

    // other cases
    CHECK(glob_match("a\\b", "a\\\\b"));
    CHECK(!glob_match("axb", "a\\\\b"));
    CHECK(glob_match("axb", "a\\xb"));
    CHECK(!glob_match("awb", "a\\xb"));
}
