#pragma once

#include <ostream>
#include <stdexcept>
#include <fmt/core.h>

namespace nplex {

/**
 * @brief Exception throwed by nplex routines.
 * @details Inherits std::exception (use e.what() to retrieve message).
 * @details Supports placeholders '{}' in messages (fmt library).
 */
class nplex_exception : public std::runtime_error
{
  public:

    using std::runtime_error::runtime_error;

    template <typename... Args>
    nplex_exception(fmt::format_string<Args...> fmt_string, Args&&... args) :
        runtime_error(fmt::format(fmt_string, std::forward<Args>(args)...)) {}

    virtual ~nplex_exception() noexcept = default;
};

struct invalid_config : public nplex_exception {
    using nplex_exception::nplex_exception;
};

/**
 * @details Prints the Exception in an output stream.
 * @param[out] os Output stream.
 * @param[in] excp Exception to write.
 * @return Reference to the output stream.
 */
inline std::ostream & operator<<(std::ostream &os, const nplex::nplex_exception &excp) {
    return os << excp.what();
}

} // namespace nplex
