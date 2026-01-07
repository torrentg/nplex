# nplex

TODO

## Features

TODO

## Building

```bash
cd build
cmake -DENABLE_SANITIZERS=ON ..
make VERBOSE=1
ctest -V
valgrind --tool=memcheck --leak-check=yes ./nplex -D test -l debug
cmake -DENABLE_COVERAGE=ON ..
```

## Dependencies

### Static

* [cppcrc](https://github.com/DarrenLevine/cppcrc). A very small, fast, header-only, C++ library for generating CRCs. MIT license.
* [cstring](https://github.com/torrentg/cstring). A C++ immutable C-string with reference counting. LGPL-3.0 license.
* [cqueue](https://github.com/torrentg/cqueue). A C++ circular queue. LGPL-3.0 license.
* [journal](https://github.com/torrentg/journal). A simple log-structured database. MIT license.
* [doctest](https://github.com/doctest/doctest). The fastest feature-rich C++11/14/17/20/23 single-header testing framework. MIT license.
* [FastGlobbing](https://github.com/Robert-van-Engelen/FastGlobbing). Wildcard string matching and globbing library. CPOL license.
* [inih](https://github.com/benhoyt/inih). Simple .INI file parser in C. BSD-3-clause license.
* [utf8.h](https://github.com/sheredom/utf8.h). Utf8 string functions for C and C++. Unlicense license.
* [spdlog](https://github.com/gabime/spdlog). Fast C++ logging library. MIT license.

### Shared

* [{fmt}](https://github.com/fmtlib/fmt). A string formatting library. MIT license.
* [libuv](https://github.com/libuv/libuv). Cross-platform asynchronous I/O. MIT license.
* [flatbuffers](https://github.com/google/flatbuffers). Memory efficient serialization library. Apache-2.0 license .

## Maintainers

This project is mantained by Gerard Torrent ([torrentg](https://github.com/torrentg/)).

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
