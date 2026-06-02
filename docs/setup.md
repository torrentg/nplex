# Getting Started

This document covers building, configuring, running, and understanding the internals of nplex.

## Building

### Prerequisites

Install the required system packages according to your distro (see [Dependencies](#dependencies)): 

`C++ compiler, cmake, pkg-config, fmtlib, libuv, flatbuffers, spdlog, jemalloc`

### Compile (release mode)

```bash
git clone https://github.com/torrentg/nplex.git
cd nplex
mkdir -p build && cd build
cmake -DCMAKE_BUILD_TYPE=Release ..
make -j$(nproc)
```

This produces two binaries in the `build/` directory:

| Binary       | Description |
|--------------|-------------|
| `nplex`      | The database server |
| `nplex_dump` | Dumps file contents as JSON |

## Running nplex

### Getting help

```bash
./nplex --help
```

```
Nplex is a key-value stream database.

Usage:
  nplex -D datadir [OPTION]...

Options:
  -D DATADIR       Database directory.
  -a HOST:PORT     Address to listen on (ex: localhost:14022).
  -l LOGLEVEL      Log level (trace, debug, info, warning, error).
  -c               Check journal file at startup.
  -d               Run the program as a daemon.
  -F               Turn fsync off.
  -V, --version    Output version information, then exit.
  -h, --help       Show this help, then exit.

Signals:
  SIGHUP           Recreate log file (when daemonized).
  SIGINT           Graceful shutdown.
  SIGTERM          Graceful shutdown.
  SIGPIPE          Ignored.

Exit status:
  0   finished without errors
  1   finished with errors

Nplex home page: <https://github.com/torrentg/nplex>.
```

### Launching Nplex

```bash
./nplex -D /path/to/datadir
```

If the directory does not exist, Nplex creates it automatically.\
If the [`nplex.ini`](../conf/nplex.ini) configuration file does not exist, Nplex create a default one.\
See the [Configuration Reference](config.md) section below for a full description of all
settings.

After running, the Nplex data directory contains:

| File | Description |
|------|-------------|
| `nplex.ini` | Configuration file (created if not exist) |
| `journal.dat` | Append-only transaction journal |
| `journal.idx` | Journal index file (recreated if removed) |
| `journal-<rev>.dat` | Archived transaction journal |
| `journal-<rev>.idx` | Archived journal index file |
| `snapshot-<rev>.dat` | Periodic snapshots (created according to config file rules) |
| `nplex.log` | Log file (when running as daemon with `-d`) |

### Running a client

Nplex requires a client library to interact with the database. The reference client is
[nplex-cpp](https://github.com/torrentg/nplex-cpp). Follow its instructions to install 
and compile some examples.

```bash
# Example: run the functional tests
./testfunc -u admin -p s3cr3t -s localhost:14022

# Example: run the stress test (flooder)
./flooder -u admin -p s3cr3t -s localhost:14022 -n 50000 -m 30
```

### Support Tools

Use `nplex_dump` to export a journal or snapshot file as JSON.

```bash
# Getting help
./nplex_dump -h

# Dumps snapshot
./nplex_dump /path/to/datadir/snapshot-1234.dat
```

## Advanced Build Options

Quick reference for development and quality checks.

```bash
# Address + Undefined Behavior Sanitizers
cmake -DENABLE_SANITIZERS=ON ..
make -j$(nproc)

# Thread Sanitizer (cannot be combined with ENABLE_SANITIZERS)
cmake -DENABLE_THREAD_SANITIZER=ON ..
make -j$(nproc)

# Profiling
cmake -DENABLE_PROFILER=ON ..
make -j$(nproc)
./nplex -D test
gprof ./nplex gmon.out

# Flame graph
cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo \
      -DCMAKE_CXX_FLAGS="-fno-omit-frame-pointer" \
      -DCMAKE_C_FLAGS="-fno-omit-frame-pointer" ..
perf record -F 999 -g ./nplex -D test
perf script | stackcollapse-perf.pl | flamegraph.pl > flamegraph.svg
firefox flamegraph.svg

# Code coverage
cmake -DENABLE_COVERAGE=ON ..
make -j$(nproc)

# Valgrind
cmake ..
make -j$(nproc)
valgrind --tool=memcheck --leak-check=yes ./nplex -D test -l debug

# clang-tidy (uncomment CMAKE_CXX_CLANG_TIDY in CMakeLists.txt)
cmake ..
make -j$(nproc)

# cppcheck
cppcheck --enable=all --inconclusive --std=c++20 --force \
  --suppress=missingIncludeSystem --suppress=unusedFunction \
  --suppress=noExplicitConstructor --suppress=normalCheckLevelMaxBranches \
  -Ideps -Isrc src

# Lines of code
cloc --exclude-content="automatically generated" src

# Code complexity
pip install lizard
lizard -C 20 --language cpp --exclude src/schema.hpp src/*.[ch]pp
```

## Dependencies

### Static (bundled in `deps/`)

| Library | Description | License |
|---------|-------------|---------|
| [FastGlobbing](https://github.com/Robert-van-Engelen/FastGlobbing) | Wildcard pattern matching | CPOL |
| [cstring](https://github.com/torrentg/cstring) | Immutable C-string with reference counting | LGPL-3.0 |
| [journal](https://github.com/torrentg/journal) | Log-structured append-only storage | MIT |
| [cqueue](https://github.com/torrentg/cqueue) | Circular queue | LGPL-3.0 |
| [base64](https://github.com/tobiaslocker/base64) | Base64 encoder / decoder | MIT |
| [utf8.h](https://github.com/sheredom/utf8.h) | UTF-8 string functions | Unlicense |
| [inih](https://github.com/benhoyt/inih) | INI file parser | BSD-3-Clause |

### Shared (system packages)

| Library | Description | License |
|---------|-------------|---------|
| [{fmt}](https://github.com/fmtlib/fmt) | String formatting | MIT |
| [libuv](https://github.com/libuv/libuv) | Cross-platform async I/O | MIT |
| [flatbuffers](https://github.com/google/flatbuffers) | Efficient binary serialization | Apache-2.0 |
| [spdlog](https://github.com/gabime/spdlog) | Logging library | MIT |
| [jemalloc](https://github.com/jemalloc/jemalloc) | A general purpose malloc implementation | FreeBSD |
