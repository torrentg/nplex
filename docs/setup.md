# Getting Started

This document covers building, configuring, running, and understanding the internals of nplex.

## Building

### Prerequisites

Install the required system packages according to your distro (see [Dependencies](#dependencies)): 

`C++ compiler, cmake, fmtlib, libuv, flatbuffers and spdlog`

### Compile (release mode)

```bash
git clone https://github.com/torrentg/nplex.git
cd nplex
mkdir -p build && cd build
cmake -DCMAKE_BUILD_TYPE=Release ..
make -j$(nproc)
```

This produces three binaries in the `build/` directory:

| Binary     | Description |
|------------|-------------|
| `nplex`    | The database server |
| `nplexcat` | File viewer (dumps content as JSON) |

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
```

### Launching Nplex

```bash
./nplex -D /path/to/datadir
```

If the directory does not exist, Nplex creates it automatically.\
If the [`nplex.ini`](../conf/nplex.ini) configuration file does not exist, Nplex create a default one.\
See the [Configuration Reference](#configuration-reference) section below for a full description of all
settings.

After running, the Nplex data directory contains:

| File | Description |
|------|-------------|
| `nplex.ini` | Configuration file (created if not exist) |
| `journal.dat` | Append-only transaction journal |
| `journal.idx` | Journal index file (recreated if removed) |
| `snapshot-<rev>.dat` | Periodic snapshots (created according to config file rules) |
| `nplex.log` | Log file (when running as daemon with `-d`) |

### Running a client

Nplex requires a client library to interact with the database. The reference client is
[nplex-cpp](https://github.com/torrentg/nplex-cpp). Follow its instructions to install 
and compile it.

```bash
# Example: run the functional tests
./testfunc -u admin -p s3cr3t -s localhost:14022

# Example: run the stress test (flooder)
./flooder -u admin -p s3cr3t -s localhost:14022 -n 50000 -m 30
```

### Support Tools

Use `nplexcat` to dump a journal or snapshot file as JSON.

```bash
# Getting help
./nplexcat -h

# Dumps snapshot
./nplexcat /path/to/datadir/snapshot-1234.dat
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
cloc --exclude-content="automatically generated" src deps
```

## Configuration Reference

The configuration file (`nplex.ini`) uses INI format. Below is a complete reference of all
parameters.

### Global Section

| Parameter | Default | Description |
|-----------|---------|-------------|
| `addr` | `localhost:14022` | Address and port to listen on. |
| `log-level` | `info` | Log verbosity: `trace`, `debug`, `info`, `warning`, `error`. |
| `disable-fsync` | `false` | Disable `fsync` after journal writes. Faster but risks data loss on crash. |
| `max-sessions` | `64` | Maximum simultaneous client connections. |
| `max-msg-bytes` | `2MB` | Maximum size of a single outbound message. |
| `write-queue-max-length` | `1000` | Maximum number of updates queued for disk write. |
| `write-queue-max-bytes` | `350MB` | Maximum bytes queued for disk write. |
| `flush-max-entries` | `50` | Maximum updates per disk write batch. |
| `flush-max-bytes` | `25MB` | Maximum bytes per disk write batch. |
| `max-updates-between-snapshots` | `50000` | Trigger a new snapshot after this many updates. |
| `max-bytes-between-snapshots` | `100MB` | Trigger a new snapshot after this many bytes of updates. |
| `tombstone-retention-max` | `20000` | Maximum number of revisions to retain tombstones. |
| `tombstone-retention-min` | `5` | Minimum number of revisions guaranteed for tombstone retention. |
| `max-tombstones` | `1500` | Maximum number of tombstone entries. When exceeded, oldest tombstones are purged. |
| `cache-max-bytes` | `500MB` | Maximum bytes for the in-memory update cache. |
| `cache-max-entries` | `50000` | Maximum entries in the in-memory update cache. |

### `[user-defaults]` Section

Default values applied to all users unless overridden in their individual section.

| Parameter | Default | Description |
|-----------|---------|-------------|
| `active` | `true` | Whether the user is enabled. |
| `can-force` | `false` | Allow forced (dirty) transactions. |
| `max-connections` | `5` | Maximum simultaneous connections for this user. |
| `keepalive-millis` | `1000` | Interval (ms) between keepalive messages. `0` = disabled. |
| `timeout-factor` | `3.0` | Connection timeout = `keepalive-millis × timeout-factor`. |
| `max-unack-msg` | `1000` | Maximum unacknowledged outbound messages before backpressure. |
| `max-unack-bytes` | `100MB` | Maximum unacknowledged outbound bytes before backpressure. |

### User Sections (e.g. `[admin]`, `[jdoe]`)

Each user is declared as an INI section. Parameters inherit from `[user-defaults]` and can be
overridden individually.

| Parameter | Description |
|-----------|-------------|
| `password` | User password (plain text). |
| `active` | Override default active status. |
| `can-force` | Override default force permission. |
| `max-connections` | Override default max connections. |
| `keepalive-millis` | Override default keepalive interval. |
| `timeout-factor` | Override default timeout factor. |
| `max-unack-msg` | Override default max unacknowledged messages. |
| `max-unack-bytes` | Override default max unacknowledged bytes. |
| `acl` | Access control rule. Format: `[crud]:<pattern>`. Multiple `acl` entries are allowed. |

**ACL format:**

```
acl = <mode>:<pattern>
```

- **mode**: A 4-character string where each position controls create (`c`), read (`r`), update (`u`),
  and delete (`d`). Use `-` to deny a specific operation.
- **pattern**: A glob pattern matched against key names (`*` matches any segment, `**` matches recursively).

ACLs are evaluated **in order** — the first matching pattern determines the permission.

**Example:**

```ini
[jdoe]
password = s3cr3t
acl = -r--:/sensors/**
acl = -ru-:/actuators/**
acl = cru-:/alarms/**
```

This gives user `jdoe`:
- Read-only access to keys under `/sensors/`.
- Read and update access to keys under `/actuators/`.
- Create, read, and update access to keys under `/alarms/`.
- No access to any other keys.

### Tombstones

When a key is deleted, a **tombstone** is retained to prevent resurrection during concurrent
operations. The tombstone parameters control how long deletions are remembered:

- `tombstone-retention-max`: Upper bound on revisions to keep tombstones.
- `tombstone-retention-min`: Lower bound — tombstones are never purged before this many revisions.
- `max-tombstones`: Hard limit on the total number of tombstones. When exceeded, the oldest are removed
  regardless of retention settings.

## Dependencies

### Static (bundled in `deps/`)

| Library | Description | License |
|---------|-------------|---------|
| [FastGlobbing](https://github.com/Robert-van-Engelen/FastGlobbing) | Wildcard pattern matching | CPOL |
| [cstring](https://github.com/torrentg/cstring) | Immutable C-string with reference counting | LGPL-3.0 |
| [journal](https://github.com/torrentg/journal) | Log-structured append-only storage | MIT |
| [cppcrc](https://github.com/DarrenLevine/cppcrc) | Header-only CRC generation | MIT |
| [cqueue](https://github.com/torrentg/cqueue) | Circular queue | LGPL-3.0 |
| [utf8.h](https://github.com/sheredom/utf8.h) | UTF-8 string functions | Unlicense |
| [inih](https://github.com/benhoyt/inih) | INI file parser | BSD-3-Clause |

### Shared (system packages)

| Library | Description | License |
|---------|-------------|---------|
| [{fmt}](https://github.com/fmtlib/fmt) | String formatting | MIT |
| [libuv](https://github.com/libuv/libuv) | Cross-platform async I/O | MIT |
| [flatbuffers](https://github.com/google/flatbuffers) | Efficient binary serialization | Apache-2.0 |
| [spdlog](https://github.com/gabime/spdlog) | Logging library | MIT |
