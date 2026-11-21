# nplex

A reactive, key-value database with ACID guarantees, designed for scenarios where
low-latency reads, real-time change notification, and minimal resource consumption matter.

## Features

**Core database**

- **Key-value store** — Text keys, arbitrary binary values.
- **Client-server architecture** — Clients connect over TCP using a compact binary protocol.
- **Historical snapshots** — Retrieve a consistent database snapshot at any revision.
- **Revision history** — Stream or query all updates since a given revision.
- **ACID guarantees** — Atomic, consistent, isolated, and durable transactions.
- **Persistence** — Append-only journal for durability, periodic snapshots for fast recovery.
- **Lock-free atomic transactions** — Transactions are managed client-side; no server-side locks.
- **Isolation levels** — Read-committed, repeatable-read, and serializable.

**What makes it different**

- **Reactive** — Clients receive real-time change notifications. You can build reactors that respond
  to every commit as it happens.
- **Blazing-fast reads** — Data lives in client memory. Reads never hit the server.
- **High write throughput** — Sub-millisecond commits, > 40,000 tx/s on modest hardware.
- **Low resource footprint** — Minimal CPU, memory, and disk usage.

**Additional capabilities**

- Historical snapshots (point-in-time database state at any revision).
- Update streaming for real-time synchronization.
- Journal-based audit trail (complete history of modifications).
- Basic ACL security (CRUD permissions over key patterns).
- Glob-pattern key filtering.

**Transaction features**

- Optimistic concurrency control.
- Isolation levels (read-committed, repeatable-read, and serializable).
- Ensures (assert that selected keys have not changed since the transaction revision).
- Force mode (bypass serialization checks; restricted by ACL permissions).

## How It Works

1. The client connects and authenticates.
2. The client requests a snapshot at a specific revision.
3. The server sends the requested snapshot filtered by the client's permissions.
3. The client requests a stream of updates starting from the requested revision.
4. The server sends the requested existing updates filtered by the client's permissions.
5. The server sends the requested existing updates filtered by the client's permissions.
6. From that point, the server pushes every update in real time, filtered by the client's permissions.
7. Reads are local — the client queries its own in-memory copy, never the server.
8. Writes go through the server — the client submits a transaction; the server validates, persists
   (journal), and broadcasts the update to all connected clients.

Each accepted transaction increments the database revision. Every key-value pair is tagged with
the revision that last modified it. Transactions are serializable: if two clients modify the same
key concurrently, the first commit wins and the second is rejected (optimistic concurrency control). 
Transactions are atomic: if any change in a transaction fails, the entire transaction is rolled back.

Transactions are managed on the client side. Each client maintains an up-to-date local view of the 
database state and continuously receives revision updates from the server. If a newly received 
revision conflicts with an open transaction, that transaction is automatically invalidated and 
must be rebuilt against the latest database state.

The client supports the READ_COMMITTED, REPEATABLE_READ, and SERIALIZABLE isolation levels. 
Open transactions are continuously validated against incoming revisions and automatically invalidated 
when their isolation guarantees can no longer be preserved.

## Clients

The only way to interact with Nplex is through a client library. There is no built-in CLI
or GUI - you write the client that fits your use case.

Currently available:

| Library | Language | Notes |
|---------|----------|-------|
| [nplex-cpp](https://github.com/torrentg/nplex-cpp) | C++ | High performance — a single client can sustain >40,000 commits/s on a modest machine (NUC7i7BNH). |

> An administration console (`nplexctl`) is planned but not yet available.

## Current Limitations

| Area | Constraint |
|------|-----------|
| Clients | Tens of concurrent clients (~50) |
| Data size | Must fit in memory (~300 MB) |
| Configuration | Clients are pre-declared in config file |
| Encryption | TLS not yet implemented |
| Admin console | Not yet available |
| Metrics | No Prometheus/SNMP export yet |
| Replication | No Raft-based replication yet |
| Journal management | Single journal file (rotation not yet supported) |
| Compression | No compression for snapshots or updates |

## Use Case Example — Industrial SCADA

Consider a **car chassis assembly line** — a set of robots and mechanisms controlled by PLCs.
Most data points (sensors, actuators, setpoints) are known in advance.

Several modules share configuration and live data through Nplex:

```
┌─────────┐    ┌─────────┐    ┌─────────┐    ┌─────────┐
│   I/O   │    │  Stats  │    │ Control │    │ Display │
│ module  │    │ module  │    │ module  │    │ module  │
└────┬────┘    └────┬────┘    └────┬────┘    └────┬────┘
     └──────────────┴──────┬───────┴──────────────┘
                       ┌───┴───┐
                       │ nplex │
                       └───────┘
```

- **I/O** — Reads from physical devices (Modbus, PLCs) and writes values to Nplex. Reacts to
  variable changes notified by Nplex by sending commands to actuators.
- **Stats** — Computes KPIs, counters, and statistics in real time as data arrives, storing
  results back into Nplex.
- **Control** — Business rules that operate on system state: alarms, actuation orders. Can be
  organized into sub-modules (energy, general, safety…).
- **Display** — A web application (e.g. Java) serving dashboards over HTTPS + WebSockets.
  Shows a command panel (alarms, KPIs), SVG-based system diagrams, and live data updates
  powered by Nplex reactivity + WebSockets.
- **nplex** — It manages system configuration and state preservation while ensuring durable data persistence and integrity. Controlled redistribution of data across components.

## Project Status

- Initial development complete.
  - Architecture designed for performance.
  - Compact codebase — under 5,000 lines of application code.
  - Modern, pragmatic C++20 — clarity and simplicity over cleverness.
  - Code quality checked with static analysis, Valgrind, and sanitizers.
- Not yet profiled — there is room for performance optimization.
- Missing lateral features to become a full product (TLS, admin console, metrics…).
- Active development is not planned unless there is community interest.
- No active installations at this time.

**Bottom line:** Nplex can be a good fit if you need tight control over this piece of your
architecture and are willing to invest time in it. Otherwise, consider proven alternatives that
cover parts of what Nplex offers — [etcd](https://etcd.io/),
[FoundationDB](https://www.foundationdb.org/), or [TiKV](https://tikv.org/).

## Documentation

- **[Getting Started](docs/setup.md)** — Building, running, and architecture details.
- **[Configuration Reference](docs/config.md)** — All configuration parameters with defaults and descriptions.
- **[Project History](docs/history.md)** — The story behind Nplex: motivations and lessons learned.

## Maintainers

This project is maintained by Gerard Torrent ([torrentg](https://github.com/torrentg/)).

## License

This project is licensed under the MIT License — see the [LICENSE](LICENSE) file for details.

For dependency licenses, see the [Dependencies](docs/setup.md#dependencies) section.
