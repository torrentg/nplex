# nplex

A reactive, key-value database with ACID guarantees, designed for scenarios where
low-latency reads, real-time change notification, and minimal resource consumption matter.

## Features

**Core database**

- **Key-value store** — Text keys, arbitrary binary values.
- **Client-server architecture** — Clients connect over TCP using a compact binary protocol.
- **ACID guarantees** — Atomic, consistent, isolated, and durable transactions.
- **Persistence** — Append-only journal for durability, periodic snapshots for fast recovery.
- **Lock-free atomic transactions** — Transactions are managed client-side; no server-side locks.
- **Isolation levels** — Read-committed, repeatable-read, and serializable.

**What makes it different**

- **Reactive** — Clients receive real-time change notifications. You can build reactors that respond
  to every commit as it happens.
- **Blazing-fast reads** — Data lives in client memory. Reads never hit the server.
- **High write throughput** — Sub-millisecond commits, >10,000 tx/s on modest hardware.
- **Low resource footprint** — Minimal CPU, memory, and disk usage.

**Additional capabilities**

- Snapshot support (point-in-time database state).
- Update feed/stream for real-time synchronization.
- Journal-based audit trail (full history of modifications).
- Glob-pattern filtering on keys.
- Basic ACL security (CRUD permissions over key patterns).
- Serializable transactions with optional ensures (integrity checks at commit time).

## How It Works

1. A client connects and authenticates.
2. The server sends a **snapshot** — the full database state filtered by the client's permissions.
3. From that point, the server pushes every **update** in real time.
4. **Reads are local** — the client queries its own in-memory copy, never the server.
5. **Writes go through the server** — the client submits a transaction; the server validates, persists
   (journal), and broadcasts the update to all connected clients.

Each accepted transaction increments the database **revision**. Every key-value pair is tagged with
the revision that last modified it. Transactions are **serializable**: if two clients modify the same
key concurrently, the first commit wins and the second is rejected. Transactions are **atomic**: if
any change in a transaction fails, the entire transaction is rolled back.

Additional transaction features:

- **Ensures** — Assert that certain keys have not changed since the client's revision (optimistic concurrency).
- **Force mode** — Override serialization checks (god mode, restricted by ACL).

## Clients

The only way to interact with Nplex is through a client library. There is no built-in CLI
or GUI — you write the client that fits your use case.

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
| Connection visibility | No real-time client connect/disconnect notifications |
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

- **[Getting Started](docs/setup.md)** — Building, configuration, architecture, and protocol details.
- **[Project History](docs/history.md)** — The story behind Nplex: motivations, lessons learned, and sub-projects.

## Maintainers

This project is maintained by Gerard Torrent ([torrentg](https://github.com/torrentg/)).

## License

This project is licensed under the MIT License — see the [LICENSE](LICENSE) file for details.

For dependency licenses, see the [Dependencies](docs/setup.md#dependencies) section.
