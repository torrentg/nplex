# Configuration

The configuration file (`nplex.ini`) uses the INI format.

Below is a complete reference of all parameters.

## Global Section

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

When a key is deleted, a tombstone entry is retained to prevent resurrection during concurrent
operations.

## `[user-defaults]` Section

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

## User Sections (e.g. `[admin]`, `[jdoe]`)

Each user is declared as an INI section.

User parameters inherit from `[user-defaults]` and can be overridden individually.

| Parameter | Default |Description |
|-----------|---------|-------------|
| `password` | none | User password (plain text). |
| `active` | inherited | Override default active status. |
| `can-force` | inherited | Override default force permission. |
| `max-connections` | inherited | Override default max connections. |
| `keepalive-millis` | inherited | Override default keepalive interval. |
| `timeout-factor` | inherited | Override default timeout factor. |
| `max-unack-msg` | inherited | Override default max unacknowledged messages. |
| `max-unack-bytes` | inherited | Override default max unacknowledged bytes. |
| `acl` | inherited | Access control rule.<br/>Format: `[crud]:<glob pattern>`.<br/>Multiple `acl` entries are allowed.<br/>They are evaluated *in order* (first match applies). |

Example user section:

```ini
[jdoe]
password = s3cr3t
acl = -r--:/sensors/**
acl = -ru-:/actuators/**
acl = cru-:/alarms/**
```

This gives to the user `jdoe`:
- Read-only access to keys under `/sensors/`.
- Read and update access to keys under `/actuators/`.
- Create, read, and update access to keys under `/alarms/`.
- No access to any other keys.
