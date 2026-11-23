# Configuration

The configuration file ([`nplex.ini`](../conf/nplex.ini)) uses the INI format.

Below is a complete reference of all parameters.

## Global Section

| Parameter | Default | Description |
|-----------|---------|-------------|
| `addr` | `localhost:14022` | Address and port to listen on. |
| `log-level` | `info` | Log verbosity: `trace`, `debug`, `info`, `warning`, `error`. |
| `fsync` | `false` | Call `fsync` after journal writes (slow). |
| `max-sessions` | `64` | Maximum number of concurrent sessions (0 for unlimited). |
| `msg-max-bytes` | `2MB` | Limits the number of updates in a single network message (0 for unlimited). |
| `write-queue-max-entries` | `1000` | Maximum queued updates awaiting disk write (0 for unlimited). |
| `write-queue-max-bytes` | `350MB` | Maximum queued bytes awaiting disk writing (0 for unlimited). |
| `write-flush-max-entries` | `50` | Maximum updates per disk write batch (0 for unlimited). |
| `write-flush-max-bytes` | `25MB` | Maximum bytes per disk write batch (0 for unlimited). |
| `tombstone-max-entries` | `1500` | Maximum number of tombstones. When exceeded, the oldest tombstones are purged (0 for unlimited). |
| `tombstone-max-age` | `20000` | Maximum tombstone age in revisions before eligible for purging (0 for unlimited). |
| `tombstone-min-age` | `5` | Minimum tombstone age in revisions before eligible for purging. |
| `cache-max-entries` | `50000` | Maximum number of updates in the in-memory cache (0 for unlimited). |
| `cache-max-bytes` | `500MB` | Maximum number of bytes in the in-memory cache (0 for unlimited). |
| `journal-max-entries` | `100000` | Recommended maximum number of journal entries (0 for unlimited). |
| `journal-max-bytes` | `100MB` | Recommended maximum journal size in bytes (0 for unlimited). |
| `snapshot-max-entries` | `50000` | Recommended maximum cumulated update entries (0 for unlimited). |
| `snapshot-max-bytes` | `50MB` | Recommended maximum snapshot size in bytes (0 for unlimited). |

When a key is deleted, a tombstone entry is retained to prevent resurrection during concurrent
operations.

## `[user-defaults]` Section

Default values applied to all users unless overridden in their individual section.

| Parameter | Default | Description |
|-----------|---------|-------------|
| `active` | `true` | Enables or disables the user account. |
| `can-force` | `false` | Allows committing transactions despite conflicts. |
| `can-monitor` | `false` | Allows monitoring events from other sessions. |
| `max-connections` | `5` | Maximum number of concurrent connections for the user (0 for unlimited). |
| `keepalive-millis` | `3000` | Maximum idle time before sending a keepalive, in milliseconds (0 to disable). |
| `timeout-factor` | `3.0` | Timeout factor for the user (multiplier for keepalive interval). |
| `max-unack-msg` | `1000` | Disconnect the client if the number of unacknowledged messages exceeds this limit (0 for unlimited). |
| `max-unack-bytes` | `100MB` | Disconnect the client if unacknowledged data exceeds this limit (0 for unlimited). |

## User Sections (e.g. `[admin]`, `[jdoe]`)

Each user is declared as an INI section.

User parameters inherit from `[user-defaults]` and can be overridden individually.

| Parameter | Default |Description |
|-----------|---------|-------------|
| `password` | none | User password (plain text). |
| `active` | inherited | Override default active status. |
| `can-force` | inherited | Override default force permission. |
| `can-monitor` | inherited | Override default monitoring permission. |
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
