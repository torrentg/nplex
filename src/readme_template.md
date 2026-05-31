
# nplex data directory

This directory is managed by [nplex](https://github.com/torrentg/nplex),
a key-value stream database.

The system is designed to let you maintain its files (copy, move, compress, rotate, etc.).

Please follow these rules:

* Do not leave gaps between journals.
* Do not modify the active journal while the system is running.
* The available data range is determined by the smallest revision among all journals and the smallest snapshot with that revision.
* When a client requests a journal, it is rebuilt from the nearest lower snapshot and the subsequent journal entries up to the requested revision.

## Files

- `nplex.ini` — Configuration file.
  See the nplex documentation for details.

- `nplex.log` — Log file.
  Present only when running daemonized (`-d` flag).
  Otherwise, you can redirect stdout to this file if needed.
  You can use `logrotate` to manage this file by sending `SIGHUP`.

- `journal.dat` — Active journal (data).
  This file is locked while the database is running.
  Use `nplex_dump` to inspect the contents.
  Use `jtools` to manage this file (check, repair, split, etc.)

- `journal.idx` — Active journal (index).
  This file is locked while the database is running.
  This file is rebuilt at startup if it does not exist or is corrupted.

- `journal-N.dat` — Archived journal (N = max revision in the archive).
  These files are open in read-only mode and closed after use.
  You can move or delete these files as needed.
  Use `nplex_dump` to inspect the contents.
  Use `jtools` to manage these files.

- `journal-N.idx` — Archived journal (N = max revision in the archive).
  These files are open in read-only mode and closed after use.
  These files are rebuilt at startup if they do not exist or are corrupted.

- `snapshot-N.dat` — Database snapshot (N = revision number).
  These files are open in read-only mode and closed after use.
  Deleting a snapshot can reduce the available data range.
  Deleting a snapshot can increase the snapshot delivery time.
  Use `nplex_dump` to inspect the contents.

- `README.md` — This file.

## Notes

You can add leading zeros to revision numbers (e.g., rename `snapshot-5000.dat` to `snapshot-005000.dat`).

Use `ls -lv` to sort files by revision number.

`jtools` is a program for managing journal files belonging to the [journal project](https://github.com/torrentg/journal).

