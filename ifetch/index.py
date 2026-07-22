"""The persistent index: one queryable store behind every recovery workflow.

Why this exists
---------------
iFetch's original bookkeeping was two flat JSON files - ``.ifetch_state.json``
for skip decisions and ``.ifetch_manifest.json`` for digests. Both are read
whole into memory and rewritten wholesale. That is fine for answering "is this
one file unchanged?", and wrong for everything this module exists to support:

* a **dry-run plan** needs to total sizes and counts across a whole tree before
  a single byte is fetched;
* a **remote-vs-local audit** needs a set difference over two trees;
* **snapshots** need many dated copies of a tree's state, not one;
* **conflict intelligence** needs to look up "which known file has this
  digest?" - a query, not a scan;
* **crash-safe resume** needs durable per-file transfer rows that survive the
  process dying mid-write.

All of those are set operations over two collections (what iCloud has, what the
disk has). SQLite gives them indexes, transactions and crash safety for free,
from the standard library, with no new dependency.

Design notes
------------
*Remote and local are recorded separately and never merged.* The whole value of
this store is being able to ask what differs between them; collapsing them into
one row set destroys exactly the information a recovery tool needs.

*The JSON files remain.* ``.ifetch_manifest.json`` is still written as a
human-readable, signable export - it is the artifact the integrity claim rests
on, and a binary database is not something a user can inspect or archive with
confidence. The database is the working store; the manifest is the evidence.

*Migration is automatic and non-destructive.* On first open, existing JSON state
and manifest are imported and the JSON is left untouched, so downgrading to an
older iFetch keeps working.
"""

from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional, Sequence, Tuple

INDEX_FILENAME = ".ifetch_index.db"
SCHEMA_VERSION = 1

# Item kinds. A package is a directory locally but a single item remotely, so it
# is neither "file" nor "dir" and needs its own kind.
KIND_FILE = "file"
KIND_DIR = "dir"
KIND_PACKAGE = "package"

# Transfer states, used for crash-safe resume.
TRANSFER_PENDING = "pending"
TRANSFER_ACTIVE = "active"
TRANSFER_DONE = "done"
TRANSFER_FAILED = "failed"

_SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

-- What iCloud reported, captured by a scan. Never mixed with local rows.
CREATE TABLE IF NOT EXISTS remote_items (
    path            TEXT PRIMARY KEY,
    kind            TEXT NOT NULL,
    size            INTEGER,
    modified_token  TEXT,
    etag            TEXT,
    docwsid         TEXT,
    drivewsid       TEXT,
    zone            TEXT,
    share_id        TEXT,
    scan_id         INTEGER,
    seen_at         REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS remote_items_scan ON remote_items(scan_id);
CREATE INDEX IF NOT EXISTS remote_items_kind ON remote_items(kind);

-- What is actually on disk, with the digest recorded when it was written.
CREATE TABLE IF NOT EXISTS local_items (
    path        TEXT PRIMARY KEY,
    kind        TEXT NOT NULL,
    size        INTEGER,
    mtime       REAL,
    sha256      TEXT,
    recorded_at REAL NOT NULL
);
-- The index that makes rename/move/duplicate detection a lookup, not a scan.
CREATE INDEX IF NOT EXISTS local_items_sha ON local_items(sha256);

CREATE TABLE IF NOT EXISTS scans (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    icloud_path  TEXT,
    started_at   REAL NOT NULL,
    finished_at  REAL,
    item_count   INTEGER DEFAULT 0,
    total_bytes  INTEGER DEFAULT 0
);

-- Durable per-file transfer state, so a crash mid-run is resumable.
CREATE TABLE IF NOT EXISTS transfers (
    path        TEXT PRIMARY KEY,
    state       TEXT NOT NULL,
    bytes_done  INTEGER DEFAULT 0,
    total_bytes INTEGER,
    attempts    INTEGER DEFAULT 0,
    last_error  TEXT,
    updated_at  REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS transfers_state ON transfers(state);

CREATE TABLE IF NOT EXISTS snapshots (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    label      TEXT UNIQUE,
    created_at REAL NOT NULL,
    note       TEXT
);

CREATE TABLE IF NOT EXISTS snapshot_entries (
    snapshot_id INTEGER NOT NULL,
    path        TEXT NOT NULL,
    kind        TEXT NOT NULL,
    size        INTEGER,
    sha256      TEXT,
    PRIMARY KEY (snapshot_id, path),
    FOREIGN KEY (snapshot_id) REFERENCES snapshots(id) ON DELETE CASCADE
);
"""


# ---------------------------------------------------------------------------
# Row models
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class RemoteItem:
    path: str
    kind: str = KIND_FILE
    size: Optional[int] = None
    modified_token: Optional[str] = None
    etag: Optional[str] = None
    docwsid: Optional[str] = None
    drivewsid: Optional[str] = None
    zone: Optional[str] = None
    share_id: Optional[str] = None


@dataclass(frozen=True)
class LocalItem:
    path: str
    kind: str = KIND_FILE
    size: Optional[int] = None
    mtime: Optional[float] = None
    sha256: Optional[str] = None


@dataclass
class DiffEntry:
    """One difference between the remote tree and the local one."""

    path: str
    status: str
    remote_size: Optional[int] = None
    local_size: Optional[int] = None
    kind: str = KIND_FILE
    detail: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "path": self.path,
            "status": self.status,
            "kind": self.kind,
            "remote_size": self.remote_size,
            "local_size": self.local_size,
            "detail": self.detail,
        }


# Diff statuses.
DIFF_NEW = "new"                 # remote only: would be downloaded
DIFF_CHANGED = "changed"         # both, but remote moved on
DIFF_UNCHANGED = "unchanged"     # both, provably identical
DIFF_LOCAL_ONLY = "local_only"   # local only: deleted remotely, or extra
DIFF_UNKNOWN = "unknown"         # remote metadata too thin to judge


class IndexStore:
    """SQLite-backed index of one destination directory.

    Safe to share across the downloader's worker threads: the connection is
    opened with ``check_same_thread=False`` and every write goes through a lock,
    which is simpler to reason about than a connection pool and more than fast
    enough next to network I/O.
    """

    def __init__(self, root: Path, filename: str = INDEX_FILENAME):
        self.root = Path(root).resolve()
        self.path = self.root / filename
        self._lock = threading.RLock()
        self.root.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(self.path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._configure()
        self._create_schema()

    # -- lifecycle ------------------------------------------------------
    def _configure(self) -> None:
        # WAL is what makes a crash mid-run survivable rather than corrupting
        # the store; NORMAL sync is the right trade next to network latency.
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute("PRAGMA foreign_keys=ON")

    def _create_schema(self) -> None:
        with self._lock:
            self._conn.executescript(_SCHEMA)
            existing = self.get_meta("schema_version")
            if existing is None:
                self.set_meta("schema_version", str(SCHEMA_VERSION))
                self.set_meta("created_at", str(time.time()))
            self._conn.commit()

    def close(self) -> None:
        with self._lock:
            try:
                self._conn.commit()
            except sqlite3.Error:
                pass
            self._conn.close()

    def __enter__(self) -> "IndexStore":
        return self

    def __exit__(self, *exc: Any) -> bool:
        self.close()
        return False

    @contextmanager
    def _write(self) -> Iterator[sqlite3.Connection]:
        """A single committed write transaction."""
        with self._lock:
            try:
                yield self._conn
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise

    # -- meta -----------------------------------------------------------
    def get_meta(self, key: str) -> Optional[str]:
        with self._lock:
            row = self._conn.execute(
                "SELECT value FROM meta WHERE key = ?", (key,)
            ).fetchone()
        return row["value"] if row else None

    def set_meta(self, key: str, value: str) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT INTO meta(key, value) VALUES(?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (key, str(value)),
            )
            self._conn.commit()

    # -- scans ----------------------------------------------------------
    def begin_scan(self, icloud_path: Optional[str] = None) -> int:
        """Open a scan and return its id. Rows recorded after this belong to it."""
        with self._write() as conn:
            cursor = conn.execute(
                "INSERT INTO scans(icloud_path, started_at) VALUES(?, ?)",
                (icloud_path, time.time()),
            )
            return int(cursor.lastrowid)

    def finish_scan(self, scan_id: int) -> Dict[str, Any]:
        """Close a scan, recording its totals, and return them."""
        with self._write() as conn:
            row = conn.execute(
                "SELECT COUNT(*) AS n, COALESCE(SUM(size), 0) AS b "
                "FROM remote_items WHERE scan_id = ? AND kind != ?",
                (scan_id, KIND_DIR),
            ).fetchone()
            conn.execute(
                "UPDATE scans SET finished_at = ?, item_count = ?, total_bytes = ? "
                "WHERE id = ?",
                (time.time(), row["n"], row["b"], scan_id),
            )
        return {"scan_id": scan_id, "item_count": row["n"], "total_bytes": row["b"]}

    def latest_scan(self) -> Optional[Dict[str, Any]]:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM scans WHERE finished_at IS NOT NULL "
                "ORDER BY id DESC LIMIT 1"
            ).fetchone()
        return dict(row) if row else None

    def clear_remote(self) -> None:
        """Drop the remote inventory. Used before a fresh full scan."""
        with self._write() as conn:
            conn.execute("DELETE FROM remote_items")

    # -- remote inventory -----------------------------------------------
    def record_remote(self, item: RemoteItem, scan_id: Optional[int] = None) -> None:
        self.record_remote_many([item], scan_id=scan_id)

    def record_remote_many(
        self, items: Iterable[RemoteItem], scan_id: Optional[int] = None
    ) -> int:
        """Insert or replace many remote rows in one transaction.

        Batched deliberately: a 100k-file drive is 100k rows, and one
        transaction per row would dominate the wall clock of a scan.
        """
        now = time.time()
        payload = [
            (
                i.path, i.kind, i.size, i.modified_token, i.etag, i.docwsid,
                i.drivewsid, i.zone, i.share_id, scan_id, now,
            )
            for i in items
        ]
        if not payload:
            return 0
        with self._write() as conn:
            conn.executemany(
                "INSERT INTO remote_items("
                " path, kind, size, modified_token, etag, docwsid, drivewsid,"
                " zone, share_id, scan_id, seen_at"
                ") VALUES(?,?,?,?,?,?,?,?,?,?,?) "
                "ON CONFLICT(path) DO UPDATE SET "
                " kind=excluded.kind, size=excluded.size,"
                " modified_token=excluded.modified_token, etag=excluded.etag,"
                " docwsid=excluded.docwsid, drivewsid=excluded.drivewsid,"
                " zone=excluded.zone, share_id=excluded.share_id,"
                " scan_id=excluded.scan_id, seen_at=excluded.seen_at",
                payload,
            )
        return len(payload)

    def get_remote(self, path: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM remote_items WHERE path = ?", (path,)
            ).fetchone()
        return dict(row) if row else None

    def remote_count(self, include_dirs: bool = False) -> int:
        sql = "SELECT COUNT(*) AS n FROM remote_items"
        if not include_dirs:
            sql += f" WHERE kind != '{KIND_DIR}'"
        with self._lock:
            return int(self._conn.execute(sql).fetchone()["n"])

    def remote_total_bytes(self) -> int:
        with self._lock:
            row = self._conn.execute(
                "SELECT COALESCE(SUM(size), 0) AS b FROM remote_items WHERE kind != ?",
                (KIND_DIR,),
            ).fetchone()
        return int(row["b"])

    def iter_remote(self, include_dirs: bool = False) -> Iterator[Dict[str, Any]]:
        sql = "SELECT * FROM remote_items"
        if not include_dirs:
            sql += f" WHERE kind != '{KIND_DIR}'"
        sql += " ORDER BY path"
        with self._lock:
            for row in self._conn.execute(sql):
                yield dict(row)

    # -- local index ----------------------------------------------------
    def record_local(self, item: LocalItem) -> None:
        self.record_local_many([item])

    def record_local_many(self, items: Iterable[LocalItem]) -> int:
        now = time.time()
        payload = [
            (i.path, i.kind, i.size, i.mtime, i.sha256, now) for i in items
        ]
        if not payload:
            return 0
        with self._write() as conn:
            conn.executemany(
                "INSERT INTO local_items(path, kind, size, mtime, sha256, recorded_at) "
                "VALUES(?,?,?,?,?,?) "
                "ON CONFLICT(path) DO UPDATE SET "
                " kind=excluded.kind, size=excluded.size, mtime=excluded.mtime,"
                " sha256=excluded.sha256, recorded_at=excluded.recorded_at",
                payload,
            )
        return len(payload)

    def get_local(self, path: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM local_items WHERE path = ?", (path,)
            ).fetchone()
        return dict(row) if row else None

    def forget_local(self, path: str) -> None:
        with self._write() as conn:
            conn.execute("DELETE FROM local_items WHERE path = ?", (path,))

    def local_count(self) -> int:
        with self._lock:
            return int(
                self._conn.execute("SELECT COUNT(*) AS n FROM local_items").fetchone()["n"]
            )

    def iter_local(self) -> Iterator[Dict[str, Any]]:
        with self._lock:
            for row in self._conn.execute("SELECT * FROM local_items ORDER BY path"):
                yield dict(row)

    def find_by_digest(self, sha256: str, exclude_path: Optional[str] = None) -> List[Dict[str, Any]]:
        """Every local item with this digest.

        This is what makes conflict intelligence possible: a file that vanished
        from one path and appeared at another is a *move*, not a delete plus a
        download, and the digest is the only evidence that says so.
        """
        if not sha256:
            return []
        sql = "SELECT * FROM local_items WHERE sha256 = ?"
        params: List[Any] = [sha256]
        if exclude_path is not None:
            sql += " AND path != ?"
            params.append(exclude_path)
        with self._lock:
            return [dict(r) for r in self._conn.execute(sql, params)]

    def duplicate_digests(self, minimum: int = 2) -> List[Dict[str, Any]]:
        """Digests held by more than one local path, with their paths."""
        with self._lock:
            rows = self._conn.execute(
                "SELECT sha256, COUNT(*) AS n, SUM(size) AS total "
                "FROM local_items WHERE sha256 IS NOT NULL AND sha256 != '' "
                "GROUP BY sha256 HAVING n >= ? ORDER BY total DESC",
                (minimum,),
            ).fetchall()
            out = []
            for row in rows:
                paths = [
                    r["path"]
                    for r in self._conn.execute(
                        "SELECT path FROM local_items WHERE sha256 = ? ORDER BY path",
                        (row["sha256"],),
                    )
                ]
                out.append(
                    {
                        "sha256": row["sha256"],
                        "count": int(row["n"]),
                        "total_bytes": int(row["total"] or 0),
                        # Only the copies beyond the first are recoverable space.
                        "wasted_bytes": int((row["total"] or 0) * (row["n"] - 1) / row["n"]),
                        "paths": paths,
                    }
                )
            return out

    # -- diff -----------------------------------------------------------
    def diff(self, include_unchanged: bool = False) -> List[DiffEntry]:
        """Set-difference the remote inventory against the local index.

        This one query is the engine behind both the dry-run plan and the
        remote-vs-local audit; they differ only in how the result is presented.
        """
        entries: List[DiffEntry] = []

        with self._lock:
            rows = self._conn.execute(
                "SELECT r.path AS path, r.kind AS kind, r.size AS rsize,"
                "       r.modified_token AS rtoken,"
                "       l.size AS lsize, l.path AS lpath"
                "  FROM remote_items r"
                "  LEFT JOIN local_items l ON l.path = r.path"
                f" WHERE r.kind != '{KIND_DIR}'"
                " ORDER BY r.path"
            ).fetchall()

        for row in rows:
            if row["lpath"] is None:
                entries.append(
                    DiffEntry(
                        path=row["path"], status=DIFF_NEW, kind=row["kind"],
                        remote_size=row["rsize"],
                        detail="present in iCloud, absent locally",
                    )
                )
                continue

            if row["rsize"] is None:
                entries.append(
                    DiffEntry(
                        path=row["path"], status=DIFF_UNKNOWN, kind=row["kind"],
                        remote_size=None, local_size=row["lsize"],
                        detail="iCloud reported no size; cannot judge without downloading",
                    )
                )
                continue

            # A package's expanded size on disk never equals the logical size
            # iCloud reports, so a size comparison here would mark every bundle
            # permanently "changed". Reporting it as unjudgeable is honest, and
            # it is always reported - suppressing it would let a genuinely
            # changed bundle vanish from a plan without a word.
            if row["kind"] == KIND_PACKAGE:
                entries.append(
                    DiffEntry(
                        path=row["path"], status=DIFF_UNKNOWN, kind=row["kind"],
                        remote_size=row["rsize"], local_size=row["lsize"],
                        detail=(
                            "package bundle; size cannot be compared, "
                            "verify with the manifest digest"
                        ),
                    )
                )
                continue

            if row["rsize"] != row["lsize"]:
                entries.append(
                    DiffEntry(
                        path=row["path"], status=DIFF_CHANGED, kind=row["kind"],
                        remote_size=row["rsize"], local_size=row["lsize"],
                        detail="size differs from the local copy",
                    )
                )
            elif include_unchanged:
                entries.append(
                    DiffEntry(
                        path=row["path"], status=DIFF_UNCHANGED, kind=row["kind"],
                        remote_size=row["rsize"], local_size=row["lsize"],
                        detail="same size as the local copy",
                    )
                )

        # Local rows with no remote counterpart: deleted in iCloud, or never
        # from iCloud at all. Either way the user should be told.
        with self._lock:
            orphans = self._conn.execute(
                "SELECT l.path AS path, l.kind AS kind, l.size AS lsize"
                "  FROM local_items l"
                "  LEFT JOIN remote_items r ON r.path = l.path"
                " WHERE r.path IS NULL ORDER BY l.path"
            ).fetchall()

        for row in orphans:
            entries.append(
                DiffEntry(
                    path=row["path"], status=DIFF_LOCAL_ONLY, kind=row["kind"],
                    local_size=row["lsize"],
                    detail="present locally, absent from the iCloud scan",
                )
            )

        return entries

    # -- transfers ------------------------------------------------------
    def set_transfer(
        self,
        path: str,
        state: str,
        bytes_done: int = 0,
        total_bytes: Optional[int] = None,
        error: Optional[str] = None,
        bump_attempts: bool = False,
    ) -> None:
        """Record durable transfer progress for one file."""
        with self._write() as conn:
            conn.execute(
                "INSERT INTO transfers(path, state, bytes_done, total_bytes,"
                " attempts, last_error, updated_at)"
                " VALUES(?,?,?,?,?,?,?)"
                " ON CONFLICT(path) DO UPDATE SET"
                "  state=excluded.state, bytes_done=excluded.bytes_done,"
                "  total_bytes=COALESCE(excluded.total_bytes, transfers.total_bytes),"
                "  attempts=transfers.attempts + ?,"
                "  last_error=excluded.last_error, updated_at=excluded.updated_at",
                (
                    path, state, bytes_done, total_bytes,
                    1 if bump_attempts else 0, error, time.time(),
                    1 if bump_attempts else 0,
                ),
            )

    def get_transfer(self, path: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM transfers WHERE path = ?", (path,)
            ).fetchone()
        return dict(row) if row else None

    def incomplete_transfers(self) -> List[Dict[str, Any]]:
        """Transfers left pending, active or failed - i.e. resumable work.

        ``active`` rows are the important case: they mean the process died
        mid-transfer, which no in-memory state would have survived.
        """
        with self._lock:
            return [
                dict(r)
                for r in self._conn.execute(
                    "SELECT * FROM transfers WHERE state != ? ORDER BY path",
                    (TRANSFER_DONE,),
                )
            ]

    def clear_transfers(self, only_done: bool = True) -> int:
        with self._write() as conn:
            if only_done:
                cur = conn.execute("DELETE FROM transfers WHERE state = ?", (TRANSFER_DONE,))
            else:
                cur = conn.execute("DELETE FROM transfers")
            return cur.rowcount

    # -- snapshots ------------------------------------------------------
    def create_snapshot(self, label: str, note: str = "") -> int:
        """Freeze the current local index under ``label``.

        Snapshots copy rows rather than referencing them: a snapshot whose
        contents changed when the live index changed would be worthless for
        "what did my drive look like in March?".
        """
        with self._write() as conn:
            cursor = conn.execute(
                "INSERT INTO snapshots(label, created_at, note) VALUES(?,?,?)",
                (label, time.time(), note),
            )
            snapshot_id = int(cursor.lastrowid)
            conn.execute(
                "INSERT INTO snapshot_entries(snapshot_id, path, kind, size, sha256) "
                "SELECT ?, path, kind, size, sha256 FROM local_items",
                (snapshot_id,),
            )
        return snapshot_id

    def list_snapshots(self) -> List[Dict[str, Any]]:
        with self._lock:
            return [
                dict(r)
                for r in self._conn.execute(
                    "SELECT s.*, ("
                    "  SELECT COUNT(*) FROM snapshot_entries e WHERE e.snapshot_id = s.id"
                    ") AS entry_count FROM snapshots s ORDER BY s.created_at DESC"
                )
            ]

    def get_snapshot(self, label: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM snapshots WHERE label = ?", (label,)
            ).fetchone()
        return dict(row) if row else None

    def delete_snapshot(self, label: str) -> bool:
        with self._write() as conn:
            cur = conn.execute("DELETE FROM snapshots WHERE label = ?", (label,))
            return cur.rowcount > 0

    def diff_snapshots(self, older: str, newer: str) -> List[DiffEntry]:
        """What changed between two snapshots, by digest rather than by size."""
        a, b = self.get_snapshot(older), self.get_snapshot(newer)
        if a is None:
            raise KeyError(f"no snapshot labelled {older!r}")
        if b is None:
            raise KeyError(f"no snapshot labelled {newer!r}")

        # Deliberately not FULL OUTER JOIN: SQLite only gained it in 3.39, and
        # Python 3.10 can ship an older amalgamation. A LEFT JOIN plus an
        # anti-join union is equivalent and works everywhere.
        with self._lock:
            rows = self._conn.execute(
                "SELECT o.path AS path, o.sha256 AS osha, n.sha256 AS nsha,"
                "       o.size AS osize, n.size AS nsize,"
                "       COALESCE(n.kind, o.kind) AS kind"
                "  FROM snapshot_entries o"
                "  LEFT JOIN snapshot_entries n"
                "    ON n.path = o.path AND n.snapshot_id = ?"
                " WHERE o.snapshot_id = ?"
                " UNION ALL "
                "SELECT n.path AS path, NULL AS osha, n.sha256 AS nsha,"
                "       NULL AS osize, n.size AS nsize, n.kind AS kind"
                "  FROM snapshot_entries n"
                " WHERE n.snapshot_id = ?"
                "   AND NOT EXISTS ("
                "     SELECT 1 FROM snapshot_entries o2"
                "      WHERE o2.snapshot_id = ? AND o2.path = n.path)"
                " ORDER BY path",
                (b["id"], a["id"], b["id"], a["id"]),
            ).fetchall()

        entries: List[DiffEntry] = []
        for row in rows:
            if row["osha"] is None and row["nsha"] is not None:
                status, detail = DIFF_NEW, f"added since {older}"
            elif row["nsha"] is None:
                status, detail = DIFF_LOCAL_ONLY, f"present in {older}, gone in {newer}"
            elif row["osha"] != row["nsha"]:
                status, detail = DIFF_CHANGED, "contents differ between snapshots"
            else:
                continue
            entries.append(
                DiffEntry(
                    path=row["path"], status=status, kind=row["kind"] or KIND_FILE,
                    remote_size=row["nsize"], local_size=row["osize"], detail=detail,
                )
            )
        return entries

    # -- migration ------------------------------------------------------
    def migrate_from_json(
        self,
        state_path: Optional[Path] = None,
        manifest_path: Optional[Path] = None,
    ) -> Dict[str, int]:
        """Import legacy ``.ifetch_state.json`` / ``.ifetch_manifest.json``.

        Non-destructive: the JSON files are read and left exactly as they were,
        so downgrading to an older iFetch still works. Runs at most once, which
        matters because a second import would resurrect entries the user has
        since deleted.
        """
        if self.get_meta("migrated_from_json") == "1":
            return {"state": 0, "manifest": 0, "skipped": 1}

        state_path = state_path or (self.root / ".ifetch_state.json")
        manifest_path = manifest_path or (self.root / ".ifetch_manifest.json")

        imported = {"state": 0, "manifest": 0, "skipped": 0}
        local: Dict[str, LocalItem] = {}
        remote: List[RemoteItem] = []

        for path, entry in _read_state(state_path).items():
            size = entry.get("local_size")
            local[path] = LocalItem(
                path=path,
                kind=KIND_PACKAGE if entry.get("status") == "completed_package" else KIND_FILE,
                size=size if isinstance(size, int) else entry.get("local_total_bytes"),
                mtime=entry.get("local_mtime"),
            )
            remote.append(
                RemoteItem(
                    path=path,
                    kind=KIND_PACKAGE if entry.get("status") == "completed_package" else KIND_FILE,
                    size=entry.get("remote_size"),
                    modified_token=entry.get("remote_modified"),
                )
            )
            imported["state"] += 1

        # The manifest is authoritative for digests, so it is applied second and
        # merges into (rather than replaces) whatever the state file provided.
        for path, entry in _read_manifest(manifest_path).items():
            previous = local.get(path)
            local[path] = LocalItem(
                path=path,
                kind=KIND_PACKAGE if entry.get("kind") == "package" else KIND_FILE,
                size=entry.get("size", previous.size if previous else None),
                mtime=entry.get("mtime", previous.mtime if previous else None),
                sha256=entry.get("sha256"),
            )
            imported["manifest"] += 1

        if local:
            self.record_local_many(local.values())
        if remote:
            self.record_remote_many(remote)

        self.set_meta("migrated_from_json", "1")
        self.set_meta("migrated_at", str(time.time()))
        return imported


def _read_state(path: Path) -> Dict[str, Dict[str, Any]]:
    """Parse a legacy sync-state file, tolerating anything malformed."""
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError, UnicodeDecodeError):
        return {}
    files = payload.get("files") if isinstance(payload, dict) else None
    if not isinstance(files, dict):
        return {}
    return {k: v for k, v in files.items() if isinstance(k, str) and isinstance(v, dict)}


def _read_manifest(path: Path) -> Dict[str, Dict[str, Any]]:
    """Parse a legacy manifest file, tolerating anything malformed."""
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError, UnicodeDecodeError):
        return {}
    body = payload.get("manifest") if isinstance(payload, dict) else None
    if not isinstance(body, dict):
        return {}
    files = body.get("files")
    if not isinstance(files, dict):
        return {}
    return {k: v for k, v in files.items() if isinstance(k, str) and isinstance(v, dict)}


def open_index(root: Path, migrate: bool = True) -> IndexStore:
    """Open (creating if needed) the index for a destination, migrating legacy JSON."""
    store = IndexStore(root)
    if migrate:
        store.migrate_from_json()
    return store
