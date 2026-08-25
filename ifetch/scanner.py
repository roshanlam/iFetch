"""Walking iCloud Drive and the local disk **without transferring anything**.

Until now, traversal and transfer were the same pass: iFetch discovered a file
and downloaded it in one motion. That is efficient for a plain download and
useless for everything a recovery tool needs to say *before* touching the
network - how much there is, how long it will take, what would be overwritten,
whether the disk is even big enough.

This module separates the two. :class:`RemoteScanner` walks the drive tree
reading only the metadata Apple already returned in its folder listings, and
records it in the index. :class:`LocalScanner` does the same for the
destination directory. Nothing here opens a download stream, creates a file, or
modifies anything - both scanners are strictly read-only.

Cost note
---------
A scan is *listing* traffic only: one request per directory, not per file, and
no bytes of file content. On a large drive that is minutes rather than hours,
and it is what buys an accurate plan instead of a guess.
"""

from __future__ import annotations

import json
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from fnmatch import fnmatch
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple

from .index import (
    KIND_DIR,
    KIND_FILE,
    KIND_PACKAGE,
    IndexStore,
    LocalItem,
    RemoteItem,
)
from .logger import setup_logging
from .manifest import ARTIFACT_DIRS, ARTIFACT_NAMES, MANIFEST_FILENAME, Manifest
from .naming import NORMALIZE_PRESERVE, DirectorySanitizer
from .packages import is_package_name
from .utils import can_read_file

#: Files iFetch writes into the destination itself. They are not part of the
#: mirror and must never be reported as content.
_LOCAL_ARTIFACTS = set(ARTIFACT_NAMES) | {".ifetch_index.db", ".DS_Store"}
_LOCAL_ARTIFACT_DIRS = set(ARTIFACT_DIRS)
_LOCAL_ARTIFACT_SUFFIXES = (".temp", ".download", ".db-wal", ".db-shm", ".tmp")


@dataclass
class ScanStats:
    """What a scan found, for progress and for the summary line."""

    files: int = 0
    directories: int = 0
    packages: int = 0
    total_bytes: int = 0
    errors: List[Dict[str, str]] = field(default_factory=list)
    duration_seconds: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "files": self.files,
            "directories": self.directories,
            "packages": self.packages,
            "total_bytes": self.total_bytes,
            "errors": list(self.errors),
            "duration_seconds": round(self.duration_seconds, 3),
        }


def _remote_metadata(item: Any) -> Tuple[Optional[int], Optional[str]]:
    """Local import shim so scanner does not drag in the whole downloader."""
    from .downloader import remote_metadata

    return remote_metadata(item)


class RemoteScanner:
    """Walk an iCloud Drive subtree, recording metadata only.

    Directories are listed concurrently because listing latency, not
    computation, is the whole cost of a scan. A failure on one subtree is
    recorded and contained: a single unreadable folder must not cost the user
    the inventory of everything else.
    """

    def __init__(
        self,
        store: IndexStore,
        downloader: Any = None,
        max_workers: int = 8,
        include_patterns: Optional[Sequence[str]] = None,
        exclude_patterns: Optional[Sequence[str]] = None,
        portable_names: bool = False,
        normalize_names: str = NORMALIZE_PRESERVE,
        progress: Optional[Callable[[ScanStats], None]] = None,
        logger: Optional[Any] = None,
    ):
        self.store = store
        self.downloader = downloader
        self.max_workers = max(1, int(max_workers))
        self.include_patterns = list(include_patterns or [])
        self.exclude_patterns = list(exclude_patterns or [])
        self.portable_names = portable_names
        self.normalize_names = normalize_names
        self.progress = progress
        self.logger = logger or setup_logging()

        self.stats = ScanStats()
        self._lock = threading.Lock()
        self._batch: List[RemoteItem] = []

    # -- filtering ------------------------------------------------------
    def _should_include(self, rel_path: str, is_dir: bool) -> bool:
        for pattern in self.exclude_patterns:
            if fnmatch(rel_path, pattern):
                return False
        if is_dir:
            # Directories stay traversable or an include glob like "*.pdf"
            # would prune the very folders holding the matches.
            return True
        if not self.include_patterns:
            return True
        return any(fnmatch(rel_path, pattern) for pattern in self.include_patterns)

    # -- recording ------------------------------------------------------
    def _emit(self, item: RemoteItem, scan_id: int) -> None:
        """Buffer a row, flushing in batches.

        Batching matters: a per-row transaction on a 100k-file drive would make
        the database, not the network, the bottleneck.
        """
        flush: Optional[List[RemoteItem]] = None
        with self._lock:
            self._batch.append(item)
            if item.kind == KIND_DIR:
                self.stats.directories += 1
            else:
                self.stats.files += 1
                if item.kind == KIND_PACKAGE:
                    self.stats.packages += 1
                self.stats.total_bytes += item.size or 0
            if len(self._batch) >= 500:
                flush, self._batch = self._batch, []

        if flush:
            self.store.record_remote_many(flush, scan_id=scan_id)
        if self.progress is not None:
            self.progress(self.stats)

    def _flush(self, scan_id: int) -> None:
        with self._lock:
            pending, self._batch = self._batch, []
        if pending:
            self.store.record_remote_many(pending, scan_id=scan_id)

    def _record_error(self, path: str, error: Exception) -> None:
        with self._lock:
            self.stats.errors.append({"path": path, "error": str(error)})
        self.logger.error(json.dumps({
            "event": "scan_listing_error", "path": path, "error": str(error),
        }))

    # -- traversal ------------------------------------------------------
    def scan(self, icloud_path: str, node: Any = None) -> ScanStats:
        """Walk ``icloud_path`` and record everything found. Returns stats."""
        started = time.time()
        if node is None:
            if self.downloader is None:
                raise ValueError("RemoteScanner needs a downloader or a node to scan")
            if getattr(self.downloader, "api", None) is None:
                self.downloader.authenticate()
            node = self.downloader.get_drive_item(icloud_path)

        scan_id = self.store.begin_scan(icloud_path)
        self.store.clear_remote()

        try:
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                self._walk(node, "", scan_id, executor)
        finally:
            self._flush(scan_id)
            self.stats.duration_seconds = time.time() - started
            # The listing failures go into the scan row, not just into the
            # stats object that dies with this process. A folder that failed to
            # list contributed no rows, and later readers cannot tell those
            # missing rows from deleted files unless the failure was recorded.
            self.store.finish_scan(scan_id, errors=self.stats.errors)

        self.logger.info(json.dumps({
            "event": "remote_scan_completed",
            "icloud_path": icloud_path,
            **self.stats.to_dict(),
        }))
        return self.stats

    def _walk(self, node: Any, prefix: str, scan_id: int, executor: ThreadPoolExecutor) -> None:
        if can_read_file(node):
            self._record_file(node, prefix or str(getattr(node, "name", "") or ""), scan_id)
            return

        if not hasattr(node, "dir"):
            return

        try:
            contents = node.dir()
        except Exception as exc:
            self._record_error(prefix or "/", exc)
            return

        if not contents:
            return

        names = list(contents.keys()) if hasattr(contents, "keys") else list(contents)
        sanitizer = DirectorySanitizer(
            portable=self.portable_names, normalize=self.normalize_names
        )

        children: List[Tuple[Any, str]] = []
        for name in names:
            local_name, _ = sanitizer.assign(name)
            child_rel = f"{prefix}/{local_name}" if prefix else local_name
            try:
                child = node[name]
            except Exception as exc:
                self._record_error(child_rel, exc)
                continue
            children.append((child, child_rel))

        futures = []
        for child, child_rel in children:
            if can_read_file(child):
                if self._should_include(child_rel, is_dir=False):
                    self._record_file(child, child_rel, scan_id)
                continue

            if not self._should_include(child_rel, is_dir=True):
                continue
            self._emit(RemoteItem(path=child_rel, kind=KIND_DIR), scan_id)
            futures.append(executor.submit(self._walk, child, child_rel, scan_id, executor))

        for future in as_completed(futures):
            try:
                future.result()
            except Exception as exc:  # pragma: no cover - defensive
                self._record_error(prefix or "/", exc)

    def _record_file(self, node: Any, rel_path: str, scan_id: int) -> None:
        size, token = _remote_metadata(node)
        data = getattr(node, "data", None) or {}

        # A package cannot be confirmed without downloading it (the payload has
        # to actually be a ZIP), so at scan time the extension is the only
        # available signal. Marking it here keeps the planner from comparing a
        # bundle by size, which would always look "changed".
        kind = KIND_PACKAGE if is_package_name(rel_path) else KIND_FILE

        self._emit(
            RemoteItem(
                path=rel_path,
                kind=kind,
                size=size,
                modified_token=token,
                etag=data.get("etag") if isinstance(data, dict) else None,
                docwsid=data.get("docwsid") if isinstance(data, dict) else None,
                drivewsid=data.get("drivewsid") if isinstance(data, dict) else None,
                zone=data.get("zone") if isinstance(data, dict) else None,
                share_id=data.get("shareID") if isinstance(data, dict) else None,
            ),
            scan_id,
        )


def is_local_artifact(rel_path: str) -> bool:
    """True for files iFetch writes into the destination itself."""
    parts = Path(rel_path).parts
    if not parts:
        return True
    if parts[0] in _LOCAL_ARTIFACT_DIRS:
        return True
    name = parts[-1]
    if name in _LOCAL_ARTIFACTS:
        return True
    return name.endswith(_LOCAL_ARTIFACT_SUFFIXES)


class LocalScanner:
    """Index what is actually on disk in the destination.

    Digests are reused from the manifest wherever size and mtime still agree
    with what was recorded, so a rescan of an untouched mirror costs one
    ``stat`` per file rather than re-hashing gigabytes. ``rehash=True`` forces
    a full re-read, which is what an integrity audit wants and what a routine
    plan does not.
    """

    def __init__(
        self,
        store: IndexStore,
        root: Path,
        manifest: Optional[Manifest] = None,
        rehash: bool = False,
        progress: Optional[Callable[[ScanStats], None]] = None,
        logger: Optional[Any] = None,
    ):
        self.store = store
        self.root = Path(root).resolve()
        self.manifest = manifest
        self.rehash = rehash
        self.progress = progress
        self.logger = logger or setup_logging()
        self.stats = ScanStats()

    def scan(self, package_paths: Optional[Iterable[str]] = None) -> ScanStats:
        """Walk the destination and record every file. Read-only."""
        started = time.time()
        packages = set(package_paths or self._known_packages())
        batch: List[LocalItem] = []

        for current, dirs, files in os.walk(self.root):
            dirs[:] = [d for d in dirs if d not in _LOCAL_ARTIFACT_DIRS]

            current_path = Path(current)
            rel_dir = self._relative(current_path)

            # An expanded package is one item, not a directory of items; do not
            # descend into it or every member becomes a phantom file.
            if rel_dir and rel_dir in packages:
                dirs[:] = []
                item = self._package_item(current_path, rel_dir)
                if item is not None:
                    batch.append(item)
                    self.stats.packages += 1
                    self.stats.total_bytes += item.size or 0
                continue

            pruned = []
            for name in dirs:
                child_rel = self._relative(current_path / name)
                if child_rel in packages:
                    item = self._package_item(current_path / name, child_rel)
                    if item is not None:
                        batch.append(item)
                        self.stats.packages += 1
                        self.stats.total_bytes += item.size or 0
                    continue
                pruned.append(name)
            dirs[:] = pruned
            self.stats.directories += len(pruned)

            for name in files:
                rel = self._relative(current_path / name)
                if not rel or is_local_artifact(rel):
                    continue
                item = self._file_item(current_path / name, rel)
                if item is None:
                    continue
                batch.append(item)
                self.stats.files += 1
                self.stats.total_bytes += item.size or 0

                if len(batch) >= 500:
                    self.store.record_local_many(batch)
                    batch = []
                    if self.progress is not None:
                        self.progress(self.stats)

        if batch:
            self.store.record_local_many(batch)

        self.stats.duration_seconds = time.time() - started
        self.logger.info(json.dumps({
            "event": "local_scan_completed",
            "root": str(self.root),
            **self.stats.to_dict(),
        }))
        return self.stats

    # -- helpers --------------------------------------------------------
    def _relative(self, path: Path) -> str:
        try:
            rel = path.resolve().relative_to(self.root).as_posix()
        except ValueError:
            return ""
        return "" if rel == "." else rel

    def _known_packages(self) -> List[str]:
        """Paths previously recorded as expanded package bundles."""
        return [
            row["path"]
            for row in self.store.iter_local()
            if row["kind"] == KIND_PACKAGE
        ]

    def _file_item(self, path: Path, rel: str) -> Optional[LocalItem]:
        try:
            stat = path.stat()
        except OSError as exc:
            self.stats.errors.append({"path": rel, "error": str(exc)})
            return None

        digest = None if self.rehash else self._reuse_digest(rel, stat)
        if digest is None:
            try:
                from .manifest import sha256_file

                digest = sha256_file(path) if self.rehash else None
            except OSError as exc:
                self.stats.errors.append({"path": rel, "error": str(exc)})
                digest = None

        return LocalItem(
            path=rel, kind=KIND_FILE, size=stat.st_size,
            mtime=stat.st_mtime, sha256=digest,
        )

    def _package_item(self, path: Path, rel: str) -> Optional[LocalItem]:
        from .packages import directory_fingerprint

        try:
            count, total = directory_fingerprint(path)
        except OSError as exc:
            self.stats.errors.append({"path": rel, "error": str(exc)})
            return None

        digest = None
        if self.rehash:
            try:
                from .manifest import sha256_directory

                digest = sha256_directory(path)
            except OSError:
                digest = None
        else:
            digest = self._manifest_digest(rel)

        del count
        return LocalItem(path=rel, kind=KIND_PACKAGE, size=total, sha256=digest)

    def _manifest_digest(self, rel: str) -> Optional[str]:
        if self.manifest is None:
            return None
        entry = self.manifest.get(self.root / rel)
        return entry.get("sha256") if entry else None

    def _reuse_digest(self, rel: str, stat: os.stat_result) -> Optional[str]:
        """Reuse a recorded digest only when the file demonstrably has not moved.

        Size *and* mtime must both still match what was recorded. That is not
        proof the contents are unchanged - which is exactly why an integrity
        audit passes ``rehash=True`` - but it is enough for a plan, and it
        avoids re-hashing a terabyte to answer "what needs downloading?".
        """
        if self.manifest is None:
            return None
        entry = self.manifest.get(self.root / rel)
        if not entry:
            return None
        if entry.get("size") != stat.st_size:
            return None
        recorded_mtime = entry.get("mtime")
        if isinstance(recorded_mtime, (int, float)) and abs(recorded_mtime - stat.st_mtime) > 1e-6:
            return None
        return entry.get("sha256")
