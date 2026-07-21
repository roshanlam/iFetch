"""Version management utilities for iFetch.

This module keeps a persistent JSON map of every file that iFetch downloads
and stores *previous* versions in a hidden `.versions` directory next to the
original file tree.  It enables two key capabilities:

1.  **Delta downloads** – we can skip unchanged files quickly by consulting the
    stored checksum/version meta.
2.  **Rollback** – every time a file changes we rename the old copy into the
    `.versions` area so users can restore it later.

The restore half of (2) lives in :mod:`ifetch.restore`, which reads the
metadata written here.  On-disk layout::

    <root>/.ifetch_versions.json    {rel_path: [entry, entry, ...]}
    <root>/.versions/<rel_path>.v<N>_<YYYYmmddTHHMMSS>

Each metadata entry looks like::

    {
      "version": 3,                       # monotonically increasing, per path
      "checksum": "<sha256 hex>",         # checksum of the archived bytes
      "archived_path": "/abs/path",       # where the old copy now lives
      "timestamp": "20260721T134501",     # local time the copy was archived
      "epoch": 1784...,                   # same instant, seconds since epoch
      "size": 12345                       # bytes
    }

``epoch`` and ``size`` were added later; metadata written by older iFetch
releases lacks them and every reader in this codebase must cope with their
absence (see :func:`entry_epoch` / :func:`entry_size`).
"""

from __future__ import annotations

import hashlib
import json
import shutil
import time
import threading
import copy
from pathlib import Path
from typing import Dict, List, Any, Optional

TIMESTAMP_FORMAT = "%Y%m%dT%H%M%S"

_CHECKSUM_BLOCK = 1024 * 1024


def compute_checksum(file_path: Path) -> str:
    """Return the SHA-256 hex digest of *file_path*.

    Mirrors ``DownloadManager.calculate_checksum`` so checksums recorded by the
    downloader and checksums computed during a restore are comparable.
    """
    sha256 = hashlib.sha256()
    with Path(file_path).open("rb") as fh:
        for block in iter(lambda: fh.read(_CHECKSUM_BLOCK), b""):
            sha256.update(block)
    return sha256.hexdigest()


def parse_timestamp(value: Any) -> Optional[float]:
    """Best-effort conversion of a recorded timestamp into a POSIX epoch.

    Accepts the native ``%Y%m%dT%H%M%S`` form, a handful of ISO-ish variants
    and raw numbers.  Returns ``None`` when nothing can be made of it.
    """
    if value is None:
        return None
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)

    text = str(value).strip()
    if not text:
        return None

    for fmt in (
        TIMESTAMP_FORMAT,
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d",
        "%Y%m%d",
    ):
        try:
            return time.mktime(time.strptime(text, fmt))
        except ValueError:
            continue

    try:  # bare epoch given as a string
        return float(text)
    except ValueError:
        return None


def format_timestamp(epoch: Optional[float]) -> Optional[str]:
    """Render *epoch* using the on-disk timestamp format."""
    if epoch is None:
        return None
    return time.strftime(TIMESTAMP_FORMAT, time.localtime(epoch))


def entry_epoch(entry: Dict[str, Any]) -> Optional[float]:
    """Epoch of a metadata *entry*, falling back to its textual timestamp."""
    epoch = entry.get("epoch")
    if isinstance(epoch, (int, float)) and not isinstance(epoch, bool):
        return float(epoch)
    return parse_timestamp(entry.get("timestamp"))


def entry_size(entry: Dict[str, Any]) -> Optional[int]:
    """Size of a metadata *entry*, falling back to the archived file on disk."""
    size = entry.get("size")
    if isinstance(size, int) and not isinstance(size, bool):
        return size
    archived = entry.get("archived_path")
    if archived:
        try:
            return Path(archived).stat().st_size
        except OSError:
            return None
    return None


def load_metadata(meta_path: Path) -> Dict[str, List[Dict[str, Any]]]:
    """Read a ``.ifetch_versions.json`` file defensively.

    Missing, unreadable or corrupt files yield an empty mapping; entries that
    are not path -> list are dropped rather than blowing up a restore.
    """
    meta_path = Path(meta_path)
    if not meta_path.exists():
        return {}
    try:
        loaded = json.loads(meta_path.read_text())
    except (json.JSONDecodeError, OSError, UnicodeDecodeError):
        return {}
    if not isinstance(loaded, dict):
        return {}
    return {
        str(key): [v for v in value if isinstance(v, dict)]
        for key, value in loaded.items()
        if isinstance(value, list)
    }


def is_within(path: Path, parent: Path) -> bool:
    """True when *path* resolves to *parent* or something beneath it."""
    try:
        resolved = Path(path).resolve()
        parent_resolved = Path(parent).resolve()
    except OSError:
        return False
    if resolved == parent_resolved:
        return True
    try:
        resolved.relative_to(parent_resolved)
    except ValueError:
        return False
    return True


class VersionManager:
    """Maintain on-disk history of downloaded files."""

    META_FILENAME = ".ifetch_versions.json"
    VERSIONS_DIRNAME = ".versions"

    def __init__(self, root: Path):
        self.root = Path(root).resolve()
        self.meta_path = self.root / self.META_FILENAME
        self.versions_dir = self.root / self.VERSIONS_DIRNAME
        self.versions_dir.mkdir(parents=True, exist_ok=True)
        self._data: Dict[str, List[Dict[str, Any]]] = {}
        self._lock = threading.Lock()
        # Version numbers handed out but not yet committed to ``_data``.
        # Without this two concurrent ``record_version`` calls for the same
        # path can pick the same number and clobber each other's archive.
        self._reserved: Dict[str, int] = {}
        self._load()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    @staticmethod
    def key_for(rel_path: Any) -> str:
        """Normalise *rel_path* into the string used as a metadata key."""
        if isinstance(rel_path, Path):
            return rel_path.as_posix()
        return str(rel_path).replace("\\", "/")

    def latest_checksum(self, rel_path: Path) -> Optional[str]:
        key = self.key_for(rel_path)
        versions = self._data.get(key, [])
        return versions[-1]["checksum"] if versions else None

    def list_paths(self) -> List[str]:
        """Every path that has version metadata, sorted."""
        with self._lock:
            return sorted(self._data.keys())

    def versions_for(self, rel_path: Any) -> List[Dict[str, Any]]:
        """Copy of the recorded versions for *rel_path* (oldest first)."""
        key = self.key_for(rel_path)
        with self._lock:
            return copy.deepcopy(self._data.get(key, []))

    def record_version(
        self, rel_path: Path, checksum: str, src_file: Path
    ) -> Optional[Dict[str, Any]]:
        """Move *src_file* into versions dir and update metadata.

        Returns the recorded metadata entry, or ``None`` when nothing was
        archived (the move failed, or the destination would escape the
        versions directory).
        """
        key = self.key_for(rel_path)

        with self._lock:
            # Peek at existing versions *without* mutating state yet
            existing: List[Dict[str, Any]] | None = self._data.get(key)
            highest = max((v.get("version", 0) for v in existing), default=0) if existing else 0
            version_nr = max(highest, self._reserved.get(key, 0)) + 1
            self._reserved[key] = version_nr

        ts = time.strftime(TIMESTAMP_FORMAT)
        dest = self.versions_dir / f"{key}.v{version_nr}_{ts}"

        # Refuse to archive outside the versions directory: a crafted relative
        # path ("../../etc/passwd") must not let us write anywhere we like.
        if not is_within(dest.parent, self.versions_dir):
            return None

        try:
            size = Path(src_file).stat().st_size
        except OSError:
            size = None

        try:
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(src_file), dest)
        except (shutil.Error, OSError):
            # Move failed => do *not* touch metadata
            return None

        entry = {
            "version": version_nr,
            "checksum": checksum,
            "archived_path": str(dest),
            "timestamp": ts,
            "epoch": time.time(),
            "size": size,
        }

        with self._lock:
            # Ensure list exists now that move succeeded
            versions = self._data.setdefault(key, [])
            versions.append(entry)
        self._save()
        return entry

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _load(self):
        self._data = load_metadata(self.meta_path)

    def _save(self):
        try:
            with self._lock:
                # Deep copy to avoid "dictionary changed size during iteration"
                data_snapshot = copy.deepcopy(self._data)
            with self.meta_path.open("w") as fp:
                json.dump(data_snapshot, fp, indent=2)
        except (OSError, RuntimeError):
            pass
