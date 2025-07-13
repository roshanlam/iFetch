"""Version management utilities for iFetch.

This module keeps a persistent JSON map of every file that iFetch downloads
and stores *previous* versions in a hidden `.versions` directory next to the
original file tree.  It enables two key capabilities:

1.  **Delta downloads** – we can skip unchanged files quickly by consulting the
    stored checksum/version meta.
2.  **Rollback** – every time a file changes we rename the old copy into the
    `.versions` area so users can restore it later.
"""

from __future__ import annotations

import json
import shutil
import time
from pathlib import Path
from typing import Dict, List, Any, Optional


class VersionManager:
    """Maintain on-disk history of downloaded files."""

    META_FILENAME = ".ifetch_versions.json"
    VERSIONS_DIRNAME = ".versions"

    def __init__(self, root: Path):
        self.root = root.resolve()
        self.meta_path = self.root / self.META_FILENAME
        self.versions_dir = self.root / self.VERSIONS_DIRNAME
        self.versions_dir.mkdir(parents=True, exist_ok=True)
        self._data: Dict[str, List[Dict[str, Any]]] = {}
        self._load()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def latest_checksum(self, rel_path: Path) -> Optional[str]:
        key = str(rel_path.as_posix())
        versions = self._data.get(key, [])
        return versions[-1]["checksum"] if versions else None

    def record_version(self, rel_path: Path, checksum: str, src_file: Path) -> None:
        """Move *src_file* into versions dir and update metadata."""
        key = str(rel_path.as_posix())

        # Peek at existing versions *without* mutating state yet
        existing: List[Dict[str, Any]] | None = self._data.get(key)
        version_nr = (existing[-1]["version"] + 1) if existing else 1

        ts = time.strftime("%Y%m%dT%H%M%S")
        dest = self.versions_dir / f"{rel_path.as_posix()}.v{version_nr}_{ts}"
        dest.parent.mkdir(parents=True, exist_ok=True)
        try:
            shutil.move(str(src_file), dest)
        except (shutil.Error, OSError):
            # Move failed => do *not* touch metadata
            return

        # Ensure list exists now that move succeeded
        versions = self._data.setdefault(key, [])

        versions.append(
            {
                "version": version_nr,
                "checksum": checksum,
                "archived_path": str(dest),
                "timestamp": ts,
            }
        )
        self._save()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _load(self):
        if self.meta_path.exists():
            try:
                self._data = json.loads(self.meta_path.read_text())
            except json.JSONDecodeError:
                self._data = {}

    def _save(self):
        try:
            with self.meta_path.open("w") as fp:
                json.dump(self._data, fp, indent=2)
        except OSError:
            pass 