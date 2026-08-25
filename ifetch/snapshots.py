"""Dated snapshots of a mirror, and restoring from them.

The existing ``.versions`` mechanism archives a file's previous copy whenever a
download is about to overwrite it. That answers "give me back this one file",
and it is useless for the question people actually ask after something goes
wrong:

    *What did my drive look like last month, and what has changed since?*

A snapshot answers that. It records the state of the whole mirror under a label
- every path, size and digest - so two points in time can be compared, and so a
file that has since been damaged or deleted can be identified and recovered.

What a snapshot is and is not
-----------------------------
A snapshot is **metadata, not a copy**. Recording one is instant and costs
kilobytes regardless of how large the mirror is. That is what makes taking one
before every sync practical.

The consequence is that a snapshot alone cannot restore contents. Recovery
draws from two places:

* ``.versions`` - real archived bytes, held for files that a download replaced;
* iCloud itself - for anything still present remotely.

So :func:`plan_restore` never claims it can restore a file whose bytes exist
nowhere. It reports that file as unrecoverable, by name, rather than silently
omitting it from the plan.
"""

from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence

from .index import (
    DIFF_CHANGED,
    DIFF_LOCAL_ONLY,
    DIFF_NEW,
    IndexStore,
)
from .manifest import sha256_directory, sha256_file
from .render import human_bytes, key_values, plural, rule, table
from .versioning import VersionManager

#: Labels become filenames and command arguments, so they are constrained.
_LABEL_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")


class SnapshotError(Exception):
    """A snapshot could not be created, found, or restored from."""


def validate_label(label: str) -> str:
    """Check a snapshot label, or raise with what is allowed.

    Labels are typed on a command line and printed in reports, so they are kept
    to characters that need no quoting and cannot be confused for a path.
    """
    label = (label or "").strip()
    if not _LABEL_PATTERN.match(label):
        raise SnapshotError(
            f"invalid snapshot label {label!r}. Use letters, digits, dot, dash "
            "or underscore, starting with a letter or digit, up to 64 characters."
        )
    return label


def auto_label(prefix: str = "auto", now: Optional[float] = None) -> str:
    """A timestamped label, for snapshots taken automatically before a sync."""
    stamp = time.strftime("%Y%m%d-%H%M%S", time.localtime(now or time.time()))
    return f"{prefix}-{stamp}"


# ---------------------------------------------------------------------------
# Creating
# ---------------------------------------------------------------------------

def create_snapshot(
    store: IndexStore,
    label: str,
    note: str = "",
    replace: bool = False,
) -> Dict[str, Any]:
    """Freeze the current local index under ``label``.

    Requires the index to have been populated by a local scan; snapshotting an
    empty index would record "the mirror was empty", which is a claim, not an
    absence of one.
    """
    label = validate_label(label)

    if store.get_snapshot(label) is not None:
        if not replace:
            raise SnapshotError(
                f"a snapshot named {label!r} already exists. Choose another name "
                "or pass --replace."
            )
        store.delete_snapshot(label)

    if store.local_count() == 0:
        raise SnapshotError(
            "the local index is empty, so a snapshot would record an empty "
            "mirror. Run 'ifetch plan' or 'ifetch snapshot create' after a sync "
            "so the index reflects what is on disk."
        )

    without_digest = sum(1 for row in store.iter_local() if not row["sha256"])
    if without_digest and without_digest == store.local_count():
        raise SnapshotError(
            "no file in the index has a recorded digest, so this snapshot could "
            "only ever detect added and removed files - never a modified or "
            "corrupted one, and it could not drive a restore. Re-run without "
            "--fast so the files are hashed."
        )

    snapshot_id = store.create_snapshot(label, note=note)
    return {
        "id": snapshot_id,
        "label": label,
        "entries": store.local_count(),
        "entries_without_digest": without_digest,
        "note": note,
    }


# ---------------------------------------------------------------------------
# Restoring
# ---------------------------------------------------------------------------

SOURCE_VERSIONS = "versions"
SOURCE_ICLOUD = "icloud"
SOURCE_NONE = "unrecoverable"

RESTORE_MISSING = "missing"        # in the snapshot, absent now
RESTORE_MODIFIED = "modified"      # present now, different contents
RESTORE_MATCHES = "matches"        # already as the snapshot recorded


@dataclass
class RestoreCandidate:
    """One file that differs from the snapshot, and where it could come from."""

    path: str
    state: str
    source: str
    size: Optional[int] = None
    expected_sha256: Optional[str] = None
    current_sha256: Optional[str] = None
    detail: str = ""

    @property
    def recoverable(self) -> bool:
        return self.source != SOURCE_NONE

    def to_dict(self) -> Dict[str, Any]:
        return {
            "path": self.path,
            "state": self.state,
            "source": self.source,
            "size": self.size,
            "expected_sha256": self.expected_sha256,
            "current_sha256": self.current_sha256,
            "recoverable": self.recoverable,
            "detail": self.detail,
        }


@dataclass
class RestorePlan:
    """What restoring to a snapshot would involve. Nothing is changed by building it."""

    label: str
    root: str
    candidates: List[RestoreCandidate] = field(default_factory=list)

    @property
    def recoverable(self) -> List[RestoreCandidate]:
        return [c for c in self.candidates if c.recoverable]

    @property
    def unrecoverable(self) -> List[RestoreCandidate]:
        return [c for c in self.candidates if not c.recoverable]

    def by_source(self, source: str) -> List[RestoreCandidate]:
        return [c for c in self.candidates if c.source == source]

    def counts(self) -> Dict[str, int]:
        tally: Dict[str, int] = {}
        for candidate in self.candidates:
            tally[candidate.state] = tally.get(candidate.state, 0) + 1
        return tally

    def to_dict(self) -> Dict[str, Any]:
        return {
            "label": self.label,
            "root": self.root,
            "counts": self.counts(),
            "recoverable": len(self.recoverable),
            "unrecoverable": len(self.unrecoverable),
            "candidates": [c.to_dict() for c in self.candidates],
        }


def plan_restore(
    store: IndexStore,
    root: Path,
    label: str,
    version_manager: Optional[VersionManager] = None,
) -> RestorePlan:
    """Work out what it would take to bring the mirror back to ``label``.

    Purely analytical: reads the snapshot, hashes what is on disk now, and asks
    for each difference whether the recorded bytes still exist anywhere. It
    changes nothing.
    """
    root = Path(root).resolve()
    snapshot = store.get_snapshot(label)
    if snapshot is None:
        raise SnapshotError(f"no snapshot named {label!r}")

    plan = RestorePlan(label=label, root=str(root))
    versions = version_manager or VersionManager(root)
    remote_paths = {row["path"] for row in store.iter_remote()}

    for entry in _snapshot_entries(store, snapshot["id"]):
        path = entry["path"]
        target = root / path
        expected = entry["sha256"]

        current = _current_digest(target, entry["kind"])
        if current is not None and expected is not None and current == expected:
            continue  # Already as recorded; nothing to do.

        state = RESTORE_MISSING if current is None else RESTORE_MODIFIED
        source, detail = _find_source(path, expected, versions, remote_paths)

        plan.candidates.append(
            RestoreCandidate(
                path=path, state=state, source=source, size=entry["size"],
                expected_sha256=expected, current_sha256=current, detail=detail,
            )
        )

    plan.candidates.sort(key=lambda c: c.path)
    return plan


def _snapshot_entries(store: IndexStore, snapshot_id: int) -> List[Dict[str, Any]]:
    with store._lock:
        return [
            dict(row)
            for row in store._conn.execute(
                "SELECT path, kind, size, sha256 FROM snapshot_entries "
                "WHERE snapshot_id = ? ORDER BY path",
                (snapshot_id,),
            )
        ]


def _current_digest(target: Path, kind: str) -> Optional[str]:
    """Hash what is at ``target`` now, or ``None`` if nothing is."""
    try:
        if target.is_dir():
            return sha256_directory(target)
        if target.is_file():
            return sha256_file(target)
    except OSError:
        return None
    return None


def _find_source(
    path: str,
    expected: Optional[str],
    versions: VersionManager,
    remote_paths: Iterable[str],
) -> tuple:
    """Where could this file's recorded contents come from?

    ``.versions`` is preferred over iCloud: it holds the *exact* bytes the
    snapshot recorded, whereas iCloud holds whatever is there now, which may
    itself be the damaged copy.
    """
    if expected:
        for version in versions.versions_for(Path(path)):
            if version.get("checksum") == expected:
                archived = version.get("archived_path")
                if archived and Path(archived).exists():
                    return SOURCE_VERSIONS, (
                        "exact bytes are archived in .versions from "
                        f"{version.get('timestamp', 'an earlier run')}"
                    )

    if path in set(remote_paths):
        return SOURCE_ICLOUD, (
            "still present in iCloud - re-download with 'ifetch'. Note this "
            "fetches the current remote copy, which may differ from the snapshot."
        )

    return SOURCE_NONE, (
        "no archived copy in .versions and not in the latest iCloud scan; "
        "these bytes are not recoverable by iFetch"
    )


# ---------------------------------------------------------------------------
# Executing a restore
# ---------------------------------------------------------------------------

@dataclass
class RestoreOutcome:
    path: str
    status: str
    detail: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {"path": self.path, "status": self.status, "detail": self.detail}


def apply_restore(
    plan: RestorePlan,
    root: Path,
    version_manager: Optional[VersionManager] = None,
    dry_run: bool = True,
) -> List[RestoreOutcome]:
    """Restore the files whose exact bytes are archived in ``.versions``.

    Only ``.versions`` sources are applied. Files that would have to come from
    iCloud are left for a normal download, because fetching them is a network
    operation with its own failure modes and its own flags - silently mixing
    that into a restore would make an offline, reversible operation into
    something neither.

    Before overwriting anything, the current copy is itself archived, so a
    restore is undoable.
    """
    root = Path(root).resolve()
    versions = version_manager or VersionManager(root)
    outcomes: List[RestoreOutcome] = []

    for candidate in plan.by_source(SOURCE_VERSIONS):
        target = root / candidate.path
        archived = _archived_path(versions, candidate)

        if archived is None:
            outcomes.append(RestoreOutcome(
                candidate.path, "failed",
                "the archived copy referenced by the snapshot is gone",
            ))
            continue

        if dry_run:
            outcomes.append(RestoreOutcome(
                candidate.path, "would_restore", f"from {archived}",
            ))
            continue

        try:
            if target.exists():
                # Archive what we are about to replace, so this is reversible.
                try:
                    versions.record_version(
                        Path(candidate.path), sha256_file(target), target
                    )
                except Exception:
                    pass
            target.parent.mkdir(parents=True, exist_ok=True)
            _copy_atomic(archived, target)
            outcomes.append(RestoreOutcome(candidate.path, "restored", f"from {archived}"))
        except OSError as exc:
            outcomes.append(RestoreOutcome(candidate.path, "failed", str(exc)))

    return outcomes


def _archived_path(versions: VersionManager, candidate: RestoreCandidate) -> Optional[Path]:
    for version in versions.versions_for(Path(candidate.path)):
        if version.get("checksum") != candidate.expected_sha256:
            continue
        archived = version.get("archived_path")
        if archived and Path(archived).exists():
            return Path(archived)
    return None


def _copy_atomic(source: Path, target: Path) -> None:
    """Copy via a temporary file so an interrupted restore cannot truncate."""
    import shutil

    temp = target.with_suffix(target.suffix + ".ifetch-restore")
    try:
        shutil.copy2(source, temp)
        temp.replace(target)
    finally:
        if temp.exists():
            try:
                temp.unlink()
            except OSError:
                pass


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------

def render_snapshot_list(snapshots: Sequence[Dict[str, Any]]) -> str:
    if not snapshots:
        return "No snapshots yet. Create one with 'ifetch snapshot create <label>'."

    rows = [
        [
            s["label"],
            time.strftime("%Y-%m-%d %H:%M", time.localtime(s["created_at"])),
            f"{s['entry_count']:,}",
            (s.get("note") or "")[:40],
        ]
        for s in snapshots
    ]
    return table(["label", "created", "files", "note"], rows, align=["<", "<", ">", "<"])


def render_snapshot_diff(entries: Sequence[Any], older: str, newer: str, show: int = 40) -> str:
    lines = [
        rule("="),
        f"Changes between snapshot '{older}' and '{newer}'",
        rule("="),
    ]
    if not entries:
        lines.append("No differences - the two snapshots are identical.")
        lines.append(rule("="))
        return "\n".join(lines)

    labels = {DIFF_NEW: "added", DIFF_LOCAL_ONLY: "removed", DIFF_CHANGED: "changed"}
    counts: Dict[str, int] = {}
    for entry in entries:
        counts[entry.status] = counts.get(entry.status, 0) + 1

    lines.append(key_values([
        (labels.get(status, status).capitalize(), f"{count:,}")
        for status, count in sorted(counts.items())
    ]))
    lines.append("")
    lines.append(table(
        ["change", "size", "path"],
        [[labels.get(e.status, e.status), human_bytes(e.remote_size or e.local_size), e.path]
         for e in entries[:show]],
        align=["<", ">", "<"],
    ))
    if len(entries) > show:
        lines.append(f"  ... and {len(entries) - show:,} more")
    lines.append(rule("="))
    return "\n".join(lines)


def render_restore_plan(plan: RestorePlan, show: int = 40) -> str:
    counts = plan.counts()
    lines = [
        rule("="),
        f"Restore plan for snapshot '{plan.label}'",
        rule("="),
        key_values([
            ("Local path", plan.root),
            ("Missing now", f"{counts.get(RESTORE_MISSING, 0):,}"),
            ("Modified since", f"{counts.get(RESTORE_MODIFIED, 0):,}"),
            ("Recoverable", f"{len(plan.recoverable):,}"),
            ("Not recoverable", f"{len(plan.unrecoverable):,}"),
        ]),
        "",
    ]

    if not plan.candidates:
        lines.append(f"The mirror already matches snapshot '{plan.label}'.")
        lines.append(rule("="))
        return "\n".join(lines)

    from_versions = plan.by_source(SOURCE_VERSIONS)
    from_icloud = plan.by_source(SOURCE_ICLOUD)

    if from_versions:
        lines.append(
            f"{len(from_versions):,} {plural(len(from_versions), 'file')} can be "
            "restored offline from .versions (exact recorded bytes):"
        )
        lines.append(table(
            ["state", "size", "path"],
            [[c.state, human_bytes(c.size), c.path] for c in from_versions[:show]],
            align=["<", ">", "<"],
        ))
        lines.append("")

    if from_icloud:
        lines.append(
            f"{len(from_icloud):,} {plural(len(from_icloud), 'file')} would have to "
            "be re-downloaded from iCloud (which returns the current remote copy, "
            "not necessarily the snapshot's):"
        )
        lines.append(table(
            ["state", "size", "path"],
            [[c.state, human_bytes(c.size), c.path] for c in from_icloud[:show]],
            align=["<", ">", "<"],
        ))
        lines.append("")

    if plan.unrecoverable:
        lines.append(
            f"! {len(plan.unrecoverable):,} {plural(len(plan.unrecoverable), 'file')} "
            "cannot be recovered - no archived copy and not in the latest iCloud scan:"
        )
        lines.append(table(
            ["state", "size", "path"],
            [[c.state, human_bytes(c.size), c.path] for c in plan.unrecoverable[:show]],
            align=["<", ">", "<"],
        ))
        lines.append("")

    lines.append(rule("="))
    return "\n".join(lines)
