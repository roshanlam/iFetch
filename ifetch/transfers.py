"""The transfer journal: what was in flight when the process died.

Why this exists
---------------
iFetch already resumes a single interrupted file. :class:`~ifetch.tracker.
DownloadTracker` writes a byte position beside the ``.temp`` file, and the next
run trims the ranges it has already fetched. That part works and this module
does not replace it.

What it could not do is answer a question one level up: **what was left
unfinished?** The evidence was scattered across the destination tree as
``.temp``/``.download`` pairs, with no way to enumerate them short of walking
everything, and the record of what *failed* lived only in memory - the summary
report is written after ``download()`` returns, so a run that is killed takes
its own failure list with it. The practical consequences:

* resuming meant re-running the whole command, which re-lists every folder in
  the drive to rediscover the three files that did not finish;
* a file that fails on every single run does so silently, because nothing
  counts attempts across runs;
* partial artifacts accumulate in a mirror with nothing able to find them.

The ``transfers`` table was built for this and had no callers. This module is
the caller.

Design notes
------------
*The journal is optional.* Every method tolerates a missing store, and the
downloader treats a journal write exactly as it treats a manifest write: useful
bookkeeping that must never be the reason a download fails. A mirror with no
index behaves precisely as it did before.

*It records skips too, not just the hard parts.* A journal that only holds
failures cannot tell "this run finished and had three failures" from "this run
died three files in". The distinction is the whole point.

*Nothing here decides a file is intact.* :func:`build_repair_report` reports
that a digest disagrees with the manifest; it does not repair by guessing which
side is right. Re-downloading is the only honest answer and it is what
``--apply`` queues.
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence

from .index import (
    TRANSFER_ACTIVE,
    TRANSFER_DONE,
    TRANSFER_FAILED,
    TRANSFER_PENDING,
    IndexStore,
)
from .render import colour, human_bytes, key_values, plural, rule, supports_colour, table

#: Suffixes iFetch leaves beside a file while a transfer is in progress.
TEMP_SUFFIX = ".temp"
PROGRESS_SUFFIX = ".download"


class TransferJournal:
    """Durable per-file transfer state for one destination directory.

    Thin on purpose: it owns the vocabulary the downloader speaks (a transfer
    begins, makes progress, completes or fails) and keeps SQL out of the
    download path. Every method swallows storage errors - the caller is in the
    middle of a network transfer and has better things to fail on.
    """

    def __init__(self, store: Optional[IndexStore], root: Optional[Path] = None):
        self.store = store
        self.root = Path(root).resolve() if root is not None else None

    # -- keys -----------------------------------------------------------
    def key_for(self, local_path: Path) -> str:
        """POSIX-relative key, matching how every other table stores a path."""
        target = Path(local_path)
        if self.root is None:
            return target.as_posix()
        try:
            return target.resolve().relative_to(self.root).as_posix()
        except (ValueError, OSError):
            return target.as_posix()

    @property
    def enabled(self) -> bool:
        return self.store is not None

    # -- recording ------------------------------------------------------
    def _set(self, path: Path, state: str, **kwargs: Any) -> None:
        if self.store is None:
            return
        try:
            self.store.set_transfer(self.key_for(path), state, **kwargs)
        except Exception:
            # Bookkeeping must never be the reason a download fails.
            pass

    def begin(
        self,
        local_path: Path,
        total_bytes: Optional[int] = None,
        remote_path: Optional[str] = None,
        resume_from: int = 0,
    ) -> None:
        """Mark a transfer as running, before the first byte is requested.

        ``attempts`` is bumped here rather than on failure: a process that is
        killed never reaches a failure handler, and an attempt that left no
        trace is exactly the one worth counting.
        """
        self._set(
            local_path, TRANSFER_ACTIVE, bytes_done=max(0, int(resume_from)),
            total_bytes=total_bytes, remote_path=remote_path, bump_attempts=True,
        )

    def progress(self, local_path: Path, bytes_done: int) -> None:
        """Record how far a running transfer has got."""
        self._set(local_path, TRANSFER_ACTIVE, bytes_done=max(0, int(bytes_done)))

    def complete(self, local_path: Path, total_bytes: Optional[int] = None) -> None:
        self._set(
            local_path, TRANSFER_DONE,
            bytes_done=int(total_bytes or 0), total_bytes=total_bytes,
        )

    def fail(self, local_path: Path, error: str, bytes_done: int = 0) -> None:
        self._set(
            local_path, TRANSFER_FAILED,
            bytes_done=max(0, int(bytes_done)), error=str(error)[:2000],
        )

    def requeue(self, local_path: Path, remote_path: Optional[str] = None) -> None:
        """Mark a path as owing a fresh download from byte zero."""
        self._set(
            local_path, TRANSFER_PENDING, bytes_done=0, remote_path=remote_path,
        )

    # -- querying -------------------------------------------------------
    def incomplete(self) -> List[Dict[str, Any]]:
        if self.store is None:
            return []
        try:
            return self.store.incomplete_transfers()
        except Exception:
            return []

    def get(self, local_path: Path) -> Optional[Dict[str, Any]]:
        if self.store is None:
            return None
        try:
            return self.store.get_transfer(self.key_for(local_path))
        except Exception:
            return None

    def prune_completed(self) -> int:
        """Drop finished rows. The journal is a work list, not a history."""
        if self.store is None:
            return 0
        try:
            return self.store.clear_transfers(only_done=True)
        except Exception:
            return 0


# ---------------------------------------------------------------------------
# Repair
# ---------------------------------------------------------------------------

FINDING_INTERRUPTED = "interrupted"   # journal says active: the process died
FINDING_FAILED = "failed"             # journal says failed, with a reason
FINDING_PENDING = "pending"           # queued and never started
FINDING_ORPHAN = "orphan_artifact"    # partial files on disk, no journal row
FINDING_CORRUPT = "digest_mismatch"   # on disk, but not the bytes we recorded


@dataclass
class RepairFinding:
    """One thing standing between this mirror and a complete, verified copy."""

    path: str
    kind: str
    detail: str
    size: Optional[int] = None
    bytes_done: Optional[int] = None
    attempts: int = 0
    remote_path: Optional[str] = None
    artifacts: List[str] = field(default_factory=list)
    recoverable: bool = True

    @property
    def resumable(self) -> bool:
        """Whether the bytes already fetched can be built on.

        Only a byte-for-byte prefix is resumable. A file whose digest disagrees
        with the manifest is not partially right - it is wrong, and continuing
        from where it stopped would preserve the damage.
        """
        return self.kind != FINDING_CORRUPT and bool(self.bytes_done)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "path": self.path,
            "kind": self.kind,
            "detail": self.detail,
            "size": self.size,
            "bytes_done": self.bytes_done,
            "attempts": self.attempts,
            "remote_path": self.remote_path,
            "artifacts": list(self.artifacts),
            "resumable": self.resumable,
            "recoverable": self.recoverable,
        }


@dataclass
class RepairReport:
    root: str = ""
    findings: List[RepairFinding] = field(default_factory=list)
    notes: List[str] = field(default_factory=list)
    checked_digests: int = 0

    def by_kind(self, kind: str) -> List[RepairFinding]:
        return [f for f in self.findings if f.kind == kind]

    def counts(self) -> Dict[str, int]:
        tally: Dict[str, int] = {}
        for finding in self.findings:
            tally[finding.kind] = tally.get(finding.kind, 0) + 1
        return tally

    @property
    def bytes_already_fetched(self) -> int:
        return sum(f.bytes_done or 0 for f in self.findings if f.resumable)

    @property
    def needs_network(self) -> List[RepairFinding]:
        """Findings that only a download can settle."""
        return [f for f in self.findings if f.kind != FINDING_ORPHAN]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "root": self.root,
            "counts": self.counts(),
            "checked_digests": self.checked_digests,
            "bytes_already_fetched": self.bytes_already_fetched,
            "findings": [f.to_dict() for f in self.findings],
            "notes": list(self.notes),
        }


def find_orphan_artifacts(root: Path, known: Iterable[str] = ()) -> Dict[str, List[str]]:
    """Partial-download artifacts on disk, grouped by the file they belong to.

    ``.temp`` and ``.download`` files are deliberately *not* deleted when a
    transfer fails - they are what makes the next run resume instead of
    restarting. That is correct, and it means a mirror can accumulate them
    silently: a run whose index was deleted, or one from an iFetch old enough to
    predate the journal, leaves artifacts nothing knows about.
    """
    root = Path(root).resolve()
    known_set = set(known)
    found: Dict[str, List[str]] = {}

    for current, dirs, files in os.walk(root):
        dirs[:] = [d for d in dirs if d not in {".versions", ".ifetch_versions"}]
        for name in files:
            if not name.endswith((TEMP_SUFFIX, PROGRESS_SUFFIX)):
                continue
            artifact = Path(current) / name
            suffix = TEMP_SUFFIX if name.endswith(TEMP_SUFFIX) else PROGRESS_SUFFIX
            owner = Path(current) / name[: -len(suffix)]
            try:
                key = owner.relative_to(root).as_posix()
            except ValueError:  # pragma: no cover - os.walk stays under root
                continue
            if key in known_set:
                continue
            found.setdefault(key, []).append(artifact.relative_to(root).as_posix())

    for artifacts in found.values():
        artifacts.sort()
    return found


def build_repair_report(
    store: Optional[IndexStore],
    root: Path,
    manifest: Optional[Any] = None,
    check_digests: bool = False,
) -> RepairReport:
    """Everything blocking a complete mirror, from the journal and from disk.

    Read-only. Two independent sources are consulted because neither is
    sufficient: the journal knows what iFetch was doing and nothing about what
    is on disk now, and the disk holds artifacts the journal may never have seen.
    """
    root = Path(root).resolve()
    report = RepairReport(root=str(root))

    if store is None:
        report.notes.append(
            "No index exists for this directory, so nothing is known about "
            "previous runs. Only on-disk artifacts could be looked for."
        )
        rows: List[Dict[str, Any]] = []
    else:
        rows = store.incomplete_transfers()

    journal_paths = set()
    for row in rows:
        journal_paths.add(row["path"])
        kind = {
            TRANSFER_ACTIVE: FINDING_INTERRUPTED,
            TRANSFER_FAILED: FINDING_FAILED,
            TRANSFER_PENDING: FINDING_PENDING,
        }.get(row["state"], FINDING_FAILED)

        if kind == FINDING_INTERRUPTED:
            detail = (
                "the journal still says this transfer was running, which means "
                "iFetch was killed mid-download rather than stopping cleanly"
            )
        elif kind == FINDING_PENDING:
            detail = "queued for download and never started"
        else:
            detail = row["last_error"] or "the transfer failed without recording a reason"

        report.findings.append(RepairFinding(
            path=row["path"], kind=kind, detail=detail,
            size=row["total_bytes"], bytes_done=row["bytes_done"],
            attempts=int(row["attempts"] or 0),
            remote_path=row["remote_path"] if "remote_path" in row.keys() else None,
            artifacts=_artifacts_for(root, row["path"]),
        ))

    for path, artifacts in find_orphan_artifacts(root, known=journal_paths).items():
        report.findings.append(RepairFinding(
            path=path, kind=FINDING_ORPHAN,
            detail=(
                "partial download files are on disk but no run recorded them - "
                "from an interrupted run whose index was removed, or an iFetch "
                "older than the transfer journal"
            ),
            artifacts=artifacts,
            bytes_done=_artifact_bytes(root, artifacts),
        ))

    if check_digests and manifest is not None:
        _check_digests(report, root, manifest)
    elif check_digests:
        report.notes.append(
            "Digest checking was requested but no manifest was found, so "
            "corruption could not be looked for at all."
        )

    report.findings.sort(key=lambda f: (f.kind, f.path))
    return report


def _artifacts_for(root: Path, rel_path: str) -> List[str]:
    """Which partial artifacts exist for a journalled path."""
    out = []
    for suffix in (TEMP_SUFFIX, PROGRESS_SUFFIX):
        candidate = root / (rel_path + suffix)
        if candidate.exists():
            out.append(rel_path + suffix)
    return out


def _artifact_bytes(root: Path, artifacts: Sequence[str]) -> int:
    total = 0
    for rel in artifacts:
        if not rel.endswith(TEMP_SUFFIX):
            continue
        try:
            total += (root / rel).stat().st_size
        except OSError:
            continue
    return total


def _check_digests(report: RepairReport, root: Path, manifest: Any) -> None:
    """Compare recorded digests against the bytes actually on disk.

    Only files the manifest already vouches for are checked - a file with no
    recorded digest cannot be found corrupt, only unverifiable, and reporting
    those here would drown the real findings.
    """
    from .manifest import sha256_directory, sha256_file

    # Only an unfinished-transfer finding supersedes a digest check: it already
    # says "this file is not finished", which subsumes "its bytes are wrong".
    # A stray-artifact finding does not - it is a statement about leftovers, and
    # letting it mask corruption would hide the more serious fact of the two.
    already = {
        f.path for f in report.findings
        if f.kind in (FINDING_INTERRUPTED, FINDING_FAILED, FINDING_PENDING)
    }

    for rel in manifest.paths():
        entry = manifest.get(root / rel)
        if not entry or not entry.get("sha256"):
            continue
        target = root / rel
        report.checked_digests += 1

        try:
            if target.is_dir():
                current = sha256_directory(target)
            elif target.is_file():
                current = sha256_file(target)
            else:
                continue  # Absent: 'ifetch recover missing' is that report.
        except OSError as exc:
            report.notes.append(f"could not read '{rel}': {exc}")
            continue

        if current == entry["sha256"]:
            continue
        if rel in already:
            continue  # Already reported as an unfinished transfer.

        # Absorb any stray-artifact finding for the same file, so the partial is
        # attached to the corruption and gets discarded with it rather than
        # being reported twice and left in place.
        artifacts = _artifacts_for(root, rel)
        report.findings = [
            f for f in report.findings
            if not (f.kind == FINDING_ORPHAN and f.path == rel)
        ]

        report.findings.append(RepairFinding(
            path=rel, kind=FINDING_CORRUPT,
            detail=(
                "the bytes on disk do not match the digest recorded when this "
                "file was downloaded; it has been modified or damaged since"
            ),
            size=entry.get("size"),
            bytes_done=None,
            artifacts=artifacts,
        ))


# ---------------------------------------------------------------------------
# Applying a repair
# ---------------------------------------------------------------------------

REPAIR_WOULD = "would_repair"
REPAIR_QUEUED = "queued"
REPAIR_CLEARED = "cleared"
REPAIR_FAILED = "failed"


@dataclass
class RepairOutcome:
    path: str
    status: str
    detail: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {"path": self.path, "status": self.status, "detail": self.detail}


def apply_repair(
    store: Optional[IndexStore],
    root: Path,
    report: RepairReport,
    dry_run: bool = True,
    discard_partials: bool = False,
) -> List[RepairOutcome]:
    """Queue the affected files for a fresh fetch, and optionally bin partials.

    Repair here means *making the next run do the right thing*, not
    reconstructing bytes locally - iFetch cannot invent the missing tail of a
    file. Every finding becomes a pending journal row, so ``ifetch resume``
    fetches exactly those paths and nothing else.

    ``discard_partials`` deletes the ``.temp``/``.download`` pairs. It is off by
    default because those artifacts are precisely what lets a resume avoid
    re-fetching gigabytes; discarding them is the right move only when the
    partial is not trustworthy, which is why a corrupt finding always discards
    regardless of the flag.
    """
    root = Path(root).resolve()
    outcomes: List[RepairOutcome] = []
    journal = TransferJournal(store, root)

    for finding in report.findings:
        # A partial that is a proven-wrong prefix must never be built on.
        discard = discard_partials or finding.kind == FINDING_CORRUPT

        if dry_run:
            action = "re-fetch" if finding.kind != FINDING_ORPHAN else "clean up"
            outcomes.append(RepairOutcome(
                finding.path, REPAIR_WOULD,
                f"would queue for {action}"
                + (f" and discard {len(finding.artifacts)} partial "
                   f"{plural(len(finding.artifacts), 'file')}"
                   if discard and finding.artifacts else ""),
            ))
            continue

        if discard:
            removed, error = _discard(root, finding.artifacts)
            if error is not None:
                outcomes.append(RepairOutcome(finding.path, REPAIR_FAILED, error))
                continue
        else:
            removed = 0

        if finding.kind == FINDING_ORPHAN and not discard:
            # Nothing recorded it and nothing was cleaned, so there is no work
            # to queue - saying "queued" would be a lie.
            outcomes.append(RepairOutcome(
                finding.path, REPAIR_CLEARED,
                "left in place; pass --discard-partials to remove it",
            ))
            continue

        journal.requeue(root / finding.path, remote_path=finding.remote_path)
        outcomes.append(RepairOutcome(
            finding.path, REPAIR_QUEUED,
            "queued for a fresh download"
            + (f"; {removed} partial {plural(removed, 'file')} discarded"
               if removed else ""),
        ))

    return outcomes


def _discard(root: Path, artifacts: Sequence[str]) -> tuple:
    removed = 0
    for rel in artifacts:
        target = root / rel
        try:
            if target.exists():
                target.unlink()
                removed += 1
        except OSError as exc:
            return removed, f"could not remove '{rel}': {exc}"
    return removed, None


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------

_KIND_LABELS = {
    FINDING_INTERRUPTED: ("interrupted", "yellow"),
    FINDING_FAILED: ("failed", "red"),
    FINDING_PENDING: ("pending", "cyan"),
    FINDING_ORPHAN: ("stray partial", "blue"),
    FINDING_CORRUPT: ("digest mismatch", "red"),
}


def render_repair(
    report: RepairReport,
    show: int = 40,
    use_colour: Optional[bool] = None,
) -> str:
    tint = supports_colour() if use_colour is None else use_colour
    counts = report.counts()

    out = [
        rule("="),
        colour("iFetch repair report (read-only)", "bold", tint),
        rule("="),
        key_values([
            ("Local path", report.root),
            ("Interrupted", f"{counts.get(FINDING_INTERRUPTED, 0):,}"),
            ("Failed", f"{counts.get(FINDING_FAILED, 0):,}"),
            ("Queued, not started", f"{counts.get(FINDING_PENDING, 0):,}"),
            ("Stray partials", f"{counts.get(FINDING_ORPHAN, 0):,}"),
            ("Digest mismatches", f"{counts.get(FINDING_CORRUPT, 0):,}"),
            ("Already fetched", human_bytes(report.bytes_already_fetched)),
        ]),
        "",
    ]

    if report.notes:
        out.append(colour("Not examined", "bold", tint))
        out.extend(f"  - {note}" for note in report.notes)
        out.append("")

    if not report.findings:
        out.append(
            "Nothing to repair - no transfer was left unfinished and no partial "
            "download files are lying around."
        )
        out.append(rule("="))
        return "\n".join(out)

    out.append(colour("Findings", "bold", tint))
    rows = []
    for finding in report.findings[:show]:
        label, tone = _KIND_LABELS.get(finding.kind, (finding.kind, "reset"))
        progress = (
            f"{human_bytes(finding.bytes_done)} of {human_bytes(finding.size)}"
            if finding.bytes_done else human_bytes(finding.size)
        )
        rows.append([
            colour(label, tone, tint), progress,
            f"{finding.attempts:,}" if finding.attempts else "-",
            finding.path,
        ])
    out.append(table(
        ["finding", "progress", "tries", "path"], rows,
        align=["<", ">", ">", "<"],
    ))
    if len(report.findings) > show:
        out.append(f"  ... and {len(report.findings) - show:,} more")
    out.append("")

    repeated = [f for f in report.findings if f.attempts >= 3]
    if repeated:
        out.append(colour("Failing repeatedly", "bold", tint))
        out.append(
            f"  {len(repeated):,} {plural(len(repeated), 'file')} "
            f"{plural(len(repeated), 'has', 'have')} now failed three times or "
            "more. Retrying is unlikely to help on its own:"
        )
        for finding in repeated[:show]:
            out.append(f"    {finding.path}")
            out.append(f"      {finding.detail}")
        out.append("")

    corrupt = report.by_kind(FINDING_CORRUPT)
    if corrupt:
        out.append(colour("Digest mismatches", "bold", tint))
        out.append(
            "  These files are on disk but are not the bytes iFetch recorded. "
            "iFetch cannot tell whether you edited them deliberately or they "
            "were damaged, so it will not overwrite them without being asked; "
            "'ifetch-restore' can bring back an archived copy."
        )
        out.append("")

    out.append(rule("="))
    resumable = sum(1 for f in report.findings if f.resumable)
    total = len(report.needs_network)
    if total:
        out.append(
            f"{total:,} {plural(total, 'file')} would be re-fetched "
            f"({resumable:,} of them resuming from a partial download, "
            f"{human_bytes(report.bytes_already_fetched)} already on disk). "
            "Run 'ifetch repair --apply' to queue them, then 'ifetch resume'."
        )
    else:
        out.append(
            "Only stray partial files were found. 'ifetch repair --apply "
            "--discard-partials' removes them."
        )
    return "\n".join(out)


def render_resume_plan(
    rows: Sequence[Dict[str, Any]],
    use_colour: Optional[bool] = None,
) -> str:
    tint = supports_colour() if use_colour is None else use_colour
    if not rows:
        return (
            "Nothing to resume - the journal holds no unfinished transfers. "
            "If a download was interrupted before iFetch could record anything, "
            "run 'ifetch repair' to look for partial files on disk."
        )

    known = [r for r in rows if r.get("remote_path")]
    unknown = len(rows) - len(known)

    out = [
        rule("="),
        colour("iFetch resume", "bold", tint),
        rule("="),
        key_values([
            ("Unfinished transfers", f"{len(rows):,}"),
            ("Resumable directly", f"{len(known):,}"),
        ]),
        "",
    ]
    if unknown:
        out.append(
            f"  ! {unknown:,} {plural(unknown, 'transfer')} "
            f"{plural(unknown, 'has', 'have')} no recorded remote path - they "
            "were journalled by an older iFetch. Re-run the original 'ifetch' "
            "command to pick them up.\n"
        )
    out.append(table(
        ["progress", "tries", "path"],
        [[f"{human_bytes(r['bytes_done'])} of {human_bytes(r['total_bytes'])}",
          f"{r['attempts']:,}", r["path"]] for r in rows[:40]],
        align=[">", ">", "<"],
    ))
    out.append(rule("="))
    return "\n".join(out)
