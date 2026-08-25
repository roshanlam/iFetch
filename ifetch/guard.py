"""``ifetch guard`` - the files your backups are quietly skipping.

With "Optimize Mac Storage" on, macOS may remove the contents of any iCloud
Drive file to reclaim space, leaving the name, the icon, the date and the
*size* behind. Finder shows a 4 GB video. The directory entry says 4 GB. The
disk holds nothing.

Anything that copies files by reading them inherits that: Time Machine backs up
the placeholder, and Backblaze, Arq, Carbon Copy Cloner and rsync read zero
bytes and report success. Nothing fails at backup time, because nothing did
fail. It fails at restore time, when the file comes back empty and the original
is gone.

This module reports how much of a folder is really on the disk and how much
only exists on Apple's servers, and with ``--materialize --apply`` downloads the
missing contents.

What it refuses to do
---------------------
**It never reports a clean folder it did not fully check.** Detection is
:class:`ifetch.recovery.PlaceholderDetector`, which uses two signals:

``brick`` (certain)
    A ``.name.ext.icloud`` stub next to where the file was. The stub is an
    ordinary file, so this works on any OS - including a drive pulled out of a
    Mac and read on Linux.

``dataless`` (likely)
    Full size, zero blocks allocated. This is macOS-only and cannot tell an
    evicted file from a sparse one, so elsewhere a folder of files evicted
    without stubs looks pristine. :class:`GuardReport` says so instead of
    reporting zero.

Folders that could not be read are named. Symlinks are recorded and never
followed, or the byte total ends up describing someone else's disk. Files whose
stub recorded no size are counted apart from files whose size is known, and the
headline then calls itself a floor rather than a total.

**It never trusts an exit code over the disk.** ``brctl download`` returns 0 for
work it has only queued and may later drop, and a fetcher can return success
having written nothing. Every path touched by ``--materialize`` is re-checked
afterwards, and anything still missing is named.

**It never writes without ``--apply``.** The default prints the list it would
fetch.

Why iFetch can fix this rather than just report it
--------------------------------------------------
``brctl download`` asks the component that evicted the file to change its mind,
which it may quietly decline to do. iFetch can instead sign in to iCloud and
download the contents over HTTPS itself, with the FileProvider out of the loop.
That is the reliable route, and the reason this is a command and not a shell
alias.
"""

from __future__ import annotations

import os
import platform
import shutil
import subprocess
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

from .recovery import (
    CONFIDENCE_CERTAIN,
    CONFIDENCE_LIKELY,
    EVIDENCE_BRICK,
    EVIDENCE_DATALESS,
    Placeholder,
    PlaceholderDetector,
    PlaceholderReport,
    brick_target,
    write_csv,
)
from .render import human_bytes, key_values, plural, rule, table
from .scanner import is_local_artifact

#: Where macOS keeps the local half of iCloud Drive. The tilde in the middle of
#: the container name is Apple's, not a typo.
ICLOUD_DRIVE_RELATIVE = "Library/Mobile Documents/com~apple~CloudDocs"

#: Fetch the bytes from iCloud over HTTPS, writing them ourselves. Reliable.
STRATEGY_FETCH = "fetch"
#: Ask the FileProvider nicely via ``brctl download``. macOS only, unreliable.
STRATEGY_BRCTL = "brctl"
#: Neither route is available here, which is a finding, not a clean result.
STRATEGY_NONE = "none"

MATERIALIZE_WOULD = "would-fetch"
MATERIALIZE_DONE = "done"
MATERIALIZE_FAILED = "failed"
MATERIALIZE_UNVERIFIED = "unverified"
MATERIALIZE_REFUSED = "refused"

#: Confidence wording, stated once so the report and the JSON agree.
EVIDENCE_NOTES = {
    EVIDENCE_BRICK: (
        CONFIDENCE_CERTAIN,
        "a sibling '.name.ext.icloud' stub sits where the contents used to be; "
        "the stub is a real file, so this signal works on any OS",
    ),
    EVIDENCE_DATALESS: (
        CONFIDENCE_LIKELY,
        "full reported size, zero blocks allocated on disk - macOS/APFS only, "
        "and a sparse file looks identical, so confirm before deleting anything",
    ),
}


class GuardError(Exception):
    """A condition that stops the command rather than degrading the report."""


#: ``(relative_path, destination) -> True if the bytes were written``.
#:
#: Deliberately the smallest possible surface. ``guard_cli`` wires a real
#: :class:`ifetch.downloader.DownloadManager` behind it; tests inject a fake and
#: never touch a network or a credential. Returning ``True`` is a *claim*, not
#: proof - :func:`materialize` verifies every one of them against the disk.
Fetcher = Callable[[str, Path], bool]


def default_icloud_folder() -> Path:
    """The local iCloud Drive folder on this machine.

    Returned whether or not it exists; the caller decides what to say about a
    missing one. Scanning ``~`` because iCloud Drive is not there would be the
    worst possible fallback - it would produce a confident, enormous, entirely
    unrelated number.
    """
    return Path.home() / ICLOUD_DRIVE_RELATIVE


# ---------------------------------------------------------------------------
# Byte accounting
# ---------------------------------------------------------------------------

@dataclass
class ByteAccount:
    """Logical size against bytes that are really here, for one scope.

    ``logical_bytes`` is what Finder shows and what a naive ``du`` over an
    unevicted tree would show. ``resident_bytes`` is what a backup would
    actually be able to read. ``evicted_bytes`` is the difference, and it is the
    number this whole module exists to produce.

    ``evicted_unknown_size`` is kept out of every total on purpose: those are
    files known to be evicted whose size the stub did not record. Folding a
    guess into ``evicted_bytes`` would make the headline unfalsifiable, so they
    are counted and reported beside it instead.
    """

    label: str = ""
    files: int = 0
    logical_bytes: int = 0
    resident_files: int = 0
    resident_bytes: int = 0
    evicted_files: int = 0
    evicted_bytes: int = 0
    evicted_unknown_size: int = 0

    def add_resident(self, size: int) -> None:
        self.files += 1
        self.resident_files += 1
        self.resident_bytes += size
        self.logical_bytes += size

    def add_evicted(self, size: Optional[int]) -> None:
        self.files += 1
        self.evicted_files += 1
        if size is None:
            self.evicted_unknown_size += 1
            return
        self.evicted_bytes += size
        self.logical_bytes += size

    @property
    def exposure_percent(self) -> float:
        """Share of the tree, by size, that no backup of this machine holds."""
        if self.logical_bytes <= 0:
            return 0.0
        return round(self.evicted_bytes * 100.0 / self.logical_bytes, 2)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "label": self.label,
            "files": self.files,
            "logical_bytes": self.logical_bytes,
            "resident_files": self.resident_files,
            "resident_bytes": self.resident_bytes,
            "evicted_files": self.evicted_files,
            "evicted_bytes": self.evicted_bytes,
            "evicted_unknown_size": self.evicted_unknown_size,
            "exposure_percent": self.exposure_percent,
        }


@dataclass
class EvictedFile:
    """One file that is in the listing and not on the disk."""

    path: str
    size: Optional[int]
    evidence: str
    confidence: str
    detail: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "path": self.path,
            "size": self.size,
            "evidence": self.evidence,
            "confidence": self.confidence,
            "detail": self.detail,
        }


@dataclass
class GuardReport:
    """What is exposed, and - equally load-bearing - what was not examined."""

    root: str
    generated_at: str = ""
    platform_name: str = ""
    total: ByteAccount = field(default_factory=lambda: ByteAccount(label="(total)"))
    by_folder: List[ByteAccount] = field(default_factory=list)
    evicted: List[EvictedFile] = field(default_factory=list)
    signals_available: List[str] = field(default_factory=list)
    signals_unavailable: List[Dict[str, str]] = field(default_factory=list)
    unreadable: List[Dict[str, str]] = field(default_factory=list)
    symlinks: List[str] = field(default_factory=list)
    stub_files: int = 0
    stub_bytes: int = 0
    materialization: Optional["MaterializeReport"] = None

    @property
    def complete(self) -> bool:
        """False when any signal, directory or file could not be examined.

        A report with ``complete`` False may not be read as "nothing wrong". It
        means "nothing wrong *in the part I could see*", and the rendered output
        says exactly that.
        """
        return not self.signals_unavailable and not self.unreadable

    @property
    def unevaluated_signals(self) -> List[str]:
        return [gap["signal"] for gap in self.signals_unavailable]

    def largest(self, count: int) -> List[EvictedFile]:
        """The biggest offenders, unknown sizes last rather than first."""
        return sorted(
            self.evicted, key=lambda e: (e.size is None, -(e.size or 0), e.path)
        )[:count]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "root": self.root,
            "generated_at": self.generated_at,
            "platform": self.platform_name,
            "complete": self.complete,
            "totals": self.total.to_dict(),
            "by_folder": [a.to_dict() for a in self.by_folder],
            "evicted": [e.to_dict() for e in self.evicted],
            "signals_available": [
                {
                    "signal": name,
                    "confidence": EVIDENCE_NOTES[name][0],
                    "evidence": EVIDENCE_NOTES[name][1],
                }
                for name in self.signals_available
                if name in EVIDENCE_NOTES
            ],
            "signals_unavailable": list(self.signals_unavailable),
            "unreadable": list(self.unreadable),
            "symlinks": list(self.symlinks),
            "stub_files": self.stub_files,
            "stub_bytes": self.stub_bytes,
            "materialization": (
                self.materialization.to_dict() if self.materialization else None
            ),
        }


class GuardScanner:
    """Account for every byte in a tree: here, or only on Apple's servers.

    Detection is not reimplemented. :class:`PlaceholderDetector` decides what is
    evicted and with what evidence; this class walks the same tree once more to
    attach sizes to the answer, group them, and - the part the detector's
    ``os.walk`` cannot do, since it swallows errors - record every directory it
    was refused entry to.
    """

    def __init__(self, root: Path, check_dataless: Optional[bool] = None):
        self.root = Path(root).expanduser().resolve()
        self.check_dataless = check_dataless
        self.is_macos = platform.system() == "Darwin"

    def scan(self) -> GuardReport:
        if not self.root.is_dir():
            raise GuardError(f"'{self.root}' is not a directory")

        detection = PlaceholderDetector(
            self.root, check_dataless=self.check_dataless
        ).scan()

        report = GuardReport(
            root=str(self.root),
            generated_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            platform_name=platform.system(),
            signals_available=list(detection.signals_available),
            signals_unavailable=list(detection.signals_unavailable),
        )
        self._account(report, detection)
        return report

    # -- internals ----------------------------------------------------------

    def _account(self, report: GuardReport, detection: PlaceholderReport) -> None:
        evicted: Dict[str, Placeholder] = {p.path: p for p in detection.placeholders}
        folders: Dict[str, ByteAccount] = {}
        seen: set = set()

        def account(rel: str) -> ByteAccount:
            parts = rel.split("/")
            label = parts[0] if len(parts) > 1 else "(root)"
            if label not in folders:
                folders[label] = ByteAccount(label=label)
            return folders[label]

        def note_error(path: Path, exc: OSError) -> None:
            report.unreadable.append({
                "path": self._display(path),
                "error": exc.strerror or str(exc),
            })

        for current, dirs, files in os.walk(self.root, onerror=lambda e: note_error(
            Path(getattr(e, "filename", "") or self.root), e
        )):
            current_path = Path(current)

            # ``.versions`` is iFetch's own history store, and a symlinked
            # directory is somebody else's tree wearing this one's name. Neither
            # belongs in a number that claims to describe *this* folder.
            kept = []
            for name in dirs:
                if name == ".versions":
                    continue
                child = current_path / name
                if child.is_symlink():
                    report.symlinks.append(self._display(child))
                    continue
                kept.append(name)
            dirs[:] = kept

            for name in files:
                full = current_path / name
                rel = self._relative(full)
                if rel is None:
                    continue

                target = brick_target(name)
                if target is not None:
                    # The stub itself is real, tiny, and not user content. Its
                    # bytes are counted apart so they cannot inflate "resident".
                    report.stub_files += 1
                    try:
                        report.stub_bytes += full.stat().st_size
                    except OSError as exc:
                        note_error(full, exc)
                    continue

                if is_local_artifact(rel):
                    continue

                if full.is_symlink():
                    report.symlinks.append(self._display(full))
                    continue

                seen.add(rel)
                placeholder = evicted.get(rel)

                try:
                    size = full.stat().st_size
                except OSError as exc:
                    note_error(full, exc)
                    continue

                bucket = account(rel)
                if placeholder is not None:
                    reported = placeholder.reported_size
                    reported = size if reported is None else reported
                    bucket.add_evicted(reported)
                    report.total.add_evicted(reported)
                    report.evicted.append(self._evicted_file(placeholder, reported))
                else:
                    bucket.add_resident(size)
                    report.total.add_resident(size)

        # A brick-evicted file has no file at its own path - only the stub the
        # loop above counted - so it is added here, from the detector's finding.
        for rel, placeholder in evicted.items():
            if rel in seen:
                continue
            bucket = account(rel)
            bucket.add_evicted(placeholder.reported_size)
            report.total.add_evicted(placeholder.reported_size)
            report.evicted.append(
                self._evicted_file(placeholder, placeholder.reported_size)
            )

        report.evicted.sort(key=lambda e: e.path)
        report.symlinks.sort()
        report.unreadable.sort(key=lambda d: d["path"])
        report.by_folder = sorted(
            folders.values(), key=lambda a: (-a.evicted_bytes, -a.logical_bytes, a.label)
        )

    def _evicted_file(self, placeholder: Placeholder, size: Optional[int]) -> EvictedFile:
        return EvictedFile(
            path=placeholder.path,
            size=size,
            evidence=placeholder.evidence,
            confidence=placeholder.confidence,
            detail=placeholder.detail,
        )

    def _relative(self, path: Path) -> Optional[str]:
        try:
            return path.relative_to(self.root).as_posix()
        except ValueError:
            return None

    def _display(self, path: Path) -> str:
        rel = self._relative(path)
        return rel if rel is not None else str(path)


# ---------------------------------------------------------------------------
# Materialisation
# ---------------------------------------------------------------------------

@dataclass
class MaterializeOutcome:
    path: str
    status: str
    size: Optional[int] = None
    detail: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "path": self.path,
            "status": self.status,
            "size": self.size,
            "detail": self.detail,
        }


@dataclass
class MaterializeReport:
    """What was attempted, by which route, and what is *still* not here."""

    strategy: str
    strategy_reason: str
    dry_run: bool = True
    outcomes: List[MaterializeOutcome] = field(default_factory=list)
    verified: bool = False

    def by_status(self, status: str) -> List[MaterializeOutcome]:
        return [o for o in self.outcomes if o.status == status]

    @property
    def recovered_bytes(self) -> int:
        return sum(o.size or 0 for o in self.by_status(MATERIALIZE_DONE))

    @property
    def still_evicted(self) -> List[MaterializeOutcome]:
        """Everything that is not now readable, whatever the fetcher claimed."""
        return [
            o for o in self.outcomes
            if o.status in (MATERIALIZE_FAILED, MATERIALIZE_UNVERIFIED,
                            MATERIALIZE_REFUSED)
        ]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "strategy": self.strategy,
            "strategy_reason": self.strategy_reason,
            "dry_run": self.dry_run,
            "verified": self.verified,
            "recovered_bytes": self.recovered_bytes,
            "still_evicted": [o.path for o in self.still_evicted],
            "outcomes": [o.to_dict() for o in self.outcomes],
        }


def brctl_available() -> bool:
    """True when Apple's FileProvider CLI is present on this machine."""
    return platform.system() == "Darwin" and shutil.which("brctl") is not None


def choose_strategy(
    fetcher: Optional[Fetcher],
    prefer: str = "auto",
    brctl_present: Optional[bool] = None,
) -> Tuple[str, str]:
    """Pick a materialisation route and say, in words, why.

    ``fetch`` wins every tie it is eligible for. ``brctl download`` hands the
    request back to the same FileProvider that evicted the file: it is
    asynchronous, it returns success for work it has only queued, and it can
    decline for reasons it does not report. Downloading from iCloud over HTTPS
    and writing the bytes here leaves nothing to interpret.
    """
    present = brctl_available() if brctl_present is None else brctl_present

    if prefer == STRATEGY_FETCH:
        if fetcher is None:
            return STRATEGY_NONE, (
                "--strategy fetch was requested but no iCloud connection was "
                "supplied, so there is nothing to download with"
            )
        return STRATEGY_FETCH, (
            "downloading from iCloud over HTTPS and writing the bytes here, "
            "with the FileProvider out of the loop"
        )

    if prefer == STRATEGY_BRCTL:
        if not present:
            return STRATEGY_NONE, (
                "--strategy brctl was requested but 'brctl' is not available "
                f"on this machine ({platform.system()})"
            )
        return STRATEGY_BRCTL, (
            "asking the FileProvider to download the files, because it was "
            "explicitly requested. Its exit code means only that the request "
            "was accepted, so every path is re-checked afterwards"
        )

    if fetcher is not None:
        return STRATEGY_FETCH, (
            "downloading from iCloud over HTTPS and writing the bytes here, "
            "with the FileProvider out of the loop - the only route whose "
            "result does not depend on the component that evicted the files"
        )
    if present:
        return STRATEGY_BRCTL, (
            "no iCloud connection was supplied, so the FileProvider is being "
            "asked instead. This is asynchronous and may silently do nothing, "
            "so every path is re-checked afterwards"
        )
    return STRATEGY_NONE, (
        "no iCloud connection was supplied and 'brctl' is not available on "
        f"this machine ({platform.system()}), so nothing can be materialised "
        "here. Sign in with --email to use the direct download route."
    )


def materialize(
    report: GuardReport,
    root: Path,
    fetcher: Optional[Fetcher] = None,
    dry_run: bool = True,
    limit: Optional[int] = None,
    prefer: str = "auto",
    check_dataless: Optional[bool] = None,
    brctl_runner: Optional[Callable[[Sequence[str]], int]] = None,
    brctl_present: Optional[bool] = None,
) -> MaterializeReport:
    """Pull evicted files back onto this disk, then check that they arrived.

    Dry by default: with ``dry_run`` the filesystem is not touched at all and
    every candidate is listed as ``would-fetch``.

    Two refusals happen before any transfer, because a path in this list came
    out of a directory listing rather than out of this program:

    * a path that resolves outside ``root`` is never written to;
    * a path with no usable name is never written to.

    Afterwards - and this is the point - detection is re-run and every touched
    path is checked against the disk. ``brctl download`` returns 0 for requests
    it has merely queued, and a fetcher can return ``True`` having written
    nothing; neither claim is allowed to close a finding. Anything that is not
    readable at the end is reported by name as ``unverified``.
    """
    root = Path(root).expanduser().resolve()
    strategy, reason = choose_strategy(fetcher, prefer=prefer, brctl_present=brctl_present)
    result = MaterializeReport(strategy=strategy, strategy_reason=reason, dry_run=dry_run)

    candidates = report.largest(limit) if limit else sorted(
        report.evicted, key=lambda e: e.path
    )

    if strategy == STRATEGY_NONE:
        for item in candidates:
            result.outcomes.append(MaterializeOutcome(
                item.path, MATERIALIZE_REFUSED, item.size,
                "no materialisation route is available here",
            ))
        return result

    touched: List[EvictedFile] = []
    for item in candidates:
        destination = (root / item.path).resolve()
        if not _within(root, destination):
            result.outcomes.append(MaterializeOutcome(
                item.path, MATERIALIZE_REFUSED, item.size,
                "resolves outside the folder being guarded; refusing to write there",
            ))
            continue

        if dry_run:
            result.outcomes.append(MaterializeOutcome(
                item.path, MATERIALIZE_WOULD, item.size,
                f"would fetch via {strategy}",
            ))
            continue

        ok, detail = _attempt(strategy, item, root, destination, fetcher, brctl_runner)
        if not ok:
            result.outcomes.append(
                MaterializeOutcome(item.path, MATERIALIZE_FAILED, item.size, detail)
            )
            continue
        touched.append(item)

    if dry_run or not touched:
        return result

    unresolved = verify_resident(
        root, [item.path for item in touched], check_dataless=check_dataless
    )
    result.verified = True
    for item in touched:
        problem = unresolved.get(item.path)
        if problem is None:
            result.outcomes.append(
                MaterializeOutcome(item.path, MATERIALIZE_DONE, item.size,
                                   "verified: the bytes are on this disk")
            )
        else:
            result.outcomes.append(MaterializeOutcome(
                item.path, MATERIALIZE_UNVERIFIED, item.size,
                f"reported success but {problem}",
            ))

    result.outcomes.sort(key=lambda o: o.path)
    return result


def verify_resident(
    root: Path,
    paths: Sequence[str],
    check_dataless: Optional[bool] = None,
) -> Dict[str, str]:
    """Re-run detection and return ``path -> why it is still not really here``.

    Paths that came back clean are absent from the mapping. Detection is the
    same :class:`PlaceholderDetector` used for the scan, so a file cannot be
    called recovered by a looser standard than the one that flagged it.
    """
    root = Path(root).expanduser().resolve()
    detection = PlaceholderDetector(root, check_dataless=check_dataless).scan()
    flagged = {p.path: p for p in detection.placeholders}

    problems: Dict[str, str] = {}
    for rel in paths:
        target = (root / rel).resolve()
        if not _within(root, target):
            problems[rel] = "resolves outside the folder being guarded"
            continue
        if rel in flagged:
            problems[rel] = flagged[rel].detail or "is still detected as a placeholder"
            continue
        if not target.is_file():
            problems[rel] = "no file exists at that path"
            continue
        try:
            if target.stat().st_size <= 0:
                problems[rel] = "the file is zero bytes"
        except OSError as exc:
            problems[rel] = f"could not be read back: {exc.strerror or exc}"
    return problems


def _attempt(
    strategy: str,
    item: EvictedFile,
    root: Path,
    destination: Path,
    fetcher: Optional[Fetcher],
    brctl_runner: Optional[Callable[[Sequence[str]], int]],
) -> Tuple[bool, str]:
    if strategy == STRATEGY_FETCH:
        assert fetcher is not None  # choose_strategy guarantees this
        try:
            destination.parent.mkdir(parents=True, exist_ok=True)
            claimed = bool(fetcher(item.path, destination))
        except Exception as exc:  # a fetcher failure is a finding, not a crash
            return False, f"the download raised {type(exc).__name__}: {exc}"
        if not claimed:
            return False, "the download reported failure"
        return True, ""

    runner = brctl_runner if brctl_runner is not None else _run_brctl
    try:
        code = runner(["brctl", "download", str(destination)])
    except Exception as exc:
        return False, f"brctl could not be run: {type(exc).__name__}: {exc}"
    if code != 0:
        return False, f"brctl exited {code}"
    # A zero exit means the request was accepted, nothing more. The caller
    # verifies against the disk.
    return True, ""


def _run_brctl(command: Sequence[str]) -> int:
    completed = subprocess.run(
        list(command), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False
    )
    return completed.returncode


def _within(root: Path, path: Path) -> bool:
    """True when ``path`` really is inside ``root``, both already resolved."""
    try:
        path.relative_to(root)
    except ValueError:
        return False
    return path != root


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------

def render_guard(report: GuardReport, show: int = 20) -> str:
    total = report.total
    lines = [
        rule("="),
        "iFetch backup-exposure report",
        rule("="),
        key_values([
            ("Folder", report.root),
            ("Platform", report.platform_name or "unknown"),
            ("Files examined", f"{total.files:,}"),
            ("Size as Finder shows it", human_bytes(total.logical_bytes)),
            ("Really on this disk",
             f"{human_bytes(total.resident_bytes)}  "
             f"({total.resident_files:,} {plural(total.resident_files, 'file')})"),
            ("Only on Apple's servers",
             f"{human_bytes(total.evicted_bytes)}  "
             f"({total.evicted_files:,} {plural(total.evicted_files, 'file')})"),
            ("Exposure", f"{total.exposure_percent}% of this folder by size"),
        ]),
        "",
    ]

    lines.append("What this means for your backups")
    if total.evicted_files:
        lines.append(
            f"  {human_bytes(total.evicted_bytes)} across {total.evicted_files:,} "
            f"{plural(total.evicted_files, 'file')} is not on this Mac. Those files "
            "have a name,"
        )
        lines.append(
            "  a date and a size, and no contents. Time Machine, third-party backup"
        )
        lines.append(
            "  software and cloud backup agents all read them as empty and report"
        )
        lines.append(
            "  success, so a restore returns the names and none of the data."
        )
        if total.evicted_unknown_size:
            lines.append(
                f"  {total.evicted_unknown_size:,} of them recorded no size, so the "
                "figure above is a floor, not a total."
            )
    else:
        lines.append(
            "  Every file examined has its contents on this disk, so a backup of "
            "this folder"
        )
        lines.append("  would copy real bytes.")
    lines.append("")

    lines.append("Evidence and confidence")
    for signal in report.signals_available:
        if signal not in EVIDENCE_NOTES:
            continue
        confidence, note = EVIDENCE_NOTES[signal]
        lines.append(f"  {signal:<9} {confidence:<8} {note}")
    lines.append("")

    if not report.complete:
        lines.append("Coverage gaps - this scan did NOT examine everything")
        for gap in report.signals_unavailable:
            lines.append(
                f"  ! '{gap['signal']}' detection is unevaluated here: {gap['reason']}"
            )
        if report.unreadable:
            count = len(report.unreadable)
            lines.append(
                f"  ! {count:,} {plural(count, 'path')} could not be read, so "
                f"{plural(count, 'it was', 'they were')} not counted at all:"
            )
            for entry in report.unreadable[:show]:
                lines.append(f"      {entry['path']}  ({entry['error']})")
            if count > show:
                lines.append(f"      ... and {count - show:,} more")
        lines.append("")

    if report.symlinks:
        count = len(report.symlinks)
        lines.append(
            f"{count:,} {plural(count, 'symlink')} {plural(count, 'was', 'were')} "
            f"not followed, so {plural(count, 'its', 'their')} target "
            f"{plural(count, 'is', 'are')} not counted above:"
        )
        for link in report.symlinks[:show]:
            lines.append(f"  {link}")
        if count > show:
            lines.append(f"  ... and {count - show:,} more")
        lines.append("")

    if report.by_folder:
        lines.append("By top-level folder")
        lines.append(table(
            ["only in iCloud", "on disk", "evicted", "files", "folder"],
            [
                [human_bytes(a.evicted_bytes), human_bytes(a.resident_bytes),
                 f"{a.evicted_files:,}", f"{a.files:,}", a.label]
                for a in report.by_folder[:show]
            ],
            align=[">", ">", ">", ">", "<"],
        ))
        lines.append("")

    biggest = report.largest(show)
    if biggest:
        lines.append("Largest files that are not really here")
        lines.append(table(
            ["size", "confidence", "evidence", "path"],
            [[human_bytes(e.size), e.confidence, e.evidence, e.path] for e in biggest],
            align=[">", "<", "<", "<"],
        ))
        if len(report.evicted) > show:
            lines.append(f"  ... and {len(report.evicted) - show:,} more")
        if report.materialization is None:
            lines.append("")
            lines.append(
                "Run 'ifetch guard --materialize' to see what would be downloaded, "
                "then add"
            )
            lines.append(
                "--apply to fetch it. Until then, do not treat a backup of this "
                "folder as complete."
            )
    elif report.complete:
        lines.append(
            "No evicted files found, and every signal was evaluated. A backup of "
            "this folder would be a real one."
        )
    else:
        gaps = ", ".join(report.unevaluated_signals) or "some checks"
        lines.append(
            f"No evicted files found by the signals available here - but '{gaps}' "
            f"could not be evaluated on {report.platform_name or 'this platform'}, "
            "so this is not a clean result."
        )
        lines.append(
            "Files evicted without leaving a '.icloud' stub would be invisible to "
            "this scan. Re-run it on the Mac that holds the folder before trusting it."
        )

    if report.materialization is not None:
        lines.append("")
        lines.append(render_materialization(report.materialization, show=show))

    lines.append(rule("="))
    return "\n".join(lines)


def render_materialization(result: MaterializeReport, show: int = 20) -> str:
    lines = [
        rule("-"),
        "Materialisation",
        rule("-"),
        key_values([
            ("Strategy", result.strategy),
            ("Why", result.strategy_reason),
            ("Mode", "dry run - nothing was written" if result.dry_run else "applied"),
        ]),
        "",
    ]

    if not result.outcomes:
        lines.append("Nothing to materialise.")
        return "\n".join(lines)

    rows = [[o.status, human_bytes(o.size), o.path] for o in result.outcomes[:show]]
    lines.append(table(["status", "size", "path"], rows, align=["<", ">", "<"]))
    if len(result.outcomes) > show:
        lines.append(f"  ... and {len(result.outcomes) - show:,} more")
    lines.append("")

    if result.dry_run:
        lines.append(
            f"Nothing has been written. Re-run with --apply to fetch these "
            f"{len(result.outcomes):,} {plural(len(result.outcomes), 'file')}."
        )
        return "\n".join(lines)

    done = result.by_status(MATERIALIZE_DONE)
    lines.append(
        f"{len(done):,} {plural(len(done), 'file')} verified resident, "
        f"{human_bytes(result.recovered_bytes)} recovered."
    )

    outstanding = result.still_evicted
    if outstanding:
        lines.append("")
        lines.append(
            f"{len(outstanding):,} {plural(len(outstanding), 'file')} did NOT become "
            "resident and remain missing from every backup:"
        )
        for outcome in outstanding[:show]:
            lines.append(f"  - {outcome.path}  ({outcome.detail})")
        if len(outstanding) > show:
            lines.append(f"  ... and {len(outstanding) - show:,} more")
    elif result.verified:
        lines.append("Every file was re-checked against the disk after fetching.")

    return "\n".join(lines)


def evicted_csv_rows(report: GuardReport) -> List[List[Any]]:
    return [
        [e.path, e.size, e.evidence, e.confidence, e.detail] for e in report.evicted
    ]


EVICTED_CSV_HEADERS = ["path", "bytes", "evidence", "confidence", "detail"]

__all__ = [
    "ByteAccount",
    "EVICTED_CSV_HEADERS",
    "EVIDENCE_NOTES",
    "EvictedFile",
    "Fetcher",
    "GuardError",
    "GuardReport",
    "GuardScanner",
    "ICLOUD_DRIVE_RELATIVE",
    "MATERIALIZE_DONE",
    "MATERIALIZE_FAILED",
    "MATERIALIZE_REFUSED",
    "MATERIALIZE_UNVERIFIED",
    "MATERIALIZE_WOULD",
    "MaterializeOutcome",
    "MaterializeReport",
    "STRATEGY_BRCTL",
    "STRATEGY_FETCH",
    "STRATEGY_NONE",
    "brctl_available",
    "choose_strategy",
    "default_icloud_folder",
    "evicted_csv_rows",
    "materialize",
    "render_guard",
    "render_materialization",
    "verify_resident",
    "write_csv",
]
