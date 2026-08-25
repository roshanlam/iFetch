"""iCloud-native recovery workflows.

Generic sync tools answer "make these two trees match". Recovery asks different
questions, and they are the ones people actually have when something has gone
wrong:

* *Which of my files are not really here?* A file can exist in Finder, show a
  size, and contain nothing - it has been evicted to iCloud and only a
  placeholder remains. Copying that "file" to a backup drive copies nothing.
  Every byte-count and every checksum over such a tree is a lie.
* *What did I lose?* Files that iCloud has and the disk does not, plus files a
  previous run recorded that have since vanished locally.
* *What is actually in my account?* Where the space went, by folder and by file
  type, and the largest items - the inventory you want before deciding what to
  pull down.

Placeholders, honestly
----------------------
There is no portable way to ask "is this file really here?". Two signals are
used, and each is reported with the evidence behind it rather than as a bare
verdict:

``brick``
    A sibling ``.name.ext.icloud`` file. macOS writes these when it evicts a
    file's contents; the real file disappears and this stub takes its place.
    This signal is filesystem-independent - the stub is a real file, so it is
    detectable on Linux or Windows too, which matters when recovering a drive
    pulled out of a Mac.

``dataless``
    The file exists at full size but occupies zero blocks on disk. On APFS this
    is how a materialised-but-evicted file looks. It is only meaningful on
    macOS, and it cannot distinguish an evicted file from a sparse one, so it
    is reported as a strong indication rather than a certainty.

On platforms where a signal cannot be evaluated, :class:`PlaceholderReport`
says so explicitly. It never silently reports "no placeholders found" when what
it means is "I could not look".
"""

from __future__ import annotations

import json
import os
import platform
import sys
import time
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from .index import IndexStore, KIND_PACKAGE
from .render import human_bytes, key_values, plural, rule, table
from .scanner import is_local_artifact

#: Suffix macOS gives the stub it leaves behind when it evicts a file.
BRICK_SUFFIX = ".icloud"

EVIDENCE_BRICK = "brick"
EVIDENCE_DATALESS = "dataless"

CONFIDENCE_CERTAIN = "certain"
CONFIDENCE_LIKELY = "likely"


# ---------------------------------------------------------------------------
# Placeholders
# ---------------------------------------------------------------------------

@dataclass
class Placeholder:
    """A local path whose contents are not actually on this disk."""

    path: str
    evidence: str
    confidence: str
    reported_size: Optional[int] = None
    detail: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "path": self.path,
            "evidence": self.evidence,
            "confidence": self.confidence,
            "reported_size": self.reported_size,
            "detail": self.detail,
        }


@dataclass
class PlaceholderReport:
    """Placeholders found, and - just as important - what could not be checked."""

    root: str
    placeholders: List[Placeholder] = field(default_factory=list)
    files_checked: int = 0
    signals_available: List[str] = field(default_factory=list)
    signals_unavailable: List[Dict[str, str]] = field(default_factory=list)

    @property
    def total_reported_bytes(self) -> int:
        return sum(p.reported_size or 0 for p in self.placeholders)

    @property
    def complete(self) -> bool:
        """False when some signal could not be evaluated on this platform."""
        return not self.signals_unavailable

    def to_dict(self) -> Dict[str, Any]:
        return {
            "root": self.root,
            "files_checked": self.files_checked,
            "placeholders_found": len(self.placeholders),
            "total_reported_bytes": self.total_reported_bytes,
            "signals_available": list(self.signals_available),
            "signals_unavailable": list(self.signals_unavailable),
            "complete": self.complete,
            "placeholders": [p.to_dict() for p in self.placeholders],
        }


def brick_target(name: str) -> Optional[str]:
    """Return the real filename a ``.icloud`` stub stands in for.

    macOS names the stub ``.<original name>.icloud`` - leading dot, original
    name including its extension, then the suffix.
    """
    if not name.endswith(BRICK_SUFFIX) or not name.startswith("."):
        return None
    inner = name[1:-len(BRICK_SUFFIX)]
    return inner or None


class PlaceholderDetector:
    """Find local files whose contents are not really present.

    ``dataless`` detection is macOS-only; on other platforms it is reported as
    unavailable rather than skipped silently. Placeholder detection that
    quietly returns "none found" on Linux would be worse than not offering it,
    because it invites a user to trust a mirror that is full of empty shells.
    """

    def __init__(self, root: Path, check_dataless: Optional[bool] = None):
        self.root = Path(root).resolve()
        self.is_macos = platform.system() == "Darwin"
        self.check_dataless = self.is_macos if check_dataless is None else check_dataless

    def scan(self) -> PlaceholderReport:
        report = PlaceholderReport(root=str(self.root))
        report.signals_available.append(EVIDENCE_BRICK)

        if self.check_dataless:
            report.signals_available.append(EVIDENCE_DATALESS)
        else:
            report.signals_unavailable.append({
                "signal": EVIDENCE_DATALESS,
                "reason": (
                    f"zero-block detection is only meaningful on macOS/APFS; "
                    f"this is {platform.system()}. Files evicted by iCloud that "
                    f"left no .icloud stub cannot be detected here."
                ),
            })

        for current, dirs, files in os.walk(self.root):
            dirs[:] = [d for d in dirs if d != ".versions"]
            current_path = Path(current)

            for name in files:
                full = current_path / name
                try:
                    rel = full.relative_to(self.root).as_posix()
                except ValueError:
                    continue

                target = brick_target(name)
                if target is not None:
                    real_rel = (
                        f"{Path(rel).parent.as_posix()}/{target}"
                        if Path(rel).parent.as_posix() != "." else target
                    )
                    report.placeholders.append(
                        Placeholder(
                            path=real_rel,
                            evidence=EVIDENCE_BRICK,
                            confidence=CONFIDENCE_CERTAIN,
                            reported_size=self._brick_size(full),
                            detail=(
                                "iCloud evicted this file's contents and left a "
                                f"'{name}' stub. The data is in iCloud, not on this disk."
                            ),
                        )
                    )
                    continue

                if is_local_artifact(rel):
                    continue

                report.files_checked += 1
                if self.check_dataless:
                    placeholder = self._check_dataless(full, rel)
                    if placeholder is not None:
                        report.placeholders.append(placeholder)

        report.placeholders.sort(key=lambda p: p.path)
        return report

    def _brick_size(self, stub: Path) -> Optional[int]:
        """Read the evicted file's real size out of the stub, if it says.

        The stub is a binary plist; rather than parse it, scan for the
        ``NSURLFileSizeKey`` integer that macOS records. Failure is fine - the
        stub's existence is the finding, its size is a bonus.
        """
        try:
            import plistlib

            with stub.open("rb") as handle:
                payload = plistlib.load(handle)
        except Exception:
            return None
        if isinstance(payload, dict):
            for key in ("NSURLFileSizeKey", "size", "FileSize"):
                value = payload.get(key)
                if isinstance(value, int):
                    return value
        return None

    def _check_dataless(self, path: Path, rel: str) -> Optional[Placeholder]:
        try:
            stat = path.stat()
        except OSError:
            return None

        if stat.st_size <= 0:
            return None
        blocks = getattr(stat, "st_blocks", None)
        if blocks is None or blocks > 0:
            return None

        return Placeholder(
            path=rel,
            evidence=EVIDENCE_DATALESS,
            confidence=CONFIDENCE_LIKELY,
            reported_size=stat.st_size,
            detail=(
                f"reports {human_bytes(stat.st_size)} but occupies no blocks on "
                "disk. Usually an iCloud-evicted file; a sparse file looks the "
                "same, so confirm before acting."
            ),
        )


# ---------------------------------------------------------------------------
# Missing files
# ---------------------------------------------------------------------------

MISSING_NEVER_DOWNLOADED = "never_downloaded"
MISSING_DISAPPEARED = "disappeared"
MISSING_PLACEHOLDER = "placeholder_only"


@dataclass
class MissingItem:
    path: str
    reason: str
    size: Optional[int] = None
    sha256: Optional[str] = None
    detail: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "path": self.path,
            "reason": self.reason,
            "size": self.size,
            "sha256": self.sha256,
            "detail": self.detail,
        }


@dataclass
class MissingReport:
    root: str
    items: List[MissingItem] = field(default_factory=list)
    generated_at: str = ""

    @property
    def total_bytes(self) -> int:
        return sum(i.size or 0 for i in self.items)

    def by_reason(self, reason: str) -> List[MissingItem]:
        return [i for i in self.items if i.reason == reason]

    def counts(self) -> Dict[str, int]:
        tally: Dict[str, int] = {}
        for item in self.items:
            tally[item.reason] = tally.get(item.reason, 0) + 1
        return tally

    def to_dict(self) -> Dict[str, Any]:
        return {
            "root": self.root,
            "generated_at": self.generated_at,
            "counts": self.counts(),
            "total_bytes": self.total_bytes,
            "items": [i.to_dict() for i in self.items],
        }


def build_missing_report(
    store: IndexStore,
    root: Path,
    placeholders: Optional[PlaceholderReport] = None,
) -> MissingReport:
    """Everything iCloud has that this disk does not really have.

    Three distinct kinds of missing, kept separate because they call for
    different actions:

    * **never downloaded** - in the remote scan, no local copy. Fetch it.
    * **disappeared** - a previous run recorded it and it is gone now. That is
      data loss on the local side, and worth knowing about before it is
      quietly re-fetched and forgotten.
    * **placeholder only** - present in name, evicted in fact. It counts
      towards a mirror's file count and towards nothing else.
    """
    report = MissingReport(
        root=str(root),
        generated_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    )

    local_paths = {row["path"] for row in store.iter_local()}
    placeholder_paths = {p.path: p for p in (placeholders.placeholders if placeholders else [])}

    for row in store.iter_remote():
        path = row["path"]
        on_disk = (root / path).exists()

        if path in placeholder_paths:
            report.items.append(
                MissingItem(
                    path=path, reason=MISSING_PLACEHOLDER, size=row["size"],
                    detail=placeholder_paths[path].detail,
                )
            )
        elif not on_disk and path in local_paths:
            report.items.append(
                MissingItem(
                    path=path, reason=MISSING_DISAPPEARED, size=row["size"],
                    detail="recorded by a previous run but no longer on disk",
                )
            )
        elif not on_disk:
            report.items.append(
                MissingItem(
                    path=path, reason=MISSING_NEVER_DOWNLOADED, size=row["size"],
                    detail="present in iCloud, never downloaded here",
                )
            )

    # A recorded file that vanished and is *also* gone from iCloud will not
    # appear above, because the remote loop never sees it. It is the most
    # alarming case of all - gone from both sides - so it is found separately.
    remote_paths = {row["path"] for row in store.iter_remote()}
    for row in store.iter_local():
        path = row["path"]
        if path in remote_paths or (root / path).exists():
            continue
        report.items.append(
            MissingItem(
                path=path, reason=MISSING_DISAPPEARED, size=row["size"],
                sha256=row["sha256"],
                detail=(
                    "recorded by a previous run, absent from disk AND from the "
                    "latest iCloud scan. If this was not an intentional delete, "
                    "restore it from .versions or an older snapshot."
                ),
            )
        )

    report.items.sort(key=lambda i: i.path)
    return report


# ---------------------------------------------------------------------------
# Storage inventory
# ---------------------------------------------------------------------------

@dataclass
class InventoryReport:
    """Where the space went, from the remote scan."""

    icloud_path: str = ""
    file_count: int = 0
    total_bytes: int = 0
    by_folder: List[Dict[str, Any]] = field(default_factory=list)
    by_extension: List[Dict[str, Any]] = field(default_factory=list)
    largest: List[Dict[str, Any]] = field(default_factory=list)
    account: Optional[Dict[str, Any]] = None
    generated_at: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "icloud_path": self.icloud_path,
            "generated_at": self.generated_at,
            "file_count": self.file_count,
            "total_bytes": self.total_bytes,
            "account": self.account,
            "by_folder": self.by_folder,
            "by_extension": self.by_extension,
            "largest": self.largest,
        }


def build_inventory(
    store: IndexStore,
    icloud_path: str = "",
    top: int = 25,
    depth: int = 1,
) -> InventoryReport:
    """Aggregate the remote inventory by folder, by type, and by size."""
    report = InventoryReport(
        icloud_path=icloud_path,
        generated_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    )

    folders: Dict[str, List[int]] = defaultdict(lambda: [0, 0])
    extensions: Dict[str, List[int]] = defaultdict(lambda: [0, 0])
    largest: List[Tuple[int, str, str]] = []

    for row in store.iter_remote():
        size = row["size"] or 0
        path = row["path"]
        report.file_count += 1
        report.total_bytes += size

        parts = path.split("/")
        folder = "/".join(parts[:depth]) if len(parts) > depth else "(root)"
        folders[folder][0] += 1
        folders[folder][1] += size

        suffix = Path(path).suffix.lower().lstrip(".") or "(no extension)"
        extensions[suffix][0] += 1
        extensions[suffix][1] += size

        largest.append((size, path, row["kind"]))

    report.by_folder = sorted(
        ({"folder": k, "files": v[0], "bytes": v[1]} for k, v in folders.items()),
        key=lambda d: d["bytes"], reverse=True,
    )
    report.by_extension = sorted(
        ({"extension": k, "files": v[0], "bytes": v[1]} for k, v in extensions.items()),
        key=lambda d: d["bytes"], reverse=True,
    )
    largest.sort(reverse=True)
    report.largest = [
        {"path": path, "bytes": size, "kind": kind} for size, path, kind in largest[:top]
    ]
    return report


def fetch_account_storage(api: Any) -> Optional[Dict[str, Any]]:
    """Read Apple's own quota figures, if the account exposes them.

    Entirely optional. Apple's storage endpoint is not part of the Drive API and
    can be absent or fail independently of everything else, so a failure here
    degrades the report rather than failing it.
    """
    try:
        usage = api.account.storage.usage
    except Exception:
        return None

    def read(name: str) -> Optional[int]:
        try:
            value = getattr(usage, name)
            return int(value) if value is not None else None
        except Exception:
            return None

    total = read("total_storage_in_bytes")
    used = read("used_storage_in_bytes")
    if total is None and used is None:
        return None

    payload: Dict[str, Any] = {
        "total_bytes": total,
        "used_bytes": used,
        "available_bytes": read("available_storage_in_bytes"),
    }
    if total:
        payload["used_percent"] = round((used or 0) * 100.0 / total, 2)
    return payload


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------

def render_placeholders(report: PlaceholderReport, show: int = 40) -> str:
    lines = [
        rule("="),
        "iFetch placeholder report",
        rule("="),
        key_values([
            ("Local path", report.root),
            ("Files checked", f"{report.files_checked:,}"),
            ("Placeholders found", f"{len(report.placeholders):,}"),
            ("Space they claim", human_bytes(report.total_reported_bytes)),
        ]),
        "",
    ]

    if report.signals_unavailable:
        lines.append("Coverage")
        for gap in report.signals_unavailable:
            lines.append(f"  ! '{gap['signal']}' detection unavailable: {gap['reason']}")
        lines.append("")

    if report.placeholders:
        rows = [
            [p.confidence, human_bytes(p.reported_size), p.evidence, p.path]
            for p in report.placeholders[:show]
        ]
        lines.append(table(
            ["confidence", "claims", "evidence", "path"], rows, align=["<", ">", "<", "<"]
        ))
        if len(report.placeholders) > show:
            lines.append(f"  ... and {len(report.placeholders) - show:,} more")
        lines.append("")
        lines.append(
            "These files are not really on this disk. Re-download them with "
            "'ifetch' before treating this directory as a complete backup."
        )
    else:
        found = "No placeholders found"
        if not report.complete:
            found += " by the signals available on this platform"
        lines.append(found + ".")

    lines.append(rule("="))
    return "\n".join(lines)


_MISSING_LABELS = {
    MISSING_NEVER_DOWNLOADED: "never downloaded",
    MISSING_DISAPPEARED: "disappeared",
    MISSING_PLACEHOLDER: "placeholder only",
}


def render_missing(report: MissingReport, show: int = 40) -> str:
    counts = report.counts()
    lines = [
        rule("="),
        "iFetch missing-file report",
        rule("="),
        key_values([
            ("Local path", report.root),
            ("Never downloaded", f"{counts.get(MISSING_NEVER_DOWNLOADED, 0):,}"),
            ("Disappeared locally", f"{counts.get(MISSING_DISAPPEARED, 0):,}"),
            ("Placeholders only", f"{counts.get(MISSING_PLACEHOLDER, 0):,}"),
            ("Total size", human_bytes(report.total_bytes)),
        ]),
        "",
    ]

    disappeared = report.by_reason(MISSING_DISAPPEARED)
    if disappeared:
        count = len(disappeared)
        lines.append(
            f"! {count:,} {plural(count, 'file')} recorded by an earlier run "
            f"{plural(count, 'is', 'are')} gone from this disk."
        )
        lines.append("")

    if report.items:
        rows = [
            [_MISSING_LABELS.get(i.reason, i.reason), human_bytes(i.size), i.path]
            for i in report.items[:show]
        ]
        lines.append(table(["reason", "size", "path"], rows, align=["<", ">", "<"]))
        if len(report.items) > show:
            lines.append(f"  ... and {len(report.items) - show:,} more")
    else:
        lines.append("Nothing missing - every scanned iCloud file is present locally.")

    lines.append(rule("="))
    return "\n".join(lines)


def render_inventory(report: InventoryReport, top: int = 15) -> str:
    lines = [
        rule("="),
        "iFetch iCloud storage inventory",
        rule("="),
        key_values([
            ("Remote path", report.icloud_path or "(root)"),
            ("Files", f"{report.file_count:,}"),
            ("Total size", human_bytes(report.total_bytes)),
        ]),
    ]

    if report.account:
        account = report.account
        lines.append("")
        lines.append("Apple account storage")
        pairs = [
            ("Used", human_bytes(account.get("used_bytes"))),
            ("Total", human_bytes(account.get("total_bytes"))),
            ("Available", human_bytes(account.get("available_bytes"))),
        ]
        if account.get("used_percent") is not None:
            pairs.append(("Used percent", f"{account['used_percent']}%"))
        lines.append(key_values(pairs))

    if report.by_folder:
        lines.append("")
        lines.append("Largest folders")
        lines.append(table(
            ["size", "files", "folder"],
            [[human_bytes(f["bytes"]), f"{f['files']:,}", f["folder"]]
             for f in report.by_folder[:top]],
            align=[">", ">", "<"],
        ))

    if report.by_extension:
        lines.append("")
        lines.append("By file type")
        lines.append(table(
            ["size", "files", "type"],
            [[human_bytes(e["bytes"]), f"{e['files']:,}", e["extension"]]
             for e in report.by_extension[:top]],
            align=[">", ">", "<"],
        ))

    if report.largest:
        lines.append("")
        lines.append("Largest files")
        lines.append(table(
            ["size", "path"],
            [[human_bytes(f["bytes"]), f["path"]] for f in report.largest[:top]],
            align=[">", "<"],
        ))

    lines.append(rule("="))
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# CSV export
# ---------------------------------------------------------------------------

def write_csv(path: Path, headers: Sequence[str], rows: Iterable[Sequence[Any]]) -> None:
    """Write a CSV export.

    CSV because the audience for an inventory is often a spreadsheet, and
    "which folders should I stop paying for?" is a sorting question.
    """
    import csv

    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(headers)
        writer.writerows(rows)
