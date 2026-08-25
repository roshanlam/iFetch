"""The dry-run planner: say exactly what a run would do, before it does it.

A download tool asks you to trust it with hours of transfer and an unknown
amount of disk. This module removes the guesswork: it answers how many files
would be fetched, how many bytes, whether the disk can hold them, what would be
overwritten, what is being skipped and why, and roughly how long it will take -
all from the index, with no transfer.

Honesty rules
-------------
Two things this module refuses to do, because a plan that overstates its
confidence is worse than no plan:

* **It never invents a throughput.** An estimate is produced only from measured
  throughput of previous runs, or a figure the user supplies. Otherwise the ETA
  is reported as unknown rather than as a number pulled from the air.
* **It never claims a package bundle is unchanged.** Apple's reported size for a
  bundle cannot be compared with its expanded size on disk, so bundles are
  listed as needing verification rather than silently counted as up to date.
"""

from __future__ import annotations

import json
import shutil
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

from .index import (
    DIFF_CHANGED,
    DIFF_LOCAL_ONLY,
    DIFF_NEW,
    DIFF_UNCHANGED,
    DIFF_UNKNOWN,
    KIND_PACKAGE,
    DiffEntry,
    IndexStore,
)
from .render import (
    colour,
    plural,
    human_bytes,
    human_duration,
    key_values,
    rule,
    supports_colour,
    table,
)

#: Index meta key holding measured bytes/second from completed runs.
THROUGHPUT_KEY = "measured_bytes_per_second"

#: Headroom kept free beyond the raw byte requirement. Filesystems need slack,
#: and a plan that fills the disk to the last byte is a plan that fails.
DISK_HEADROOM_FRACTION = 0.05
DISK_HEADROOM_MINIMUM = 64 * 1024 * 1024


@dataclass
class PlannedItem:
    """One file the plan has an opinion about."""

    path: str
    action: str
    size: Optional[int] = None
    reason: str = ""
    kind: str = "file"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "path": self.path,
            "action": self.action,
            "size": self.size,
            "kind": self.kind,
            "reason": self.reason,
        }


# Planned actions.
ACTION_DOWNLOAD = "download"
ACTION_OVERWRITE = "overwrite"
ACTION_SKIP = "skip"
ACTION_VERIFY = "verify"
ACTION_ORPHAN = "orphan"


@dataclass
class Plan:
    """The complete answer to "what would happen if I ran this?"."""

    icloud_path: str
    local_path: str
    items: List[PlannedItem] = field(default_factory=list)
    scanned_at: Optional[float] = None
    throughput_bytes_per_second: Optional[float] = None
    throughput_source: str = "unknown"
    disk_free_bytes: Optional[int] = None
    scan_errors: List[Dict[str, str]] = field(default_factory=list)

    # -- groupings ------------------------------------------------------
    def by_action(self, action: str) -> List[PlannedItem]:
        return [i for i in self.items if i.action == action]

    @property
    def downloads(self) -> List[PlannedItem]:
        return [i for i in self.items if i.action in (ACTION_DOWNLOAD, ACTION_OVERWRITE)]

    @property
    def bytes_to_transfer(self) -> int:
        return sum(i.size or 0 for i in self.downloads)

    @property
    def unknown_size_count(self) -> int:
        """Downloads whose size Apple did not report.

        Reported separately because they make ``bytes_to_transfer`` a lower
        bound rather than a total, and a user comparing it against free disk
        deserves to know that.
        """
        return sum(1 for i in self.downloads if i.size is None)

    @property
    def disk_required_bytes(self) -> int:
        """Bytes needed, plus headroom, minus what overwrites reclaim.

        An overwrite frees the old copy, but only after the new one is written,
        so the peak requirement is the full transfer size regardless.
        """
        raw = self.bytes_to_transfer
        return raw + max(int(raw * DISK_HEADROOM_FRACTION), DISK_HEADROOM_MINIMUM if raw else 0)

    @property
    def disk_sufficient(self) -> Optional[bool]:
        if self.disk_free_bytes is None:
            return None
        return self.disk_free_bytes >= self.disk_required_bytes

    @property
    def estimated_seconds(self) -> Optional[float]:
        rate = self.throughput_bytes_per_second
        if not rate or rate <= 0:
            return None
        return self.bytes_to_transfer / rate

    @property
    def has_risks(self) -> bool:
        return bool(
            self.by_action(ACTION_OVERWRITE)
            or self.by_action(ACTION_ORPHAN)
            or self.scan_errors
            or self.disk_sufficient is False
        )

    def counts(self) -> Dict[str, int]:
        tally: Dict[str, int] = {}
        for item in self.items:
            tally[item.action] = tally.get(item.action, 0) + 1
        return tally

    def to_dict(self, include_items: bool = True) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "icloud_path": self.icloud_path,
            "local_path": self.local_path,
            "scanned_at": self.scanned_at,
            "counts": self.counts(),
            "bytes_to_transfer": self.bytes_to_transfer,
            "unknown_size_count": self.unknown_size_count,
            "disk_required_bytes": self.disk_required_bytes,
            "disk_free_bytes": self.disk_free_bytes,
            "disk_sufficient": self.disk_sufficient,
            "estimated_seconds": self.estimated_seconds,
            "throughput_bytes_per_second": self.throughput_bytes_per_second,
            "throughput_source": self.throughput_source,
            "scan_errors": list(self.scan_errors),
            "has_risks": self.has_risks,
        }
        if include_items:
            payload["items"] = [i.to_dict() for i in self.items]
        return payload


class Planner:
    """Turn the index's diff into a plan a person can act on."""

    def __init__(
        self,
        store: IndexStore,
        local_path: Path,
        icloud_path: str = "",
        throughput_override: Optional[float] = None,
    ):
        self.store = store
        self.local_path = Path(local_path)
        self.icloud_path = icloud_path
        self.throughput_override = throughput_override

    def build(self, scan_errors: Optional[Sequence[Dict[str, str]]] = None) -> Plan:
        plan = Plan(
            icloud_path=self.icloud_path,
            local_path=str(self.local_path),
            scan_errors=list(scan_errors or []),
        )

        latest = self.store.latest_scan()
        plan.scanned_at = latest["finished_at"] if latest else None

        for entry in self.store.diff(include_unchanged=True):
            plan.items.append(self._classify(entry))

        rate, source = self._throughput()
        plan.throughput_bytes_per_second = rate
        plan.throughput_source = source
        plan.disk_free_bytes = self._free_disk()
        return plan

    def _classify(self, entry: DiffEntry) -> PlannedItem:
        if entry.status == DIFF_NEW:
            return PlannedItem(
                path=entry.path, action=ACTION_DOWNLOAD, size=entry.remote_size,
                kind=entry.kind, reason="not present locally",
            )
        if entry.status == DIFF_CHANGED:
            return PlannedItem(
                path=entry.path, action=ACTION_OVERWRITE, size=entry.remote_size,
                kind=entry.kind,
                reason=(
                    f"local copy is {human_bytes(entry.local_size)}, "
                    f"iCloud has {human_bytes(entry.remote_size)}; "
                    "the current copy is archived to .versions first"
                ),
            )
        if entry.status == DIFF_UNCHANGED:
            return PlannedItem(
                path=entry.path, action=ACTION_SKIP, size=entry.remote_size,
                kind=entry.kind, reason="same size as the local copy",
            )
        if entry.status == DIFF_LOCAL_ONLY:
            return PlannedItem(
                path=entry.path, action=ACTION_ORPHAN, size=entry.local_size,
                kind=entry.kind,
                reason=(
                    "present locally but not in iCloud - deleted remotely, or "
                    "never came from this iCloud path. iFetch never deletes it."
                ),
            )

        # DIFF_UNKNOWN: cannot be judged from metadata alone.
        if entry.kind == KIND_PACKAGE:
            reason = "package bundle; size cannot be compared, verify by digest"
        else:
            reason = "iCloud reported no size; will be checked over the network"
        return PlannedItem(
            path=entry.path, action=ACTION_VERIFY, size=entry.remote_size,
            kind=entry.kind, reason=reason,
        )

    def _throughput(self) -> tuple:
        if self.throughput_override:
            return float(self.throughput_override), "supplied by --throughput"
        recorded = self.store.get_meta(THROUGHPUT_KEY)
        if recorded:
            try:
                value = float(recorded)
                if value > 0:
                    return value, "measured from previous runs"
            except ValueError:
                pass
        return None, "unknown"

    def _free_disk(self) -> Optional[int]:
        target = self.local_path
        # A destination that does not exist yet still has a filesystem; walk up
        # to the nearest existing ancestor rather than reporting nothing.
        while not target.exists() and target != target.parent:
            target = target.parent
        try:
            return shutil.disk_usage(str(target)).free
        except (OSError, ValueError):
            return None


def record_throughput(store: IndexStore, bytes_transferred: int, seconds: float) -> None:
    """Fold a completed run's throughput into the stored estimate.

    An exponential moving average, so a single anomalous run (a stall, a burst
    off a fast link) shifts the estimate without dominating it.
    """
    if bytes_transferred <= 0 or seconds <= 0:
        return
    observed = bytes_transferred / seconds
    previous = store.get_meta(THROUGHPUT_KEY)
    try:
        blended = (0.7 * float(previous) + 0.3 * observed) if previous else observed
    except (TypeError, ValueError):
        blended = observed
    store.set_meta(THROUGHPUT_KEY, f"{blended:.2f}")


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------

_ACTION_LABELS = {
    ACTION_DOWNLOAD: ("download", "green"),
    ACTION_OVERWRITE: ("overwrite", "yellow"),
    ACTION_SKIP: ("skip", "dim"),
    ACTION_VERIFY: ("verify", "cyan"),
    ACTION_ORPHAN: ("local only", "blue"),
}


def render_plan(plan: Plan, show_files: int = 20, use_colour: Optional[bool] = None) -> str:
    """Render a plan as a terminal report."""
    tint = supports_colour() if use_colour is None else use_colour
    counts = plan.counts()
    out: List[str] = [
        rule("="),
        colour("iFetch migration plan (dry run - nothing will be transferred)", "bold", tint),
        rule("="),
    ]

    scanned = (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(plan.scanned_at))
        if plan.scanned_at else "never"
    )
    out.append(key_values([
        ("Remote path", plan.icloud_path or "(root)"),
        ("Local path", plan.local_path),
        ("Scanned", scanned),
    ]))
    out.append("")

    summary = [
        ("New downloads", f"{counts.get(ACTION_DOWNLOAD, 0):,}"),
        ("Overwrites", f"{counts.get(ACTION_OVERWRITE, 0):,}"),
        ("Needs verification", f"{counts.get(ACTION_VERIFY, 0):,}"),
        ("Skipped (unchanged)", f"{counts.get(ACTION_SKIP, 0):,}"),
        ("Local only", f"{counts.get(ACTION_ORPHAN, 0):,}"),
    ]
    out.append(colour("What would happen", "bold", tint))
    out.append(key_values(summary))
    out.append("")

    transfer_line = human_bytes(plan.bytes_to_transfer)
    if plan.unknown_size_count:
        transfer_line += f"  (at least - {plan.unknown_size_count:,} files have no reported size)"

    disk = [
        ("To transfer", transfer_line),
        ("Disk required", human_bytes(plan.disk_required_bytes) + "  (includes headroom)"),
        ("Disk free", human_bytes(plan.disk_free_bytes)),
    ]
    if plan.disk_sufficient is False:
        shortfall = plan.disk_required_bytes - (plan.disk_free_bytes or 0)
        disk.append((
            "VERDICT",
            colour(f"NOT ENOUGH SPACE - short by {human_bytes(shortfall)}", "red", tint),
        ))
    elif plan.disk_sufficient:
        disk.append(("Verdict", colour("enough free space", "green", tint)))

    estimate = human_duration(plan.estimated_seconds)
    if plan.estimated_seconds is None:
        estimate += f"  ({plan.throughput_source}; pass --throughput to estimate)"
    else:
        estimate += f"  ({plan.throughput_source})"
    disk.append(("Estimated time", estimate))

    out.append(colour("Cost", "bold", tint))
    out.append(key_values(disk))
    out.append("")

    risks: List[str] = []
    overwrites = len(plan.by_action(ACTION_OVERWRITE))
    if overwrites:
        risks.append(
            f"{overwrites:,} local {plural(overwrites, 'file')} would be replaced "
            "(previous copies are archived to .versions first)"
        )
    orphans = len(plan.by_action(ACTION_ORPHAN))
    if orphans:
        risks.append(
            f"{orphans:,} local {plural(orphans, 'file')} "
            f"{plural(orphans, 'is', 'are')} not in iCloud "
            "(iFetch never deletes them - listed for your awareness)"
        )
    if plan.scan_errors:
        count = len(plan.scan_errors)
        risks.append(
            f"{count:,} {plural(count, 'folder')} could not be listed - "
            f"{plural(count, 'its', 'their')} contents are missing from this plan"
        )
    if plan.disk_sufficient is False:
        risks.append("the destination does not have enough free space")

    if risks:
        out.append(colour("Risks", "bold", tint))
        out.extend(f"  ! {risk}" for risk in risks)
        out.append("")

    if show_files:
        affected = sorted(
            (i for i in plan.items if i.action != ACTION_SKIP),
            key=lambda i: (i.size or 0),
            reverse=True,
        )
        shown = affected[:show_files]
        if shown:
            out.append(colour("Files", "bold", tint))
            rows = []
            for item in shown:
                label, tone = _ACTION_LABELS.get(item.action, (item.action, "reset"))
                rows.append([colour(label, tone, tint), human_bytes(item.size), item.path])
            out.append(table(["action", "size", "path"], rows, align=["<", ">", "<"]))
            remaining = len(affected) - len(shown)
            if remaining > 0:
                out.append(f"  ... and {remaining:,} more (use --show-files 0 for all, or --json)")
            out.append("")

    out.append(rule("="))
    if not plan.downloads:
        out.append("Nothing to transfer - the local copy is already up to date.")
    else:
        count = len(plan.downloads)
        out.append(
            f"Would transfer {count:,} {plural(count, 'file')} "
            f"({human_bytes(plan.bytes_to_transfer)}). Run the same command with "
            "'ifetch' instead of 'ifetch plan' to start."
        )
    return "\n".join(out)


def render_audit(plan: Plan, show_files: int = 40, use_colour: Optional[bool] = None) -> str:
    """Render the same data as a 'what exists remotely vs locally' report."""
    tint = supports_colour() if use_colour is None else use_colour
    counts = plan.counts()

    remote_total = len([i for i in plan.items if i.action != ACTION_ORPHAN])
    local_total = len([
        i for i in plan.items
        if i.action in (ACTION_SKIP, ACTION_OVERWRITE, ACTION_ORPHAN, ACTION_VERIFY)
    ])

    out = [
        rule("="),
        colour("iFetch remote vs local audit (read-only)", "bold", tint),
        rule("="),
        key_values([
            ("Remote path", plan.icloud_path or "(root)"),
            ("Local path", plan.local_path),
            ("Items in iCloud", f"{remote_total:,}"),
            ("Items on disk", f"{local_total:,}"),
        ]),
        "",
        colour("Reconciliation", "bold", tint),
        key_values([
            ("In both, matching", f"{counts.get(ACTION_SKIP, 0):,}"),
            ("In both, differing", f"{counts.get(ACTION_OVERWRITE, 0):,}"),
            ("Missing locally", f"{counts.get(ACTION_DOWNLOAD, 0):,}"),
            ("Missing in iCloud", f"{counts.get(ACTION_ORPHAN, 0):,}"),
            ("Undetermined", f"{counts.get(ACTION_VERIFY, 0):,}"),
        ]),
        "",
    ]

    interesting = [
        i for i in plan.items
        if i.action in (ACTION_DOWNLOAD, ACTION_OVERWRITE, ACTION_ORPHAN)
    ]
    if interesting and show_files:
        out.append(colour("Discrepancies", "bold", tint))
        rows = []
        for item in interesting[:show_files]:
            label = {
                ACTION_DOWNLOAD: "missing locally",
                ACTION_OVERWRITE: "differs",
                ACTION_ORPHAN: "missing in iCloud",
            }[item.action]
            tone = _ACTION_LABELS[item.action][1]
            rows.append([colour(label, tone, tint), human_bytes(item.size), item.path])
        out.append(table(["status", "size", "path"], rows, align=["<", ">", "<"]))
        if len(interesting) > show_files:
            out.append(f"  ... and {len(interesting) - show_files:,} more")
        out.append("")

    out.append(rule("="))
    found = len(interesting)
    out.append(
        "Local copy matches iCloud." if not found
        else f"{found:,} {plural(found, 'discrepancy', 'discrepancies')} found."
    )
    return "\n".join(out)
