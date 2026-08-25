"""Files that have disappeared **from iCloud**, and how long you have left.

Every other report in iFetch asks what your local copy is missing. This one asks
the opposite: what did iCloud have last time that it does not have now?

Because iFetch only ever downloads, its copy keeps everything it has seen.
Comparing that against a fresh listing is the only way most people will find out
that a bad sync, a family member, a compromised account or one of Apple's own
bugs quietly removed a file. iCloud empties the Trash after about 30 days, so
there is a clock on it: found in week one it is a two-click restore, found in
week six the cloud copy is gone.

Four kinds of disappearance
---------------------------
"Gone from iCloud" is not one finding. Each of these needs a different response:

``safe_local_copy``
    Gone from iCloud, still on this disk. Nothing lost. Where a checksum was
    recorded it is quoted, because "a file of that name exists" and "what we
    downloaded is still here" are different claims.
``lost``
    Gone from iCloud and not on this disk. Real data loss, and the only class
    this module raises its voice about.
``placeholder_only``
    Gone from iCloud, and the local copy is an empty shell - full size in
    Finder, no contents. Looks like the safe case, behaves like the lost one.
    :class:`ifetch.recovery.PlaceholderDetector` decides this.
``ambiguous``
    It may not have vanished at all - it may have been renamed or moved.
    :mod:`ifetch.conflicts` says which of those it can prove. A proven move is
    not reported as a deletion; an unprovable one is listed with its
    alternatives rather than settled by a guess.

The deadline is a latest-possible date
--------------------------------------
iFetch does not know when a file was deleted, only when it first *noticed* it
missing - which is some time after, possibly long after. So the purge happens at
or before that, plus the retention window, and every time this module prints is
that upper bound, described in those words. "Your file will be purged on the
14th" would be a guess, and wrong in the direction that costs you the file.

The first sighting is saved per path (``IndexStore.note_missing``) so the bound
is the earliest ever recorded rather than resetting to today on every run. It
only ever moves earlier. A file that reappears has its record cleared, so a
second disappearance starts a fresh clock.

Why a big disappearance is not reported as a big deletion
---------------------------------------------------------
If a large share of the drive vanishes at once, a listing failure, a partial
walk, an expired token or a drive that never mounted are all likelier than a
real deletion. Above a threshold - a count and a percentage, both configurable -
this module refuses to present the results as findings, and names what it cannot
rule out.

The same refusal covers evidence that is simply broken: a scan that recorded
listing errors, one that was never finished, or one that returned nothing cannot
support a deletion claim at any count. A folder that failed to list contributes
no rows, and missing rows look exactly like deleted files from here. That is why
:meth:`IndexStore.finish_scan` records the failure count at all.

What it will not do
-------------------
It does not delete, restore, re-download or touch iCloud. It reports. The one
thing it writes is the sighting timestamp above, because not remembering would
make every deadline it prints vaguer than the evidence allows; ``record=False``
turns even that off.
"""

from __future__ import annotations

import time
import unicodedata
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

from .conflicts import (
    CONFIDENCE_PROVEN,
    ConflictError,
    MovedFile,
    RenameCandidate,
    detect_moves,
    detect_renames,
)
from .index import KIND_FILE, KIND_PACKAGE, IndexStore
from .recovery import PlaceholderReport
from .render import human_bytes, key_values, plural, rule, say_once, table

#: How long iCloud keeps a deleted item in Trash before purging it. Apple
#: documents "up to 30 days" and does not publish a guarantee, so this is used
#: only to derive an upper bound and never to promise a file is still there.
TRASH_RETENTION_DAYS = 30
_DAY = 86400.0

#: Classifications. Each one calls for a different action, which is the whole
#: reason they are kept apart.
CLASS_SAFE = "safe_local_copy"
CLASS_LOST = "lost"
CLASS_PLACEHOLDER = "placeholder_only"
CLASS_AMBIGUOUS = "ambiguous"

#: Where the "what iCloud used to have" evidence came from.
BASELINE_INDEX = "index"
BASELINE_SNAPSHOT = "snapshot"

#: Why the circuit breaker tripped.
BREAKER_NONE = ""
BREAKER_COUNT = "absolute_count"
BREAKER_FRACTION = "fraction_of_baseline"
BREAKER_SCAN = "unusable_scan"

#: Defaults for the breaker. 500 files or a quarter of the baseline vanishing in
#: one run is far outside what an ordinary week of deletions looks like;
#: ``min_baseline`` keeps a three-file test directory from tripping the
#: proportional rule on a single legitimate delete.
DEFAULT_MAX_VANISHED = 500
DEFAULT_MAX_FRACTION = 0.25
DEFAULT_MIN_BASELINE = 20

#: Ordering for output: the findings that cost the user something come first.
_CLASS_RANK = {
    CLASS_LOST: 0,
    CLASS_PLACEHOLDER: 1,
    CLASS_AMBIGUOUS: 2,
    CLASS_SAFE: 3,
}

CLASS_LABELS = {
    CLASS_LOST: "LOST",
    CLASS_PLACEHOLDER: "placeholder",
    CLASS_AMBIGUOUS: "ambiguous",
    CLASS_SAFE: "safe copy",
}

#: Explanations for a mass disappearance that iFetch cannot distinguish from a
#: real one. Listed in full rather than summarised, because "something is wrong"
#: is not actionable and "it might be any of these six things" is.
MASS_ALTERNATIVES = [
    "a directory listing that failed, or returned only part of its contents",
    "an expired token or a failed sign-in, which can make iCloud answer with an "
    "empty or truncated drive rather than an error",
    "the network dropping partway through the scan",
    "an external disk or network mount that had not come up when the scan ran",
    "a scan taken against a different iCloud folder than the baseline was "
    "recorded from",
    "a genuine mass deletion - which is precisely why this is being shown to "
    "you instead of filed as an ordinary finding",
]


class VanishedError(Exception):
    """The disappearance analysis could not be carried out."""


def _nfc(path: str) -> str:
    """Normalisation key for path comparison.

    Apple returns filenames in NFD and a mirror may hold NFC, so a byte
    comparison reports every accented filename on the drive as deleted. Case is
    deliberately *not* folded in: two names differing only in case are two files
    on a case-sensitive filesystem, and merging them here would hide a real
    deletion behind an unrelated file.
    """
    return unicodedata.normalize("NFC", path)


def _iso(timestamp: Optional[float]) -> str:
    if timestamp is None:
        return "unknown"
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(timestamp))


# ---------------------------------------------------------------------------
# Is the evidence usable at all?
# ---------------------------------------------------------------------------

@dataclass
class ScanEvidence:
    """The health of the scan this report would be drawn from.

    ``usable`` is False whenever the scan cannot support a claim that something
    was deleted. Every reason is named in ``problems``; an empty ``problems``
    with ``usable`` False is not a state this produces.
    """

    usable: bool = False
    scan_id: Optional[int] = None
    icloud_path: str = ""
    finished_at: Optional[float] = None
    item_count: int = 0
    error_count: int = 0
    errors: List[Dict[str, str]] = field(default_factory=list)
    problems: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "usable": self.usable,
            "scan_id": self.scan_id,
            "icloud_path": self.icloud_path,
            "finished_at": self.finished_at,
            "finished_at_iso": _iso(self.finished_at),
            "item_count": self.item_count,
            "error_count": self.error_count,
            "errors": list(self.errors),
            "problems": list(self.problems),
        }


def assess_scan(store: IndexStore) -> ScanEvidence:
    """Decide whether the latest scan can be read as "what iCloud contains".

    Four ways it cannot, all of which look identical to a deletion if nobody
    checks: there is no finished scan; the finished scan recorded listing
    failures; a later scan was started and died; or the scan came back empty.
    """
    latest = store.latest_scan()
    if latest is None:
        return ScanEvidence(problems=[
            "no completed iCloud scan is recorded in this index, so there is "
            "nothing to compare against. Run with --refresh (needs credentials) "
            "or run 'ifetch plan' first."
        ])

    evidence = ScanEvidence(
        scan_id=int(latest["id"]),
        icloud_path=latest.get("icloud_path") or "",
        finished_at=latest.get("finished_at"),
        item_count=int(latest.get("item_count") or 0),
        error_count=int(latest.get("error_count") or 0),
        errors=store.scan_errors(int(latest["id"])),
    )

    if evidence.error_count:
        count = evidence.error_count
        evidence.problems.append(
            f"the scan behind this report recorded {count:,} listing "
            f"{plural(count, 'failure')}. Every file under a folder that failed "
            "to list is absent from the inventory, and an absent file and a "
            "deleted file are the same thing from here. Re-scan before treating "
            "anything below as a deletion."
        )

    if evidence.item_count <= 0:
        evidence.problems.append(
            "the completed scan recorded no items at all. An account that is "
            "genuinely empty and a sign-in that silently returned nothing look "
            "identical, so this scan is evidence of nothing."
        )

    for row in store.unfinished_scans(after=evidence.scan_id):
        evidence.problems.append(
            f"a scan of {row.get('icloud_path') or '(root)'!r} started at "
            f"{_iso(row.get('started_at'))} and never finished. It has already "
            "overwritten part of the remote inventory, so what is in the index "
            "now is a truncated listing, not a complete one."
        )

    evidence.usable = not evidence.problems
    return evidence


# ---------------------------------------------------------------------------
# The circuit breaker
# ---------------------------------------------------------------------------

@dataclass
class BreakerVerdict:
    """Whether the disappearances may be reported as ordinary findings."""

    tripped: bool = False
    reason: str = BREAKER_NONE
    vanished_count: int = 0
    baseline_count: int = 0
    max_count: int = DEFAULT_MAX_VANISHED
    max_fraction: float = DEFAULT_MAX_FRACTION
    min_baseline: int = DEFAULT_MIN_BASELINE
    detail: str = ""
    cannot_rule_out: List[str] = field(default_factory=list)

    @property
    def fraction(self) -> float:
        if self.baseline_count <= 0:
            return 0.0
        return self.vanished_count / float(self.baseline_count)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "tripped": self.tripped,
            "reason": self.reason,
            "vanished_count": self.vanished_count,
            "baseline_count": self.baseline_count,
            "fraction": round(self.fraction, 6),
            "max_count": self.max_count,
            "max_fraction": self.max_fraction,
            "min_baseline": self.min_baseline,
            "detail": self.detail,
            "cannot_rule_out": list(self.cannot_rule_out),
        }


def check_breaker(
    vanished_count: int,
    baseline_count: int,
    scan: ScanEvidence,
    max_count: int = DEFAULT_MAX_VANISHED,
    max_fraction: float = DEFAULT_MAX_FRACTION,
    min_baseline: int = DEFAULT_MIN_BASELINE,
) -> BreakerVerdict:
    """Refuse to call a suspicious result a set of deletions.

    Order matters. Broken evidence is checked *before* volume, so a scan that
    failed partway is never described as a mass deletion - it is described as a
    scan that failed, which is what it is. Only then is volume considered, and
    both the absolute count and the proportion trip at ``>=`` their threshold:
    the thresholds are the point at which a result stops being ordinary, not the
    point after it.
    """
    verdict = BreakerVerdict(
        vanished_count=vanished_count,
        baseline_count=baseline_count,
        max_count=int(max_count),
        max_fraction=float(max_fraction),
        min_baseline=int(min_baseline),
    )

    if not scan.usable:
        verdict.tripped = True
        verdict.reason = BREAKER_SCAN
        verdict.detail = (
            "the scan this report would be drawn from is not usable evidence, "
            "so nothing below can be called a deletion. " + " ".join(scan.problems)
        )
        verdict.cannot_rule_out = list(scan.problems) + [
            "that the files are all still in iCloud and simply were not listed"
        ]
        return verdict

    if vanished_count <= 0:
        return verdict

    if vanished_count >= verdict.max_count:
        verdict.tripped = True
        verdict.reason = BREAKER_COUNT
        verdict.detail = (
            f"{vanished_count:,} files disappeared in a single run, at or above "
            f"the limit of {verdict.max_count:,} beyond which iFetch stops "
            "treating disappearances as ordinary findings."
        )
    elif (
        baseline_count >= verdict.min_baseline
        and verdict.fraction >= verdict.max_fraction
    ):
        verdict.tripped = True
        verdict.reason = BREAKER_FRACTION
        verdict.detail = (
            f"{vanished_count:,} of {baseline_count:,} known files "
            f"({verdict.fraction * 100:.1f}%) disappeared in a single run, at or "
            f"above the limit of {verdict.max_fraction * 100:.1f}%."
        )

    if verdict.tripped:
        verdict.cannot_rule_out = list(MASS_ALTERNATIVES)
    return verdict


# ---------------------------------------------------------------------------
# Findings
# ---------------------------------------------------------------------------

@dataclass
class VanishedItem:
    """One path the baseline knew about and the current iCloud listing does not."""

    path: str
    classification: str
    kind: str = KIND_FILE
    size: Optional[int] = None
    sha256: Optional[str] = None
    first_missing_at: Optional[float] = None
    purge_deadline_before: Optional[float] = None
    evidence: str = ""
    detail: str = ""
    alternatives: List[str] = field(default_factory=list)

    #: Always true, and always carried into the payload: a consumer of the JSON
    #: must not be able to read the deadline as a scheduled date by accident.
    deadline_is_upper_bound: bool = True

    def days_remaining_at_most(self, now: float) -> Optional[float]:
        """Days until the cloud copy is certainly gone. Never a promise it lives."""
        if self.purge_deadline_before is None:
            return None
        return (self.purge_deadline_before - now) / _DAY

    def to_dict(self) -> Dict[str, Any]:
        return {
            "path": self.path,
            "classification": self.classification,
            "kind": self.kind,
            "size": self.size,
            "sha256": self.sha256,
            "first_observed_missing_at": self.first_missing_at,
            "first_observed_missing_at_iso": _iso(self.first_missing_at),
            "purge_deadline_before": self.purge_deadline_before,
            "purge_deadline_before_iso": _iso(self.purge_deadline_before),
            "deadline_is_upper_bound": self.deadline_is_upper_bound,
            "deadline_basis": (
                "deletion happened at or before the first observed absence, so "
                "the Trash purge happens at or before this instant"
            ),
            "evidence": self.evidence,
            "detail": self.detail,
            "alternatives": list(self.alternatives),
        }


@dataclass
class VanishedReport:
    """What is gone from iCloud, what is still safe, and what could not be told."""

    root: str = ""
    baseline_kind: str = BASELINE_INDEX
    baseline_label: str = ""
    baseline_count: int = 0
    has_baseline: bool = False
    items: List[VanishedItem] = field(default_factory=list)
    moved: List[Dict[str, Any]] = field(default_factory=list)
    scan: ScanEvidence = field(default_factory=ScanEvidence)
    breaker: BreakerVerdict = field(default_factory=BreakerVerdict)
    notes: List[str] = field(default_factory=list)
    unexamined: List[Dict[str, str]] = field(default_factory=list)
    retention_days: int = TRASH_RETENTION_DAYS
    now: float = 0.0
    generated_at: str = ""

    @property
    def refused(self) -> bool:
        """True when the findings must not be presented as deletions."""
        return self.breaker.tripped

    @property
    def baseline_description(self) -> str:
        if not self.has_baseline:
            return "none"
        if self.baseline_kind == BASELINE_SNAPSHOT:
            return f"snapshot '{self.baseline_label}'"
        return "the local index"

    def by_class(self, classification: str) -> List[VanishedItem]:
        return [i for i in self.items if i.classification == classification]

    def counts(self) -> Dict[str, int]:
        tally: Dict[str, int] = {}
        for item in self.items:
            tally[item.classification] = tally.get(item.classification, 0) + 1
        return tally

    @property
    def total_bytes(self) -> int:
        """Bytes of the vanished files that have a recorded size. A floor."""
        return sum(i.size or 0 for i in self.items)

    @property
    def sizeless(self) -> int:
        return sum(1 for i in self.items if i.size is None)

    @property
    def soonest_deadline(self) -> Optional[float]:
        bounds = [i.purge_deadline_before for i in self.items
                  if i.purge_deadline_before is not None]
        return min(bounds) if bounds else None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "root": self.root,
            "generated_at": self.generated_at,
            "baseline": {
                "kind": self.baseline_kind,
                "label": self.baseline_label,
                "description": self.baseline_description,
                "count": self.baseline_count,
                "present": self.has_baseline,
            },
            "scan": self.scan.to_dict(),
            "breaker": self.breaker.to_dict(),
            "refused": self.refused,
            "trash_retention_days": self.retention_days,
            "deadlines_are_upper_bounds": True,
            "counts": self.counts(),
            "total_bytes": self.total_bytes,
            "files_without_a_recorded_size": self.sizeless,
            "soonest_purge_deadline_before": self.soonest_deadline,
            "soonest_purge_deadline_before_iso": _iso(self.soonest_deadline),
            "items": [i.to_dict() for i in self.items],
            "moved_not_deleted": list(self.moved),
            "notes": list(self.notes),
            "unexamined": list(self.unexamined),
        }


# ---------------------------------------------------------------------------
# Baselines
# ---------------------------------------------------------------------------

def collect_baseline(
    store: IndexStore, since: Optional[str] = None
) -> Tuple[str, str, List[Dict[str, Any]]]:
    """The record of what iCloud used to hold, and where it came from.

    Two sources, and the difference matters when reading the result:

    * **the local index** - every path iFetch has ever recorded on disk. Broad,
      always available, and it cannot distinguish a file iCloud deleted from a
      file that was never in iCloud to begin with. That caveat is reported.
    * **a named snapshot** - the mirror frozen at a known moment. Narrower and
      dated, which makes "since March" answerable.
    """
    if since:
        try:
            rows = store.snapshot_entries(since)
        except KeyError as exc:
            raise VanishedError(
                f"no snapshot named {since!r}. Run 'ifetch snapshot list' to see "
                "which ones exist."
            ) from exc
        return BASELINE_SNAPSHOT, since, rows

    rows = [
        {
            "path": row["path"], "kind": row["kind"],
            "size": row["size"], "sha256": row["sha256"],
        }
        for row in store.iter_local()
    ]
    return BASELINE_INDEX, "", rows


def _remote_keys(store: IndexStore) -> Dict[str, str]:
    """Normalisation key -> the path iCloud actually reported it under."""
    return {
        _nfc(row["path"]): row["path"]
        for row in store.iter_remote(include_dirs=True)
    }


# ---------------------------------------------------------------------------
# The analysis
# ---------------------------------------------------------------------------

def analyse(
    store: IndexStore,
    root: Path,
    since: Optional[str] = None,
    placeholders: Optional[PlaceholderReport] = None,
    check_renames: bool = True,
    record: bool = True,
    max_count: int = DEFAULT_MAX_VANISHED,
    max_fraction: float = DEFAULT_MAX_FRACTION,
    min_baseline: int = DEFAULT_MIN_BASELINE,
    now: Optional[float] = None,
) -> VanishedReport:
    """Classify everything the baseline knew about and iCloud no longer lists.

    Nothing is downloaded, deleted or restored. The single write is the
    first-observed-missing timestamp, and ``record=False`` suppresses it at the
    cost of a purge bound that resets on every run.
    """
    now = time.time() if now is None else float(now)
    root = Path(root)

    scan = assess_scan(store)
    baseline_kind, baseline_label, baseline = collect_baseline(store, since)

    report = VanishedReport(
        root=str(root),
        baseline_kind=baseline_kind,
        baseline_label=baseline_label,
        baseline_count=len(baseline),
        has_baseline=bool(baseline),
        scan=scan,
        now=now,
        generated_at=_iso(now),
    )

    # The absence was observed by the scan, not by this command. Using the
    # scan's finish time rather than the wall clock is what lets the bound
    # tighten when a report is run against a scan taken days earlier.
    observed_at = min(now, scan.finished_at) if scan.finished_at else now

    remote = _remote_keys(store)
    gone: List[Dict[str, Any]] = []
    returned: List[str] = []
    normalisation_matches = 0

    for row in baseline:
        key = _nfc(row["path"])
        if key in remote:
            returned.append(row["path"])
            if remote[key] != row["path"]:
                normalisation_matches += 1
            continue
        gone.append(row)

    if record and returned:
        # A file that came back must not keep the clock it started last time.
        store.forget_missing(returned)

    if normalisation_matches:
        report.notes.append(
            f"{normalisation_matches:,} baseline {plural(normalisation_matches, 'path')} "
            "matched an iCloud path only after Unicode normalisation (Apple "
            "returns NFD, the mirror may hold NFC). They are the same files and "
            "are not reported as vanished."
        )

    proven_moves, ambiguous_moves = _moves(store, since, report)
    renames = _renames(store, report) if check_renames else {}

    if not report.has_baseline:
        report.notes.append(
            "No baseline. This index holds no earlier record of what iCloud "
            "contained, so nothing can be said to have vanished - which is not "
            "the same as nothing having vanished."
        )
        # No baseline means no findings, and refusing to report findings that do
        # not exist would be theatre. The scan's problems are still stated,
        # because they are part of why this answer is thin.
        for problem in scan.problems:
            report.unexamined.append({
                "what": "the iCloud scan", "count": "1", "why": problem,
            })
        return report

    placeholder_paths = _placeholder_index(placeholders)
    if placeholders is None:
        report.unexamined.append({
            "what": "placeholder detection",
            "count": "all",
            "why": (
                "it was not run, so a local copy reported as intact may be an "
                "iCloud-evicted shell with no bytes underneath. Pass a "
                "PlaceholderReport, or run 'ifetch recover placeholders'."
            ),
        })
    else:
        for gap in placeholders.signals_unavailable:
            report.unexamined.append({
                "what": f"placeholder signal '{gap['signal']}'",
                "count": "all",
                "why": gap["reason"],
            })

    no_digest = 0
    packages = 0

    for row in gone:
        path = row["path"]
        if path in proven_moves:
            move = proven_moves[path]
            report.moved.append(move.to_dict())
            continue

        kind = row.get("kind") or KIND_FILE
        if kind == KIND_PACKAGE:
            packages += 1

        item = VanishedItem(
            path=path,
            classification=CLASS_LOST,
            kind=kind,
            size=row.get("size"),
            sha256=row.get("sha256"),
        )

        present, failure = _on_disk(root, path)
        if failure is not None:
            report.unexamined.append({
                "what": path,
                "count": "1",
                "why": (
                    f"the local copy could not be checked ({failure}), so "
                    "whether the bytes are still on this disk is unknown"
                ),
            })

        if path in ambiguous_moves or path in renames:
            _as_ambiguous(item, ambiguous_moves.get(path), renames.get(path))
        elif _nfc(path) in placeholder_paths:
            placeholder = placeholder_paths[_nfc(path)]
            item.classification = CLASS_PLACEHOLDER
            item.evidence = f"placeholder evidence: {placeholder.evidence}"
            item.detail = (
                "gone from iCloud, and the local copy is an evicted placeholder "
                "- it looks like a file in Finder and holds no bytes. "
                + placeholder.detail
            )
        elif present:
            item.classification = CLASS_SAFE
            if item.sha256:
                item.evidence = f"sha256:{item.sha256}"
                item.detail = (
                    "gone from iCloud; the mirror still holds these bytes and "
                    "the recorded digest identifies them."
                )
            else:
                no_digest += 1
                item.evidence = "a file exists at this path; no digest recorded"
                item.detail = (
                    "gone from iCloud; a local file exists at this path, but no "
                    "digest was recorded for it, so iFetch can say it is there "
                    "and not that it is intact. Run 'ifetch verify' to settle it."
                )
        elif failure is not None:
            _as_ambiguous(item, None, None)
            item.detail = (
                "gone from iCloud, and the local copy could not be read, so "
                "whether anything was lost is unknown."
            )
        else:
            item.classification = CLASS_LOST
            item.evidence = "absent from the iCloud listing and absent from this disk"
            item.detail = (
                "gone from iCloud and not on this disk. If this was not an "
                "intentional delete, recover it from iCloud Trash before the "
                "purge bound below, or from '.versions' or an older snapshot."
            )

        report.items.append(item)

    _apply_deadlines(store, report, observed_at, record, scan.scan_id)

    report.items.sort(key=lambda i: (_CLASS_RANK.get(i.classification, 9), i.path))
    report.moved.sort(key=lambda m: m["old_path"])

    if no_digest:
        report.unexamined.append({
            "what": "local copies without a recorded digest",
            "count": str(no_digest),
            "why": (
                "they were reported as safe because a file exists at the path, "
                "which is weaker than proof that the downloaded bytes survive"
            ),
        })
    if packages:
        report.notes.append(
            f"{packages:,} vanished {plural(packages, 'item')} "
            f"{plural(packages, 'is', 'are')} a package bundle. Apple's reported "
            "size for a bundle is not its expanded size on disk, so the sizes "
            "below are not comparable with a local directory total."
        )
    if report.sizeless:
        report.notes.append(
            f"{report.sizeless:,} vanished {plural(report.sizeless, 'file')} had "
            "no size recorded by iCloud, so the byte total is a floor, not a total."
        )
    if baseline_kind == BASELINE_INDEX and report.items:
        report.notes.append(
            "The baseline is the local index, which records every file iFetch "
            "has seen on disk. A file that was never in iCloud looks exactly "
            "like one iCloud deleted; use --since <snapshot> for a dated "
            "baseline that does not have that ambiguity."
        )

    _coverage_notes(report, since)

    report.breaker = check_breaker(
        len(report.items), report.baseline_count, scan,
        max_count, max_fraction, min_baseline,
    )
    return report


def _on_disk(root: Path, path: str) -> Tuple[bool, Optional[str]]:
    """Is the local copy there? Returns presence and, if not knowable, why not."""
    try:
        return (root / path).exists(), None
    except OSError as exc:  # pragma: no cover - defensive, needs a broken mount
        return False, str(exc)


def _placeholder_index(
    placeholders: Optional[PlaceholderReport],
) -> Dict[str, Any]:
    if placeholders is None:
        return {}
    return {_nfc(p.path): p for p in placeholders.placeholders}


def _as_ambiguous(
    item: VanishedItem,
    move: Optional[MovedFile],
    rename: Optional[RenameCandidate],
) -> None:
    """Record a disappearance that may not be one, naming every alternative."""
    item.classification = CLASS_AMBIGUOUS
    if move is not None:
        item.alternatives = list(move.alternatives) or [move.new_path]
        item.evidence = move.detail or "identical digest found at another path"
        item.detail = (
            "this path is absent from iCloud, but these exact bytes now exist "
            f"at {len(item.alternatives):,} other "
            f"{plural(len(item.alternatives), 'path')}, so a copy or a move "
            "fits the evidence as well as a deletion does. iFetch will not "
            "guess which."
        )
        return
    if rename is not None:
        item.alternatives = list(rename.alternatives) or [rename.new_path]
        item.evidence = rename.evidence
        item.detail = (
            "this path is absent from iCloud, and a file that iCloud is now "
            f"offering at {item.alternatives[0]!r} could be the same file "
            "renamed. Apple publishes no content hash, so a rename and a delete "
            "cannot be told apart from metadata alone; both are listed rather "
            "than one being chosen."
        )
        return
    item.evidence = "the local copy could not be read"
    item.detail = "the evidence does not settle whether anything was lost."


def _moves(
    store: IndexStore, since: Optional[str], report: VanishedReport
) -> Tuple[Dict[str, MovedFile], Dict[str, MovedFile]]:
    """Relocations :mod:`ifetch.conflicts` can prove, and ones it cannot.

    A proven move is a digest recorded at one path in a snapshot and the same
    digest held at exactly one other path now. That is not a deletion and is not
    reported as one. Everything short of proof is handed back as ambiguous.
    """
    if not since:
        return {}, {}
    try:
        found = detect_moves(store, since, notes=report.notes)
    except ConflictError as exc:
        raise VanishedError(str(exc)) from exc

    proven: Dict[str, MovedFile] = {}
    ambiguous: Dict[str, MovedFile] = {}
    for move in found:
        if move.confidence == CONFIDENCE_PROVEN:
            proven[move.old_path] = move
        else:
            ambiguous[move.old_path] = move
    return proven, ambiguous


def _renames(store: IndexStore, report: VanishedReport) -> Dict[str, RenameCandidate]:
    """Remote renames that could explain a disappearance. Never proof.

    :func:`ifetch.conflicts.detect_renames` matches a path that left iCloud
    against one that appeared in it, on filename and byte size. That is evidence,
    not proof, and it is used here only to *downgrade* a finding to ambiguous -
    never to dismiss one.
    """
    if store.latest_scan() is None:
        return {}
    return {c.old_path: c for c in detect_renames(store, notes=report.notes)}


def _apply_deadlines(
    store: IndexStore,
    report: VanishedReport,
    observed_at: float,
    record: bool,
    scan_id: Optional[int],
) -> None:
    """Attach the purge bound to every finding.

    The bound is derived from the *earliest* observation on record, which is why
    it is read back from the store rather than assumed to be ``observed_at``: a
    run three weeks after the first one must not restart the clock.
    """
    if not report.items:
        return

    if record:
        firsts = store.note_missing_many(
            [
                {
                    "path": i.path, "kind": i.kind, "size": i.size,
                    "sha256": i.sha256, "scan_id": scan_id,
                }
                for i in report.items
            ],
            observed_at,
        )
    else:
        firsts = {}
        for item in report.items:
            existing = store.get_missing_observation(item.path)
            firsts[item.path] = min(
                observed_at,
                float(existing["first_missing_at"]) if existing else observed_at,
            )

    for item in report.items:
        first = firsts.get(item.path, observed_at)
        item.first_missing_at = first
        item.purge_deadline_before = first + TRASH_RETENTION_DAYS * _DAY


def _coverage_notes(report: VanishedReport, since: Optional[str]) -> None:
    """State what the analysis could not look at, whatever the outcome was."""
    if since and report.baseline_kind == BASELINE_SNAPSHOT:
        blind = sum(1 for i in report.items if not i.sha256)
        if blind:
            report.unexamined.append({
                "what": "baseline entries with no recorded digest",
                "count": str(blind),
                "why": (
                    f"a move of {plural(blind, 'that file', 'those files')} "
                    "could not be ruled out, because ruling it out needs a "
                    "digest to match against. Re-take the snapshot with hashing "
                    "enabled."
                ),
            })


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------

_DEADLINE_EXPLANATION = (
    "iFetch does not know when these files were deleted. It knows when it first\n"
    "observed them missing, and a deletion happens at or before it is noticed.\n"
    f"iCloud purges Trash roughly {TRASH_RETENTION_DAYS} days after a delete, so "
    "every instant below is an\nUPPER BOUND: the cloud copy is gone no later "
    "than that, quite possibly sooner.\nIt is a ceiling, not an appointment."
)


def render_vanished(report: VanishedReport, show: int = 40) -> str:
    """The text report. Refuses to table findings the evidence cannot support."""
    counts = report.counts()
    lines = [
        rule("="),
        "iFetch vanished-from-iCloud report",
        rule("="),
        key_values([
            ("Local mirror", report.root),
            ("Baseline", f"{report.baseline_description} "
                         f"({report.baseline_count:,} {plural(report.baseline_count, 'file')})"),
            ("iCloud scan", _scan_line(report.scan)),
            ("Gone from iCloud", f"{len(report.items):,}"),
            ("  lost outright", f"{counts.get(CLASS_LOST, 0):,}"),
            ("  placeholder only", f"{counts.get(CLASS_PLACEHOLDER, 0):,}"),
            ("  ambiguous", f"{counts.get(CLASS_AMBIGUOUS, 0):,}"),
            ("  safe local copy", f"{counts.get(CLASS_SAFE, 0):,}"),
            ("Size of the above", human_bytes(report.total_bytes)),
        ]),
        "",
    ]

    if report.unexamined:
        lines.append("Not examined")
        for gap in report.unexamined:
            lines.append(f"  ! {gap['what']} ({gap['count']}): {gap['why']}")
        lines.append("")

    if not report.has_baseline:
        lines.append("No baseline.")
        lines.append(
            "  This index holds no earlier record of what iCloud contained, so\n"
            "  nothing can be reported as vanished. That is not a clean bill of\n"
            "  health - it is an absence of evidence. Run 'ifetch plan' or a sync\n"
            "  to establish a baseline, or take a snapshot, and ask again."
        )
        lines.append(rule("="))
        return "\n".join(lines)

    if report.refused:
        lines.extend(_refusal_block(report))
        lines.append(rule("="))
        return "\n".join(lines)

    lost = report.by_class(CLASS_LOST)
    placeholder = report.by_class(CLASS_PLACEHOLDER)
    if lost or placeholder:
        loud = len(lost) + len(placeholder)
        lines.append(
            f"! {loud:,} {plural(loud, 'file')} {plural(loud, 'is', 'are')} gone "
            "from iCloud with no usable copy on this disk."
        )
        lines.append("")

    if report.items:
        lines.append(_DEADLINE_EXPLANATION)
        lines.append("")
        rows = [
            [
                CLASS_LABELS.get(i.classification, i.classification),
                human_bytes(i.size),
                _deadline_cell(i, report.now),
                i.path,
            ]
            for i in report.items[:show]
        ]
        lines.append(table(
            ["class", "size", "purged by, no later than", "path"],
            rows, align=["<", ">", "<", "<"],
        ))
        if len(report.items) > show:
            lines.append(f"  ... and {len(report.items) - show:,} more")
    else:
        lines.append(
            "Nothing has vanished: every file in the baseline is still listed "
            "by iCloud."
        )

    ambiguous = report.by_class(CLASS_AMBIGUOUS)
    if ambiguous:
        lines.append("")
        lines.append("Ambiguous - these may not be deletions at all")
        for item in ambiguous[:show]:
            lines.append(f"  {item.path}")
            for alternative in item.alternatives[:5]:
                lines.append(f"      could be: {alternative}")
            lines.append(f"      {item.detail}")

    if report.moved:
        lines.append("")
        lines.append(
            f"{len(report.moved):,} {plural(len(report.moved), 'file')} were not "
            "reported as deletions: an identical digest proves "
            f"{plural(len(report.moved), 'it', 'they')} moved."
        )
        for move in report.moved[:show]:
            lines.append(f"  {move['old_path']} -> {move['new_path']}")

    if report.notes:
        lines.append("")
        lines.append("Notes")
        for note in report.notes:
            lines.append(f"  - {note}")

    lines.append(rule("="))
    return "\n".join(lines)


def _scan_line(scan: ScanEvidence) -> str:
    if scan.scan_id is None:
        return "none recorded"
    return (
        f"finished {_iso(scan.finished_at)}, {scan.item_count:,} items, "
        f"{scan.error_count:,} listing {plural(scan.error_count, 'error')}"
    )


def _deadline_cell(item: VanishedItem, now: float) -> str:
    if item.purge_deadline_before is None:
        return "unknown"
    remaining = item.days_remaining_at_most(now)
    stamp = _iso(item.purge_deadline_before)
    if remaining is None:
        return stamp
    if remaining <= 0:
        return f"{stamp} (may already be purged)"
    return f"{stamp} (<= {remaining:.0f}d)"


def _refusal_block(report: VanishedReport) -> List[str]:
    breaker = report.breaker

    fresh = say_once()

    lines = [
        rule("-"),
        "REFUSED: this result is not being reported as a set of deletions.",
        rule("-"),
    ]
    if fresh(breaker.detail):
        lines.append(f"  {breaker.detail}")

    alternatives = [alt for alt in breaker.cannot_rule_out if fresh(alt)]
    if alternatives:
        lines.append("")
        lines.append("  What this could be instead - none of it ruled out:")
        lines.extend(f"    - {alternative}" for alternative in alternatives)
    lines.append("")
    if report.items:
        count = len(report.items)
        lines.extend([
            f"  The {count:,} affected {plural(count, 'path')} "
            f"{plural(count, 'is', 'are')} in the JSON payload "
            "(--json / --report),",
            "  but not listed here as findings, because this evidence cannot "
            "support",
            "  calling them deletions.",
        ])
    else:
        lines.extend([
            "  Nothing appeared to have vanished - and this evidence could not "
            "have shown",
            "  it if something had. Read this as 'unknown', never as 'all clear'.",
        ])
    lines.extend([
        "",
        "  Do this first: re-run the scan with --refresh on a working "
        "connection and",
        "  compare. If the same files are still gone from a clean scan, they "
        "are gone.",
    ])
    problems = [problem for problem in report.scan.problems if fresh(problem)]
    if problems:
        lines.append("")
        lines.append("  Problems with the scan behind this report:")
        lines.extend(f"    ! {problem}" for problem in problems)
    return lines


def csv_rows(report: VanishedReport) -> Tuple[List[str], List[List[Any]]]:
    """Headers and rows for the CSV export, deadlines labelled as bounds."""
    headers = [
        "path", "classification", "kind", "size", "sha256",
        "first_observed_missing_at_iso", "purge_deadline_before_iso",
        "deadline_is_upper_bound", "alternatives", "detail",
    ]
    rows = [
        [
            item.path, item.classification, item.kind, item.size, item.sha256,
            _iso(item.first_missing_at), _iso(item.purge_deadline_before),
            "yes", " | ".join(item.alternatives), item.detail,
        ]
        for item in report.items
    ]
    return headers, rows


def render_observations(rows: Sequence[Dict[str, Any]], show: int = 40) -> str:
    """The recorded first-observed-missing timestamps, for inspection."""
    lines = [
        rule("="),
        "iFetch recorded absences",
        rule("="),
        key_values([("Paths with a recorded absence", f"{len(rows):,}")]),
        "",
    ]
    if not rows:
        lines.append(
            "No absences recorded. Either nothing has vanished since this index "
            "was created, or no vanish report has been run against it - those "
            "are different things and this cannot tell them apart."
        )
        lines.append(rule("="))
        return "\n".join(lines)

    lines.append(table(
        ["first seen missing", "purged by, no later than", "path"],
        [
            [
                _iso(row["first_missing_at"]),
                _iso(row["first_missing_at"] + TRASH_RETENTION_DAYS * _DAY),
                row["path"],
            ]
            for row in rows[:show]
        ],
        align=["<", "<", "<"],
    ))
    if len(rows) > show:
        lines.append(f"  ... and {len(rows) - show:,} more")
    lines.append(rule("="))
    return "\n".join(lines)
