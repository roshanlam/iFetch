"""Conflict intelligence: renames, moves and duplicates.

The naive reconciliation in :mod:`ifetch.planner` compares paths. That is
correct and, for the three situations below, badly wasteful:

* you renamed a folder in iCloud, so every file under it is "new" remotely and
  "local only" on disk - and a plan says download all of it again;
* you reorganised the mirror on disk, so iFetch thinks it lost the files and
  offers to fetch them back to the old paths;
* the same bytes sit at five paths and nothing ever says so.

All three are answerable from the index. Two of them are answerable with
*proof*, and one is not, and this module is careful about which is which.

What can and cannot be proved
-----------------------------
Apple publishes no content hash. That single fact fixes the shape of everything
here:

* **Duplicates are proved.** Both copies are on this disk and both were hashed.
* **Local moves are proved.** The digest recorded in a snapshot and the digest
  of the file now sitting at a different path are the same digest.
* **Remote renames cannot be proved.** A file that appeared in iCloud can only
  be matched against a local file by *metadata* - name and size - because
  nothing else about the remote copy is knowable without downloading it, which
  is the cost the match exists to avoid.

So :func:`detect_renames` grades every match and says what the evidence was,
:func:`relocate` will only act on the strongest grade, and a match that could
go two ways is reported as ambiguous rather than resolved by tie-break. The
fallback for every uncertain case - re-download it - is always available and
always safe, which is why guessing here would be a poor trade.

What is excluded, and why it is said out loud
---------------------------------------------
Three classes of file are skipped by the rename matcher, and each one is
counted and reported rather than quietly dropped:

* **package bundles** - Apple's reported size for a bundle is not its expanded
  size on disk, so a size comparison is meaningless for them;
* **files iCloud reported no size for** - there is nothing to match on;
* **empty files** - every empty file has the same size and the same digest, so
  matching them is guaranteed ambiguity in exchange for saving zero bytes.

A report that says "no renames found" when it means "I could not look" is the
failure mode this module is written to avoid.
"""

from __future__ import annotations

import os
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

from .index import (
    DIFF_LOCAL_ONLY,
    DIFF_NEW,
    KIND_PACKAGE,
    IndexStore,
    LocalItem,
)
from .render import colour, human_bytes, key_values, plural, rule, supports_colour, table

#: How much a match is worth trusting.
CONFIDENCE_PROVEN = "proven"        # digest equality; not a guess at all
CONFIDENCE_STRONG = "strong"        # same filename and same size, unique both ways
CONFIDENCE_WEAK = "weak"            # same size only, unique both ways
CONFIDENCE_AMBIGUOUS = "ambiguous"  # more than one equally good answer

#: Ordering used by ``--min-confidence`` and by :func:`relocate`.
_CONFIDENCE_RANK = {
    CONFIDENCE_AMBIGUOUS: 0,
    CONFIDENCE_WEAK: 1,
    CONFIDENCE_STRONG: 2,
    CONFIDENCE_PROVEN: 3,
}


class ConflictError(Exception):
    """A conflict analysis could not be carried out."""


# ---------------------------------------------------------------------------
# Findings
# ---------------------------------------------------------------------------

@dataclass
class RenameCandidate:
    """A local file that may be the same file iCloud is offering at a new path.

    ``old_path`` is on disk and absent from iCloud; ``new_path`` is in iCloud
    and absent from disk. If they are the same file, relocating costs a
    ``rename(2)`` and saves ``size`` bytes of transfer.
    """

    old_path: str
    new_path: str
    size: Optional[int] = None
    confidence: str = CONFIDENCE_WEAK
    evidence: str = ""
    sha256: Optional[str] = None
    alternatives: List[str] = field(default_factory=list)

    @property
    def bytes_saved(self) -> int:
        """Transfer avoided if the match is right. Zero for an unresolved one."""
        if self.confidence == CONFIDENCE_AMBIGUOUS:
            return 0
        return int(self.size or 0)

    @property
    def renamed(self) -> bool:
        return Path(self.old_path).name != Path(self.new_path).name

    @property
    def moved(self) -> bool:
        return Path(self.old_path).parent != Path(self.new_path).parent

    def to_dict(self) -> Dict[str, Any]:
        return {
            "old_path": self.old_path,
            "new_path": self.new_path,
            "size": self.size,
            "confidence": self.confidence,
            "evidence": self.evidence,
            "sha256": self.sha256,
            "alternatives": list(self.alternatives),
            "bytes_saved": self.bytes_saved,
            "renamed": self.renamed,
            "moved": self.moved,
        }


@dataclass
class MovedFile:
    """A file that changed path on disk, established by digest."""

    old_path: str
    new_path: str
    sha256: str
    size: Optional[int] = None
    confidence: str = CONFIDENCE_PROVEN
    alternatives: List[str] = field(default_factory=list)
    detail: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "old_path": self.old_path,
            "new_path": self.new_path,
            "sha256": self.sha256,
            "size": self.size,
            "confidence": self.confidence,
            "alternatives": list(self.alternatives),
            "detail": self.detail,
        }


@dataclass
class DuplicateGroup:
    """One set of local paths holding byte-identical contents."""

    sha256: str
    paths: List[str]
    size: int = 0

    @property
    def count(self) -> int:
        return len(self.paths)

    @property
    def total_bytes(self) -> int:
        return self.size * self.count

    @property
    def wasted_bytes(self) -> int:
        """Space a perfect deduplication would reclaim - every copy but one."""
        return self.size * (self.count - 1)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "sha256": self.sha256,
            "paths": list(self.paths),
            "size": self.size,
            "count": self.count,
            "total_bytes": self.total_bytes,
            "wasted_bytes": self.wasted_bytes,
        }


@dataclass
class ConflictReport:
    """Everything the analysis found, plus everything it could not look at."""

    root: str = ""
    renames: List[RenameCandidate] = field(default_factory=list)
    moves: List[MovedFile] = field(default_factory=list)
    duplicates: List[DuplicateGroup] = field(default_factory=list)
    notes: List[str] = field(default_factory=list)

    @property
    def actionable(self) -> List[RenameCandidate]:
        """Renames confident enough to act on."""
        return [r for r in self.renames if r.confidence == CONFIDENCE_STRONG]

    @property
    def ambiguous(self) -> List[RenameCandidate]:
        return [r for r in self.renames if r.confidence == CONFIDENCE_AMBIGUOUS]

    @property
    def bytes_saved(self) -> int:
        return sum(r.bytes_saved for r in self.actionable)

    @property
    def wasted_bytes(self) -> int:
        return sum(g.wasted_bytes for g in self.duplicates)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "root": self.root,
            "renames": [r.to_dict() for r in self.renames],
            "moves": [m.to_dict() for m in self.moves],
            "duplicates": [g.to_dict() for g in self.duplicates],
            "notes": list(self.notes),
            "bytes_saved": self.bytes_saved,
            "wasted_bytes": self.wasted_bytes,
        }


# ---------------------------------------------------------------------------
# Duplicates - provable
# ---------------------------------------------------------------------------

def detect_duplicates(
    store: IndexStore,
    min_size: int = 1,
    notes: Optional[List[str]] = None,
) -> List[DuplicateGroup]:
    """Local paths holding byte-identical contents, largest waste first.

    ``min_size`` defaults to 1 because every empty file is a duplicate of every
    other empty file and deduplicating them reclaims nothing; listing them would
    bury the groups that matter.
    """
    groups: List[DuplicateGroup] = []
    empties = 0

    if notes is not None:
        undigested = sum(1 for row in store.iter_local() if not row["sha256"])
        if undigested:
            notes.append(
                f"{undigested:,} local {plural(undigested, 'file')} have no "
                "recorded digest and were not compared at all - duplicates among "
                f"{plural(undigested, 'it', 'them')} would not be found. "
                "Re-index with hashing enabled."
            )

    for row in store.duplicate_digests(minimum=2):
        paths = list(row["paths"])
        count = len(paths)
        if count < 2:
            continue
        # Every path in the group has the same digest, so it has the same size;
        # dividing is just recovering the per-copy figure from the group total.
        per_copy = int(row["total_bytes"] or 0) // count
        if per_copy < max(0, int(min_size)):
            if per_copy == 0:
                empties += count
            continue
        groups.append(DuplicateGroup(sha256=row["sha256"], paths=paths, size=per_copy))

    groups.sort(key=lambda g: (-g.wasted_bytes, g.sha256))

    if notes is not None and empties:
        notes.append(
            f"{empties:,} empty {plural(empties, 'file')} were not reported as "
            "duplicates: every empty file is identical to every other one, and "
            "deduplicating them reclaims nothing."
        )
    return groups


# ---------------------------------------------------------------------------
# Local moves - provable
# ---------------------------------------------------------------------------

def detect_moves(
    store: IndexStore,
    since: str,
    notes: Optional[List[str]] = None,
) -> List[MovedFile]:
    """Files that changed path on disk since snapshot ``since``.

    This is the provable half of the problem. The snapshot recorded a digest at
    path A; the current index holds that same digest at path B and nothing at A.
    No metadata heuristic is involved and no alternative explanation fits.

    A file whose old path *still exists* with the same contents was copied, not
    moved, so it is left to :func:`detect_duplicates` rather than reported here
    as a move that did not happen.
    """
    snapshot = store.get_snapshot(since)
    if snapshot is None:
        raise ConflictError(
            f"no snapshot named {since!r}. Run 'ifetch snapshot list' to see "
            "which ones exist."
        )

    with store._lock:
        entries = [
            dict(row)
            for row in store._conn.execute(
                "SELECT path, kind, size, sha256 FROM snapshot_entries "
                "WHERE snapshot_id = ? ORDER BY path",
                (snapshot["id"],),
            )
        ]

    snapshot_paths = {e["path"] for e in entries}
    current = {row["path"]: row for row in store.iter_local()}

    moves: List[MovedFile] = []
    undigested = 0
    copies = 0

    for entry in entries:
        digest = entry["sha256"]
        old_path = entry["path"]

        if not digest:
            undigested += 1
            continue
        if old_path in current:
            # Still there. Either unchanged, or modified - neither is a move.
            continue

        # Where else does this digest live now? Anything already named by the
        # snapshot is a pre-existing copy, not the destination of this move.
        candidates = [
            row["path"]
            for row in store.find_by_digest(digest, exclude_path=old_path)
            if row["path"] not in snapshot_paths
        ]

        if not candidates:
            continue  # Genuinely gone; 'ifetch recover missing' is that report.

        if len(candidates) > 1:
            copies += 1
            moves.append(MovedFile(
                old_path=old_path, new_path=candidates[0], sha256=digest,
                size=entry["size"], confidence=CONFIDENCE_AMBIGUOUS,
                alternatives=sorted(candidates),
                detail=(
                    f"these exact bytes now exist at {len(candidates):,} new "
                    "paths, so this was a copy rather than a move; all of them "
                    "are listed"
                ),
            ))
            continue

        moves.append(MovedFile(
            old_path=old_path, new_path=candidates[0], sha256=digest,
            size=entry["size"], confidence=CONFIDENCE_PROVEN,
            detail=(
                f"identical digest, recorded at '{old_path}' in snapshot "
                f"'{since}' and now held at this path"
            ),
        ))

    if notes is not None and undigested:
        notes.append(
            f"{undigested:,} {plural(undigested, 'entry', 'entries')} in "
            f"snapshot '{since}' have no recorded digest, so a move of "
            f"{plural(undigested, 'that file', 'those files')} could not be "
            "detected. Re-take the snapshot without --fast."
        )
    if notes is not None and copies:
        notes.append(
            f"{copies:,} {plural(copies, 'file')} had their contents duplicated "
            "to more than one new path; those are reported as ambiguous because "
            "a copy is not a move."
        )
    return moves


# ---------------------------------------------------------------------------
# Remote renames - inferred, never proved
# ---------------------------------------------------------------------------

def detect_renames(
    store: IndexStore,
    min_size: int = 1,
    notes: Optional[List[str]] = None,
) -> List[RenameCandidate]:
    """Match "new in iCloud" against "only on disk" to avoid re-downloading.

    Requires a remote scan in the index. The matching is deliberately
    conservative:

    * candidates are grouped by exact byte size, because a rename does not
      change contents and a size mismatch settles it immediately;
    * within a size group, an identical filename on both sides - unique on both
      sides - is graded ``strong``; that is a folder that was renamed, which is
      the case worth optimising;
    * a size group with exactly one candidate left on each side is graded
      ``weak``: probably the same file under a new name, but the only evidence
      is a size coincidence;
    * anything else is graded ``ambiguous`` and every alternative is listed. A
      tie-break here would be inventing an answer, and the cost of being wrong
      (wrong bytes silently left at the new path) is far higher than the cost of
      being unhelpful (re-download it).
    """
    entries = store.diff()
    olds: List[Tuple[str, int, Optional[str]]] = []
    news: List[Tuple[str, int]] = []

    packages = 0
    sizeless = 0
    empties = 0
    floor = max(1, int(min_size))

    for entry in entries:
        if entry.kind == KIND_PACKAGE:
            if entry.status in (DIFF_NEW, DIFF_LOCAL_ONLY):
                packages += 1
            continue

        if entry.status == DIFF_NEW:
            size = entry.remote_size
            if size is None:
                sizeless += 1
                continue
            if size == 0:
                empties += 1
                continue
            if size < floor:
                continue
            news.append((entry.path, int(size)))

        elif entry.status == DIFF_LOCAL_ONLY:
            size = entry.local_size
            if size is None or size == 0:
                if size == 0:
                    empties += 1
                continue
            if size < floor:
                continue
            row = store.get_local(entry.path)
            olds.append((entry.path, int(size), row.get("sha256") if row else None))

    if notes is not None:
        if packages:
            notes.append(
                f"{packages:,} package {plural(packages, 'bundle')} were not "
                "considered: Apple's reported size for a bundle is not its "
                "expanded size on disk, so there is nothing to match on."
            )
        if sizeless:
            notes.append(
                f"{sizeless:,} remote {plural(sizeless, 'file')} have no size "
                "reported by iCloud, so no local file can be matched to "
                f"{plural(sizeless, 'it', 'them')}."
            )
        if empties:
            notes.append(
                f"{empties:,} empty {plural(empties, 'file')} were ignored: "
                "every empty file matches every other, and re-downloading one "
                "costs nothing."
            )

    by_size_old: Dict[int, List[Tuple[str, int, Optional[str]]]] = defaultdict(list)
    by_size_new: Dict[int, List[Tuple[str, int]]] = defaultdict(list)
    for item in olds:
        by_size_old[item[1]].append(item)
    for item in news:
        by_size_new[item[1]].append(item)

    candidates: List[RenameCandidate] = []
    for size, old_group in by_size_old.items():
        new_group = by_size_new.get(size)
        if not new_group:
            continue
        candidates.extend(_match_size_group(old_group, list(new_group), size))

    candidates.sort(
        key=lambda c: (-_CONFIDENCE_RANK[c.confidence], -(c.size or 0), c.old_path)
    )
    return candidates


def _match_size_group(
    olds: Sequence[Tuple[str, int, Optional[str]]],
    news: List[Tuple[str, int]],
    size: int,
) -> List[RenameCandidate]:
    """Resolve one same-size bucket into graded matches."""
    remaining_old = list(olds)
    remaining_new = list(news)
    out: List[RenameCandidate] = []

    # Pass one: identical filenames. A renamed *folder* leaves every file's own
    # name intact, which is both the commonest case and the strongest signal
    # available without a content hash.
    old_by_name: Dict[str, List[Tuple[str, int, Optional[str]]]] = defaultdict(list)
    new_by_name: Dict[str, List[Tuple[str, int]]] = defaultdict(list)
    for item in remaining_old:
        old_by_name[Path(item[0]).name].append(item)
    for item in remaining_new:
        new_by_name[Path(item[0]).name].append(item)

    for name, old_matches in old_by_name.items():
        new_matches = new_by_name.get(name)
        if not new_matches or len(old_matches) != 1 or len(new_matches) != 1:
            continue
        old_path, _, digest = old_matches[0]
        new_path = new_matches[0][0]
        out.append(RenameCandidate(
            old_path=old_path, new_path=new_path, size=size,
            confidence=CONFIDENCE_STRONG, sha256=digest,
            evidence=(
                "same filename and same byte size, and the only such file on "
                "either side. Apple publishes no content hash, so this is "
                "strong evidence rather than proof."
            ),
        ))
        remaining_old.remove(old_matches[0])
        remaining_new.remove(new_matches[0])

    if not remaining_old or not remaining_new:
        return out

    # Pass two: one left on each side. A rename that also changed the filename.
    if len(remaining_old) == 1 and len(remaining_new) == 1:
        old_path, _, digest = remaining_old[0]
        out.append(RenameCandidate(
            old_path=old_path, new_path=remaining_new[0][0], size=size,
            confidence=CONFIDENCE_WEAK, sha256=digest,
            evidence=(
                "the only remaining file of this exact size on either side, but "
                "the names differ - a size coincidence would look identical"
            ),
        ))
        return out

    # Pass three: unresolvable. List every alternative instead of picking one.
    alternatives = sorted(path for path, _ in remaining_new)
    for old_path, _, digest in remaining_old:
        out.append(RenameCandidate(
            old_path=old_path, new_path=alternatives[0], size=size,
            confidence=CONFIDENCE_AMBIGUOUS, sha256=digest,
            alternatives=alternatives,
            evidence=(
                f"{len(alternatives):,} remote {plural(len(alternatives), 'file')} "
                f"of this size could be this one, and {len(remaining_old):,} local "
                f"{plural(len(remaining_old), 'file')} could be any of them; "
                "iFetch will not guess"
            ),
        ))
    return out


# ---------------------------------------------------------------------------
# The combined analysis
# ---------------------------------------------------------------------------

def analyse(
    store: IndexStore,
    root: Path,
    since: Optional[str] = None,
    min_size: int = 1,
) -> ConflictReport:
    """Run every detector that has the inputs it needs, and say which ran."""
    report = ConflictReport(root=str(Path(root).resolve()))

    if store.local_count() == 0:
        report.notes.append(
            "The local index is empty, so nothing could be compared. Run "
            "'ifetch plan' or 'ifetch snapshot create' to index the mirror."
        )
        return report

    report.duplicates = detect_duplicates(store, min_size=min_size, notes=report.notes)

    if store.latest_scan() is None:
        report.notes.append(
            "No iCloud scan exists for this directory, so remote renames could "
            "not be looked for. Run 'ifetch plan' first."
        )
    else:
        report.renames = detect_renames(store, min_size=min_size, notes=report.notes)

    if since:
        report.moves = detect_moves(store, since, notes=report.notes)

    return report


# ---------------------------------------------------------------------------
# Acting on a rename
# ---------------------------------------------------------------------------

RELOCATE_WOULD = "would_relocate"
RELOCATE_DONE = "relocated"
RELOCATE_SKIPPED = "skipped"
RELOCATE_FAILED = "failed"


@dataclass
class RelocationOutcome:
    old_path: str
    new_path: str
    status: str
    detail: str = ""
    bytes_saved: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "old_path": self.old_path,
            "new_path": self.new_path,
            "status": self.status,
            "detail": self.detail,
            "bytes_saved": self.bytes_saved,
        }


def relocate(
    store: IndexStore,
    root: Path,
    candidates: Sequence[RenameCandidate],
    dry_run: bool = True,
    min_confidence: str = CONFIDENCE_STRONG,
    manifest: Optional[Any] = None,
) -> List[RelocationOutcome]:
    """Move local files to the paths iCloud now uses, instead of re-downloading.

    Deliberately narrow. Only matches at ``min_confidence`` or better are
    touched, and each one is re-checked against the disk immediately before the
    move - the index may be minutes old, and acting on stale rows is how a tool
    like this destroys data.

    Three refusals, all of them silent-corruption guards:

    * the source is gone, or is no longer the size that was matched;
    * something already exists at the destination - iFetch will not overwrite a
      file to satisfy a *guess* about a rename;
    * the move would leave the mirror.

    Both the index and the manifest are re-pointed at the new path, because a
    manifest still naming the old one would report a missing file and an
    untracked one at the next ``ifetch-verify``.
    """
    root = Path(root).resolve()
    floor = _CONFIDENCE_RANK.get(min_confidence, _CONFIDENCE_RANK[CONFIDENCE_STRONG])
    outcomes: List[RelocationOutcome] = []

    for candidate in candidates:
        if _CONFIDENCE_RANK[candidate.confidence] < floor:
            continue

        # Resolved, not joined: ``relative_to`` is a lexical test, so a path
        # containing ".." - or a symlinked parent - passes it while pointing
        # anywhere on the filesystem. These paths come from an iCloud listing,
        # which is not this program's data.
        source = (root / candidate.old_path).resolve()
        target = (root / candidate.new_path).resolve()

        if not _within(root, source) or not _within(root, target):
            outcomes.append(RelocationOutcome(
                candidate.old_path, candidate.new_path, RELOCATE_FAILED,
                "the move would leave the mirror directory",
            ))
            continue

        if not source.is_file():
            outcomes.append(RelocationOutcome(
                candidate.old_path, candidate.new_path, RELOCATE_SKIPPED,
                "the local file is no longer there; the index is out of date",
            ))
            continue

        try:
            actual = source.stat().st_size
        except OSError as exc:
            outcomes.append(RelocationOutcome(
                candidate.old_path, candidate.new_path, RELOCATE_FAILED, str(exc),
            ))
            continue

        if candidate.size is not None and actual != candidate.size:
            outcomes.append(RelocationOutcome(
                candidate.old_path, candidate.new_path, RELOCATE_SKIPPED,
                f"the local file is now {human_bytes(actual)}, not the "
                f"{human_bytes(candidate.size)} that was matched",
            ))
            continue

        if target.exists():
            outcomes.append(RelocationOutcome(
                candidate.old_path, candidate.new_path, RELOCATE_SKIPPED,
                "something already exists at the destination; iFetch will not "
                "overwrite a file to satisfy an inferred rename",
            ))
            continue

        if dry_run:
            outcomes.append(RelocationOutcome(
                candidate.old_path, candidate.new_path, RELOCATE_WOULD,
                f"would move, saving {human_bytes(actual)} of transfer",
                bytes_saved=actual,
            ))
            continue

        try:
            target.parent.mkdir(parents=True, exist_ok=True)
            os.replace(source, target)
        except OSError as exc:
            outcomes.append(RelocationOutcome(
                candidate.old_path, candidate.new_path, RELOCATE_FAILED, str(exc),
            ))
            continue

        _repoint(store, root, candidate, target, manifest)
        outcomes.append(RelocationOutcome(
            candidate.old_path, candidate.new_path, RELOCATE_DONE,
            f"moved, saving {human_bytes(actual)} of transfer",
            bytes_saved=actual,
        ))

    if manifest is not None and any(o.status == RELOCATE_DONE for o in outcomes):
        try:
            manifest.save()
        except Exception:  # pragma: no cover - a failed save is not a failed move
            pass

    return outcomes


def _within(root: Path, path: Path) -> bool:
    """True when ``path`` really is inside ``root``, both already resolved."""
    try:
        path.relative_to(root)
    except ValueError:
        return False
    return path != root


def _repoint(
    store: IndexStore,
    root: Path,
    candidate: RenameCandidate,
    target: Path,
    manifest: Optional[Any],
) -> None:
    """Follow the file: index row and manifest entry both move with it."""
    try:
        stat = target.stat()
        mtime: Optional[float] = stat.st_mtime
        size: Optional[int] = stat.st_size
    except OSError:  # pragma: no cover - the move just succeeded
        mtime, size = None, candidate.size

    store.forget_local(candidate.old_path)
    store.record_local(LocalItem(
        path=candidate.new_path, size=size, mtime=mtime, sha256=candidate.sha256,
    ))

    if manifest is None:
        return
    try:
        manifest.forget(root / candidate.old_path)
        manifest.record_file(target, sha256=candidate.sha256)
    except Exception:  # pragma: no cover - defensive
        pass


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------

_CONFIDENCE_TONE = {
    CONFIDENCE_PROVEN: "green",
    CONFIDENCE_STRONG: "green",
    CONFIDENCE_WEAK: "yellow",
    CONFIDENCE_AMBIGUOUS: "red",
}


def _notes_block(notes: Sequence[str], tint: bool) -> List[str]:
    if not notes:
        return []
    return [colour("Not examined", "bold", tint)] + [f"  - {n}" for n in notes] + [""]


def render_renames(
    candidates: Sequence[RenameCandidate],
    notes: Sequence[str] = (),
    show: int = 40,
    use_colour: Optional[bool] = None,
) -> str:
    tint = supports_colour() if use_colour is None else use_colour
    strong = [c for c in candidates if c.confidence == CONFIDENCE_STRONG]
    weak = [c for c in candidates if c.confidence == CONFIDENCE_WEAK]
    unclear = [c for c in candidates if c.confidence == CONFIDENCE_AMBIGUOUS]
    saved = sum(c.bytes_saved for c in strong)

    out = [
        rule("="),
        colour("iFetch rename detection (read-only)", "bold", tint),
        rule("="),
        key_values([
            ("Strong matches", f"{len(strong):,}"),
            ("Weak matches", f"{len(weak):,}"),
            ("Ambiguous", f"{len(unclear):,}"),
            ("Transfer avoidable", human_bytes(saved)),
        ]),
        "",
    ]
    out.extend(_notes_block(notes, tint))

    if not candidates:
        out.append(
            "No renamed or moved files found - every file iCloud offers that is "
            "missing locally looks like a genuinely new file."
        )
        out.append(rule("="))
        return "\n".join(out)

    out.append(
        "Apple publishes no content hash, so none of these is proof. The "
        "evidence for each is stated; re-downloading is always the safe answer."
    )
    out.append("")

    for group, title in (
        (strong, "Strong - same filename and size, unique on both sides"),
        (weak, "Weak - same size only, names differ"),
        (unclear, "Ambiguous - iFetch will not choose between these"),
    ):
        if not group:
            continue
        out.append(colour(title, "bold", tint))
        rows = []
        for candidate in group[:show]:
            label = colour(
                candidate.confidence,
                _CONFIDENCE_TONE.get(candidate.confidence, "reset"),
                tint,
            )
            rows.append([
                label, human_bytes(candidate.size),
                candidate.old_path, candidate.new_path,
            ])
        out.append(table(
            ["confidence", "size", "on disk at", "iCloud now has"],
            rows, align=["<", ">", "<", "<"],
        ))
        if len(group) > show:
            out.append(f"  ... and {len(group) - show:,} more")
        out.append("")

    for candidate in unclear[:show]:
        if len(candidate.alternatives) > 1:
            out.append(f"  {candidate.old_path} could be any of:")
            out.extend(f"    - {alt}" for alt in candidate.alternatives)
    if unclear:
        out.append("")

    out.append(rule("="))
    if strong:
        count = len(strong)
        out.append(
            f"{count:,} strong {plural(count, 'match', 'matches')} "
            f"({human_bytes(saved)}) can be resolved by moving the local "
            "files instead of downloading them: 'ifetch conflicts renames --apply'."
        )
    else:
        out.append(
            "No match is strong enough to act on. Run 'ifetch' to download "
            "these files normally."
        )
    return "\n".join(out)


def render_duplicates(
    groups: Sequence[DuplicateGroup],
    notes: Sequence[str] = (),
    show: int = 40,
    use_colour: Optional[bool] = None,
) -> str:
    tint = supports_colour() if use_colour is None else use_colour
    wasted = sum(g.wasted_bytes for g in groups)
    copies = sum(g.count - 1 for g in groups)

    out = [
        rule("="),
        colour("iFetch duplicate detection (read-only)", "bold", tint),
        rule("="),
        key_values([
            ("Duplicate sets", f"{len(groups):,}"),
            ("Redundant copies", f"{copies:,}"),
            ("Space they occupy", human_bytes(wasted)),
        ]),
        "",
    ]
    out.extend(_notes_block(notes, tint))

    if not groups:
        out.append("No duplicate files found in the index.")
        out.append(rule("="))
        return "\n".join(out)

    out.append(colour("Largest duplicate sets first", "bold", tint))
    out.append(table(
        ["copies", "each", "reclaimable", "path"],
        [[f"{g.count:,}", human_bytes(g.size), human_bytes(g.wasted_bytes), g.paths[0]]
         for g in groups[:show]],
        align=[">", ">", ">", "<"],
    ))
    if len(groups) > show:
        out.append(f"  ... and {len(groups) - show:,} more")
    out.append("")

    out.append(colour("Where each set lives", "bold", tint))
    for group in groups[:show]:
        out.append(f"  {human_bytes(group.size)} x{group.count}  [{group.sha256[:12]}]")
        out.extend(f"    - {path}" for path in group.paths)
    out.append("")

    out.append(rule("="))
    out.append(
        f"{human_bytes(wasted)} is held in redundant copies. iFetch does not "
        "delete anything - these are listed so you can decide. Deleting a copy "
        "here does not delete it in iCloud, and the next sync will fetch it back."
    )
    return "\n".join(out)


def render_moves(
    moves: Sequence[MovedFile],
    since: str,
    notes: Sequence[str] = (),
    show: int = 40,
    use_colour: Optional[bool] = None,
) -> str:
    tint = supports_colour() if use_colour is None else use_colour
    proven = [m for m in moves if m.confidence == CONFIDENCE_PROVEN]
    unclear = [m for m in moves if m.confidence == CONFIDENCE_AMBIGUOUS]

    out = [
        rule("="),
        colour(f"iFetch local moves since snapshot '{since}' (read-only)", "bold", tint),
        rule("="),
        key_values([
            ("Moved files", f"{len(proven):,}"),
            ("Copied to several places", f"{len(unclear):,}"),
        ]),
        "",
    ]
    out.extend(_notes_block(notes, tint))

    if not moves:
        out.append(f"Nothing has moved on disk since snapshot '{since}'.")
        out.append(rule("="))
        return "\n".join(out)

    out.append(
        "These are established by digest, not inferred: the same bytes are now "
        "at a different path."
    )
    out.append("")
    out.append(table(
        ["confidence", "size", "was at", "now at"],
        [[colour(m.confidence, _CONFIDENCE_TONE.get(m.confidence, "reset"), tint),
          human_bytes(m.size), m.old_path, m.new_path]
         for m in moves[:show]],
        align=["<", ">", "<", "<"],
    ))
    if len(moves) > show:
        out.append(f"  ... and {len(moves) - show:,} more")
    out.append("")

    out.append(rule("="))
    out.append(
        "iFetch mirrors iCloud's paths, so a file you moved locally will be "
        "downloaded again at its original path on the next sync. Move it back, "
        "or rename it in iCloud so the two agree."
    )
    return "\n".join(out)
