"""Putting missing files back into iCloud - and nothing else.

iFetch downloads. That is the whole design, and it is why the README says so.
But a backup you cannot restore from is a filing cabinet, and until now the
answer to "iCloud lost my folder, now what?" was "drag it back in Finder".

This module is the one write path worth having, and it is deliberately narrow.

The contract
------------
**Upload only files that are missing from iCloud. Never overwrite, never
delete, never rename.** This is not two-way sync and must not become it. There
is no code here that can modify or remove a remote file; the connection wrapper
:class:`DriveUplink` exposes four operations - resolve a folder, list it, create
a folder, send a new file - and nothing more.

What it refuses to upload, and always names
-------------------------------------------
A refusal is listed in the report with its reason. Nothing is dropped quietly.

* **Anything, if the scan is not usable evidence.** If the remote listing
  failed, was truncated or came back empty, every local file looks missing and
  this feature would push an entire mirror back into the account. The scan
  assessment and circuit breaker from :mod:`ifetch.vanished` decide that, and a
  broken scan refuses at any count - including one file.
* **A placeholder.** An evicted local file has a size in Finder and no bytes.
  Uploading it would put an empty file into iCloud under a real filename.
* **A file that no longer matches its recorded digest.** Corruption is not
  something to replicate into the cloud.
* **Anything resolving outside the mirror root**, including through a symlink.
* **Package bundles** (``.key``, ``.pages``, ``.numbers``, ``.xcodeproj``).
  They are directories on disk and Apple expects a single archive. iFetch
  cannot reliably reconstruct what Apple accepts, and a silently mangled
  Keynote is worse than an honest refusal.

Everything is checked twice: once when the plan is built, and again in the
moment before the file is sent, because a plan can be minutes old. If the file
has appeared in iCloud in between, it is skipped rather than overwritten.

Nothing is sent unless ``dry_run=False``. A failure on one file is recorded and
the rest of the run continues, and every success is written to the index so an
interrupted run resumes and a repeated one is a no-op.
"""

from __future__ import annotations

import time
import unicodedata
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

from .index import (
    KIND_DIR,
    KIND_FILE,
    KIND_PACKAGE,
    UPLOAD_DONE,
    UPLOAD_FAILED,
    IndexStore,
)
from .manifest import Manifest, sha256_file
from .packages import is_package_name
from .recovery import PlaceholderDetector, PlaceholderReport
from .render import human_bytes, key_values, plural, rule, say_once, table
from .vanished import (
    DEFAULT_MAX_FRACTION,
    DEFAULT_MAX_VANISHED,
    DEFAULT_MIN_BASELINE,
    BreakerVerdict,
    ScanEvidence,
    assess_scan,
    check_breaker,
)

#: The breaker thresholds are the ones :mod:`ifetch.vanished` uses, and for the
#: same reason: a run where hundreds of files, or a quarter of the mirror, look
#: absent from iCloud is far more likely to be a bad listing than a real loss.
DEFAULT_MAX_UPLOADS = DEFAULT_MAX_VANISHED

#: Why a file will not be uploaded.
REFUSE_PLACEHOLDER = "placeholder"
REFUSE_DIGEST_MISMATCH = "digest_mismatch"
REFUSE_OUTSIDE_ROOT = "outside_root"
REFUSE_PACKAGE = "package_bundle"
REFUSE_MISSING_LOCALLY = "not_on_disk"
REFUSE_NOT_A_FILE = "not_a_file"
REFUSE_UNREADABLE = "unreadable"

#: What happened to a candidate during a run.
STATUS_WOULD_UPLOAD = "would_upload"
STATUS_UPLOADED = "uploaded"
STATUS_SKIPPED = "skipped"
STATUS_FAILED = "failed"

_REFUSAL_LABELS = {
    REFUSE_PLACEHOLDER: "placeholder",
    REFUSE_DIGEST_MISMATCH: "digest mismatch",
    REFUSE_OUTSIDE_ROOT: "outside the mirror",
    REFUSE_PACKAGE: "package bundle",
    REFUSE_MISSING_LOCALLY: "gone from disk",
    REFUSE_NOT_A_FILE: "not a file",
    REFUSE_UNREADABLE: "unreadable",
}


class UplinkError(Exception):
    """An upload plan could not be built, or a run could not be started."""


def _nfc(path: str) -> str:
    """Normalisation key for comparing a local path against an iCloud one.

    Apple returns NFD and a mirror may hold NFC, so a byte comparison reports
    every accented filename as missing from iCloud - and this module would then
    upload a duplicate of a file that was there all along. Case is not folded:
    two names differing only in case are two files.
    """
    return unicodedata.normalize("NFC", path)


def _iso(timestamp: Optional[float]) -> str:
    if timestamp is None:
        return "unknown"
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(timestamp))


# ---------------------------------------------------------------------------
# The plan
# ---------------------------------------------------------------------------

@dataclass
class UploadCandidate:
    """One local file that iCloud does not have and that passed every check."""

    path: str
    remote_path: str
    parent: str
    name: str
    size: Optional[int] = None
    sha256: Optional[str] = None
    digest_source: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "path": self.path,
            "remote_path": self.remote_path,
            "parent": self.parent,
            "name": self.name,
            "size": self.size,
            "sha256": self.sha256,
            "digest_source": self.digest_source or "none recorded",
        }


@dataclass
class Refusal:
    """One file that will not be uploaded, and the reason in plain words."""

    path: str
    reason: str
    detail: str
    size: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "path": self.path,
            "reason": self.reason,
            "detail": self.detail,
            "size": self.size,
        }


@dataclass
class QuotaCheck:
    """Whether the account has room, or whether that could not be established.

    ``sufficient`` is ``None`` when Apple did not supply the figures. That is
    reported as unchecked and never as a pass: "we could not ask" and "there is
    room" are different answers.
    """

    checked: bool = False
    available_bytes: Optional[int] = None
    required_bytes: int = 0
    sufficient: Optional[bool] = None
    detail: str = ""

    @property
    def blocks_upload(self) -> bool:
        return self.sufficient is False

    def to_dict(self) -> Dict[str, Any]:
        return {
            "checked": self.checked,
            "available_bytes": self.available_bytes,
            "required_bytes": self.required_bytes,
            "sufficient": self.sufficient,
            "detail": self.detail,
        }


@dataclass
class UploadPlan:
    """What restoring the missing files would send, and what it would not.

    Building one changes nothing, locally or remotely.
    """

    root: str = ""
    icloud_path: str = ""
    candidates: List[UploadCandidate] = field(default_factory=list)
    refusals: List[Refusal] = field(default_factory=list)
    folders_to_create: List[str] = field(default_factory=list)
    scan: ScanEvidence = field(default_factory=ScanEvidence)
    breaker: BreakerVerdict = field(default_factory=BreakerVerdict)
    quota: QuotaCheck = field(default_factory=QuotaCheck)
    already_uploaded: List[str] = field(default_factory=list)
    missing_paths: List[str] = field(default_factory=list)
    local_count: int = 0
    missing_count: int = 0
    notes: List[str] = field(default_factory=list)
    unexamined: List[Dict[str, str]] = field(default_factory=list)
    generated_at: str = ""

    @property
    def refused(self) -> bool:
        """True when nothing may be uploaded, whatever the candidate list says."""
        return self.breaker.tripped or self.quota.blocks_upload

    @property
    def refusal_detail(self) -> str:
        if self.breaker.tripped:
            return self.breaker.detail
        if self.quota.blocks_upload:
            return self.quota.detail
        return ""

    @property
    def total_bytes(self) -> int:
        return sum(c.size or 0 for c in self.candidates)

    def refusals_by_reason(self, reason: str) -> List[Refusal]:
        return [r for r in self.refusals if r.reason == reason]

    def refusal_counts(self) -> Dict[str, int]:
        tally: Dict[str, int] = {}
        for refusal in self.refusals:
            tally[refusal.reason] = tally.get(refusal.reason, 0) + 1
        return tally

    def to_dict(self) -> Dict[str, Any]:
        return {
            "root": self.root,
            "icloud_path": self.icloud_path,
            "generated_at": self.generated_at,
            "scan": self.scan.to_dict(),
            "breaker": self.breaker.to_dict(),
            "quota": self.quota.to_dict(),
            "refused": self.refused,
            "refusal_detail": self.refusal_detail,
            "local_files": self.local_count,
            "missing_from_icloud": self.missing_count,
            "upload_count": len(self.candidates),
            "total_bytes": self.total_bytes,
            "folders_to_create": list(self.folders_to_create),
            "already_uploaded": list(self.already_uploaded),
            "missing_paths": list(self.missing_paths),
            "refusal_counts": self.refusal_counts(),
            "candidates": [c.to_dict() for c in self.candidates],
            "refusals": [r.to_dict() for r in self.refusals],
            "notes": list(self.notes),
            "unexamined": list(self.unexamined),
        }


def plan_uploads(
    store: IndexStore,
    root: Path,
    icloud_path: Optional[str] = None,
    placeholders: Optional[PlaceholderReport] = None,
    manifest: Optional[Manifest] = None,
    account_storage: Optional[Dict[str, Any]] = None,
    max_count: int = DEFAULT_MAX_UPLOADS,
    max_fraction: float = DEFAULT_MAX_FRACTION,
    min_baseline: int = DEFAULT_MIN_BASELINE,
    now: Optional[float] = None,
) -> UploadPlan:
    """Work out which local files iCloud does not have, and which may be sent.

    Every safety rule is applied here so the dry run shows the truth rather than
    an optimistic list that shrinks when ``--apply`` runs. They are applied a
    second time at upload time, because the disk and the account can both change
    between the two.
    """
    now = time.time() if now is None else float(now)
    root = Path(root).resolve()

    scan = assess_scan(store)
    latest = store.latest_scan()
    base = icloud_path if icloud_path is not None else (
        (latest or {}).get("icloud_path") or ""
    )

    plan = UploadPlan(
        root=str(root),
        icloud_path=base,
        scan=scan,
        generated_at=_iso(now),
    )

    if placeholders is None:
        placeholders = PlaceholderDetector(root).scan()
    placeholder_paths = {_nfc(p.path): p for p in placeholders.placeholders}
    for gap in placeholders.signals_unavailable:
        plan.unexamined.append({
            "what": f"placeholder signal '{gap['signal']}'",
            "count": "all",
            "why": gap["reason"] + " A file evicted without a stub would be "
                   "uploaded as the empty shell it is.",
        })

    remote = {_nfc(row["path"]) for row in store.iter_remote(include_dirs=True)}
    remote_dirs = {
        _nfc(row["path"])
        for row in store.iter_remote(include_dirs=True)
        if row["kind"] == KIND_DIR
    }

    local_rows = [
        row for row in store.iter_local() if row["kind"] != KIND_DIR
    ]
    plan.local_count = len(local_rows)

    missing = [row for row in local_rows if _nfc(row["path"]) not in remote]
    plan.missing_count = len(missing)
    plan.missing_paths = sorted(row["path"] for row in missing)

    plan.breaker = check_breaker(
        len(missing), plan.local_count, scan, max_count, max_fraction, min_baseline,
    )

    if plan.breaker.tripped:
        # Nothing below can be offered as an upload, so nothing below is worth
        # computing - examining every file would mean re-hashing a whole mirror
        # to produce a list the refusal forbids acting on. The paths are in
        # ``missing_paths`` and in the JSON payload.
        _coverage_notes(plan, 0, len(placeholder_paths))
        return plan

    done = set(store.uploaded_paths())
    no_digest = 0
    wanted_dirs: List[str] = []

    for row in missing:
        path = row["path"]
        if path in done:
            plan.already_uploaded.append(path)
            continue

        refusal = _examine(root, path, row, placeholder_paths, manifest)
        if refusal is not None:
            plan.refusals.append(refusal)
            continue

        digest, source = _recorded_digest(root, path, row, manifest)
        if digest is None:
            no_digest += 1

        parent = _parent(path)
        candidate = UploadCandidate(
            path=path,
            remote_path=_join(base, path),
            parent=parent,
            name=Path(path).name,
            size=_size_on_disk(root / path, row.get("size")),
            sha256=digest,
            digest_source=source,
        )
        plan.candidates.append(candidate)

        for ancestor in _ancestors(parent):
            if _nfc(ancestor) not in remote_dirs and ancestor not in wanted_dirs:
                wanted_dirs.append(ancestor)

    plan.candidates.sort(key=lambda c: c.path)
    plan.refusals.sort(key=lambda r: (r.reason, r.path))
    plan.already_uploaded.sort()
    plan.folders_to_create = sorted(wanted_dirs, key=lambda p: (p.count("/"), p))

    plan.quota = _check_quota(account_storage, plan.total_bytes)

    _coverage_notes(plan, no_digest, len(placeholder_paths))
    return plan


def _examine(
    root: Path,
    path: str,
    row: Dict[str, Any],
    placeholder_paths: Dict[str, Any],
    manifest: Optional[Manifest],
    check_digest: bool = True,
) -> Optional[Refusal]:
    """Every local safety rule, in one place, run at plan time and again at send.

    Returns the refusal, or ``None`` when the file may be uploaded. Order is
    chosen so the most specific reason wins: a package bundle that is also
    outside the root should be reported as escaping the mirror, because that is
    the more alarming fact.
    """
    target = root / path
    size = row.get("size")

    try:
        resolved = target.resolve()
    except OSError as exc:
        return Refusal(path, REFUSE_UNREADABLE, str(exc), size)

    if not _within(root, resolved):
        return Refusal(
            path, REFUSE_OUTSIDE_ROOT,
            f"resolves to '{resolved}', which is outside the mirror at "
            f"'{root}'. A path that leaves the mirror - directly or through a "
            "symlink - is never uploaded.",
            size,
        )

    if row.get("kind") == KIND_PACKAGE or is_package_name(path):
        return Refusal(
            path, REFUSE_PACKAGE,
            "this is a package bundle: a directory on this disk, and a single "
            "archive as far as Apple is concerned. iFetch cannot reliably "
            "rebuild the archive Apple accepts, so it is skipped rather than "
            "uploaded in a form that may not open. Restore it by hand.",
            size,
        )

    # Before "is there a file here?", because the two placeholder shapes answer
    # that question differently: an evicted file with a ``.icloud`` stub is not
    # on disk at all, while a dataless one is there at full size. Both are
    # placeholders, and that is the reason worth printing for either.
    placeholder = placeholder_paths.get(_nfc(path))
    if placeholder is not None:
        return Refusal(
            path, REFUSE_PLACEHOLDER,
            "the local copy is an evicted placeholder - it has a size in "
            f"Finder and no bytes underneath ({placeholder.evidence}). "
            "Uploading it would put an empty file into iCloud under a real "
            "filename. " + (placeholder.detail or ""),
            size,
        )

    if target.is_dir():
        return Refusal(
            path, REFUSE_NOT_A_FILE,
            "the index records a file at this path and there is a directory "
            "there now. iFetch uploads files, one at a time, and will not "
            "guess what this should have been.",
            size,
        )

    if not target.is_file():
        return Refusal(
            path, REFUSE_MISSING_LOCALLY,
            "the index records this file but there is nothing at that path "
            "now, so there are no bytes to send.",
            size,
        )

    if not check_digest:
        return None

    expected, source = _recorded_digest(root, path, row, manifest)
    if not expected:
        return None
    try:
        actual = sha256_file(target)
    except OSError as exc:
        return Refusal(path, REFUSE_UNREADABLE, str(exc), size)
    if actual != expected:
        return Refusal(
            path, REFUSE_DIGEST_MISMATCH,
            f"the bytes on disk hash to {actual[:16]}... and {source} records "
            f"{expected[:16]}.... The local copy has changed or been damaged "
            "since it was recorded, and iFetch will not push that into iCloud.",
            size,
        )
    return None


def _recorded_digest(
    root: Path,
    path: str,
    row: Dict[str, Any],
    manifest: Optional[Manifest],
) -> Tuple[Optional[str], str]:
    """The digest recorded for this file, and which record it came from.

    The manifest is preferred because it is the signable artifact the integrity
    claim rests on. The index row is accepted as a fallback: a digest from a
    local scan is weaker evidence than a signed one, but it is far better than
    uploading with no integrity check at all.
    """
    if manifest is not None:
        entry = manifest.get(root / path)
        if entry and entry.get("sha256"):
            return str(entry["sha256"]), "the manifest"
    if row.get("sha256"):
        return str(row["sha256"]), "the index"
    return None, ""


def _within(root: Path, path: Path) -> bool:
    try:
        path.relative_to(root)
    except ValueError:
        return False
    return path != root


def _parent(path: str) -> str:
    parent = Path(path).parent.as_posix()
    return "" if parent == "." else parent


def _ancestors(directory: str) -> List[str]:
    """Every folder on the way to ``directory``, outermost first."""
    parts = [p for p in directory.split("/") if p]
    return ["/".join(parts[: i + 1]) for i in range(len(parts))]


def _join(base: str, relative: str) -> str:
    base = (base or "").strip("/")
    relative = (relative or "").strip("/")
    if not base:
        return relative
    return f"{base}/{relative}" if relative else base


def _size_on_disk(target: Path, recorded: Optional[int]) -> Optional[int]:
    try:
        return target.stat().st_size
    except OSError:
        return recorded


def _check_quota(
    account_storage: Optional[Dict[str, Any]], required: int
) -> QuotaCheck:
    """Compare what this would send against what Apple says is free.

    Apple's storage endpoint is not part of the Drive API and can be absent or
    fail on its own. When it gives nothing, the check is reported as skipped -
    the upload still proceeds, but the report does not pretend room was
    confirmed.
    """
    quota = QuotaCheck(required_bytes=required)

    available = None
    if account_storage:
        available = account_storage.get("available_bytes")
        if available is None:
            total = account_storage.get("total_bytes")
            used = account_storage.get("used_bytes")
            if isinstance(total, int) and isinstance(used, int):
                available = total - used

    if not isinstance(available, int):
        quota.detail = (
            "Apple did not return usable storage figures, so free space was "
            "NOT checked. This is unknown, not confirmed: an upload may still "
            "fail for want of room."
        )
        return quota

    quota.checked = True
    quota.available_bytes = available
    quota.sufficient = available >= required
    if quota.sufficient:
        quota.detail = (
            f"{human_bytes(available)} free in iCloud, "
            f"{human_bytes(required)} to send."
        )
    else:
        quota.detail = (
            f"this would send {human_bytes(required)} and iCloud reports only "
            f"{human_bytes(available)} free. Nothing is uploaded: a run that "
            "fills the account partway through leaves you with neither a "
            "restore nor the space to finish one."
        )
    return quota


def _coverage_notes(plan: UploadPlan, no_digest: int, placeholders: int) -> None:
    """State what the plan could not confirm, whatever the outcome was."""
    if plan.local_count == 0:
        plan.unexamined.append({
            "what": "the local mirror",
            "count": "all",
            "why": (
                "the index holds no local files, so nothing could be compared "
                "against iCloud. That is not 'nothing is missing'. Run "
                "'ifetch plan' to index the mirror first."
            ),
        })

    if no_digest:
        plan.unexamined.append({
            "what": "files with no recorded digest",
            "count": str(no_digest),
            "why": (
                "neither the manifest nor the index holds a digest for them, so "
                "iFetch can say the bytes are there and not that they are the "
                "bytes it downloaded. Run 'ifetch verify' first if that matters."
            ),
        })

    packages = len(plan.refusals_by_reason(REFUSE_PACKAGE))
    if packages:
        plan.notes.append(
            f"{packages:,} package {plural(packages, 'bundle')} "
            f"{plural(packages, 'was', 'were')} skipped. They are listed below "
            "with the reason; they are not uploaded in any form."
        )
    if placeholders:
        plan.notes.append(
            f"{placeholders:,} local {plural(placeholders, 'file')} "
            f"{plural(placeholders, 'is', 'are')} an evicted placeholder. Any "
            "that were also missing from iCloud are refused below."
        )
    if plan.already_uploaded:
        count = len(plan.already_uploaded)
        plan.notes.append(
            f"{count:,} {plural(count, 'file')} {plural(count, 'was', 'were')} "
            "already uploaded by an earlier run and will not be sent again."
        )
    if plan.candidates and not plan.scan.finished_at:
        plan.notes.append(
            "The remote listing behind this plan has no recorded finish time, "
            "so how old it is cannot be stated."
        )


# ---------------------------------------------------------------------------
# The connection to iCloud
# ---------------------------------------------------------------------------

class DriveUplink:
    """The only slice of iCloud Drive this feature is allowed to touch.

    Four operations: resolve a folder, list the names in it, create a folder,
    and send a new file into one. There is no delete, no rename and no
    overwrite here, so no bug in the calling code can express one.

    ``downloader`` is anything with ``get_drive_item(path)`` - in production a
    :class:`~ifetch.downloader.DownloadManager`.
    """

    def __init__(self, downloader: Any, base: str = ""):
        self.downloader = downloader
        self.base = (base or "").strip("/")
        self.created: List[str] = []

    # -- lookups --------------------------------------------------------
    def resolve(self, relative_dir: str) -> Any:
        """The node for a folder that already exists. Raises if it does not."""
        return self.downloader.get_drive_item(_join(self.base, relative_dir))

    def child_names(self, node: Any) -> Optional[Set[str]]:
        """The names in a folder, or ``None`` when the listing could not be read.

        ``None`` is not "empty". A caller that treated it as empty would decide
        a file is absent on the strength of a failed request and overwrite it.
        """
        try:
            contents = node.dir()
        except Exception:
            return None
        if contents is None:
            return None
        names = list(contents.keys()) if hasattr(contents, "keys") else list(contents)
        return {_nfc(str(name)) for name in names}

    def walk(self, relative_dir: str) -> Tuple[Optional[Any], bool]:
        """Follow a folder path by listing. Returns ``(node, reachable)``.

        ``node`` is ``None`` when the folder genuinely is not there; when
        ``reachable`` is False a listing failed and nothing at all may be
        concluded. Keeping those two apart is the point: "the folder is not
        there" proves the file is not there, and "the listing failed" proves
        nothing.
        """
        try:
            node = self.resolve("")
        except Exception:
            return None, False

        for part in [p for p in relative_dir.split("/") if p]:
            names = self.child_names(node)
            if names is None:
                return None, False
            if _nfc(part) not in names:
                return None, True
            child = self._child(node, part)
            if child is None:
                return None, False
            node = child
        return node, True

    def exists(self, relative_dir: str, name: str) -> Optional[bool]:
        """Is ``name`` in that folder right now? ``None`` when it cannot be told.

        The listing is re-read on every call rather than cached for the run.
        That costs one request per file, which is the right trade when the
        alternative is overwriting something on the strength of a stale answer.
        """
        node, reachable = self.walk(relative_dir)
        if not reachable:
            return None
        if node is None:
            return False  # The folder itself does not exist, so nor does the file.
        names = self.child_names(node)
        if names is None:
            return None
        return _nfc(name) in names

    # -- writes ---------------------------------------------------------
    def ensure_folder(self, relative_dir: str) -> Any:
        """Return the node for ``relative_dir``, creating what is missing.

        Only folders are created, and only ones that are absent - an existing
        folder is reused, never recreated.
        """
        node = self.resolve("")
        if not relative_dir:
            return node

        walked: List[str] = []
        for part in [p for p in relative_dir.split("/") if p]:
            walked.append(part)
            child = self._child(node, part)
            if child is None:
                node.mkdir(part)
                self.created.append("/".join(walked))
                child = self._child(node, part, force=True)
                if child is None:
                    raise UplinkError(
                        f"created the folder '{'/'.join(walked)}' but iCloud did "
                        "not then list it, so its contents cannot be addressed"
                    )
            node = child
        return node

    def send(self, node: Any, local_path: Path) -> Any:
        """Upload one file into ``node``. Apple names it after the local file."""
        with Path(local_path).open("rb") as handle:
            return node.upload(handle)

    # -- helpers --------------------------------------------------------
    def _child(self, node: Any, name: str, force: bool = False) -> Optional[Any]:
        if force:
            refresh = getattr(node, "get_children", None)
            if callable(refresh):
                try:
                    refresh(force=True)
                except Exception:
                    pass
        names = self.child_names(node)
        if names is None or _nfc(name) not in names:
            return None
        try:
            return node[name]
        except Exception:
            # Apple returned the name in a different normalisation than we
            # asked for; find the spelling it actually used.
            for actual in self._raw_names(node):
                if _nfc(actual) == _nfc(name):
                    try:
                        return node[actual]
                    except Exception:
                        return None
        return None

    def _raw_names(self, node: Any) -> List[str]:
        try:
            contents = node.dir()
        except Exception:
            return []
        if contents is None:
            return []
        return [
            str(n) for n in
            (contents.keys() if hasattr(contents, "keys") else contents)
        ]


# ---------------------------------------------------------------------------
# The run
# ---------------------------------------------------------------------------

@dataclass
class UploadOutcome:
    path: str
    remote_path: str
    status: str
    detail: str = ""
    size: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "path": self.path,
            "remote_path": self.remote_path,
            "status": self.status,
            "detail": self.detail,
            "size": self.size,
        }


@dataclass
class UploadRun:
    """What a run did, per file. A failure never removes a file from this list."""

    root: str = ""
    dry_run: bool = True
    refused: bool = False
    refusal_detail: str = ""
    outcomes: List[UploadOutcome] = field(default_factory=list)
    folders_created: List[str] = field(default_factory=list)
    generated_at: str = ""

    def by_status(self, status: str) -> List[UploadOutcome]:
        return [o for o in self.outcomes if o.status == status]

    def counts(self) -> Dict[str, int]:
        tally: Dict[str, int] = {}
        for outcome in self.outcomes:
            tally[outcome.status] = tally.get(outcome.status, 0) + 1
        return tally

    @property
    def uploaded_bytes(self) -> int:
        return sum(o.size or 0 for o in self.by_status(STATUS_UPLOADED))

    @property
    def planned_bytes(self) -> int:
        return sum(o.size or 0 for o in self.by_status(STATUS_WOULD_UPLOAD))

    @property
    def failed(self) -> List[UploadOutcome]:
        return self.by_status(STATUS_FAILED)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "root": self.root,
            "generated_at": self.generated_at,
            "dry_run": self.dry_run,
            "refused": self.refused,
            "refusal_detail": self.refusal_detail,
            "counts": self.counts(),
            "uploaded_bytes": self.uploaded_bytes,
            "planned_bytes": self.planned_bytes,
            "folders_created": list(self.folders_created),
            "outcomes": [o.to_dict() for o in self.outcomes],
        }


def apply_uploads(
    plan: UploadPlan,
    store: IndexStore,
    root: Path,
    drive: Optional[DriveUplink] = None,
    dry_run: bool = True,
    placeholders: Optional[PlaceholderReport] = None,
    manifest: Optional[Manifest] = None,
    now: Optional[float] = None,
) -> UploadRun:
    """Send the planned files, or say exactly what sending them would do.

    ``dry_run`` is the default and contacts nobody. With ``dry_run=False`` each
    candidate is re-checked against the disk and against iCloud in the moment
    before it is sent: a plan is minutes old by the time it is applied, and the
    file may have come back, been evicted, or been damaged since.

    One file failing is recorded against that file and the run continues. Every
    success is written to the index before the next file starts, so an
    interrupted run resumes where it stopped.
    """
    now = time.time() if now is None else float(now)
    root = Path(root).resolve()
    run = UploadRun(root=str(root), dry_run=dry_run, generated_at=_iso(now))

    if plan.refused:
        # The single most dangerous failure mode in this feature: if the scan
        # is unusable every file looks missing, and this would upload the whole
        # mirror. Nothing is sent, at any count, and the reason is carried out.
        run.refused = True
        run.refusal_detail = plan.refusal_detail
        return run

    if not dry_run and drive is None:
        raise UplinkError(
            "applying an upload plan needs an authenticated connection to "
            "iCloud; none was supplied"
        )

    if dry_run:
        for candidate in plan.candidates:
            run.outcomes.append(UploadOutcome(
                candidate.path, candidate.remote_path, STATUS_WOULD_UPLOAD,
                f"would create {candidate.remote_path}", candidate.size,
            ))
        # ``folders_created`` counts folders this run made. A dry run made
        # none; what it *would* make is in the plan.
        return run

    if placeholders is None:
        placeholders = PlaceholderDetector(root).scan()
    placeholder_paths = {_nfc(p.path): p for p in placeholders.placeholders}

    for candidate in plan.candidates:
        outcome = _send_one(
            candidate, store, root, drive, placeholder_paths, manifest, now,
        )
        run.outcomes.append(outcome)

    run.folders_created = list(drive.created) if drive is not None else []
    return run


def _send_one(
    candidate: UploadCandidate,
    store: IndexStore,
    root: Path,
    drive: DriveUplink,
    placeholder_paths: Dict[str, Any],
    manifest: Optional[Manifest],
    now: float,
) -> UploadOutcome:
    """One file, with every check repeated. Never raises; records instead."""
    path = candidate.path

    recorded = store.get_upload(path)
    if recorded is not None and recorded.get("state") == UPLOAD_DONE:
        return UploadOutcome(
            path, candidate.remote_path, STATUS_SKIPPED,
            "already uploaded by an earlier run at "
            f"{_iso(recorded.get('updated_at'))}; not sent again.",
            candidate.size,
        )

    row = store.get_local(path) or {
        "path": path, "kind": KIND_FILE,
        "size": candidate.size, "sha256": candidate.sha256,
    }
    refusal = _examine(root, path, row, placeholder_paths, manifest)
    if refusal is not None:
        return UploadOutcome(
            path, candidate.remote_path, STATUS_FAILED,
            f"refused ({_REFUSAL_LABELS.get(refusal.reason, refusal.reason)}): "
            + refusal.detail,
            candidate.size,
        )

    present = drive.exists(candidate.parent, candidate.name)
    if present is True:
        return UploadOutcome(
            path, candidate.remote_path, STATUS_SKIPPED,
            "it is in iCloud now - it appeared between the plan and this "
            "upload. iFetch never overwrites, so it was left alone.",
            candidate.size,
        )
    if present is None:
        # Not knowing is not the same as knowing it is absent, and the cost of
        # confusing the two is an overwritten file.
        detail = (
            "could not confirm the file is still absent from iCloud - the "
            "folder listing failed. Nothing was uploaded, because an upload "
            "on an unconfirmed absence risks overwriting."
        )
        _record(store, candidate, UPLOAD_FAILED, detail, now)
        return UploadOutcome(
            path, candidate.remote_path, STATUS_FAILED, detail, candidate.size,
        )

    try:
        node = drive.ensure_folder(candidate.parent)
        drive.send(node, root / path)
    except Exception as exc:
        detail = f"upload failed: {exc}"
        _record(store, candidate, UPLOAD_FAILED, detail, now)
        return UploadOutcome(
            path, candidate.remote_path, STATUS_FAILED, detail, candidate.size,
        )

    _record(store, candidate, UPLOAD_DONE, None, now)
    return UploadOutcome(
        path, candidate.remote_path, STATUS_UPLOADED,
        f"uploaded to {candidate.remote_path}", candidate.size,
    )


def _record(
    store: IndexStore,
    candidate: UploadCandidate,
    state: str,
    error: Optional[str],
    now: float,
) -> None:
    """Journal one upload. A journal write must never fail the run itself."""
    try:
        store.record_upload(
            candidate.path, remote_path=candidate.remote_path, state=state,
            size=candidate.size, sha256=candidate.sha256, error=error, at=now,
        )
    except Exception:  # pragma: no cover - a failed write is not a failed upload
        pass


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------

def render_plan(plan: UploadPlan, show: int = 40) -> str:
    """The dry-run report: what would be sent, where, and how much of it."""
    lines = [
        rule("="),
        "iFetch upload plan - missing files only, nothing overwritten",
        rule("="),
        key_values([
            ("Local mirror", plan.root),
            ("iCloud folder", plan.icloud_path or "(root)"),
            ("Files in the mirror", f"{plan.local_count:,}"),
            ("Missing from iCloud", f"{plan.missing_count:,}"),
            ("Would upload", f"{len(plan.candidates):,}"),
            ("Bytes to send", human_bytes(plan.total_bytes)),
            ("Refused", f"{len(plan.refusals):,}"),
            ("Folders to create", f"{len(plan.folders_to_create):,}"),
            ("Quota", _quota_line(plan.quota)),
        ]),
        "",
    ]

    if plan.unexamined:
        lines.append("Not examined")
        for gap in plan.unexamined:
            lines.append(f"  ! {gap['what']} ({gap['count']}): {gap['why']}")
        lines.append("")

    if plan.refused:
        lines.extend(_refusal_block(plan))
        lines.append(rule("="))
        return "\n".join(lines)

    if plan.candidates:
        lines.append(table(
            ["size", "would upload to"],
            [[human_bytes(c.size), c.remote_path] for c in plan.candidates[:show]],
            align=[">", "<"],
        ))
        if len(plan.candidates) > show:
            lines.append(f"  ... and {len(plan.candidates) - show:,} more")
        lines.append("")
    else:
        lines.append(
            "Nothing to upload: every file in this mirror is already listed by "
            "iCloud, or was refused below."
        )
        lines.append("")

    if plan.folders_to_create:
        lines.append("Folders that would be created")
        lines.extend(f"  + {_join(plan.icloud_path, d)}"
                     for d in plan.folders_to_create[:show])
        if len(plan.folders_to_create) > show:
            lines.append(f"  ... and {len(plan.folders_to_create) - show:,} more")
        lines.append("")

    lines.extend(_refusals_block(plan, show))

    if plan.notes:
        lines.append("Notes")
        lines.extend(f"  - {note}" for note in plan.notes)
        lines.append("")

    lines.append(rule("="))
    lines.append(
        "Nothing has been uploaded. Pass --apply to send the files above; only "
        "files iCloud does not have are sent, and nothing is overwritten."
    )
    return "\n".join(lines)


def render_run(run: UploadRun, plan: Optional[UploadPlan] = None, show: int = 40) -> str:
    """What a run actually did, per file."""
    counts = run.counts()
    pairs = [
        ("Local mirror", run.root),
        ("Uploaded", f"{counts.get(STATUS_UPLOADED, 0):,}"),
        ("Would upload", f"{counts.get(STATUS_WOULD_UPLOAD, 0):,}"),
        ("Skipped", f"{counts.get(STATUS_SKIPPED, 0):,}"),
        ("Failed", f"{counts.get(STATUS_FAILED, 0):,}"),
    ]
    if run.dry_run:
        pairs.append(("Bytes to send", human_bytes(run.planned_bytes)))
    else:
        pairs.append(("Bytes sent", human_bytes(run.uploaded_bytes)))
        pairs.append(("Folders created", f"{len(run.folders_created):,}"))
    if plan is not None:
        pairs.append(("Quota", _quota_line(plan.quota)))

    lines = [
        rule("="),
        "iFetch upload run" + (" (dry run - nothing was sent)" if run.dry_run else ""),
        rule("="),
        key_values(pairs),
        "",
    ]

    if plan is not None and plan.unexamined:
        lines.append("Not examined")
        for gap in plan.unexamined:
            lines.append(f"  ! {gap['what']} ({gap['count']}): {gap['why']}")
        lines.append("")

    if run.refused:
        lines.append("REFUSED: nothing was uploaded.")
        lines.append(f"  {run.refusal_detail}")
        if plan is not None:
            lines.append("")
            lines.extend(_refusal_block(plan))
        lines.append(rule("="))
        return "\n".join(lines)

    if run.outcomes:
        lines.append(table(
            ["status", "size", "path"],
            [[o.status, human_bytes(o.size), o.remote_path]
             for o in run.outcomes[:show]],
            align=["<", ">", "<"],
        ))
        if len(run.outcomes) > show:
            lines.append(f"  ... and {len(run.outcomes) - show:,} more")
        lines.append("")
    else:
        lines.append("Nothing to do.")
        lines.append("")

    for status, title in (
        (STATUS_FAILED, "Failed - each one, with the reason"),
        (STATUS_SKIPPED, "Skipped"),
    ):
        group = run.by_status(status)
        if not group:
            continue
        lines.append(title)
        for outcome in group[:show]:
            lines.append(f"  {outcome.path}")
            lines.append(f"      {outcome.detail}")
        if len(group) > show:
            lines.append(f"  ... and {len(group) - show:,} more")
        lines.append("")

    if plan is not None:
        lines.extend(_refusals_block(plan, show))
        if run.dry_run and plan.folders_to_create:
            lines.append("Folders that would be created")
            lines.extend(f"  + {_join(plan.icloud_path, d)}"
                         for d in plan.folders_to_create[:show])
            lines.append("")

    lines.append(rule("="))
    if run.dry_run:
        lines.append(
            "Nothing was uploaded. Pass --apply to send the files above; only "
            "files iCloud does not have are sent, and nothing is overwritten."
        )
    return "\n".join(lines)


def render_uploads(rows: Sequence[Dict[str, Any]], show: int = 40) -> str:
    """The journal of everything iFetch has ever sent to iCloud."""
    lines = [
        rule("="),
        "iFetch upload history",
        rule("="),
        key_values([("Recorded uploads", f"{len(rows):,}")]),
        "",
    ]
    if not rows:
        lines.append(
            "iFetch has never uploaded anything from this mirror. That is the "
            "normal state: it is a downloader."
        )
        lines.append(rule("="))
        return "\n".join(lines)

    lines.append(table(
        ["state", "when", "size", "path"],
        [
            [
                row.get("state", ""), _iso(row.get("updated_at")),
                human_bytes(row.get("size")), row.get("remote_path") or row["path"],
            ]
            for row in rows[:show]
        ],
        align=["<", "<", ">", "<"],
    ))
    if len(rows) > show:
        lines.append(f"  ... and {len(rows) - show:,} more")
    lines.append(rule("="))
    return "\n".join(lines)


def _quota_line(quota: QuotaCheck) -> str:
    if not quota.checked:
        return "NOT CHECKED"
    return ("enough room" if quota.sufficient else "NOT ENOUGH ROOM") + \
        f" ({human_bytes(quota.available_bytes)} free)"


def _refusals_block(plan: UploadPlan, show: int) -> List[str]:
    if not plan.refusals:
        return []
    lines = [
        f"Refused - {len(plan.refusals):,} "
        f"{plural(len(plan.refusals), 'file')} will not be uploaded"
    ]
    lines.append(table(
        ["reason", "size", "path"],
        [[_REFUSAL_LABELS.get(r.reason, r.reason), human_bytes(r.size), r.path]
         for r in plan.refusals[:show]],
        align=["<", ">", "<"],
    ))
    if len(plan.refusals) > show:
        lines.append(f"  ... and {len(plan.refusals) - show:,} more")
    lines.append("")
    for refusal in plan.refusals[:show]:
        lines.append(f"  {refusal.path}")
        lines.append(f"      {refusal.detail}")
    lines.append("")
    return lines


def _refusal_block(plan: UploadPlan) -> List[str]:
    """The whole-run refusal notice. Says once why nothing may be sent."""
    # The headline is built by appending the underlying reason to a lead-in, so
    # the first alternative usually arrives as a substring of what was just
    # printed. See ifetch.render.say_once.
    fresh = say_once()
    lines = [
        rule("-"),
        "REFUSED: nothing will be uploaded.",
        rule("-"),
    ]
    if fresh(plan.refusal_detail):
        lines.append(f"  {plan.refusal_detail}")
    if plan.breaker.tripped:
        alternatives = [alt for alt in plan.breaker.cannot_rule_out if fresh(alt)]
        if alternatives:
            lines.append("")
            lines.append("  What this could be instead - none of it ruled out:")
            lines.extend(f"    - {alt}" for alt in alternatives)
        lines.extend([
            "",
            f"  {plan.missing_count:,} local "
            f"{plural(plan.missing_count, 'file')} did not appear in the iCloud "
            "listing.",
            "  They are in the JSON payload and are deliberately not offered as "
            "uploads:",
            "  if the listing is wrong, uploading them would push this mirror "
            "back into",
            "  the account wholesale.",
            "",
            "  Do this first: re-scan with --refresh on a working connection, "
            "then re-run",
            "  this plan. If the same files are still missing from a clean "
            "scan, they are",
            "  missing.",
        ])
    return lines


def csv_rows(plan: UploadPlan, run: Optional[UploadRun] = None):
    """Headers and rows for the CSV export: every candidate and every refusal."""
    headers = ["path", "remote_path", "disposition", "size", "sha256", "detail"]
    statuses = {o.path: o for o in (run.outcomes if run is not None else [])}
    rows: List[List[Any]] = []

    for candidate in plan.candidates:
        outcome = statuses.get(candidate.path)
        rows.append([
            candidate.path, candidate.remote_path,
            outcome.status if outcome else STATUS_WOULD_UPLOAD,
            candidate.size, candidate.sha256,
            outcome.detail if outcome else "planned",
        ])
    for refusal in plan.refusals:
        rows.append([
            refusal.path, _join(plan.icloud_path, refusal.path),
            f"refused:{refusal.reason}", refusal.size, None, refusal.detail,
        ])
    return headers, rows
