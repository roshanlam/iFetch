"""Read-only integrity verification of a local iCloud Drive mirror.

This module compares what is on disk against what iCloud reports, so that a
backup can be *proven* rather than assumed.

READ-ONLY GUARANTEE
-------------------
Verification NEVER mutates the user's files.  Nothing in this module writes,
truncates, renames, moves or deletes anything inside the local tree: local
files are opened strictly in binary read mode, remote files (in
``redownload`` mode) are streamed into memory and hashed chunk-by-chunk and
are never persisted to disk.  The only file this module may create is the
optional JSON report, and only at the explicit path the caller passes to
``--report``.

Verification levels
-------------------
``size`` (default, fast)
    Compare the remote ``size`` attribute with ``Path.stat().st_size``.
    No file content is read and no bytes are transferred.

``checksum`` (thorough, local read only)
    Everything ``size`` does, plus a SHA-256 of the local file.  A remote
    checksum is used for the comparison *only if* the drive node exposes one
    (see ``remote_checksum``); Apple's iCloud Drive item metadata does not
    currently include a content hash, so in practice this level records the
    local digest and reports ``checksum_unavailable`` rather than claiming a
    match it cannot prove.  Useful for building a local manifest and for
    catching local bit-rot across runs.

``redownload`` (thorough, bandwidth-expensive, OPT-IN)
    Streams the remote file and compares a SHA-256 of the stream against a
    SHA-256 of the local file.  This is the only level that can actually prove
    byte-for-byte equality with iCloud.  It re-downloads every byte of every
    file, so it must be explicitly requested.
"""

from __future__ import annotations

import argparse
import fnmatch
import hashlib
import json
import os
import sys
import threading
import time
import traceback
from dataclasses import dataclass, field, asdict
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, Union

if __package__ in (None, ""):  # pragma: no cover - standalone script support
    sys.path.append(str(Path(__file__).resolve().parent.parent))
    from ifetch.downloader import DownloadManager  # type: ignore
    from ifetch.logger import setup_logging  # type: ignore
    from ifetch.naming import NORMALIZE_PRESERVE, DirectorySanitizer  # type: ignore
    from ifetch.utils import can_read_file  # type: ignore
else:
    from .downloader import DownloadManager
    from .logger import setup_logging
    from .naming import NORMALIZE_PRESERVE, DirectorySanitizer
    from .utils import can_read_file


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

LEVEL_SIZE = "size"
LEVEL_CHECKSUM = "checksum"
LEVEL_REDOWNLOAD = "redownload"
LEVELS = (LEVEL_SIZE, LEVEL_CHECKSUM, LEVEL_REDOWNLOAD)

STATUS_VERIFIED = "verified"
STATUS_SIZE_MISMATCH = "size_mismatch"
STATUS_MISSING_LOCAL = "missing_local"
STATUS_EXTRA_LOCAL = "extra_local"
STATUS_CHECKSUM_MISMATCH = "checksum_mismatch"
STATUS_CHECKSUM_UNAVAILABLE = "checksum_unavailable"
STATUS_ERROR = "error"

#: Statuses that make the whole run fail (exit code 1).
FAILURE_STATUSES = frozenset({
    STATUS_SIZE_MISMATCH,
    STATUS_MISSING_LOCAL,
    STATUS_CHECKSUM_MISMATCH,
    STATUS_ERROR,
})

#: Files iFetch itself writes into the download tree.  They have no remote
#: counterpart and must not be reported as "extra".
IFETCH_ARTIFACTS = frozenset({
    "download_report.json",
    ".ifetch_versions.json",
    ".DS_Store",
})
IFETCH_ARTIFACT_DIRS = frozenset({".versions"})
IFETCH_ARTIFACT_SUFFIXES = (".temp", ".download")

#: Keys that *might* carry a remote content hash.  Apple does not populate any
#: of them for iCloud Drive nodes today (see module docstring), but a future
#: pyicloud/Apple change would light this up automatically.
REMOTE_CHECKSUM_KEYS = (
    "fileChecksum",
    "checksum",
    "contentChecksum",
    "signature",
    "referenceChecksum",
)

_READ_CHUNK = 1024 * 1024


# ---------------------------------------------------------------------------
# Result models
# ---------------------------------------------------------------------------

@dataclass
class VerificationResult:
    """Outcome of verifying a single file."""

    path: str
    status: str
    local_size: Optional[int] = None
    remote_size: Optional[int] = None
    local_checksum: Optional[str] = None
    remote_checksum: Optional[str] = None
    reason: str = ""

    @property
    def ok(self) -> bool:
        return self.status not in FAILURE_STATUSES

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class VerificationSummary:
    """Aggregate outcome of a verification run."""

    level: str
    icloud_path: str
    local_root: str
    results: List[VerificationResult] = field(default_factory=list)
    duration_seconds: float = 0.0
    timestamp: str = ""

    @property
    def counts(self) -> Dict[str, int]:
        counts = {
            "total": len(self.results),
            STATUS_VERIFIED: 0,
            STATUS_SIZE_MISMATCH: 0,
            STATUS_MISSING_LOCAL: 0,
            STATUS_EXTRA_LOCAL: 0,
            STATUS_CHECKSUM_MISMATCH: 0,
            STATUS_CHECKSUM_UNAVAILABLE: 0,
            STATUS_ERROR: 0,
        }
        for result in self.results:
            counts[result.status] = counts.get(result.status, 0) + 1
        return counts

    @property
    def failures(self) -> List[VerificationResult]:
        return [r for r in self.results if r.status in FAILURE_STATUSES]

    @property
    def ok(self) -> bool:
        """True when no file failed verification."""
        return not self.failures

    def to_dict(self) -> Dict[str, Any]:
        return {
            "summary": {
                "level": self.level,
                "icloud_path": self.icloud_path,
                "local_root": self.local_root,
                "timestamp": self.timestamp,
                "duration_seconds": round(self.duration_seconds, 3),
                "ok": self.ok,
                "counts": self.counts,
            },
            "details": [r.to_dict() for r in self.results],
        }


ProgressCallback = Callable[[int, int, VerificationResult], None]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def remote_checksum(item: Any) -> Optional[str]:
    """Return a remote content checksum for a drive node, if Apple exposes one.

    As of pyicloud 2.6.5 / the current iCloud Drive web API, item metadata
    returned by ``retrieveItemDetailsInFolders`` contains ``size``, ``etag``,
    ``dateChanged``/``dateModified``, ``docwsid``/``drivewsid`` and ``zone`` --
    but no content hash.  ``etag`` is a mutation counter (e.g. ``"3::5"``), not
    a digest, and is deliberately ignored here.  ``fileChecksum`` only appears
    on the *upload* path and is an Apple-proprietary wrapped signature, not a
    plain digest of the file bytes.

    This helper therefore returns ``None`` in practice; it exists so that the
    verifier picks up a real digest automatically should Apple start
    publishing one.
    """
    data = getattr(item, "data", None)
    if not isinstance(data, dict):
        return None
    for key in REMOTE_CHECKSUM_KEYS:
        value = data.get(key)
        if isinstance(value, str) and value:
            return value
    return None


def sha256_file(path: Path, chunk_size: int = _READ_CHUNK) -> str:
    """SHA-256 a local file.  Opens read-only; never mutates the file."""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(chunk_size), b""):
            digest.update(chunk)
    return digest.hexdigest()


def sha256_stream(response: Any, chunk_size: int = _READ_CHUNK) -> Tuple[str, int]:
    """SHA-256 a streamed HTTP response without ever writing it to disk.

    Returns ``(hexdigest, bytes_seen)``.
    """
    digest = hashlib.sha256()
    total = 0
    iterator = getattr(response, "iter_content", None)
    if callable(iterator):
        stream = iterator(chunk_size=chunk_size)
    else:  # pragma: no cover - defensive: raw file-like fallback
        stream = iter(lambda: response.read(chunk_size), b"")
    for chunk in stream:
        if not chunk:
            continue
        digest.update(chunk)
        total += len(chunk)
    return digest.hexdigest(), total


# ---------------------------------------------------------------------------
# Verifier
# ---------------------------------------------------------------------------

class Verifier:
    """Compare a local mirror against iCloud Drive.  Strictly read-only.

    The verifier composes a :class:`~ifetch.downloader.DownloadManager` rather
    than reimplementing authentication or path traversal; pass one in via
    ``downloader`` (already authenticated or not) or let the verifier build one
    from ``email``.
    """

    def __init__(
        self,
        local_root: Union[str, Path],
        downloader: Optional[DownloadManager] = None,
        email: Optional[str] = None,
        level: str = LEVEL_SIZE,
        max_workers: int = 4,
        progress_callback: Optional[ProgressCallback] = None,
        include_patterns: Optional[List[str]] = None,
        exclude_patterns: Optional[List[str]] = None,
        chunk_size: int = _READ_CHUNK,
        logger: Optional[Any] = None,
        portable_names: Optional[bool] = None,
        normalize_names: Optional[str] = None,
    ):
        if level not in LEVELS:
            raise ValueError(
                f"Unknown verification level '{level}'. Choose one of: {', '.join(LEVELS)}"
            )

        self.local_root = Path(local_root).expanduser().resolve()
        self.level = level
        self.max_workers = max(1, int(max_workers))
        self.progress_callback = progress_callback
        self.include_patterns = include_patterns or []
        self.exclude_patterns = exclude_patterns or []
        self.chunk_size = chunk_size
        self.logger = logger or setup_logging()

        if downloader is None:
            downloader = DownloadManager(email=email, max_workers=self.max_workers)
        self.downloader = downloader

        # Default to whatever the downloader is configured with, so verifying a
        # mirror never disagrees with the run that produced it about what a
        # given file is called. Explicit arguments still win, which is what lets
        # a mirror written with different settings be checked.
        self.portable_names = (
            getattr(downloader, "portable_names", False)
            if portable_names is None
            else portable_names
        )
        self.normalize_names = (
            getattr(downloader, "normalize_names", NORMALIZE_PRESERVE)
            if normalize_names is None
            else normalize_names
        )

        self._lock = threading.Lock()
        self._completed = 0
        self._total = 0

    # -- remote tree walking ------------------------------------------------

    def _authenticate_if_needed(self) -> None:
        if getattr(self.downloader, "api", None) is None:
            self.downloader.authenticate()

    def _should_check(self, rel_path: str) -> bool:
        for pattern in self.exclude_patterns:
            if fnmatch.fnmatch(rel_path, pattern):
                return False
        if not self.include_patterns:
            return True
        return any(fnmatch.fnmatch(rel_path, pattern) for pattern in self.include_patterns)

    def collect_remote_files(self, node: Any, prefix: str = "") -> List[Tuple[str, Any]]:
        """Walk a drive node and return ``[(relative_posix_path, node), ...]``.

        Traversal errors on one subtree are logged and contained; the rest of
        the tree is still walked.
        """
        found: List[Tuple[str, Any]] = []

        if can_read_file(node):
            rel = prefix or str(getattr(node, "name", "") or "")
            if rel and self._should_check(rel):
                found.append((rel, node))
            return found

        if not hasattr(node, "dir"):
            return found

        try:
            contents = node.dir()
        except Exception as exc:
            self.logger.error(json.dumps({
                "event": "verify_listing_error",
                "path": prefix or "/",
                "error": str(exc),
            }))
            return found

        if not contents:
            return found

        names = list(contents.keys()) if hasattr(contents, "keys") else list(contents)
        # The verifier must derive local names exactly as the downloader did,
        # or every sanitised or de-collided name looks like a missing file.
        sanitizer = DirectorySanitizer(
            portable=self.portable_names, normalize=self.normalize_names
        )
        for name in names:
            local_name, _ = sanitizer.assign(name)
            child_rel = f"{prefix}/{local_name}" if prefix else local_name
            try:
                child = node[name]
            except Exception as exc:
                self.logger.error(json.dumps({
                    "event": "verify_listing_error",
                    "path": child_rel,
                    "error": str(exc),
                }))
                continue
            found.extend(self.collect_remote_files(child, child_rel))

        return found

    # -- local tree walking -------------------------------------------------

    @staticmethod
    def _is_artifact(rel_path: str) -> bool:
        parts = rel_path.split("/")
        name = parts[-1]
        if name in IFETCH_ARTIFACTS:
            return True
        if any(part in IFETCH_ARTIFACT_DIRS for part in parts[:-1]):
            return True
        return name.endswith(IFETCH_ARTIFACT_SUFFIXES)

    def collect_local_files(self, base: Optional[Path] = None) -> List[str]:
        """Return relative posix paths of every real file under ``base``."""
        base = (base or self.local_root)
        if not base.is_dir():
            return []

        collected: List[str] = []
        for dirpath, dirnames, filenames in os.walk(base):
            dirnames[:] = [d for d in dirnames if d not in IFETCH_ARTIFACT_DIRS]
            for filename in filenames:
                full = Path(dirpath) / filename
                try:
                    rel = full.relative_to(base).as_posix()
                except ValueError:  # pragma: no cover - defensive
                    continue
                if self._is_artifact(rel):
                    continue
                if not self._should_check(rel):
                    continue
                collected.append(rel)
        return collected

    # -- per-file verification ---------------------------------------------

    def verify_file(self, rel_path: str, item: Any) -> VerificationResult:
        """Verify one remote file against its local counterpart."""
        try:
            local_path = self.local_root / rel_path
            remote_size = getattr(item, "size", None)
            remote_size = int(remote_size) if remote_size is not None else None

            if not local_path.is_file():
                return VerificationResult(
                    path=rel_path,
                    status=STATUS_MISSING_LOCAL,
                    remote_size=remote_size,
                    reason="File exists in iCloud but not on disk",
                )

            local_size = local_path.stat().st_size

            if remote_size is not None and local_size != remote_size:
                return VerificationResult(
                    path=rel_path,
                    status=STATUS_SIZE_MISMATCH,
                    local_size=local_size,
                    remote_size=remote_size,
                    reason=f"Local size {local_size} != remote size {remote_size}",
                )

            if self.level == LEVEL_SIZE:
                return VerificationResult(
                    path=rel_path,
                    status=STATUS_VERIFIED,
                    local_size=local_size,
                    remote_size=remote_size,
                    reason="",
                )

            local_digest = sha256_file(local_path, self.chunk_size)

            if self.level == LEVEL_CHECKSUM:
                remote_digest = remote_checksum(item)
                if remote_digest is None:
                    return VerificationResult(
                        path=rel_path,
                        status=STATUS_CHECKSUM_UNAVAILABLE,
                        local_size=local_size,
                        remote_size=remote_size,
                        local_checksum=local_digest,
                        reason=(
                            "iCloud does not publish a content checksum for this "
                            "item; size matched and the local SHA-256 was recorded. "
                            "Use level 'redownload' to prove byte equality."
                        ),
                    )
                if remote_digest.lower() != local_digest.lower():
                    return VerificationResult(
                        path=rel_path,
                        status=STATUS_CHECKSUM_MISMATCH,
                        local_size=local_size,
                        remote_size=remote_size,
                        local_checksum=local_digest,
                        remote_checksum=remote_digest,
                        reason="Local checksum does not match the remote checksum",
                    )
                return VerificationResult(
                    path=rel_path,
                    status=STATUS_VERIFIED,
                    local_size=local_size,
                    remote_size=remote_size,
                    local_checksum=local_digest,
                    remote_checksum=remote_digest,
                )

            # LEVEL_REDOWNLOAD: stream the remote bytes, hash in memory only.
            remote_digest, streamed = self._hash_remote(item, rel_path)
            if remote_digest is None:
                return VerificationResult(
                    path=rel_path,
                    status=STATUS_ERROR,
                    local_size=local_size,
                    remote_size=remote_size,
                    local_checksum=local_digest,
                    reason="Remote file could not be streamed for comparison",
                )
            if remote_digest != local_digest:
                return VerificationResult(
                    path=rel_path,
                    status=STATUS_CHECKSUM_MISMATCH,
                    local_size=local_size,
                    remote_size=streamed if streamed is not None else remote_size,
                    local_checksum=local_digest,
                    remote_checksum=remote_digest,
                    reason="Re-downloaded content does not match the local file",
                )
            return VerificationResult(
                path=rel_path,
                status=STATUS_VERIFIED,
                local_size=local_size,
                remote_size=remote_size,
                local_checksum=local_digest,
                remote_checksum=remote_digest,
            )

        except Exception as exc:
            self.logger.error(json.dumps({
                "event": "verify_error",
                "path": rel_path,
                "error": str(exc),
                "traceback": traceback.format_exc(),
            }))
            return VerificationResult(
                path=rel_path,
                status=STATUS_ERROR,
                reason=str(exc),
            )

    def _hash_remote(self, item: Any, rel_path: str) -> Tuple[Optional[str], Optional[int]]:
        """Stream a remote file and return ``(sha256, bytes_seen)``.

        The stream is consumed in memory in ``chunk_size`` pieces and is never
        written anywhere on disk.
        """
        opener = getattr(self.downloader, "_open_with_retry", None)
        if callable(opener):
            response = opener(item, remote_path=rel_path)
        else:  # pragma: no cover - defensive
            response = item.open(stream=True)

        closer = getattr(response, "close", None)
        enter = getattr(response, "__enter__", None)
        if callable(enter):
            with response as active:
                return sha256_stream(active, self.chunk_size)
        try:
            return sha256_stream(response, self.chunk_size)
        finally:
            if callable(closer):
                try:
                    closer()
                except Exception:
                    pass

    # -- driver -------------------------------------------------------------

    def _report_progress(self, result: VerificationResult) -> None:
        if not self.progress_callback:
            return
        with self._lock:
            self._completed += 1
            completed, total = self._completed, self._total
        try:
            self.progress_callback(completed, total, result)
        except Exception:
            pass  # A broken progress renderer must never abort verification.

    def verify(self, icloud_path: str) -> VerificationSummary:
        """Verify ``icloud_path`` against the local root.  Never writes files."""
        started = time.time()
        summary = VerificationSummary(
            level=self.level,
            icloud_path=icloud_path,
            local_root=str(self.local_root),
            timestamp=time.strftime("%Y-%m-%d %H:%M:%S"),
        )

        self._authenticate_if_needed()
        node = self.downloader.get_drive_item(icloud_path)
        remote_files = self.collect_remote_files(node)

        with self._lock:
            self._completed = 0
            self._total = len(remote_files)

        results: List[VerificationResult] = []
        if remote_files:
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = {
                    executor.submit(self.verify_file, rel_path, item): rel_path
                    for rel_path, item in remote_files
                }
                for future in as_completed(futures):
                    rel_path = futures[future]
                    try:
                        result = future.result()
                    except Exception as exc:  # pragma: no cover - verify_file catches
                        result = VerificationResult(
                            path=rel_path, status=STATUS_ERROR, reason=str(exc)
                        )
                    results.append(result)
                    self._report_progress(result)

        remote_paths = {rel for rel, _ in remote_files}
        for rel in self.collect_local_files():
            if rel not in remote_paths:
                results.append(VerificationResult(
                    path=rel,
                    status=STATUS_EXTRA_LOCAL,
                    local_size=self._safe_size(self.local_root / rel),
                    reason="File exists on disk but not in iCloud",
                ))

        results.sort(key=lambda r: r.path)
        summary.results = results
        summary.duration_seconds = time.time() - started

        self.logger.info(json.dumps({
            "event": "verify_completed",
            "level": self.level,
            "icloud_path": icloud_path,
            "counts": summary.counts,
        }))
        return summary

    @staticmethod
    def _safe_size(path: Path) -> Optional[int]:
        try:
            return path.stat().st_size
        except OSError:
            return None


# ---------------------------------------------------------------------------
# Standalone CLI
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ifetch-verify",
        description=(
            "Verify that a local iFetch mirror matches iCloud Drive. "
            "Strictly read-only: your files are never modified."
        ),
    )
    parser.add_argument(
        "icloud_path",
        nargs="?",
        help=(
            'Remote iCloud Drive path to verify (e.g., "Documents/MyFolder"). '
            "Not needed with --offline."
        ),
    )
    parser.add_argument(
        "local_path",
        nargs="?",
        default=".",
        help="Local directory holding the mirror (default: current directory)",
    )
    parser.add_argument(
        "--email",
        help="iCloud account email (can also use ICLOUD_EMAIL environment variable)",
    )
    parser.add_argument(
        "--level",
        choices=list(LEVELS),
        default=LEVEL_SIZE,
        help=(
            "Verification depth: 'size' compares sizes only (fast, no transfer); "
            "'checksum' also hashes local files; 'redownload' re-streams every "
            "remote file to prove byte equality (bandwidth-expensive, opt-in). "
            "Default: size"
        ),
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=4,
        help="Maximum number of files verified concurrently (default: 4)",
    )
    parser.add_argument(
        "--report",
        help="Path to write a JSON verification report",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress progress output; only errors and the exit code are reported",
    )

    offline = parser.add_argument_group(
        "offline integrity audit",
        "Re-hash the mirror against the digests iFetch recorded at download "
        "time. Needs no credentials, no network and no iCloud account, which is "
        "what makes it usable years later to prove a backup has not rotted.",
    )
    offline.add_argument(
        "--offline",
        action="store_true",
        help="Audit against .ifetch_manifest.json instead of contacting Apple",
    )
    offline.add_argument(
        "--sign-key",
        dest="sign_key",
        help="HMAC key the manifest was signed with (or set $IFETCH_MANIFEST_KEY)",
    )
    offline.add_argument(
        "--sign-key-file",
        dest="sign_key_file",
        help="Read the manifest signing key from this file",
    )
    offline.add_argument(
        "--require-signature",
        action="store_true",
        help=(
            "Fail unless the manifest carries a signature that validates. Use "
            "this when the manifest is meant to be evidence, not just a record."
        ),
    )
    offline.add_argument(
        "--show-ok",
        action="store_true",
        help="List every verified entry, not just the problems",
    )
    return parser


def run_offline_audit(args: argparse.Namespace) -> int:
    """Audit a mirror against its recorded manifest. No network, no Apple.

    Exit codes match the online verifier: 0 intact, 1 drift detected, 2
    operational error.
    """
    from .manifest import (
        Manifest,
        ManifestKeyError,
        MANIFEST_FILENAME,
        load_signing_key,
        render_audit,
    )

    # Offline mode has no remote side, so the natural invocation is
    # `ifetch-verify --offline /path/to/mirror` - one positional, meaning the
    # local directory. Argparse binds that to `icloud_path` because it comes
    # first, so re-interpret it here rather than making users pass a remote path
    # that would be ignored anyway.
    local = args.local_path
    if args.icloud_path and local == ".":
        local = args.icloud_path

    root = Path(local).expanduser().resolve()
    if not (root / MANIFEST_FILENAME).exists():
        print(
            f"Error: no {MANIFEST_FILENAME} in {root}. An offline audit can only "
            "check a mirror iFetch downloaded (the manifest is written during "
            "download).",
            file=sys.stderr,
        )
        return 2

    try:
        key = load_signing_key(
            key=args.sign_key,
            key_file=Path(args.sign_key_file).expanduser() if args.sign_key_file else None,
        )
    except ManifestKeyError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 2

    manifest = Manifest.load(root, key=key)

    quiet = bool(args.quiet)
    if not quiet:
        print("=" * 70)
        print("iFetch Offline Integrity Audit (read-only, no network)")
        print(f"Local Path: {root}")
        print(f"Entries: {len(manifest)}")
        print("=" * 70)

    def progress(done: int, total: int, result: Any) -> None:
        if not quiet:
            print(f"[{done}/{total}] {result.status:<14} {result.path}")

    audit = manifest.verify(progress=progress)

    if args.report:
        report_path = Path(args.report).expanduser()
        if report_path.parent and str(report_path.parent):
            report_path.parent.mkdir(parents=True, exist_ok=True)
        with report_path.open("w") as handle:
            json.dump(audit.to_dict(), handle, indent=2)
        if not quiet:
            print(f"\nReport written to '{report_path}'")

    if not quiet:
        print()
        print(render_audit(audit, show_ok=args.show_ok))

    if args.require_signature and audit.signature_status != "valid":
        print(
            f"\nFAIL: --require-signature was given but the signature is "
            f"'{audit.signature_status}'.",
            file=sys.stderr,
        )
        return 1

    return 0 if audit.ok else 1


def _print_summary(summary: VerificationSummary) -> None:
    counts = summary.counts
    print("\nVerification Summary:")
    print(f"- Level: {summary.level}")
    print(f"- Files checked: {counts['total']}")
    print(f"- Verified: {counts[STATUS_VERIFIED]}")
    print(f"- Size mismatches: {counts[STATUS_SIZE_MISMATCH]}")
    print(f"- Missing locally: {counts[STATUS_MISSING_LOCAL]}")
    print(f"- Extra locally: {counts[STATUS_EXTRA_LOCAL]}")
    print(f"- Checksum mismatches: {counts[STATUS_CHECKSUM_MISMATCH]}")
    print(f"- Checksum unavailable: {counts[STATUS_CHECKSUM_UNAVAILABLE]}")
    print(f"- Errors: {counts[STATUS_ERROR]}")
    if summary.failures:
        print("\nFailures:")
        for result in summary.failures:
            print(f"  [{result.status}] {result.path}: {result.reason}")


def main(argv: Optional[Sequence[str]] = None) -> int:
    """Entry point for ``ifetch-verify``.

    Exit codes: 0 = everything verified, 1 = at least one file failed
    verification, 2 = operational error (auth, bad path, unwritable report).
    """
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.offline:
        try:
            return run_offline_audit(args)
        except KeyboardInterrupt:
            print("\nAudit cancelled by user.", file=sys.stderr)
            return 2
        except Exception as exc:
            print(f"Error: {exc}", file=sys.stderr)
            return 2

    if not args.icloud_path:
        parser.error("icloud_path is required unless --offline is given")

    quiet = bool(args.quiet)

    def progress(done: int, total: int, result: VerificationResult) -> None:
        if quiet:
            return
        print(f"[{done}/{total}] {result.status:<22} {result.path}")

    try:
        if not quiet:
            print("=" * 70)
            print("iFetch Integrity Verification (read-only)")
            print(f"Remote Path: {args.icloud_path}")
            print(f"Local Path: {args.local_path}")
            print(f"Level: {args.level}")
            if args.level == LEVEL_REDOWNLOAD:
                print(
                    "WARNING: 'redownload' re-downloads every file to hash it. "
                    "This transfers the full dataset."
                )
            print("=" * 70)

        verifier = Verifier(
            local_root=args.local_path,
            email=args.email,
            level=args.level,
            max_workers=args.max_workers,
            progress_callback=progress,
        )
        summary = verifier.verify(args.icloud_path)

        if args.report:
            report_path = Path(args.report).expanduser()
            if report_path.parent and str(report_path.parent):
                report_path.parent.mkdir(parents=True, exist_ok=True)
            with report_path.open("w") as handle:
                json.dump(summary.to_dict(), handle, indent=2)
            if not quiet:
                print(f"\nReport written to '{report_path}'")

        if not quiet:
            _print_summary(summary)

        return 0 if summary.ok else 1

    except KeyboardInterrupt:
        print("\nVerification cancelled by user.", file=sys.stderr)
        return 2
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
