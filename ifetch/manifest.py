"""A signed, local record of what was downloaded and what it hashed to.

Why this exists
---------------
Apple exposes **no content hashes** for iCloud Drive files.  Not MD5, not SHA-1,
not an ETag that is a digest of the bytes.  Every tool that syncs iCloud is
therefore comparing sizes and timestamps and calling it verification, and none
of them - including iFetch's own network verifier - can answer the question that
actually matters to someone keeping a backup for years:

    *Are the bytes on my disk today still the bytes I downloaded?*

Size and mtime cannot answer it.  Silent corruption, a failing drive, a bad
cable, a botched rsync to a NAS: all preserve size, and most preserve mtime.

What a manifest is
------------------
``.ifetch_manifest.json`` in the destination root records, for every file iFetch
wrote, the SHA-256 it had at the moment it was written, alongside its size, its
mtime, and the remote metadata Apple reported.  Re-hashing the tree later and
comparing against that record detects bit-rot and drift **offline**, with no
Apple credentials and no network.

Signing
-------
A manifest is only evidence if it could not have been quietly edited alongside
the data it describes.  When a key is supplied, the manifest carries an
HMAC-SHA256 over a canonical serialisation of its contents.  Anyone with the key
can prove the record is the one iFetch wrote; anyone without it can still read
the manifest, they just cannot vouch for it.

HMAC rather than a public-key signature is deliberate: it needs no dependency
beyond the standard library, and the threat model here is accidental corruption
and casual tampering by whatever touched the disk, not a determined forger who
also holds your key.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

MANIFEST_FILENAME = ".ifetch_manifest.json"
MANIFEST_VERSION = 1
_READ_CHUNK = 1024 * 1024

#: Files iFetch itself writes into the destination; they describe the mirror
#: rather than belonging to it, so they are never manifest entries.
ARTIFACT_NAMES = frozenset(
    {
        MANIFEST_FILENAME,
        ".ifetch_state.json",
        "download_report.json",
        "verify_report.json",
    }
)

ARTIFACT_DIRS = frozenset({".versions"})

KIND_FILE = "file"
KIND_PACKAGE = "package"

# Verification outcomes.
ENTRY_OK = "ok"
ENTRY_MODIFIED = "modified"
ENTRY_MISSING = "missing"
ENTRY_UNREADABLE = "unreadable"
ENTRY_TYPE_CHANGED = "type_changed"
ENTRY_UNTRACKED = "untracked"


def sha256_file(path: Path, chunk_size: int = _READ_CHUNK) -> str:
    """SHA-256 of a file's contents, streamed so size is irrelevant."""
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(chunk_size), b""):
            digest.update(chunk)
    return digest.hexdigest()


def sha256_directory(path: Path, chunk_size: int = _READ_CHUNK) -> str:
    """A stable digest over a directory tree's structure *and* contents.

    Expanded packages are directories, so a single hash for the whole bundle is
    what makes them verifiable on the same footing as files.  Relative paths are
    sorted and fed into the digest alongside each file's own hash, so a renamed
    or moved member changes the result even when the bytes are unchanged.
    """
    root = Path(path)
    digest = hashlib.sha256()
    entries: List[Tuple[str, Path]] = []
    for current, dirs, files in os.walk(root):
        dirs.sort()
        for name in sorted(files):
            full = Path(current) / name
            entries.append((full.relative_to(root).as_posix(), full))

    for relative, full in sorted(entries):
        digest.update(relative.encode("utf-8"))
        digest.update(b"\0")
        digest.update(sha256_file(full, chunk_size).encode("ascii"))
        digest.update(b"\0")
    return digest.hexdigest()


def is_artifact(relative_path: str) -> bool:
    """True for iFetch's own bookkeeping files, which are never manifest entries."""
    parts = Path(relative_path).parts
    if not parts:
        return True
    if parts[0] in ARTIFACT_DIRS:
        return True
    return parts[-1] in ARTIFACT_NAMES


# ---------------------------------------------------------------------------
# Signing keys
# ---------------------------------------------------------------------------

class ManifestKeyError(Exception):
    """A signing key was requested but could not be obtained."""


def load_signing_key(
    key: Optional[str] = None,
    key_file: Optional[Path] = None,
    env_var: str = "IFETCH_MANIFEST_KEY",
    env: Optional[Dict[str, str]] = None,
) -> Optional[bytes]:
    """Resolve a signing key from an explicit value, a file, or the environment.

    Returns ``None`` when no key is configured anywhere, which is a valid state:
    an unsigned manifest still detects bit-rot, it just cannot prove its own
    integrity.  An *empty* key file is an error rather than "no key", because it
    almost always means a secret failed to be written.
    """
    environ = os.environ if env is None else env

    if key:
        return key.encode("utf-8")

    if key_file:
        path = Path(key_file).expanduser()
        try:
            raw = path.read_bytes().strip()
        except OSError as exc:
            raise ManifestKeyError(f"cannot read signing key from {path}: {exc}") from exc
        if not raw:
            raise ManifestKeyError(f"signing key file {path} is empty")
        return raw

    from_env = environ.get(env_var)
    if from_env:
        return from_env.encode("utf-8")

    return None


def canonical_bytes(payload: Dict[str, Any]) -> bytes:
    """Deterministic serialisation of a manifest body, for signing.

    Sorted keys and fixed separators mean the same logical manifest always
    produces identical bytes, so a signature is reproducible across machines and
    Python versions.
    """
    return json.dumps(
        payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode("utf-8")


def sign_payload(payload: Dict[str, Any], key: bytes) -> str:
    return hmac.new(key, canonical_bytes(payload), hashlib.sha256).hexdigest()


# ---------------------------------------------------------------------------
# Verification results
# ---------------------------------------------------------------------------

@dataclass
class EntryResult:
    """The verdict for one manifest entry (or one untracked local path)."""

    path: str
    status: str
    detail: str = ""
    expected_sha256: Optional[str] = None
    actual_sha256: Optional[str] = None
    expected_size: Optional[int] = None
    actual_size: Optional[int] = None

    @property
    def ok(self) -> bool:
        return self.status == ENTRY_OK

    def to_dict(self) -> Dict[str, Any]:
        return {
            "path": self.path,
            "status": self.status,
            "detail": self.detail,
            "expected_sha256": self.expected_sha256,
            "actual_sha256": self.actual_sha256,
            "expected_size": self.expected_size,
            "actual_size": self.actual_size,
        }


@dataclass
class ManifestAudit:
    """The result of checking a whole mirror against its manifest."""

    root: Path
    signature_status: str = "absent"
    entries: List[EntryResult] = field(default_factory=list)
    generated_at: Optional[str] = None

    @property
    def failures(self) -> List[EntryResult]:
        """Entries that indicate the mirror drifted from what was recorded.

        Untracked files are excluded: a user's own notes sitting beside a mirror
        are worth reporting but are not corruption of it, and failing the audit
        over them would train people to ignore the result.
        """
        return [
            e for e in self.entries if not e.ok and e.status != ENTRY_UNTRACKED
        ]

    @property
    def untracked(self) -> List[EntryResult]:
        return [e for e in self.entries if e.status == ENTRY_UNTRACKED]

    @property
    def ok(self) -> bool:
        """True when nothing drifted and the manifest is not demonstrably forged.

        An ``invalid`` signature always fails: we hold the key, and the record
        does not match it, which is positive evidence of tampering.

        ``unsigned`` and ``no_key`` do **not** fail on their own. Signing is
        optional, and an unsigned manifest still does the main job - detecting
        bit-rot. Distinguishing "the user never signed" from "someone stripped
        the signature" is not possible from the file alone, so that policy
        belongs to the caller: ``ifetch-verify --offline --require-signature``
        demands a valid signature and fails on anything less.
        """
        return self.signature_status != "invalid" and not self.failures

    def counts(self) -> Dict[str, int]:
        tally: Dict[str, int] = {}
        for entry in self.entries:
            tally[entry.status] = tally.get(entry.status, 0) + 1
        return tally

    def to_dict(self) -> Dict[str, Any]:
        return {
            "root": str(self.root),
            "generated_at": self.generated_at,
            "signature": self.signature_status,
            "ok": self.ok,
            "counts": self.counts(),
            "entries": [e.to_dict() for e in self.entries],
        }


# ---------------------------------------------------------------------------
# The manifest itself
# ---------------------------------------------------------------------------

class Manifest:
    """Read/write access to a mirror's ``.ifetch_manifest.json``.

    Instances are safe to share across the downloader's worker threads: every
    mutation takes a lock, and writes go through a temporary file plus
    ``os.replace`` so a crash mid-save cannot truncate an existing manifest.
    """

    def __init__(self, root: Path, key: Optional[bytes] = None):
        self.root = Path(root).resolve()
        self.path = self.root / MANIFEST_FILENAME
        self.key = key
        self._entries: Dict[str, Dict[str, Any]] = {}
        self._loaded_signature: Optional[str] = None
        self._loaded_body: Optional[Dict[str, Any]] = None
        self._generated_at: Optional[str] = None
        self._lock = threading.RLock()
        self._dirty = False

    # -- persistence ----------------------------------------------------
    @classmethod
    def load(cls, root: Path, key: Optional[bytes] = None) -> "Manifest":
        manifest = cls(root, key=key)
        manifest._read()
        return manifest

    def _read(self) -> None:
        if not self.path.exists():
            return
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, ValueError, UnicodeDecodeError):
            # A corrupt manifest must not take the process down; it is reported
            # as such by verify(), and a fresh one is written on the next run.
            return
        if not isinstance(payload, dict):
            return

        body = payload.get("manifest")
        if not isinstance(body, dict):
            return

        files = body.get("files")
        with self._lock:
            self._entries = {
                k: v for k, v in files.items() if isinstance(k, str) and isinstance(v, dict)
            } if isinstance(files, dict) else {}
            self._loaded_body = body
            self._loaded_signature = payload.get("signature")
            self._generated_at = body.get("generated_at")

    def _body(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "version": MANIFEST_VERSION,
                "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "hash": "sha256",
                "files": {k: dict(v) for k, v in sorted(self._entries.items())},
            }

    def save(self) -> None:
        """Atomically write the manifest, signing it when a key is configured."""
        body = self._body()
        payload: Dict[str, Any] = {"manifest": body}
        if self.key:
            payload["signature"] = sign_payload(body, self.key)
            payload["signature_algorithm"] = "hmac-sha256"

        tmp = self.path.with_suffix(self.path.suffix + ".tmp")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        try:
            tmp.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
            os.replace(tmp, self.path)
        except OSError:
            try:
                if tmp.exists():
                    tmp.unlink()
            except OSError:
                pass
            raise
        with self._lock:
            self._dirty = False
            self._generated_at = body["generated_at"]

    # -- recording ------------------------------------------------------
    def key_for(self, path: Path) -> str:
        """POSIX-relative key for a local path (absolute if outside the root)."""
        try:
            return Path(path).resolve().relative_to(self.root).as_posix()
        except ValueError:
            return Path(path).as_posix()

    def record_file(
        self,
        path: Path,
        sha256: Optional[str] = None,
        remote_size: Optional[int] = None,
        remote_modified: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """Record a downloaded file. Hashes it if no digest is supplied.

        Returns the stored entry, or ``None`` if the path is not a regular file
        - recording an entry for something absent would manufacture a future
        false "missing" report rather than prevent one.
        """
        target = Path(path)
        if not target.is_file():
            return None
        try:
            stat = target.stat()
            digest = sha256 or sha256_file(target)
        except OSError:
            return None

        entry = {
            "kind": KIND_FILE,
            "sha256": digest,
            "size": stat.st_size,
            "mtime": stat.st_mtime,
            "remote_size": remote_size,
            "remote_modified": remote_modified,
            "recorded_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }
        with self._lock:
            self._entries[self.key_for(target)] = entry
            self._dirty = True
        return entry

    def record_package(
        self,
        path: Path,
        remote_size: Optional[int] = None,
        remote_modified: Optional[str] = None,
        entries: Optional[int] = None,
    ) -> Optional[Dict[str, Any]]:
        """Record an expanded package bundle as a single verifiable unit."""
        target = Path(path)
        if not target.is_dir():
            return None
        try:
            digest = sha256_directory(target)
        except OSError:
            return None

        from .packages import directory_fingerprint

        count, total = directory_fingerprint(target)
        entry = {
            "kind": KIND_PACKAGE,
            "sha256": digest,
            "size": total,
            "file_count": count if entries is None else entries,
            "remote_size": remote_size,
            "remote_modified": remote_modified,
            "recorded_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }
        with self._lock:
            self._entries[self.key_for(target)] = entry
            self._dirty = True
        return entry

    def forget(self, path: Path) -> None:
        with self._lock:
            self._entries.pop(self.key_for(path), None)
            self._dirty = True

    def get(self, path: Path) -> Optional[Dict[str, Any]]:
        with self._lock:
            entry = self._entries.get(self.key_for(path))
            return dict(entry) if entry else None

    def __len__(self) -> int:
        with self._lock:
            return len(self._entries)

    def paths(self) -> List[str]:
        with self._lock:
            return sorted(self._entries)

    # -- verification ---------------------------------------------------
    def check_signature(self) -> str:
        """``valid`` | ``invalid`` | ``unsigned`` | ``absent`` | ``no_key``.

        ``absent`` means no manifest was loaded at all; ``unsigned`` means one
        was loaded but carries no signature; ``no_key`` means it is signed but
        we hold no key to check it with.  These are kept distinct because only
        ``invalid`` indicates tampering, and conflating them would either cry
        wolf or hide a real forgery.
        """
        with self._lock:
            body = self._loaded_body
            signature = self._loaded_signature

        if body is None:
            return "absent"
        if signature is None:
            return "unsigned"
        if not self.key:
            return "no_key"
        expected = sign_payload(body, self.key)
        return "valid" if hmac.compare_digest(expected, signature) else "invalid"

    def verify(
        self,
        report_untracked: bool = True,
        progress: Optional[Any] = None,
    ) -> ManifestAudit:
        """Re-hash the mirror and compare it against the recorded digests.

        This is the offline integrity check: no credentials, no network, no
        Apple.  Every recorded entry is re-read from disk and re-hashed, so
        corruption that preserved size and mtime is still caught.
        """
        audit = ManifestAudit(root=self.root)
        audit.signature_status = self.check_signature()
        with self._lock:
            snapshot = {k: dict(v) for k, v in self._entries.items()}
            audit.generated_at = self._generated_at

        seen: set = set()
        total = len(snapshot)
        for index, (relative, entry) in enumerate(sorted(snapshot.items()), start=1):
            result = self._verify_entry(relative, entry)
            audit.entries.append(result)
            seen.add(relative)
            if progress is not None:
                progress(index, total, result)

        if report_untracked:
            audit.entries.extend(self._find_untracked(seen))

        return audit

    def _verify_entry(self, relative: str, entry: Dict[str, Any]) -> EntryResult:
        target = self.root / relative
        kind = entry.get("kind", KIND_FILE)
        expected = entry.get("sha256")
        expected_size = entry.get("size")

        if not target.exists():
            return EntryResult(
                path=relative,
                status=ENTRY_MISSING,
                detail="recorded by a previous run but no longer on disk",
                expected_sha256=expected,
                expected_size=expected_size,
            )

        if kind == KIND_PACKAGE:
            if not target.is_dir():
                return EntryResult(
                    path=relative,
                    status=ENTRY_TYPE_CHANGED,
                    detail="recorded as an expanded package but is no longer a directory",
                    expected_sha256=expected,
                    expected_size=expected_size,
                )
            try:
                actual = sha256_directory(target)
            except OSError as exc:
                return EntryResult(
                    path=relative,
                    status=ENTRY_UNREADABLE,
                    detail=str(exc),
                    expected_sha256=expected,
                )
            actual_size = None
        else:
            if target.is_dir():
                return EntryResult(
                    path=relative,
                    status=ENTRY_TYPE_CHANGED,
                    detail="recorded as a file but is now a directory",
                    expected_sha256=expected,
                    expected_size=expected_size,
                )
            try:
                actual_size = target.stat().st_size
                actual = sha256_file(target)
            except OSError as exc:
                return EntryResult(
                    path=relative,
                    status=ENTRY_UNREADABLE,
                    detail=str(exc),
                    expected_sha256=expected,
                    expected_size=expected_size,
                )

        if expected and actual != expected:
            if kind == KIND_PACKAGE:
                # A bundle's size is not re-measured during verification, so the
                # message must not claim anything about it either way.
                detail = "bundle contents changed since download"
            elif actual_size == expected_size:
                detail = "contents changed since download: same path, different bytes"
            else:
                detail = "contents and size changed since download"
            return EntryResult(
                path=relative,
                status=ENTRY_MODIFIED,
                detail=detail,
                expected_sha256=expected,
                actual_sha256=actual,
                expected_size=expected_size,
                actual_size=actual_size,
            )

        return EntryResult(
            path=relative,
            status=ENTRY_OK,
            detail="matches the digest recorded at download time",
            expected_sha256=expected,
            actual_sha256=actual,
            expected_size=expected_size,
            actual_size=actual_size,
        )

    def _find_untracked(self, seen: Iterable[str]) -> List[EntryResult]:
        """Local files with no manifest entry.

        Reported, not failed: a user may legitimately keep notes beside a
        mirror.  It matters because an untracked file is one the manifest cannot
        vouch for, and someone auditing a backup should be told which those are.
        """
        known = set(seen)
        results: List[EntryResult] = []
        for current, dirs, files in os.walk(self.root):
            dirs[:] = [d for d in dirs if d not in ARTIFACT_DIRS]
            # An expanded package is one manifest entry, not N; do not descend
            # into it and report every member as untracked.
            pruned = []
            for name in dirs:
                rel = (Path(current) / name).relative_to(self.root).as_posix()
                if rel in known:
                    continue
                pruned.append(name)
            dirs[:] = pruned

            for name in files:
                rel = (Path(current) / name).relative_to(self.root).as_posix()
                if rel in known or is_artifact(rel):
                    continue
                if any(rel.startswith(k + "/") for k in known):
                    continue
                results.append(
                    EntryResult(
                        path=rel,
                        status=ENTRY_UNTRACKED,
                        detail="present locally but not recorded in the manifest",
                    )
                )
        return results


def render_audit(audit: ManifestAudit, show_ok: bool = False) -> str:
    """Human-readable audit report."""
    lines = [
        f"Manifest audit - {audit.root}",
        f"Recorded: {audit.generated_at or 'unknown'}",
        f"Signature: {audit.signature_status}",
        "=" * 70,
    ]
    for entry in audit.entries:
        if entry.ok and not show_ok:
            continue
        lines.append(f"[{entry.status}] {entry.path}")
        if entry.detail:
            lines.append(f"    {entry.detail}")
        if entry.status == ENTRY_MODIFIED:
            lines.append(f"    expected {entry.expected_sha256}")
            lines.append(f"    actual   {entry.actual_sha256}")
    lines.append("=" * 70)
    counts = audit.counts()
    lines.append(
        "  ".join(f"{name}={count}" for name, count in sorted(counts.items())) or "no entries"
    )
    lines.append("RESULT: " + ("intact" if audit.ok else "PROBLEMS FOUND"))
    return "\n".join(lines)
