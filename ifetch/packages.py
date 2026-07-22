"""Restore Apple package bundles as real directories instead of ZIP blobs.

The problem
-----------
On iCloud Drive, ``Report.pages``, ``Deck.key``, ``Budget.numbers``,
``Track.band`` and ``App.xcodeproj`` are **directories**, not files.  Apple's
Drive API reports their *logical* size in the folder listing, but serves their
*content* through a separate package token that returns a ZIP archive.  The two
numbers never agree.

Any tool that treats the listed size as the expected download size concludes
the transfer was corrupted and throws the file away.  Any tool that disables the
size check instead writes the ZIP verbatim to ``Deck.key`` - a path that looks
right, opens wrong, and is not what is in iCloud.

What this module does
---------------------
Detect that a downloaded payload is a package archive, then expand it into a
genuine directory at the destination path, preserving each entry's modification
time.  On macOS the result is a working bundle again; on Linux and Windows it is
an ordinary directory whose contents can be inspected and re-zipped.

Safety
------
The archive comes from the network, so extraction is hostile-input handling:

* member paths are rejected if absolute, if they escape the destination via
  ``..``, or if they contain a NUL;
* symlink and device entries are refused outright, so an archive cannot plant a
  link that later writes outside the destination;
* extraction happens into a sibling temporary directory and is only swapped into
  place once complete, so an interrupted run never leaves a half-expanded bundle
  where a valid one used to be.
"""

from __future__ import annotations

import errno
import os
import shutil
import stat
import time
import zipfile
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from typing import Any, Dict, List, Optional, Sequence, Tuple

#: The first four bytes of every ZIP archive (local file header signature).
#: ``PK\x05\x06`` is an empty archive and ``PK\x07\x08`` a spanned one; both are
#: accepted so a legitimately empty bundle is not mistaken for a plain file.
_ZIP_MAGICS = (b"PK\x03\x04", b"PK\x05\x06", b"PK\x07\x08")

#: Extensions Apple stores as packages (directories presented as single items).
#: Membership here is *not* sufficient to trigger expansion - the payload must
#: also actually be a ZIP - but it prevents a genuine ``.zip`` file a user
#: uploaded from being silently exploded into a directory.
PACKAGE_EXTENSIONS = frozenset(
    {
        # iWork
        "pages", "key", "numbers",
        # Apple creative tools
        "band", "logicx", "gbproj", "mainstage", "motn", "fcpbundle", "imovielibrary",
        "theater", "photoslibrary", "aplibrary",
        # Developer
        "xcodeproj", "xcworkspace", "playground", "swiftpm", "app", "framework",
        "bundle", "kext", "plugin", "appex", "dSYM",
        # Third-party bundles seen in iCloud Drive
        "rtfd", "oo3", "ooutline", "graffle", "scriv", "sketch", "pxm", "linea",
        "bbprojectd", "idos", "tef", "download",
    }
)


def package_extension(name: str) -> Optional[str]:
    """Return the package extension of ``name``, or ``None`` if it is not one.

    Comparison is case-insensitive because Apple is inconsistent about the case
    it reports (``.KEY`` and ``.key`` both occur), while ``dSYM`` is matched on
    its canonical casing too.
    """
    suffix = PurePosixPath(name).suffix.lstrip(".")
    if not suffix:
        return None
    lowered = suffix.lower()
    for known in PACKAGE_EXTENSIONS:
        if known.lower() == lowered:
            return suffix
    return None


def is_package_name(name: str) -> bool:
    """True when ``name`` carries an extension Apple stores as a package."""
    return package_extension(name) is not None


def looks_like_zip(path: Path) -> bool:
    """True when the file at ``path`` starts with a ZIP signature.

    Read from disk rather than trusting a Content-Type header: Apple serves
    package payloads with chunked encoding and generic types, and a header is
    not evidence about what actually landed on disk.
    """
    try:
        with Path(path).open("rb") as handle:
            head = handle.read(4)
    except OSError:
        return False
    return any(head.startswith(magic) for magic in _ZIP_MAGICS)


def should_expand(name: str, payload: Path) -> bool:
    """Both conditions must hold before a payload is expanded.

    The name must be one Apple treats as a package **and** the bytes must really
    be a ZIP.  Requiring both means a plain ``Archive.zip`` stays a file, and a
    ``Deck.key`` that for once arrived as a flat file is left alone.
    """
    return is_package_name(name) and looks_like_zip(payload)


class PackageExpansionError(Exception):
    """Extraction refused or failed; the original payload is left untouched."""


@dataclass
class ExpansionResult:
    """Outcome of expanding one package archive."""

    path: Path
    entries: int = 0
    bytes_written: int = 0
    skipped: List[str] = field(default_factory=list)
    stripped_root: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "path": str(self.path),
            "entries": self.entries,
            "bytes_written": self.bytes_written,
            "skipped": list(self.skipped),
            "stripped_root": self.stripped_root,
        }


def _is_unsafe_member(name: str) -> Optional[str]:
    """Return a reason string when ``name`` must not be extracted, else ``None``."""
    if not name:
        return "empty member name"
    if "\x00" in name:
        return "member name contains NUL"
    # Normalise the separator: archives written on Windows may use backslashes,
    # which PurePosixPath would otherwise treat as an ordinary character and
    # thereby miss a `..\\` traversal.
    normalised = name.replace("\\", "/")
    pure = PurePosixPath(normalised)
    if pure.is_absolute():
        return "absolute member path"
    if any(part == ".." for part in pure.parts):
        return "member path escapes the destination"
    if normalised.startswith("/"):
        return "absolute member path"
    return None


def _member_is_symlink(info: zipfile.ZipInfo) -> bool:
    """True for symlink, device and other non-regular entries.

    Unix mode lives in the top 16 bits of ``external_attr``.  Only the file-type
    bits are meaningful here: many writers (including Python's own
    ``ZipFile.writestr``) record permission bits with **no** type bits at all,
    and treating that absence as "not a regular file" would reject ordinary
    members.  So the rule is: judge the entry only when it actually declares a
    type, and treat an undeclared type as a regular file.
    """
    file_type = stat.S_IFMT(info.external_attr >> 16)
    if file_type == 0:
        return False
    return file_type not in (stat.S_IFREG, stat.S_IFDIR)


def _redundant_root(names: Sequence[str], package_name: str) -> Optional[str]:
    """The wrapper directory Apple adds, if this archive has one.

    Apple wraps a package's contents in a directory named after the package, so
    extracting naively yields ``Deck.key/Deck.key/Index.zip``.

    The wrapper is only stripped when every member sits beneath a *single* top
    level directory **and that directory is named after the package itself**.
    Requiring the name to match matters: an archive whose contents happen to
    share a common folder (``deep/deeper/f.txt``) is describing real structure,
    and silently dropping its top level would lose information.  Apple's
    redundant wrapper is the one case where the level is provably meaningless.
    """
    roots = set()
    for name in names:
        head = name.replace("\\", "/").split("/", 1)[0]
        if head:
            roots.add(head)
    if len(roots) != 1:
        return None

    root = roots.pop()
    if root.casefold() != package_name.casefold():
        return None
    # Every member must actually be *inside* it; a lone top-level file shares no
    # directory prefix worth removing.
    if not all(n.replace("\\", "/").startswith(root + "/") for n in names):
        return None
    return root


def _apply_mtime(target: Path, info: zipfile.ZipInfo) -> None:
    """Set ``target``'s mtime from the archive entry.

    ZIP stores local time with two-second resolution and no timezone, which is
    the best fidelity available; a failure to apply it is never fatal because
    the file's *contents* are what matters.
    """
    try:
        timestamp = time.mktime(tuple(info.date_time) + (0, 0, -1))
    except (ValueError, OverflowError, TypeError):
        return
    try:
        os.utime(target, (timestamp, timestamp))
    except OSError:
        pass


def expand_package(
    archive: Path,
    destination: Path,
    strip_root: bool = True,
    max_entries: int = 200_000,
    max_total_bytes: int = 64 * 1024**3,
) -> ExpansionResult:
    """Expand ``archive`` into a directory at ``destination``.

    ``destination`` is replaced atomically: the archive is unpacked into a
    sibling temporary directory first, and only swapped in once every member has
    been written.  If anything fails, the previous contents of ``destination``
    are left exactly as they were.

    ``max_entries`` and ``max_total_bytes`` bound a decompression bomb.  They are
    set far above any real Keynote deck; they exist so a malformed or hostile
    archive cannot fill the disk.
    """
    archive = Path(archive)
    destination = Path(destination)

    try:
        zf = zipfile.ZipFile(archive)
    except (zipfile.BadZipFile, OSError) as exc:
        raise PackageExpansionError(f"not a readable ZIP archive: {exc}") from exc

    with zf:
        infos = [i for i in zf.infolist()]
        if len(infos) > max_entries:
            raise PackageExpansionError(
                f"archive has {len(infos)} entries, above the {max_entries} limit"
            )

        declared_total = sum(max(0, i.file_size) for i in infos)
        if declared_total > max_total_bytes:
            raise PackageExpansionError(
                f"archive expands to {declared_total} bytes, above the "
                f"{max_total_bytes} limit"
            )

        result = ExpansionResult(path=destination)
        root = (
            _redundant_root([i.filename for i in infos], destination.name)
            if strip_root
            else None
        )
        result.stripped_root = root

        staging = _staging_dir(destination)
        try:
            for info in infos:
                self_reason = _is_unsafe_member(info.filename)
                if self_reason:
                    result.skipped.append(f"{info.filename}: {self_reason}")
                    continue
                if _member_is_symlink(info):
                    result.skipped.append(f"{info.filename}: non-regular entry refused")
                    continue

                relative = info.filename.replace("\\", "/")
                if root:
                    relative = relative[len(root) + 1:]
                if not relative:
                    continue  # The stripped root directory entry itself.

                target = staging / relative
                # Belt and braces: even after the name checks above, confirm the
                # resolved path really lands inside the staging directory.
                if not _within(target, staging):
                    result.skipped.append(f"{info.filename}: resolved outside destination")
                    continue

                if info.is_dir() or relative.endswith("/"):
                    target.mkdir(parents=True, exist_ok=True)
                    _apply_mtime(target, info)
                    continue

                target.parent.mkdir(parents=True, exist_ok=True)
                with zf.open(info) as source, target.open("wb") as sink:
                    written = shutil.copyfileobj(source, sink)
                    del written
                result.entries += 1
                try:
                    result.bytes_written += target.stat().st_size
                except OSError:
                    pass
                _apply_mtime(target, info)

            _swap_into_place(staging, destination)
        except PackageExpansionError:
            _remove_tree(staging)
            raise
        except (OSError, zipfile.BadZipFile) as exc:
            _remove_tree(staging)
            raise PackageExpansionError(f"failed to expand package: {exc}") from exc

    return result


def _within(candidate: Path, parent: Path) -> bool:
    """True when ``candidate`` is inside ``parent`` after normalisation.

    ``os.path.normpath`` rather than ``resolve()``: the target does not exist
    yet, and we want to reason about the literal path we are about to create
    rather than follow any symlink that happens to be on it.
    """
    try:
        c = os.path.normpath(str(candidate))
        p = os.path.normpath(str(parent))
    except ValueError:
        return False
    return c == p or c.startswith(p + os.sep)


def _staging_dir(destination: Path) -> Path:
    """A fresh sibling directory to unpack into.

    A sibling (not a temp dir elsewhere) keeps the final swap on the same
    filesystem, so it is a rename rather than a cross-device copy.
    """
    destination.parent.mkdir(parents=True, exist_ok=True)
    base = destination.parent / f".{destination.name}.ifetch-pkg"
    candidate = base
    counter = 0
    while candidate.exists():
        counter += 1
        candidate = Path(f"{base}.{counter}")
    candidate.mkdir(parents=True)
    return candidate


def _swap_into_place(staging: Path, destination: Path) -> None:
    """Move ``staging`` onto ``destination``, replacing whatever was there.

    ``os.replace`` cannot overwrite a non-empty directory, so an existing
    destination is moved aside first and only deleted once the new one is in
    place.  That ordering means a crash mid-swap leaves either the old bundle or
    the new one - never neither.
    """
    backup: Optional[Path] = None
    if destination.exists() or destination.is_symlink():
        backup = destination.parent / f".{destination.name}.ifetch-old"
        counter = 0
        while backup.exists():
            counter += 1
            backup = destination.parent / f".{destination.name}.ifetch-old.{counter}"
        os.replace(destination, backup)

    try:
        os.replace(staging, destination)
    except OSError:
        if backup is not None:
            os.replace(backup, destination)  # Put the original back.
        raise

    if backup is not None:
        _remove_tree(backup)


def _remove_tree(path: Path) -> None:
    """Best-effort recursive delete; a leftover temp directory is not fatal."""
    try:
        if path.is_dir() and not path.is_symlink():
            shutil.rmtree(path, ignore_errors=True)
        elif path.exists() or path.is_symlink():
            path.unlink()
    except OSError as exc:
        if exc.errno not in (errno.ENOENT,):
            pass


# ---------------------------------------------------------------------------
# Directory fingerprinting, for incremental re-runs
# ---------------------------------------------------------------------------

def directory_fingerprint(path: Path) -> Tuple[int, int]:
    """Return ``(file_count, total_bytes)`` for an expanded package.

    A re-run needs some cheap evidence that an expanded bundle is still intact.
    Its own size never matches the remote listing (the remote reports the
    logical size, the archive is compressed, and expansion is lossy about
    neither), so the local fingerprint is recorded at expansion time and
    compared against itself on the next run.  Counting entries and summing sizes
    costs one stat per file and catches the realistic failure - a partially
    deleted or truncated bundle - without hashing gigabytes.
    """
    count = 0
    total = 0
    for root, _dirs, files in os.walk(path):
        for name in files:
            try:
                total += (Path(root) / name).stat().st_size
                count += 1
            except OSError:
                continue
    return count, total
