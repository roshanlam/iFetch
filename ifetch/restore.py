#!/usr/bin/env python3
"""Restore (rollback) support for iFetch version history.

:mod:`ifetch.versioning` only ever *archives* the previous copy of a file it is
about to overwrite.  This module is the other half: it reads the same
``.ifetch_versions.json`` metadata and puts an archived copy back.

Guarantees, in order of importance:

* Restoring never deletes an archived version - archives are **copied** out.
* Anything a restore is about to overwrite is itself archived first (through
  :class:`~ifetch.versioning.VersionManager`), so a restore is undoable.
* ``dry_run=True`` performs every check and writes nothing.
* Archived bytes are checksum-verified against the recorded checksum before
  they are written, and the written file is verified afterwards.  A mismatch
  aborts loudly instead of silently producing corrupt data.
* Neither a corrupt metadata key nor a corrupt ``archived_path`` can read or
  write outside the managed tree.

A note on timestamps
--------------------
A version's ``timestamp`` is when the copy was *archived*, i.e. the moment it
stopped being the live content.  So the file that was live at time *T* is the
one archived at the first timestamp *after* T.  That is what ``select="content"``
(the default) means and it is what makes "undo yesterday's bad sync" work.
``select="archived"`` uses the literal reading instead - the newest version
whose own timestamp is at or before T.  An exact timestamp match always wins,
in either mode.
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

if __package__ in (None, ""):
    # Running as a standalone script: add project root to path and import absolute
    sys.path.append(str(Path(__file__).resolve().parent.parent))
    from ifetch.versioning import (  # type: ignore
        VersionManager,
        compute_checksum,
        entry_epoch,
        entry_size,
        format_timestamp,
        is_within,
        load_metadata,
        parse_timestamp,
    )
else:
    from .versioning import (  # type: ignore
        VersionManager,
        compute_checksum,
        entry_epoch,
        entry_size,
        format_timestamp,
        is_within,
        load_metadata,
        parse_timestamp,
    )


SELECT_MODES = ("content", "archived")


class RestoreError(Exception):
    """Raised when a restore cannot even be planned (bad path/version/time)."""


# ---------------------------------------------------------------------------
# Structured results
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class VersionEntry:
    """One archived version of one file, as presented to callers."""

    rel_path: str
    index: int                      # 0 == newest
    version: Optional[int]
    checksum: Optional[str]
    timestamp: Optional[str]
    epoch: Optional[float]
    archived_path: Optional[str]
    size: Optional[int]
    exists: bool
    is_current: bool                # points at the live file (baseline entry)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "path": self.rel_path,
            "index": self.index,
            "version": self.version,
            "checksum": self.checksum,
            "timestamp": self.timestamp,
            "epoch": self.epoch,
            "archived_path": self.archived_path,
            "size": self.size,
            "exists": self.exists,
            "is_current": self.is_current,
        }


@dataclass
class RestoreAction:
    """What happened (or would happen) to a single file."""

    rel_path: str
    status: str                     # restored | planned | skipped | error
    version: Optional[int] = None
    timestamp: Optional[str] = None
    source: Optional[str] = None
    destination: Optional[str] = None
    archives_current: bool = False
    archived_current: Optional[str] = None
    message: str = ""

    @property
    def failed(self) -> bool:
        return self.status == "error"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "path": self.rel_path,
            "status": self.status,
            "version": self.version,
            "timestamp": self.timestamp,
            "source": self.source,
            "destination": self.destination,
            "archives_current": self.archives_current,
            "archived_current": self.archived_current,
            "message": self.message,
        }


@dataclass
class RestoreResult:
    """Outcome of a restore or restore-all call."""

    actions: List[RestoreAction] = field(default_factory=list)
    dry_run: bool = False

    @property
    def errors(self) -> List[RestoreAction]:
        return [a for a in self.actions if a.failed]

    @property
    def restored(self) -> List[RestoreAction]:
        return [a for a in self.actions if a.status in ("restored", "planned")]

    @property
    def skipped(self) -> List[RestoreAction]:
        return [a for a in self.actions if a.status == "skipped"]

    @property
    def success(self) -> bool:
        return not self.errors

    def summary_line(self) -> str:
        prefix = "[dry-run] " if self.dry_run else ""
        verb = "would restore" if self.dry_run else "restored"
        return (
            f"{prefix}{verb} {len(self.restored)} file(s), "
            f"skipped {len(self.skipped)}, failed {len(self.errors)}"
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "dry_run": self.dry_run,
            "success": self.success,
            "restored": len(self.restored),
            "skipped": len(self.skipped),
            "failed": len(self.errors),
            "actions": [a.to_dict() for a in self.actions],
        }


# ---------------------------------------------------------------------------
# Manager
# ---------------------------------------------------------------------------
class RestoreManager:
    """Read version history and put archived copies back."""

    def __init__(
        self,
        root: Path,
        verify: bool = True,
        dry_run: bool = False,
    ):
        self.root = Path(root).expanduser().resolve()
        self.verify = verify
        self.dry_run = dry_run
        self.meta_path = self.root / VersionManager.META_FILENAME
        self.versions_dir = self.root / VersionManager.VERSIONS_DIRNAME
        self._vm: Optional[VersionManager] = None

    # -- metadata ------------------------------------------------------
    def _version_manager(self) -> VersionManager:
        """Lazily built - constructing one creates ``.versions`` on disk."""
        if self._vm is None:
            self._vm = VersionManager(self.root)
        return self._vm

    def _metadata(self) -> Dict[str, List[Dict[str, Any]]]:
        return load_metadata(self.meta_path)

    def has_history(self) -> bool:
        return bool(self._metadata())

    # -- listing -------------------------------------------------------
    def list_versions(self, rel_path: Any = None) -> Dict[str, List[VersionEntry]]:
        """Versions per path, newest first.

        With *rel_path* the result holds just that file; without it, the whole
        tree.  An unknown path raises :class:`RestoreError`; an empty or absent
        version store simply yields ``{}``.
        """
        meta = self._metadata()

        if rel_path is not None:
            key = self._normalise_key(rel_path)
            if key not in meta:
                raise RestoreError(f"no version history for '{key}'")
            keys = [key]
        else:
            keys = sorted(meta.keys())

        out: Dict[str, List[VersionEntry]] = {}
        for key in keys:
            out[key] = self._entries_for(key, meta.get(key, []))
        return out

    def _entries_for(self, key: str, raw: List[Dict[str, Any]]) -> List[VersionEntry]:
        live = self.root / key
        ordered = sorted(
            raw,
            key=lambda e: (
                entry_epoch(e) if entry_epoch(e) is not None else float("-inf"),
                e.get("version") or 0,
            ),
            reverse=True,
        )
        entries: List[VersionEntry] = []
        for idx, raw_entry in enumerate(ordered):
            archived = raw_entry.get("archived_path")
            archived_path = Path(archived) if archived else None
            try:
                exists = bool(archived_path and archived_path.exists())
            except OSError:
                exists = False
            is_current = bool(
                archived_path
                and str(archived_path) == str(live)
            )
            entries.append(
                VersionEntry(
                    rel_path=key,
                    index=idx,
                    version=raw_entry.get("version"),
                    checksum=raw_entry.get("checksum"),
                    timestamp=raw_entry.get("timestamp"),
                    epoch=entry_epoch(raw_entry),
                    archived_path=str(archived_path) if archived_path else None,
                    size=entry_size(raw_entry) if exists or raw_entry.get("size") else None,
                    exists=exists,
                    is_current=is_current,
                )
            )
        return entries

    # -- selection -----------------------------------------------------
    @staticmethod
    def _normalise_key(rel_path: Any) -> str:
        key = VersionManager.key_for(rel_path)
        while key.startswith("./"):
            key = key[2:]
        return key

    def _relative_key(self, path: Any) -> str:
        """Accept absolute paths inside the root as well as relative ones."""
        raw = Path(str(path)).expanduser()
        if raw.is_absolute():
            try:
                return self._normalise_key(raw.resolve().relative_to(self.root))
            except (ValueError, OSError):
                raise RestoreError(f"'{path}' is outside the version root {self.root}")
        return self._normalise_key(raw)

    @staticmethod
    def select_version(
        entries: List[VersionEntry],
        index: Optional[int] = None,
        version: Optional[int] = None,
        timestamp: Optional[Any] = None,
        select: str = "content",
    ) -> VersionEntry:
        """Pick one entry out of a newest-first list.

        Precedence: explicit ``version``, then ``index``, then ``timestamp``,
        otherwise the newest version.
        """
        if not entries:
            raise RestoreError("no versions recorded")

        if version is not None:
            for entry in entries:
                if entry.version == version:
                    return entry
            raise RestoreError(f"no version {version} recorded")

        if index is not None:
            if index < 0 or index >= len(entries):
                raise RestoreError(
                    f"version index {index} out of range (have {len(entries)})"
                )
            return entries[index]

        if timestamp is not None:
            target = parse_timestamp(timestamp)
            if target is None:
                raise RestoreError(f"could not parse timestamp '{timestamp}'")
            if select not in SELECT_MODES:
                raise RestoreError(
                    f"unknown selection mode '{select}' (expected one of {', '.join(SELECT_MODES)})"
                )
            dated = [e for e in entries if e.epoch is not None]
            exact = [e for e in dated if e.epoch == target]
            if exact:
                return exact[0]
            if select == "archived":
                for entry in dated:  # newest first
                    if entry.epoch <= target:
                        return entry
            else:  # "content": earliest archive taken after T
                for entry in reversed(dated):  # oldest first
                    if entry.epoch > target:
                        return entry
            raise RestoreError(
                f"no version available for timestamp '{timestamp}'"
            )

        return entries[0]

    # -- public restore API --------------------------------------------
    def restore(
        self,
        rel_path: Any,
        index: Optional[int] = None,
        version: Optional[int] = None,
        timestamp: Optional[Any] = None,
        to: Optional[Any] = None,
        select: str = "content",
        force: bool = False,
    ) -> RestoreResult:
        """Restore a single file.

        *to* may be a directory (the file lands inside it under its relative
        path) or an explicit file path.  Selection problems raise
        :class:`RestoreError`; execution problems come back as an ``error``
        action so callers always get a structured answer.
        """
        key = self._relative_key(rel_path)
        entries = self.list_versions(key)[key]
        entry = self.select_version(
            entries, index=index, version=version, timestamp=timestamp, select=select
        )

        destination = self._destination_for(key, to, single=True)
        result = RestoreResult(dry_run=self.dry_run)
        result.actions.append(self._apply(key, entry, destination, force=force))
        return result

    def restore_all(
        self,
        timestamp: Any,
        to: Optional[Any] = None,
        select: str = "content",
        force: bool = False,
    ) -> RestoreResult:
        """Point-in-time restore of the whole tree.

        Every tracked file is put back to the version that was live at
        *timestamp* (see the module docstring on selection modes).  Files with
        no applicable version are skipped, not failed.
        """
        result = RestoreResult(dry_run=self.dry_run)
        listing = self.list_versions()
        if not listing:
            return result

        for key in sorted(listing):
            entries = listing[key]
            try:
                entry = self.select_version(
                    entries, timestamp=timestamp, select=select
                )
            except RestoreError as exc:
                result.actions.append(
                    RestoreAction(rel_path=key, status="skipped", message=str(exc))
                )
                continue

            try:
                destination = self._destination_for(key, to, single=False)
            except RestoreError as exc:
                result.actions.append(
                    RestoreAction(rel_path=key, status="error", message=str(exc))
                )
                continue

            result.actions.append(self._apply(key, entry, destination, force=force))

        return result

    # -- internals -----------------------------------------------------
    def _destination_for(self, key: str, to: Optional[Any], single: bool) -> Path:
        """Work out where a restored copy should land, safely.

        A metadata key is never allowed to escape the tree it is being written
        into - that holds for the managed root and for a user-supplied ``--to``
        directory alike.
        """
        if to is None:
            base = self.root
            candidate = base / key
        else:
            to_path = Path(str(to)).expanduser()
            treat_as_dir = (not single) or to_path.is_dir() or str(to).endswith(("/", "\\"))
            if treat_as_dir:
                base = to_path.resolve() if to_path.exists() else to_path.absolute()
                candidate = base / key
            else:
                # Explicit file destination chosen by the user.
                parent = to_path.parent
                base = parent.resolve() if parent.exists() else parent.absolute()
                candidate = base / to_path.name

        if not is_within(candidate, base):
            raise RestoreError(
                f"refusing to restore '{key}': destination escapes {base}"
            )
        return candidate

    def _validated_source(self, entry: VersionEntry) -> Path:
        if not entry.archived_path:
            raise RestoreError("version metadata has no archived_path")
        source = Path(entry.archived_path)
        if not source.is_absolute():
            source = self.root / source
        if not is_within(source, self.root):
            raise RestoreError(
                f"archived path '{entry.archived_path}' is outside {self.root}"
            )
        if not source.exists():
            raise RestoreError(f"archived file is missing: {source}")
        if not source.is_file():
            raise RestoreError(f"archived path is not a file: {source}")
        return source

    def _apply(
        self,
        key: str,
        entry: VersionEntry,
        destination: Path,
        force: bool = False,
    ) -> RestoreAction:
        action = RestoreAction(
            rel_path=key,
            status="planned" if self.dry_run else "restored",
            version=entry.version,
            timestamp=entry.timestamp,
            destination=str(destination),
        )

        # 1. Source must be real, inside the tree and readable.
        try:
            source = self._validated_source(entry)
        except RestoreError as exc:
            action.status = "error"
            action.message = str(exc)
            return action
        action.source = str(source)

        # 2. Nothing to do when the archived copy *is* the live file.
        try:
            same = destination.exists() and source.resolve() == destination.resolve()
        except OSError:
            same = False
        if same:
            action.status = "skipped"
            action.message = "already the current file"
            return action

        # 3. Verify the archived bytes before trusting them.
        if self.verify and entry.checksum:
            try:
                actual = compute_checksum(source)
            except OSError as exc:
                action.status = "error"
                action.message = f"could not read archived file: {exc}"
                return action
            if actual != entry.checksum:
                action.status = "error"
                action.message = (
                    "checksum mismatch on archived copy "
                    f"(recorded {entry.checksum}, found {actual}); refusing to restore"
                )
                return action

        # 4. Protect whatever is currently at the destination.
        inside_root = is_within(destination, self.root)
        if destination.exists():
            action.archives_current = inside_root
            if not inside_root and not force:
                action.status = "error"
                action.message = (
                    "destination exists outside the version root; "
                    "pass force=True to overwrite"
                )
                return action

        if self.dry_run:
            return action

        if destination.exists() and inside_root:
            archived = self._archive_current(destination)
            if archived is None:
                action.status = "error"
                action.message = (
                    "could not archive the current file; nothing was overwritten"
                )
                return action
            action.archived_current = archived

        # 5. Copy (never move) the archived copy into place.
        try:
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, destination)
        except OSError as exc:
            action.status = "error"
            action.message = f"could not write '{destination}': {exc}"
            return action

        # 6. Verify what actually landed on disk.
        if self.verify and entry.checksum:
            try:
                written = compute_checksum(destination)
            except OSError as exc:
                written = None
                action.message = f"could not verify written file: {exc}"
            if written is not None and written != entry.checksum:
                try:
                    destination.unlink()
                except OSError:
                    pass
                action.status = "error"
                action.message = (
                    "checksum mismatch after writing "
                    f"(expected {entry.checksum}, got {written}); restore aborted"
                )
                if action.archived_current:
                    action.message += (
                        f"; the previous file is preserved at {action.archived_current}"
                    )
                return action

        return action

    def _archive_current(self, destination: Path) -> Optional[str]:
        """Archive the file currently at *destination* via VersionManager."""
        try:
            rel = destination.resolve().relative_to(self.root)
        except (ValueError, OSError):
            return None
        try:
            checksum = compute_checksum(destination)
        except OSError:
            return None
        entry = self._version_manager().record_version(rel, checksum, destination)
        if not entry:
            return None
        return entry.get("archived_path")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def _human_size(size: Optional[int]) -> str:
    if size is None:
        return "?"
    value = float(size)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if value < 1024 or unit == "TB":
            return f"{value:.0f} {unit}" if unit == "B" else f"{value:.1f} {unit}"
        value /= 1024
    return f"{value:.1f} TB"


def _pretty_timestamp(entry: VersionEntry) -> str:
    if entry.epoch is not None:
        return format_timestamp(entry.epoch) or str(entry.timestamp)
    return str(entry.timestamp or "?")


def _print_listing(listing: Dict[str, List[VersionEntry]], limit: Optional[int]) -> None:
    if not listing:
        print("No version history found.")
        return

    for key in sorted(listing):
        entries = listing[key]
        print(key)
        if not entries:
            print("  (no versions recorded)")
            continue
        shown = entries[:limit] if limit else entries
        for entry in shown:
            checksum = (entry.checksum or "-")[:16]
            flags = []
            if entry.is_current:
                flags.append("current")
            if not entry.exists:
                flags.append("MISSING")
            suffix = f"  [{', '.join(flags)}]" if flags else ""
            print(
                f"  [{entry.index}] v{entry.version}  {_pretty_timestamp(entry)}"
                f"  {_human_size(entry.size):>9}  {checksum}{suffix}"
            )
        if limit and len(entries) > limit:
            print(f"  ... {len(entries) - limit} older version(s) not shown")


def _print_actions(result: RestoreResult) -> None:
    prefix = "[dry-run] " if result.dry_run else ""
    for action in result.actions:
        if action.status == "error":
            print(f"{prefix}error   {action.rel_path}: {action.message}", file=sys.stderr)
            continue
        if action.status == "skipped":
            print(f"{prefix}skip    {action.rel_path}: {action.message}")
            continue
        verb = "would restore" if result.dry_run else "restored"
        print(
            f"{prefix}{verb} {action.rel_path} <- v{action.version} "
            f"({action.timestamp}) -> {action.destination}"
        )
        if action.archived_current:
            print(f"          previous copy archived to {action.archived_current}")
        elif action.archives_current:
            print("          previous copy would be archived first")
    print(result.summary_line())


def _common_parser() -> argparse.ArgumentParser:
    """Options accepted before *and* after the sub-command."""
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument(
        '--root',
        default=argparse.SUPPRESS,
        help='Local directory that iFetch downloaded into (default: current directory)'
    )
    common.add_argument(
        '--json',
        dest='as_json',
        action='store_true',
        default=argparse.SUPPRESS,
        help='Emit machine-readable JSON instead of human text'
    )
    return common


def build_parser() -> argparse.ArgumentParser:
    common = _common_parser()
    parser = argparse.ArgumentParser(
        prog='ifetch-restore',
        parents=[common],
        description='List and restore previous versions archived by iFetch.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # What history do we have?
  ifetch-restore --root ~/icloud list
  ifetch-restore --root ~/icloud list Documents/notes.txt

  # Roll one file back to the previous version (index 0 is the newest)
  ifetch-restore --root ~/icloud restore Documents/notes.txt --index 0

  # Pull an old copy out for inspection without touching the live file
  ifetch-restore --root ~/icloud restore Documents/notes.txt --version 2 --to /tmp/old

  # Undo yesterday's bad sync across the whole tree (check first!)
  ifetch-restore --root ~/icloud restore-all --at 2026-07-20 --dry-run
  ifetch-restore --root ~/icloud restore-all --at 2026-07-20
        """
    )

    sub = parser.add_subparsers(dest='command', metavar='COMMAND')

    list_parser = sub.add_parser(
        'list', parents=[common], help='List archived versions, newest first'
    )
    list_parser.add_argument(
        'path', nargs='?', default=None,
        help='File to list versions for (default: every tracked file)'
    )
    list_parser.add_argument(
        '--limit', type=int, default=None,
        help='Show at most this many versions per file'
    )

    restore_parser = sub.add_parser(
        'restore', parents=[common], help='Restore one file to an archived version'
    )
    restore_parser.add_argument('path', help='File to restore, relative to --root')
    selector = restore_parser.add_mutually_exclusive_group()
    selector.add_argument(
        '--index', type=int, default=None,
        help='Version to restore by list index (0 = newest)'
    )
    selector.add_argument(
        '--version', type=int, default=None,
        help='Version to restore by recorded version number'
    )
    selector.add_argument(
        '--timestamp', default=None,
        help='Restore the version that was live at this time'
    )

    all_parser = sub.add_parser(
        'restore-all', parents=[common],
        help='Point-in-time restore of every tracked file'
    )
    all_parser.add_argument(
        '--at', required=True,
        help='Point in time to restore to (e.g. 2026-07-20 or 20260720T133000)'
    )

    for target in (restore_parser, all_parser):
        target.add_argument(
            '--to', default=None,
            help='Write restored copies here instead of their original location'
        )
        target.add_argument(
            '--select', choices=list(SELECT_MODES), default='content',
            help="Timestamp semantics: 'content' (default) restores what was live "
                 "at that time; 'archived' picks the newest version archived at or "
                 "before it"
        )
        target.add_argument(
            '--dry-run', action='store_true',
            help='Report what would change without writing anything'
        )
        target.add_argument(
            '--force', action='store_true',
            help='Overwrite an existing destination outside --root '
                 '(such files cannot be archived first)'
        )
        target.add_argument(
            '--no-verify', dest='verify', action='store_false',
            help='Skip checksum verification (not recommended)'
        )

    return parser


def main(argv=None) -> int:
    """Entry point for ``ifetch-restore``. Returns 0 on success, 1 on failure."""
    parser = build_parser()
    args = parser.parse_args(argv)

    if not getattr(args, 'command', None):
        parser.print_help(sys.stderr)
        return 2

    root = Path(getattr(args, 'root', '.') or '.').expanduser()
    as_json = bool(getattr(args, 'as_json', False))

    if not root.exists():
        message = f"root directory does not exist: {root}"
        if as_json:
            print(json.dumps({"success": False, "error": message}, indent=2))
        else:
            print(f"Error: {message}", file=sys.stderr)
        return 1

    manager = RestoreManager(
        root=root,
        verify=getattr(args, 'verify', True),
        dry_run=getattr(args, 'dry_run', False),
    )

    try:
        if args.command == 'list':
            listing = manager.list_versions(args.path) if args.path else manager.list_versions()
            if as_json:
                payload = {
                    "root": str(manager.root),
                    "success": True,
                    "files": [
                        {
                            "path": key,
                            "versions": [e.to_dict() for e in listing[key]],
                        }
                        for key in sorted(listing)
                    ],
                }
                print(json.dumps(payload, indent=2))
            else:
                _print_listing(listing, args.limit)
            return 0

        if args.command == 'restore':
            result = manager.restore(
                args.path,
                index=args.index,
                version=args.version,
                timestamp=args.timestamp,
                to=args.to,
                select=args.select,
                force=args.force,
            )
        else:  # restore-all
            result = manager.restore_all(
                args.at,
                to=args.to,
                select=args.select,
                force=args.force,
            )
    except RestoreError as exc:
        if as_json:
            print(json.dumps({"success": False, "error": str(exc)}, indent=2))
        else:
            print(f"Error: {exc}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print("\nOperation cancelled by user.", file=sys.stderr)
        return 130

    if as_json:
        payload = result.to_dict()
        payload["root"] = str(manager.root)
        print(json.dumps(payload, indent=2))
    else:
        _print_actions(result)

    return 0 if result.success else 1


if __name__ == '__main__':
    sys.exit(main())
