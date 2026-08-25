"""``ifetch snapshot`` - dated states of a mirror, and getting back to them.

``create``   freeze the current state under a label
``list``     what snapshots exist
``diff``     what changed between two of them
``restore``  bring files back, dry-run by default
``delete``   remove a snapshot

Snapshots are metadata, so creating one is instant and costs kilobytes no
matter how large the mirror is. That is deliberate: a snapshot you have to
think about taking is one you will not take before the sync that breaks
something.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Optional, Sequence

from .index import open_index
from .manifest import Manifest, load_signing_key
from .render import plural
from .scanner import LocalScanner
from .snapshots import (
    SOURCE_VERSIONS,
    SnapshotError,
    apply_restore,
    auto_label,
    create_snapshot,
    plan_restore,
    render_restore_plan,
    render_snapshot_diff,
    render_snapshot_list,
)

EXIT_OK = 0
EXIT_DIFFERENCES = 1
EXIT_ERROR = 2


def _common(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--local-path", default=".",
        help="Local mirror directory (default: current directory)",
    )
    parser.add_argument("--json", dest="as_json", action="store_true",
                        help="Emit JSON instead of a text report")
    parser.add_argument("--show", type=int, default=40, metavar="N",
                        help="Rows to display (default: 40)")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ifetch snapshot",
        description="Create, compare and restore dated snapshots of a mirror.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    create = sub.add_parser("create", help="Freeze the current state under a label")
    _common(create)
    create.add_argument("label", nargs="?", help="Snapshot name (default: auto-TIMESTAMP)")
    create.add_argument("--note", default="", help="Free-text note stored with it")
    create.add_argument("--replace", action="store_true",
                        help="Overwrite an existing snapshot of the same name")
    create.add_argument(
        "--no-rescan", dest="rescan", action="store_false", default=True,
        help=(
            "Snapshot the index as it stands instead of re-indexing the disk "
            "first. Faster, but records what iFetch last saw rather than what "
            "is there now."
        ),
    )
    create.add_argument(
        "--fast", action="store_true",
        help=(
            "Skip hashing and reuse whatever digests are already recorded. "
            "Much faster, but a snapshot without digests can only detect added "
            "and removed files - not modified or corrupted ones - and cannot "
            "drive a restore."
        ),
    )

    listing = sub.add_parser("list", help="List existing snapshots")
    _common(listing)

    diff = sub.add_parser("diff", help="Compare two snapshots")
    _common(diff)
    diff.add_argument("older", help="The earlier snapshot label")
    diff.add_argument("newer", help="The later snapshot label")

    restore = sub.add_parser("restore", help="Bring files back to a snapshot's state")
    _common(restore)
    restore.add_argument("label", help="Snapshot to restore towards")
    restore.add_argument(
        "--apply", action="store_true",
        help=(
            "Actually restore. Without this, the plan is printed and nothing "
            "is written. Only files with exact archived bytes in .versions are "
            "restored; the rest are left for a normal download."
        ),
    )

    delete = sub.add_parser("delete", help="Delete a snapshot")
    _common(delete)
    delete.add_argument("label")

    return parser


def _root(args: argparse.Namespace) -> Path:
    return Path(args.local_path).expanduser().resolve()


def _emit(args: argparse.Namespace, payload: Any, text: str, stdout: Any) -> None:
    print(json.dumps(payload, indent=2) if args.as_json else text, file=stdout)


def cmd_create(args: argparse.Namespace, stdout: Any) -> int:
    root = _root(args)
    store = open_index(root)
    try:
        if args.rescan:
            if not args.as_json:
                print("Indexing local files...", file=stdout)
            manifest = Manifest.load(root, key=load_signing_key())
            # Hashing is the default: a snapshot's whole value is content
            # comparison, and one without digests cannot do the job.
            stats = LocalScanner(
                store, root, manifest=manifest, rehash=not args.fast
            ).scan()
            if not args.as_json:
                print(f"  {stats.files:,} files indexed", file=stdout)

        label = args.label or auto_label()
        result = create_snapshot(store, label, note=args.note, replace=args.replace)
        text = (
            f"Created snapshot '{result['label']}' with {result['entries']:,} entries."
        )
        if result["entries_without_digest"]:
            text += (
                f"\n  Warning: {result['entries_without_digest']:,} of them have no "
                "digest, so changes to those files will not be detected. Re-run "
                "without --fast to hash them."
            )
        _emit(args, result, text, stdout)
        return EXIT_OK
    finally:
        store.close()


def cmd_list(args: argparse.Namespace, stdout: Any) -> int:
    store = open_index(_root(args))
    try:
        snapshots = store.list_snapshots()
        _emit(args, snapshots, render_snapshot_list(snapshots), stdout)
        return EXIT_OK
    finally:
        store.close()


def cmd_diff(args: argparse.Namespace, stdout: Any) -> int:
    store = open_index(_root(args))
    try:
        entries = store.diff_snapshots(args.older, args.newer)
        _emit(
            args, [e.to_dict() for e in entries],
            render_snapshot_diff(entries, args.older, args.newer, show=args.show),
            stdout,
        )
        return EXIT_DIFFERENCES if entries else EXIT_OK
    except KeyError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return EXIT_ERROR
    finally:
        store.close()


def cmd_restore(args: argparse.Namespace, stdout: Any) -> int:
    root = _root(args)
    store = open_index(root)
    try:
        plan = plan_restore(store, root, args.label)

        if args.as_json and not args.apply:
            print(json.dumps(plan.to_dict(), indent=2), file=stdout)
            return EXIT_DIFFERENCES if plan.candidates else EXIT_OK

        if not args.apply:
            print(render_restore_plan(plan, show=args.show), file=stdout)
            if plan.by_source(SOURCE_VERSIONS):
                print(
                    "\nNothing has been changed. Re-run with --apply to restore "
                    "the files listed as recoverable from .versions.",
                    file=stdout,
                )
            return EXIT_DIFFERENCES if plan.candidates else EXIT_OK

        outcomes = apply_restore(plan, root, dry_run=False)
        payload = [o.to_dict() for o in outcomes]
        restored = sum(1 for o in outcomes if o.status == "restored")
        failed = sum(1 for o in outcomes if o.status == "failed")

        if args.as_json:
            print(json.dumps(payload, indent=2), file=stdout)
        else:
            for outcome in outcomes:
                print(f"[{outcome.status}] {outcome.path}", file=stdout)
            print(
                f"\nRestored {restored:,} {plural(restored, 'file')}, "
                f"{failed:,} failed.",
                file=stdout,
            )
            remaining = len(plan.candidates) - restored
            if remaining > 0:
                print(
                    f"{remaining:,} {plural(remaining, 'file')} still "
                    f"{plural(remaining, 'differs', 'differ')} from the snapshot; "
                    "they have no archived copy and must be re-downloaded with "
                    "'ifetch'.",
                    file=stdout,
                )
        return EXIT_ERROR if failed else EXIT_OK
    except SnapshotError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return EXIT_ERROR
    finally:
        store.close()


def cmd_delete(args: argparse.Namespace, stdout: Any) -> int:
    store = open_index(_root(args))
    try:
        if store.delete_snapshot(args.label):
            print(f"Deleted snapshot '{args.label}'.", file=stdout)
            return EXIT_OK
        print(f"Error: no snapshot named '{args.label}'.", file=sys.stderr)
        return EXIT_ERROR
    finally:
        store.close()


_COMMANDS = {
    "create": cmd_create,
    "list": cmd_list,
    "diff": cmd_diff,
    "restore": cmd_restore,
    "delete": cmd_delete,
}


def main(argv: Optional[Sequence[str]] = None, stdout: Any = None) -> int:
    stream = stdout if stdout is not None else sys.stdout
    args = build_parser().parse_args(list(argv) if argv is not None else None)
    try:
        return _COMMANDS[args.command](args, stream)
    except SnapshotError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return EXIT_ERROR
    except KeyboardInterrupt:
        print("\nCancelled.", file=sys.stderr)
        return EXIT_ERROR
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return EXIT_ERROR


if __name__ == "__main__":
    raise SystemExit(main())
