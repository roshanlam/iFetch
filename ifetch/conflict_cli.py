"""``ifetch conflicts`` - the same file under a different name.

``renames``
    What iCloud is offering as new that you already have somewhere else. With
    ``--apply``, the local file is moved instead of downloaded again.
``duplicates``
    Which local paths hold byte-identical contents, and how much that costs.
``moves``
    What has moved on disk since a snapshot. Proved by digest.

``duplicates`` and ``moves`` never touch the network. ``renames`` needs a
remote scan in the index and reuses the last one unless ``--refresh`` is given,
so it too works on a machine that can no longer sign in.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Optional, Sequence

from .conflicts import (
    CONFIDENCE_STRONG,
    CONFIDENCE_WEAK,
    RELOCATE_DONE,
    RELOCATE_FAILED,
    ConflictError,
    detect_duplicates,
    detect_moves,
    detect_renames,
    relocate,
    render_duplicates,
    render_moves,
    render_renames,
)
from .index import open_index
from .render import human_bytes, plural

EXIT_OK = 0
EXIT_FINDINGS = 1
EXIT_ERROR = 2


def _common(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "local_path", nargs="?", default=".",
        help="Local mirror directory (default: current directory)",
    )
    parser.add_argument("--json", dest="as_json", action="store_true",
                        help="Emit JSON instead of a text report")
    parser.add_argument("--report", help="Write the full result to this JSON path")
    parser.add_argument("--show", type=int, default=40, metavar="N",
                        help="Rows to display (default: 40)")
    parser.add_argument(
        "--min-size", type=int, default=1, metavar="BYTES",
        help=(
            "Ignore files smaller than this (default: 1, which excludes empty "
            "files). Raising it suppresses the small files where a size "
            "coincidence is most likely."
        ),
    )
    parser.add_argument(
        "--no-colour", dest="colour", action="store_false", default=None,
        help="Disable ANSI colour (also honours $NO_COLOR)",
    )


def _fast(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--fast", action="store_true",
        help=(
            "Reuse whatever digests are already recorded instead of hashing the "
            "mirror. Much faster on a large tree, but this command compares "
            "digests - files that have none are not compared at all, and the "
            "report says how many were skipped."
        ),
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ifetch conflicts",
        description=(
            "Detect renames, moves and duplicates so the same bytes are not "
            "downloaded, or stored, twice."
        ),
    )
    sub = parser.add_subparsers(dest="command", required=True)

    renames = sub.add_parser(
        "renames",
        help="Files iCloud offers as new that you already have under another name",
    )
    _common(renames)
    renames.add_argument(
        "--refresh", action="store_true",
        help="Re-scan iCloud first. Without this, the last scan is reused.",
    )
    renames.add_argument("icloud_path", nargs="?", default="",
                         help="Remote path to scan when --refresh is given")
    renames.add_argument("--email", help="iCloud account email (or set $ICLOUD_EMAIL)")
    renames.add_argument("--region", choices=["global", "china"], default=None)
    renames.add_argument("--password-command", dest="password_command")
    renames.add_argument("--max-workers", type=int, default=8)
    renames.add_argument(
        "--apply", action="store_true",
        help=(
            "Move the local files to the paths iCloud now uses instead of "
            "downloading them again. Without this, nothing is written."
        ),
    )
    renames.add_argument(
        "--include-weak", action="store_true",
        help=(
            "Also act on matches whose only evidence is an identical byte size. "
            "Off by default: a size coincidence and a rename look the same, and "
            "acting on the wrong one leaves the wrong bytes at the new path."
        ),
    )

    duplicates = sub.add_parser(
        "duplicates", help="Local paths holding byte-identical contents",
    )
    _common(duplicates)
    _fast(duplicates)

    moves = sub.add_parser(
        "moves", help="What has moved on disk since a snapshot (proved by digest)",
    )
    _common(moves)
    _fast(moves)
    moves.add_argument("--since", required=True, metavar="LABEL",
                       help="Snapshot label to compare against")

    return parser


def _root(args: argparse.Namespace) -> Path:
    return Path(args.local_path).expanduser().resolve()


def _emit(args: argparse.Namespace, payload: Any, text: str, stdout: Any) -> None:
    print(json.dumps(payload, indent=2) if args.as_json else text, file=stdout)
    if args.report:
        path = Path(args.report).expanduser()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        if not args.as_json:
            print(f"\nReport written to '{path}'", file=stdout)


def _refresh(args: argparse.Namespace, store: Any, stdout: Any) -> None:
    from .auth import resolve_password
    from .downloader import DownloadManager
    from .scanner import RemoteScanner

    downloader = DownloadManager(
        email=args.email,
        max_workers=args.max_workers,
        region=args.region,
        password=resolve_password(getattr(args, "password_command", None)),
    )
    print("Authenticating with iCloud...", file=stdout)
    downloader.authenticate()
    print("Scanning iCloud (listing only)...", file=stdout)
    stats = RemoteScanner(store, downloader=downloader, max_workers=args.max_workers).scan(
        args.icloud_path
    )
    print(f"  {stats.files:,} files indexed\n", file=stdout)


def _index_local(args: argparse.Namespace, store: Any, stdout: Any, hash_files: bool) -> None:
    """Refresh the local half of the index.

    ``hash_files`` is not an optimisation knob. Duplicate and move detection
    *are* digest comparisons, so without digests they cannot find anything - and
    a mirror downloaded before manifests existed, or one whose manifest was
    never written, has none recorded. Hashing by default is what stops those
    commands from reporting a confident "nothing found" about a question they
    never actually asked.
    """
    from .manifest import Manifest, load_signing_key
    from .scanner import LocalScanner

    if not args.as_json:
        print(
            "Indexing local files (hashing)..." if hash_files
            else "Indexing local files...",
            file=stdout,
        )
    manifest = Manifest.load(_root(args), key=load_signing_key())
    stats = LocalScanner(
        store, _root(args), manifest=manifest, rehash=hash_files,
    ).scan()
    if not args.as_json:
        print(f"  {stats.files:,} files indexed\n", file=stdout)


def cmd_renames(args: argparse.Namespace, stdout: Any) -> int:
    root = _root(args)
    store = open_index(root)
    try:
        if args.refresh:
            _refresh(args, store, stdout)
        elif store.latest_scan() is None:
            print(
                "Error: no iCloud scan exists for this directory yet, so there "
                "is nothing to match local files against. Run with --refresh "
                "(needs credentials) or run 'ifetch plan' first.",
                file=sys.stderr,
            )
            return EXIT_ERROR

        # Renames match on name and size, so hashing every local file would
        # be minutes of I/O for evidence this detector does not use.
        _index_local(args, store, stdout, hash_files=False)

        notes: list = []
        candidates = detect_renames(store, min_size=args.min_size, notes=notes)

        if not args.apply:
            payload = {
                "root": str(root),
                "candidates": [c.to_dict() for c in candidates],
                "notes": notes,
            }
            _emit(
                args, payload,
                render_renames(candidates, notes, show=args.show,
                               use_colour=args.colour),
                stdout,
            )
            if candidates and not args.as_json:
                print(
                    "\nNothing has been changed. Re-run with --apply to move "
                    "the strong matches instead of downloading them.",
                    file=stdout,
                )
            return EXIT_FINDINGS if candidates else EXIT_OK

        from .manifest import Manifest, load_signing_key

        manifest = Manifest.load(root, key=load_signing_key())
        floor = CONFIDENCE_WEAK if args.include_weak else CONFIDENCE_STRONG
        outcomes = relocate(
            store, root, candidates, dry_run=False,
            min_confidence=floor, manifest=manifest,
        )

        payload = {"root": str(root), "outcomes": [o.to_dict() for o in outcomes]}
        moved = [o for o in outcomes if o.status == RELOCATE_DONE]
        failed = [o for o in outcomes if o.status == RELOCATE_FAILED]
        saved = sum(o.bytes_saved for o in moved)

        if args.as_json:
            _emit(args, payload, "", stdout)
        else:
            for outcome in outcomes:
                print(
                    f"[{outcome.status}] {outcome.old_path} -> {outcome.new_path}"
                    + (f"  ({outcome.detail})" if outcome.detail else ""),
                    file=stdout,
                )
            print(
                f"\nMoved {len(moved):,} {plural(len(moved), 'file')}, avoiding "
                f"{human_bytes(saved)} of transfer. {len(failed):,} failed.",
                file=stdout,
            )
            if moved:
                print(
                    "The moved files were matched on name and size, not on "
                    "content - Apple publishes none. Run 'ifetch-verify' after "
                    "the next sync to confirm they are what iCloud has.",
                    file=stdout,
                )
        return EXIT_ERROR if failed else EXIT_OK
    finally:
        store.close()


def cmd_duplicates(args: argparse.Namespace, stdout: Any) -> int:
    store = open_index(_root(args))
    try:
        _index_local(args, store, stdout, hash_files=not args.fast)

        notes: list = []
        groups = detect_duplicates(store, min_size=args.min_size, notes=notes)
        payload = {
            "root": str(_root(args)),
            "groups": [g.to_dict() for g in groups],
            "wasted_bytes": sum(g.wasted_bytes for g in groups),
            "notes": notes,
        }
        _emit(
            args, payload,
            render_duplicates(groups, notes, show=args.show, use_colour=args.colour),
            stdout,
        )
        return EXIT_FINDINGS if groups else EXIT_OK
    finally:
        store.close()


def cmd_moves(args: argparse.Namespace, stdout: Any) -> int:
    store = open_index(_root(args))
    try:
        _index_local(args, store, stdout, hash_files=not args.fast)

        notes: list = []
        moves = detect_moves(store, args.since, notes=notes)
        payload = {
            "root": str(_root(args)),
            "since": args.since,
            "moves": [m.to_dict() for m in moves],
            "notes": notes,
        }
        _emit(
            args, payload,
            render_moves(moves, args.since, notes, show=args.show,
                         use_colour=args.colour),
            stdout,
        )
        return EXIT_FINDINGS if moves else EXIT_OK
    finally:
        store.close()


_COMMANDS = {
    "renames": cmd_renames,
    "duplicates": cmd_duplicates,
    "moves": cmd_moves,
}


def main(argv: Optional[Sequence[str]] = None, stdout: Any = None) -> int:
    stream = stdout if stdout is not None else sys.stdout
    args = build_parser().parse_args(list(argv) if argv is not None else None)
    try:
        return _COMMANDS[args.command](args, stream)
    except ConflictError as exc:
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
