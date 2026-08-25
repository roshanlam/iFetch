"""``ifetch recover`` - the questions you ask when something has gone wrong.

``placeholders``
    Which local files are not really here? Wholly offline; no credentials.
``missing``
    What does iCloud have that this disk does not, and what did this disk lose?
``inventory``
    Where did the space go - by folder, by file type, by largest item?

``placeholders`` never touches the network. ``missing`` and ``inventory`` use
the last scan by default and only contact Apple when asked to refresh it, so
they stay usable on a machine that can no longer sign in - which is exactly the
situation people are in when they need them.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Optional, Sequence

from .index import open_index
from .recovery import (
    MISSING_DISAPPEARED,
    PlaceholderDetector,
    build_inventory,
    build_missing_report,
    fetch_account_storage,
    render_inventory,
    render_missing,
    render_placeholders,
    write_csv,
)

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
    parser.add_argument("--csv", help="Write a CSV export to this path")
    parser.add_argument("--show", type=int, default=40, metavar="N",
                        help="Rows to display (default: 40)")


def _scan_options(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--refresh", action="store_true",
        help="Re-scan iCloud first. Without this, the last scan is reused.",
    )
    parser.add_argument("icloud_path", nargs="?", default="",
                        help="Remote path to scan when --refresh is given")
    parser.add_argument("--email", help="iCloud account email (or set $ICLOUD_EMAIL)")
    parser.add_argument("--region", choices=["global", "china"], default=None)
    parser.add_argument("--password-command", dest="password_command")
    parser.add_argument("--max-workers", type=int, default=8)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ifetch recover",
        description="Recovery workflows: placeholders, missing files, storage inventory.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    placeholders = sub.add_parser(
        "placeholders",
        help="Find local files whose contents are not really on this disk",
    )
    _common(placeholders)
    placeholders.add_argument(
        "--no-dataless", dest="check_dataless", action="store_false", default=None,
        help="Skip zero-block detection (macOS only) and use .icloud stubs alone",
    )

    missing = sub.add_parser(
        "missing", help="What iCloud has that this disk does not really have",
    )
    _common(missing)
    _scan_options(missing)
    missing.add_argument(
        "--no-placeholders", dest="include_placeholders",
        action="store_false", default=True,
        help="Do not treat placeholder files as missing",
    )

    inventory = sub.add_parser(
        "inventory", help="Where the space went, by folder and by file type",
    )
    _common(inventory)
    _scan_options(inventory)
    inventory.add_argument("--top", type=int, default=25,
                           help="How many entries per section (default: 25)")
    inventory.add_argument("--depth", type=int, default=1,
                           help="Folder grouping depth (default: 1)")
    inventory.add_argument(
        "--account", action="store_true",
        help="Also fetch Apple's own quota figures (needs credentials)",
    )

    return parser


def _refresh(args: argparse.Namespace, store: Any, stdout: Any) -> Any:
    """Re-scan iCloud, returning the authenticated downloader (or None)."""
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
    return downloader


def _emit(args: argparse.Namespace, payload: dict, text: str, stdout: Any) -> None:
    print(json.dumps(payload, indent=2) if args.as_json else text, file=stdout)
    if args.report:
        path = Path(args.report).expanduser()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        if not args.as_json:
            print(f"\nReport written to '{path}'", file=stdout)


def cmd_placeholders(args: argparse.Namespace, stdout: Any) -> int:
    root = Path(args.local_path).expanduser().resolve()
    report = PlaceholderDetector(root, check_dataless=args.check_dataless).scan()

    _emit(args, report.to_dict(), render_placeholders(report, show=args.show), stdout)

    if args.csv:
        write_csv(
            Path(args.csv).expanduser(),
            ["path", "evidence", "confidence", "reported_size"],
            [[p.path, p.evidence, p.confidence, p.reported_size] for p in report.placeholders],
        )
    return EXIT_FINDINGS if report.placeholders else EXIT_OK


def cmd_missing(args: argparse.Namespace, stdout: Any) -> int:
    root = Path(args.local_path).expanduser().resolve()
    store = open_index(root)
    try:
        if args.refresh:
            _refresh(args, store, stdout)
        elif store.latest_scan() is None:
            print(
                "Error: no iCloud scan exists for this directory yet. "
                "Run with --refresh (needs credentials) or run 'ifetch plan' first.",
                file=sys.stderr,
            )
            return EXIT_ERROR

        placeholders = None
        if args.include_placeholders:
            placeholders = PlaceholderDetector(root).scan()

        report = build_missing_report(store, root, placeholders=placeholders)
        _emit(args, report.to_dict(), render_missing(report, show=args.show), stdout)

        if args.csv:
            write_csv(
                Path(args.csv).expanduser(),
                ["path", "reason", "size", "sha256"],
                [[i.path, i.reason, i.size, i.sha256] for i in report.items],
            )
        return EXIT_FINDINGS if report.items else EXIT_OK
    finally:
        store.close()


def cmd_inventory(args: argparse.Namespace, stdout: Any) -> int:
    root = Path(args.local_path).expanduser().resolve()
    store = open_index(root)
    try:
        downloader = None
        if args.refresh:
            downloader = _refresh(args, store, stdout)
        elif store.latest_scan() is None:
            print(
                "Error: no iCloud scan exists for this directory yet. "
                "Run with --refresh (needs credentials) or run 'ifetch plan' first.",
                file=sys.stderr,
            )
            return EXIT_ERROR

        report = build_inventory(
            store, icloud_path=args.icloud_path, top=args.top, depth=args.depth
        )

        if args.account:
            if downloader is None:
                from .auth import resolve_password
                from .downloader import DownloadManager

                downloader = DownloadManager(
                    email=args.email, region=args.region,
                    password=resolve_password(getattr(args, "password_command", None)),
                )
                downloader.authenticate()
            report.account = fetch_account_storage(downloader.api)
            if report.account is None and not args.as_json:
                print(
                    "Note: Apple did not return account storage figures; "
                    "the inventory below is from the scan only.\n",
                    file=stdout,
                )

        _emit(args, report.to_dict(), render_inventory(report, top=args.top), stdout)

        if args.csv:
            write_csv(
                Path(args.csv).expanduser(),
                ["folder", "files", "bytes"],
                [[f["folder"], f["files"], f["bytes"]] for f in report.by_folder],
            )
        return EXIT_OK
    finally:
        store.close()


_COMMANDS = {
    "placeholders": cmd_placeholders,
    "missing": cmd_missing,
    "inventory": cmd_inventory,
}


def main(argv: Optional[Sequence[str]] = None, stdout: Any = None) -> int:
    stream = stdout if stdout is not None else sys.stdout
    args = build_parser().parse_args(list(argv) if argv is not None else None)
    try:
        return _COMMANDS[args.command](args, stream)
    except KeyboardInterrupt:
        print("\nCancelled.", file=sys.stderr)
        return EXIT_ERROR
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return EXIT_ERROR


if __name__ == "__main__":
    raise SystemExit(main())
