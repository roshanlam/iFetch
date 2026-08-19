"""``ifetch vanish`` - what iCloud used to have and does not have now.

``check``
    Compare the current iCloud listing against earlier evidence - the local
    index, or a dated snapshot - and classify everything that is gone: still
    safe on this disk, lost outright, a placeholder pretending to be safe, or
    ambiguous because a rename fits the evidence just as well.
``forget``
    Drop recorded absences for paths you deleted on purpose, so they stop
    carrying a purge deadline. Dry-run by default; ``--apply`` performs it.

Neither subcommand downloads, deletes or restores anything, and neither
contacts Apple unless ``--refresh`` is given. The default reuses the last scan
on purpose: the day you most need to know what disappeared is often the day you
can no longer sign in to ask.

Exit codes
----------
``0``
    The evidence was usable and nothing had vanished - or there was no baseline
    at all, which is reported in those words rather than as a clean result.
``1``
    Files have vanished from iCloud. Findings are in the report.
``2``
    The command could not run: no scan exists, the snapshot named does not, the
    mirror is unreadable.
``3``
    Refused. Either so much vanished at once that a listing failure, a partial
    scan, an auth problem or an absent mount is at least as likely as a
    deletion, or the scan behind the report is itself broken. The paths are in
    the JSON payload; they are deliberately not presented as findings.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Optional, Sequence

from .index import open_index
from .recovery import PlaceholderDetector, write_csv
from .vanished import (
    DEFAULT_MAX_FRACTION,
    DEFAULT_MAX_VANISHED,
    DEFAULT_MIN_BASELINE,
    VanishedError,
    analyse,
    csv_rows,
    render_observations,
    render_vanished,
)

EXIT_OK = 0
EXIT_FINDINGS = 1
EXIT_ERROR = 2
#: Distinct on purpose. A script that treats "files vanished" as an alert must
#: not treat "the evidence is unusable" as the same alert, or as an all-clear.
EXIT_REFUSED = 3


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
        prog="ifetch vanish",
        description="Detect files that have disappeared from iCloud.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    check = sub.add_parser(
        "check", help="What iCloud used to have and no longer lists",
    )
    _common(check)
    _scan_options(check)
    check.add_argument(
        "--since", metavar="SNAPSHOT",
        help="Use a named snapshot as the baseline instead of the local index",
    )
    check.add_argument(
        "--no-placeholders", dest="check_placeholders",
        action="store_false", default=True,
        help="Skip placeholder detection (a vanished file whose local copy is "
             "an evicted shell will then be reported as safe, and the report "
             "will say so)",
    )
    check.add_argument(
        "--no-rename-check", dest="check_renames",
        action="store_false", default=True,
        help="Do not consider whether a disappearance is a rename",
    )
    check.add_argument(
        "--no-record", dest="record", action="store_false", default=True,
        help="Do not persist when an absence was first observed. The purge "
             "deadline then restarts at today on every run, which makes it "
             "weaker than the evidence supports.",
    )
    check.add_argument(
        "--max-vanished", type=int, default=DEFAULT_MAX_VANISHED, metavar="N",
        help=f"Refuse to report ordinary findings at or above N vanished files "
             f"(default: {DEFAULT_MAX_VANISHED})",
    )
    check.add_argument(
        "--max-fraction", type=float, default=DEFAULT_MAX_FRACTION, metavar="F",
        help=f"Refuse at or above this fraction of the baseline, 0-1 "
             f"(default: {DEFAULT_MAX_FRACTION})",
    )
    check.add_argument(
        "--min-baseline", type=int, default=DEFAULT_MIN_BASELINE, metavar="N",
        help=f"Below this baseline size the fraction rule is not applied, "
             f"because one delete out of three is not a mass deletion "
             f"(default: {DEFAULT_MIN_BASELINE})",
    )

    forget = sub.add_parser(
        "forget", help="Drop recorded absences for paths deleted on purpose",
    )
    _common(forget)
    forget.add_argument("paths", nargs="*", help="Paths to forget (default: all)")
    forget.add_argument(
        "--apply", action="store_true",
        help="Actually forget them. Without this, nothing is changed.",
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


def cmd_check(args: argparse.Namespace, stdout: Any) -> int:
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
        if args.check_placeholders:
            placeholders = PlaceholderDetector(root).scan()

        report = analyse(
            store, root,
            since=args.since,
            placeholders=placeholders,
            check_renames=args.check_renames,
            record=args.record,
            max_count=args.max_vanished,
            max_fraction=args.max_fraction,
            min_baseline=args.min_baseline,
        )

        _emit(args, report.to_dict(), render_vanished(report, show=args.show), stdout)

        if args.csv:
            headers, rows = csv_rows(report)
            write_csv(Path(args.csv).expanduser(), headers, rows)

        if report.refused:
            return EXIT_REFUSED
        return EXIT_FINDINGS if report.items else EXIT_OK
    finally:
        store.close()


def cmd_forget(args: argparse.Namespace, stdout: Any) -> int:
    """Dry-run by default: forgetting an absence discards evidence."""
    root = Path(args.local_path).expanduser().resolve()
    store = open_index(root)
    try:
        recorded = list(store.iter_missing_observations())
        wanted = set(args.paths)
        targets = [r for r in recorded if not wanted or r["path"] in wanted]

        missing = sorted(wanted - {r["path"] for r in recorded})
        payload = {
            "root": str(root),
            "recorded": len(recorded),
            "paths": [r["path"] for r in targets],
            "not_recorded": missing,
            "applied": bool(args.apply),
        }

        if args.apply:
            store.forget_missing([r["path"] for r in targets])

        text = render_observations(targets, show=args.show)
        if missing:
            text += (
                f"\n{len(missing):,} of the paths named have no recorded "
                "absence and were skipped:\n  "
                + "\n  ".join(missing)
            )
        text += (
            f"\n\n{len(targets):,} recorded absence(s) "
            + ("forgotten." if args.apply else
               "would be forgotten. Nothing was changed; pass --apply to do it.")
        )
        _emit(args, payload, text, stdout)
        return EXIT_OK
    finally:
        store.close()


_COMMANDS = {
    "check": cmd_check,
    "forget": cmd_forget,
}


def main(argv: Optional[Sequence[str]] = None, stdout: Any = None) -> int:
    stream = stdout if stdout is not None else sys.stdout
    args = build_parser().parse_args(list(argv) if argv is not None else None)
    try:
        return _COMMANDS[args.command](args, stream)
    except VanishedError as exc:
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
