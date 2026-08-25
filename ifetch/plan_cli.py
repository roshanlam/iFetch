"""``ifetch plan`` and ``ifetch audit`` - look before you leap.

``plan``
    Dry run. Scan iCloud and the destination, then report what would be
    downloaded, how many bytes, whether the disk can hold it, what would be
    overwritten, what is skipped, and roughly how long it would take. Transfers
    nothing.

``audit``
    The same reconciliation, presented as "what exists remotely vs locally" -
    the question that matters when you are recovering an account rather than
    populating a fresh mirror.

Both are strictly read-only with respect to the user's files. The only thing
either writes is iFetch's own index, plus an optional JSON report.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Optional, Sequence

from .index import open_index
from .manifest import Manifest, load_signing_key
from .planner import Planner, render_audit, render_plan
from .render import human_bytes, human_duration
from .scanner import LocalScanner, RemoteScanner

EXIT_OK = 0
EXIT_DISCREPANCIES = 1
EXIT_ERROR = 2
EXIT_USAGE = 64


def _add_shared(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "icloud_path",
        nargs="?",
        default="",
        help='Remote iCloud Drive path (e.g. "Documents/MyFolder"). Defaults to the root.',
    )
    parser.add_argument(
        "local_path",
        nargs="?",
        default=".",
        help="Local destination directory (default: current directory)",
    )
    parser.add_argument("--email", help="iCloud account email (or set $ICLOUD_EMAIL)")
    parser.add_argument(
        "--region", choices=["global", "china"], default=None,
        help="iCloud region; 'china' uses iCloud.com.cn endpoints",
    )
    parser.add_argument(
        "--max-workers", type=int, default=8,
        help="Concurrent directory listings during the scan (default: 8)",
    )
    parser.add_argument(
        "--no-scan", action="store_true",
        help=(
            "Skip the network scan and reuse the last one. Fast, but the report "
            "is only as current as that scan."
        ),
    )
    parser.add_argument(
        "--rehash", action="store_true",
        help=(
            "Re-hash every local file instead of reusing recorded digests. "
            "Slow, and what you want when checking for corruption."
        ),
    )
    parser.add_argument("--profile", help="Named include/exclude pattern set")
    parser.add_argument("--profile-file", dest="profile_file", help="Custom profile JSON path")
    parser.add_argument("--report", help="Write the full result to this JSON path")
    parser.add_argument(
        "--json", dest="as_json", action="store_true",
        help="Emit JSON on stdout instead of a text report",
    )
    parser.add_argument(
        "--quiet", action="store_true", help="Suppress progress output"
    )
    parser.add_argument(
        "--no-colour", dest="colour", action="store_false", default=None,
        help="Disable ANSI colour (also honours $NO_COLOR)",
    )
    parser.add_argument(
        "--password-command", dest="password_command",
        help="Command printing the Apple ID password (or set $IFETCH_PASSWORD_COMMAND)",
    )
    parser.add_argument(
        "--portable-names", dest="portable_names", action="store_true",
        help="Predict names sanitized for the strictest filesystem rules",
    )
    parser.add_argument(
        "--normalize-names", dest="normalize_names",
        choices=["preserve", "nfc"], default="preserve",
        help="Local filename spelling (default: preserve Apple's)",
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ifetch plan",
        description="Dry-run planner and remote-vs-local audit. Transfers nothing.",
    )
    _add_shared(parser)
    parser.add_argument(
        "--show-files", type=int, default=20, metavar="N",
        help="List at most N affected files (0 for all). Default: 20",
    )
    parser.add_argument(
        "--throughput", type=float, metavar="MBPS",
        help=(
            "Assumed download speed in MB/s, used for the time estimate. "
            "Without it, and without a measured figure from previous runs, the "
            "estimate is reported as unknown rather than guessed."
        ),
    )
    return parser


def build_audit_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ifetch audit",
        description="Report what exists in iCloud versus what is on disk.",
    )
    _add_shared(parser)
    parser.add_argument(
        "--show-files", type=int, default=40, metavar="N",
        help="List at most N discrepancies (0 for all). Default: 40",
    )
    return parser


def _make_downloader(args: argparse.Namespace) -> Any:
    from .auth import resolve_password
    from .downloader import DownloadManager

    return DownloadManager(
        email=args.email,
        max_workers=args.max_workers,
        region=args.region,
        password=resolve_password(getattr(args, "password_command", None)),
        portable_names=args.portable_names,
        normalize_names=args.normalize_names,
    )


def _patterns(args: argparse.Namespace):
    if not args.profile:
        return [], []
    from .profiles import ProfileManager

    config = Path(args.profile_file).expanduser() if args.profile_file else None
    return ProfileManager(args.profile, config_path=config).get_patterns()


def _run(args: argparse.Namespace, mode: str, stdout: Any) -> int:
    local_path = Path(args.local_path).expanduser().resolve()
    quiet = bool(args.quiet) or bool(args.as_json)

    store = open_index(local_path)
    try:
        include, exclude = _patterns(args)
        scan_errors = []

        if not args.no_scan:
            downloader = _make_downloader(args)
            if not quiet:
                print("Authenticating with iCloud...", file=stdout)
            downloader.authenticate()

            if not quiet:
                print(
                    f"Scanning iCloud at '{args.icloud_path or '/'}' "
                    "(listing only - no files are downloaded)...",
                    file=stdout,
                )

            def progress(stats):
                if not quiet and (stats.files + stats.directories) % 500 == 0:
                    print(
                        f"  {stats.files:,} files, {stats.directories:,} folders, "
                        f"{human_bytes(stats.total_bytes)}",
                        file=stdout,
                    )

            scanner = RemoteScanner(
                store, downloader=downloader, max_workers=args.max_workers,
                include_patterns=include, exclude_patterns=exclude,
                portable_names=args.portable_names,
                normalize_names=args.normalize_names,
                progress=progress if not quiet else None,
            )
            stats = scanner.scan(args.icloud_path)
            scan_errors = stats.errors
            if not quiet:
                print(
                    f"  scanned {stats.files:,} files "
                    f"({human_bytes(stats.total_bytes)}) in "
                    f"{human_duration(stats.duration_seconds)}",
                    file=stdout,
                )
        elif store.latest_scan() is None:
            print(
                "Error: --no-scan was given but no previous scan exists for "
                f"{local_path}. Run without --no-scan first.",
                file=sys.stderr,
            )
            return EXIT_ERROR

        if not quiet:
            print("Indexing local files...", file=stdout)
        manifest = Manifest.load(local_path, key=load_signing_key())
        local_stats = LocalScanner(
            store, local_path, manifest=manifest, rehash=args.rehash
        ).scan()
        if not quiet:
            print(
                f"  indexed {local_stats.files:,} files "
                f"({human_bytes(local_stats.total_bytes)})\n",
                file=stdout,
            )

        throughput = args.throughput * 1024 * 1024 if getattr(args, "throughput", None) else None
        plan = Planner(
            store, local_path, icloud_path=args.icloud_path,
            throughput_override=throughput,
        ).build(scan_errors=scan_errors)

        if args.as_json:
            print(json.dumps(plan.to_dict(), indent=2), file=stdout)
        else:
            renderer = render_plan if mode == "plan" else render_audit
            print(
                renderer(plan, show_files=args.show_files or len(plan.items),
                         use_colour=args.colour),
                file=stdout,
            )

        if args.report:
            report_path = Path(args.report).expanduser()
            report_path.parent.mkdir(parents=True, exist_ok=True)
            report_path.write_text(json.dumps(plan.to_dict(), indent=2), encoding="utf-8")
            if not quiet:
                print(f"\nReport written to '{report_path}'", file=stdout)

        if mode == "audit":
            # An audit is a question with a yes/no answer, so its exit code
            # reports whether the two sides agreed.
            discrepancies = (
                plan.counts().get("download", 0)
                + plan.counts().get("overwrite", 0)
                + plan.counts().get("orphan", 0)
            )
            return EXIT_DISCREPANCIES if discrepancies else EXIT_OK

        return EXIT_OK
    finally:
        store.close()


def main(argv: Optional[Sequence[str]] = None, stdout: Any = None) -> int:
    stream = stdout if stdout is not None else sys.stdout
    args = build_parser().parse_args(list(argv) if argv is not None else None)
    return _dispatch(args, "plan", stream)


def audit_main(argv: Optional[Sequence[str]] = None, stdout: Any = None) -> int:
    stream = stdout if stdout is not None else sys.stdout
    args = build_audit_parser().parse_args(list(argv) if argv is not None else None)
    return _dispatch(args, "audit", stream)


def _dispatch(args: argparse.Namespace, mode: str, stream: Any) -> int:
    try:
        return _run(args, mode, stream)
    except KeyboardInterrupt:
        print("\nCancelled.", file=sys.stderr)
        return EXIT_ERROR
    except ValueError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return EXIT_USAGE
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return EXIT_ERROR


if __name__ == "__main__":
    raise SystemExit(main())
