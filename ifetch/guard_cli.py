"""``ifetch guard`` - is your iCloud Drive folder actually backed up?

With "Optimize Mac Storage" on, iCloud Drive evicts file contents and leaves
full-size placeholders behind. Time Machine, third-party backup software and
cloud backup agents copy those placeholders and report success, so the loss is
discovered at restore time. This command counts the bytes that exist only on
Apple's servers, states the confidence of every signal it used, names everything
it could not examine, and - with ``--materialize --apply`` - fetches the missing
data back so the next backup is a real one.

The scan is entirely offline and needs no credentials. Credentials are only
required for ``--materialize --apply`` with the direct-download strategy.

Exit status is designed for a monitoring job: ``0`` when the folder is fully
resident *and* fully examined, ``1`` when anything is evicted or anything could
not be looked at, ``2`` when the command itself failed.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Optional, Sequence

from .guard import (
    EVICTED_CSV_HEADERS,
    STRATEGY_BRCTL,
    STRATEGY_FETCH,
    GuardError,
    GuardScanner,
    default_icloud_folder,
    evicted_csv_rows,
    materialize,
    render_guard,
    write_csv,
)

EXIT_OK = 0
EXIT_FINDINGS = 1
EXIT_ERROR = 2


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ifetch guard",
        description=(
            "Report how much of a local iCloud Drive folder exists only on "
            "Apple's servers - and is therefore absent from every backup of "
            "this machine - and optionally download it back."
        ),
    )
    parser.add_argument(
        "local_path", nargs="?", default=None,
        help=(
            "Folder to examine (default: the local iCloud Drive folder, "
            "~/Library/Mobile Documents/com~apple~CloudDocs)"
        ),
    )
    parser.add_argument("--json", dest="as_json", action="store_true",
                        help="Emit JSON instead of a text report")
    parser.add_argument("--report", help="Write the full result to this JSON path")
    parser.add_argument("--csv", help="Write the evicted-file list to this CSV path")
    parser.add_argument("--show", type=int, default=20, metavar="N",
                        help="Rows to display per section (default: 20)")
    parser.add_argument(
        "--no-dataless", dest="check_dataless", action="store_false", default=None,
        help=(
            "Skip zero-block detection and rely on '.icloud' stubs alone. The "
            "report will name 'dataless' as unevaluated rather than imply the "
            "folder is clean."
        ),
    )

    parser.add_argument(
        "--materialize", action="store_true",
        help="Also plan (or, with --apply, perform) the download of every evicted file",
    )
    parser.add_argument(
        "--apply", action="store_true",
        help=(
            "Actually download. Without it --materialize only prints what it "
            "would fetch and writes nothing."
        ),
    )
    parser.add_argument(
        "--strategy", choices=["auto", STRATEGY_FETCH, STRATEGY_BRCTL], default="auto",
        help=(
            "How to materialise. 'fetch' downloads from iCloud over HTTPS and "
            "writes the bytes here (reliable; needs credentials). 'brctl' asks "
            "macOS's FileProvider, which is asynchronous and returns success for "
            "requests it has merely queued and may silently drop - so its result "
            "is verified against the disk regardless. 'auto' prefers 'fetch'."
        ),
    )
    parser.add_argument(
        "--limit", type=int, default=None, metavar="N",
        help="Materialise only the N largest evicted files",
    )
    parser.add_argument("--icloud-path", dest="icloud_path", default="",
                        help="Remote prefix this folder mirrors (default: Drive root)")
    parser.add_argument("--email", help="iCloud account email (or set $ICLOUD_EMAIL)")
    parser.add_argument("--region", choices=["global", "china"], default=None)
    parser.add_argument("--password-command", dest="password_command")
    parser.add_argument("--max-workers", type=int, default=8)
    return parser


def resolve_root(args: argparse.Namespace) -> Path:
    """Where to look, refusing to guess when the default is not there."""
    if args.local_path is not None:
        return Path(args.local_path).expanduser().resolve()

    default = default_icloud_folder()
    if not default.is_dir():
        raise GuardError(
            f"the local iCloud Drive folder does not exist at '{default}'. "
            "iCloud Drive may be off, this may not be a Mac, or the folder may "
            "live elsewhere - pass the path explicitly. Refusing to scan the "
            "home directory instead, because that would produce a confident "
            "number about the wrong tree."
        )
    return default.resolve()


def _build_fetcher(args: argparse.Namespace, stdout: Any):
    """Wire a real :class:`DownloadManager` behind the tiny fetcher contract."""
    from .auth import resolve_password
    from .downloader import DownloadManager

    downloader = DownloadManager(
        email=args.email,
        max_workers=args.max_workers,
        region=args.region,
        password=resolve_password(getattr(args, "password_command", None)),
    )
    print("Authenticating with iCloud...", file=stdout)
    downloader.authenticate()

    prefix = (args.icloud_path or "").strip("/")

    def fetch(relative_path: str, destination: Path) -> bool:
        remote = f"{prefix}/{relative_path}" if prefix else relative_path
        item = downloader.get_drive_item(remote)
        return bool(downloader.download_drive_item(item, destination, remote_path=remote))

    return fetch


def _emit(args: argparse.Namespace, payload: dict, text: str, stdout: Any) -> None:
    print(json.dumps(payload, indent=2) if args.as_json else text, file=stdout)
    if args.report:
        path = Path(args.report).expanduser()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        if not args.as_json:
            print(f"\nReport written to '{path}'", file=stdout)


def run(args: argparse.Namespace, stdout: Any) -> int:
    root = resolve_root(args)
    report = GuardScanner(root, check_dataless=args.check_dataless).scan()

    if args.materialize:
        fetcher = None
        if args.apply and args.strategy in ("auto", STRATEGY_FETCH):
            # Only sign in when there is something to fetch and the user has
            # actually asked for the write.
            if report.evicted:
                fetcher = _build_fetcher(args, stdout)
        report.materialization = materialize(
            report, root,
            fetcher=fetcher,
            dry_run=not args.apply,
            limit=args.limit,
            prefer=args.strategy,
            check_dataless=args.check_dataless,
        )

    _emit(args, report.to_dict(), render_guard(report, show=args.show), stdout)

    if args.csv:
        write_csv(Path(args.csv).expanduser(), EVICTED_CSV_HEADERS,
                  evicted_csv_rows(report))

    outstanding = report.evicted
    if report.materialization is not None and not report.materialization.dry_run:
        # After a real materialisation the finding is what is *still* missing.
        outstanding = report.materialization.still_evicted
    return EXIT_FINDINGS if outstanding or not report.complete else EXIT_OK


def main(argv: Optional[Sequence[str]] = None, stdout: Any = None) -> int:
    stream = stdout if stdout is not None else sys.stdout
    args = build_parser().parse_args(list(argv) if argv is not None else None)
    try:
        return run(args, stream)
    except GuardError as exc:
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
