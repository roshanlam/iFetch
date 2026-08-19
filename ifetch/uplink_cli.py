"""``ifetch uplink`` - put files back into iCloud that iCloud has lost.

``plan``
    What would be uploaded, where each file would go, how many bytes it is, and
    every file that is refused with the reason. Offline: it reads the last scan
    and never contacts Apple.
``push``
    The same plan, then the uploads. **Dry run by default**; ``--apply`` sends.
    Only files iCloud does not have are sent. Nothing is overwritten, renamed or
    deleted, and a file that has reappeared in iCloud since the plan was built
    is skipped rather than replaced.
``history``
    Everything iFetch has ever uploaded from this mirror, from the index.

Exit codes
----------
``0``
    Nothing needed uploading, or everything planned was uploaded.
``1``
    There is work to do (``plan``, or a dry run), or the run had failures or
    refusals. Details are in the report.
``2``
    The command could not run: no scan exists, the mirror is unreadable, no
    credentials for an ``--apply``.
``3``
    Refused. Either the scan behind the plan cannot support the claim that
    anything is missing - in which case every file looks missing and this would
    upload the whole mirror - or iCloud does not have room. Nothing was sent.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Optional, Sequence

from .index import open_index
from .recovery import fetch_account_storage, write_csv
from .uplink import (
    DEFAULT_MAX_FRACTION,
    DEFAULT_MAX_UPLOADS,
    DEFAULT_MIN_BASELINE,
    STATUS_FAILED,
    DriveUplink,
    UplinkError,
    apply_uploads,
    csv_rows,
    plan_uploads,
    render_plan,
    render_run,
    render_uploads,
)

EXIT_OK = 0
EXIT_FINDINGS = 1
EXIT_ERROR = 2
#: Distinct on purpose. "Refused to upload" must not be read by a script as
#: "uploaded nothing because there was nothing to upload".
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


def _credentials(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--icloud-path", dest="icloud_path", default=None,
                        help="Remote folder this mirror came from "
                             "(default: the one recorded by the last scan)")
    parser.add_argument("--email", help="iCloud account email (or set $ICLOUD_EMAIL)")
    parser.add_argument("--region", choices=["global", "china"], default=None)
    parser.add_argument("--password-command", dest="password_command")
    parser.add_argument("--max-workers", type=int, default=8)


def _breaker_options(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--max-uploads", type=int, default=DEFAULT_MAX_UPLOADS, metavar="N",
        help=f"Refuse to upload anything at or above N missing files "
             f"(default: {DEFAULT_MAX_UPLOADS})",
    )
    parser.add_argument(
        "--max-fraction", type=float, default=DEFAULT_MAX_FRACTION, metavar="F",
        help=f"Refuse at or above this fraction of the mirror, 0-1 "
             f"(default: {DEFAULT_MAX_FRACTION})",
    )
    parser.add_argument(
        "--min-baseline", type=int, default=DEFAULT_MIN_BASELINE, metavar="N",
        help=f"Below this mirror size the fraction rule is not applied "
             f"(default: {DEFAULT_MIN_BASELINE})",
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ifetch uplink",
        description=(
            "Upload files that are missing from iCloud. Never overwrites, "
            "renames or deletes anything."
        ),
    )
    sub = parser.add_subparsers(dest="command", required=True)

    plan = sub.add_parser("plan", help="What would be uploaded, and what is refused")
    _common(plan)
    _credentials(plan)
    _breaker_options(plan)

    push = sub.add_parser("push", help="Upload the missing files (dry run by default)")
    _common(push)
    _credentials(push)
    _breaker_options(push)
    push.add_argument(
        "--apply", action="store_true",
        help="Actually upload. Without this, nothing is sent and nothing in "
             "your iCloud account is touched.",
    )

    history = sub.add_parser("history", help="Everything iFetch has uploaded here")
    _common(history)

    return parser


def _emit(args: argparse.Namespace, payload: dict, text: str, stdout: Any) -> None:
    print(json.dumps(payload, indent=2) if args.as_json else text, file=stdout)
    if args.report:
        path = Path(args.report).expanduser()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        if not args.as_json:
            print(f"\nReport written to '{path}'", file=stdout)


def _connect(args: argparse.Namespace, stdout: Any) -> Any:
    """Authenticate. Only ever called for an ``--apply``."""
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
    return downloader


def _load_manifest(root: Path) -> Any:
    from .manifest import Manifest

    try:
        return Manifest.load(root)
    except Exception:
        return None


def _build(args: argparse.Namespace, store: Any, account: Optional[dict]) -> Any:
    root = Path(args.local_path).expanduser().resolve()
    return plan_uploads(
        store, root,
        icloud_path=getattr(args, "icloud_path", None),
        manifest=_load_manifest(root),
        account_storage=account,
        max_count=args.max_uploads,
        max_fraction=args.max_fraction,
        min_baseline=args.min_baseline,
    )


def _require_scan(store: Any) -> Optional[int]:
    if store.latest_scan() is None:
        print(
            "Error: no iCloud scan exists for this directory yet, so there is "
            "no way to tell which files iCloud is missing. Run 'ifetch plan' "
            "first.",
            file=sys.stderr,
        )
        return EXIT_ERROR
    return None


def cmd_plan(args: argparse.Namespace, stdout: Any) -> int:
    root = Path(args.local_path).expanduser().resolve()
    store = open_index(root)
    try:
        failure = _require_scan(store)
        if failure is not None:
            return failure

        plan = _build(args, store, None)
        _emit(args, plan.to_dict(), render_plan(plan, show=args.show), stdout)

        if args.csv:
            headers, rows = csv_rows(plan)
            write_csv(Path(args.csv).expanduser(), headers, rows)

        if plan.refused:
            return EXIT_REFUSED
        return EXIT_FINDINGS if (plan.candidates or plan.refusals) else EXIT_OK
    finally:
        store.close()


def cmd_push(args: argparse.Namespace, stdout: Any) -> int:
    root = Path(args.local_path).expanduser().resolve()
    store = open_index(root)
    try:
        failure = _require_scan(store)
        if failure is not None:
            return failure

        downloader = _connect(args, stdout) if args.apply else None
        account = fetch_account_storage(downloader.api) if downloader else None

        plan = _build(args, store, account)
        drive = None
        if downloader is not None:
            drive = DriveUplink(downloader, base=plan.icloud_path)

        run = apply_uploads(
            plan, store, root, drive=drive, dry_run=not args.apply,
            manifest=_load_manifest(root),
        )

        payload = {"plan": plan.to_dict(), "run": run.to_dict()}
        _emit(args, payload, render_run(run, plan, show=args.show), stdout)

        if args.csv:
            headers, rows = csv_rows(plan, run)
            write_csv(Path(args.csv).expanduser(), headers, rows)

        if run.refused:
            return EXIT_REFUSED
        if run.by_status(STATUS_FAILED) or plan.refusals:
            return EXIT_FINDINGS
        if run.dry_run:
            return EXIT_FINDINGS if plan.candidates else EXIT_OK
        return EXIT_OK
    finally:
        store.close()


def cmd_history(args: argparse.Namespace, stdout: Any) -> int:
    root = Path(args.local_path).expanduser().resolve()
    store = open_index(root)
    try:
        rows = list(store.iter_uploads())
        payload = {"root": str(root), "count": len(rows), "uploads": rows}
        _emit(args, payload, render_uploads(rows, show=args.show), stdout)

        if args.csv:
            write_csv(
                Path(args.csv).expanduser(),
                ["path", "remote_path", "state", "size", "sha256", "last_error"],
                [
                    [r["path"], r["remote_path"], r["state"], r["size"],
                     r["sha256"], r["last_error"]]
                    for r in rows
                ],
            )
        return EXIT_OK
    finally:
        store.close()


_COMMANDS = {
    "plan": cmd_plan,
    "push": cmd_push,
    "history": cmd_history,
}


def main(argv: Optional[Sequence[str]] = None, stdout: Any = None) -> int:
    stream = stdout if stdout is not None else sys.stdout
    args = build_parser().parse_args(list(argv) if argv is not None else None)
    try:
        return _COMMANDS[args.command](args, stream)
    except UplinkError as exc:
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
