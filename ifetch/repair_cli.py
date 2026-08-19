"""``ifetch repair`` and ``ifetch resume`` - finishing what an interrupted run started.

``repair``
    Offline. What did the last run leave unfinished, what partial files are
    lying around, and (with ``--check-digests``) which files no longer match
    the digest recorded when they were downloaded. ``--apply`` queues them.

``resume``
    Online. Re-fetch exactly the files the journal says are still owed, opening
    each one directly instead of re-walking the drive to rediscover them.

The split is deliberate. ``repair`` is the one you can run on a machine that
can no longer sign in, which is often the machine you are worried about.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Optional, Sequence

from .index import open_index
from .render import human_bytes, plural
from .transfers import (
    REPAIR_FAILED,
    REPAIR_QUEUED,
    TransferJournal,
    apply_repair,
    build_repair_report,
    render_repair,
    render_resume_plan,
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
    parser.add_argument("--show", type=int, default=40, metavar="N",
                        help="Rows to display (default: 40)")
    parser.add_argument(
        "--no-colour", dest="colour", action="store_false", default=None,
        help="Disable ANSI colour (also honours $NO_COLOR)",
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ifetch repair",
        description=(
            "Report what an interrupted run left behind - unfinished transfers, "
            "partial files, and files that no longer match their recorded "
            "digest - and queue them for a fresh fetch."
        ),
    )
    _common(parser)
    parser.add_argument(
        "--check-digests", action="store_true",
        help=(
            "Also re-hash every file the manifest vouches for, to find "
            "corruption. Reads the whole mirror, so it is slow - and it is the "
            "only way to detect damage that left the file size unchanged."
        ),
    )
    parser.add_argument(
        "--apply", action="store_true",
        help=(
            "Queue the affected files for a fresh download. Without this, "
            "nothing is written. iFetch cannot reconstruct missing bytes "
            "locally; repairing means making the next run fetch them."
        ),
    )
    parser.add_argument(
        "--discard-partials", action="store_true",
        help=(
            "Also delete the .temp/.download files. Off by default because "
            "they are exactly what lets a resume avoid re-fetching gigabytes. "
            "A file whose digest does not match is discarded regardless - a "
            "wrong prefix must never be built on."
        ),
    )
    return parser


def build_resume_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ifetch resume",
        description=(
            "Re-fetch only the transfers the journal says are unfinished, "
            "without re-listing the whole drive."
        ),
    )
    _common(parser)
    parser.add_argument("--email", help="iCloud account email (or set $ICLOUD_EMAIL)")
    parser.add_argument("--region", choices=["global", "china"], default=None)
    parser.add_argument("--password-command", dest="password_command")
    parser.add_argument("--max-workers", type=int, default=4)
    parser.add_argument("--max-retries", type=int, default=3)
    parser.add_argument(
        "--dry-run", action="store_true",
        help="List what would be resumed and exit without contacting Apple",
    )
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


def cmd_repair(args: argparse.Namespace, stdout: Any) -> int:
    root = _root(args)
    store = open_index(root)
    try:
        manifest = None
        if args.check_digests:
            from .manifest import Manifest, load_signing_key

            manifest = Manifest.load(root, key=load_signing_key())
            if len(manifest) == 0:
                manifest = None

        report = build_repair_report(
            store, root, manifest=manifest, check_digests=args.check_digests,
        )

        if not args.apply:
            _emit(
                args, report.to_dict(),
                render_repair(report, show=args.show, use_colour=args.colour),
                stdout,
            )
            if report.findings and not args.as_json:
                print(
                    "\nNothing has been changed. Re-run with --apply to queue "
                    "these for a fresh download.",
                    file=stdout,
                )
            return EXIT_FINDINGS if report.findings else EXIT_OK

        outcomes = apply_repair(
            store, root, report, dry_run=False,
            discard_partials=args.discard_partials,
        )
        payload = {"root": str(root), "outcomes": [o.to_dict() for o in outcomes]}
        queued = [o for o in outcomes if o.status == REPAIR_QUEUED]
        failed = [o for o in outcomes if o.status == REPAIR_FAILED]

        if args.as_json:
            _emit(args, payload, "", stdout)
        else:
            for outcome in outcomes:
                print(f"[{outcome.status}] {outcome.path}  ({outcome.detail})",
                      file=stdout)
            print(
                f"\nQueued {len(queued):,} {plural(len(queued), 'file')} for a "
                f"fresh download. {len(failed):,} failed.",
                file=stdout,
            )
            if queued:
                print("Run 'ifetch resume' to fetch them.", file=stdout)
        return EXIT_ERROR if failed else EXIT_OK
    finally:
        store.close()


def cmd_resume(args: argparse.Namespace, stdout: Any) -> int:
    root = _root(args)
    store = open_index(root)
    try:
        journal = TransferJournal(store, root)
        rows = journal.incomplete()

        if not rows or args.dry_run:
            _emit(
                args, {"root": str(root), "transfers": rows},
                render_resume_plan(rows, use_colour=args.colour), stdout,
            )
            return EXIT_FINDINGS if rows else EXIT_OK

        resumable = [r for r in rows if r.get("remote_path")]
        if not resumable:
            print(
                "Error: none of the unfinished transfers recorded a remote "
                "path, so iFetch cannot reopen them individually. Re-run the "
                "original 'ifetch' command instead.",
                file=sys.stderr,
            )
            return EXIT_ERROR

        downloader = _make_downloader(args, root, store)
        print("Authenticating with iCloud...", file=stdout)
        downloader.authenticate()

        succeeded, failed = 0, 0
        try:
            for row in resumable:
                target = root / row["path"]
                print(f"Resuming {row['path']}...", file=stdout)
                try:
                    item = downloader.get_drive_item(row["remote_path"])
                except Exception as exc:
                    journal.fail(target, f"could not reopen in iCloud: {exc}")
                    print(f"  failed: {exc}", file=sys.stderr)
                    failed += 1
                    continue

                if downloader.download_drive_item(
                    item, target, remote_path=row["remote_path"]
                ):
                    succeeded += 1
                else:
                    failed += 1
        finally:
            # download() would normally do this. Without it a resumed file
            # lands with no sync state and no manifest digest, so the next run
            # re-downloads it and 'ifetch-verify' cannot vouch for it.
            _persist(downloader)

        transferred = sum(r.downloaded for r in downloader.download_results)
        print(
            f"\nResumed {succeeded:,} of {len(resumable):,} "
            f"{plural(len(resumable), 'transfer')} "
            f"({human_bytes(transferred)} transferred). {failed:,} failed.",
            file=stdout,
        )
        skipped = len(rows) - len(resumable)
        if skipped:
            print(
                f"{skipped:,} {plural(skipped, 'transfer')} had no recorded "
                f"remote path and {plural(skipped, 'was', 'were')} left in the "
                f"journal; re-run the original 'ifetch' command to pick "
                f"{plural(skipped, 'it', 'them')} up.",
                file=stdout,
            )
        return EXIT_ERROR if failed else EXIT_OK
    finally:
        store.close()


def _persist(downloader: Any) -> None:
    """Flush the bookkeeping a resumed download produced."""
    for save in (
        getattr(downloader.sync_state, "save", None),
        getattr(downloader.manifest, "save", None),
    ):
        if save is None:
            continue
        try:
            save()
        except Exception:
            pass


def _make_downloader(args: argparse.Namespace, root: Path, store: Any) -> Any:
    """A downloader wired to this destination's journal, manifest and versions."""
    from .auth import resolve_password
    from .downloader import DownloadManager, SyncState
    from .manifest import Manifest, load_signing_key
    from .versioning import VersionManager

    downloader = DownloadManager(
        email=args.email,
        max_workers=args.max_workers,
        max_retries=args.max_retries,
        region=args.region,
        password=resolve_password(getattr(args, "password_command", None)),
    )
    # download() normally does this. A resume drives download_drive_item
    # directly, so the same bookkeeping has to be set up by hand or the
    # resumed files would land without sync state, digests or version history.
    downloader.root_path = root
    downloader.version_manager = VersionManager(root)
    downloader.sync_state = SyncState(root)
    downloader.manifest = Manifest.load(root, key=load_signing_key())
    # Share this command's store rather than opening a second connection to the
    # same database: one owner, one close, no lock contention between the two.
    downloader.index = None
    downloader.journal = TransferJournal(store, root)
    return downloader


def main(argv: Optional[Sequence[str]] = None, stdout: Any = None) -> int:
    stream = stdout if stdout is not None else sys.stdout
    args = build_parser().parse_args(list(argv) if argv is not None else None)
    return _dispatch(cmd_repair, args, stream)


def resume_main(argv: Optional[Sequence[str]] = None, stdout: Any = None) -> int:
    stream = stdout if stdout is not None else sys.stdout
    args = build_resume_parser().parse_args(list(argv) if argv is not None else None)
    return _dispatch(cmd_resume, args, stream)


def _dispatch(command: Any, args: argparse.Namespace, stream: Any) -> int:
    try:
        return command(args, stream)
    except KeyboardInterrupt:
        print("\nCancelled.", file=sys.stderr)
        return EXIT_ERROR
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return EXIT_ERROR


if __name__ == "__main__":
    raise SystemExit(main())
