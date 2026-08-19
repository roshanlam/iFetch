"""``ifetch sharecheck`` - run the shared-folder validation against a live account.

The procedure in ``docs/shared-folder-validation.md`` needs two Apple IDs and a
folder shared between them. That part is unavoidably manual. Everything after it
is this command.

Read-only: it lists and downloads into a temporary directory and removes it
afterwards. Nothing is written to iCloud.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Optional, Sequence

from .sharecheck import BROKEN, INCOMPLETE, ShareChecker, VALIDATED, render_report

#: Every step passed against a live share.
EXIT_OK = 0
#: A step failed. Something is genuinely wrong.
EXIT_BROKEN = 1
#: The run could not be started - bad credentials, bad arguments.
EXIT_ERROR = 2
#: Steps were skipped, so the run proves less than a pass. Deliberately its own
#: code: a CI job that treats "incomplete" as success would defeat the point of
#: running this at all.
EXIT_INCOMPLETE = 3

_EXIT_FOR_VERDICT = {
    VALIDATED: EXIT_OK,
    BROKEN: EXIT_BROKEN,
    INCOMPLETE: EXIT_INCOMPLETE,
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ifetch sharecheck",
        description=(
            "Validate iFetch against a folder shared by another Apple ID. "
            "Read-only: nothing is written to iCloud. See "
            "docs/shared-folder-validation.md for the two-account setup this needs."
        ),
    )
    parser.add_argument(
        "share_path",
        help="Path to the shared folder as it appears in your iCloud Drive "
             "(e.g. 'SharedTest')",
    )
    parser.add_argument(
        "--nested", default="nested", metavar="REL",
        help="Subfolder one level inside the share (default: nested)",
    )
    parser.add_argument(
        "--deeper", default="nested/deeper", metavar="REL",
        help="Subfolder two levels inside the share (default: nested/deeper). "
             "This is the level that fails in other iCloud clients.",
    )
    parser.add_argument(
        "--assume-shared", action="store_true",
        help="Continue even when the folder carries no shareID. Without this, a "
             "folder you own stops the run rather than passing every step and "
             "reporting a validation it did not earn.",
    )
    parser.add_argument(
        "--keep", action="store_true",
        help="Keep the downloaded files instead of deleting them afterwards",
    )
    parser.add_argument(
        "--workdir", metavar="DIR",
        help="Download into this directory instead of a temporary one "
             "(implies the directory is yours to manage)",
    )
    parser.add_argument("--json", dest="as_json", action="store_true",
                        help="Emit JSON instead of a text report")
    parser.add_argument("--report", help="Write the full result to this JSON path")
    parser.add_argument("--email", help="iCloud account email (or set $ICLOUD_EMAIL)")
    parser.add_argument("--region", choices=["global", "china"], default=None)
    parser.add_argument("--password-command", dest="password_command")
    parser.add_argument("--max-workers", type=int, default=4)
    parser.add_argument("--log-file", help="Write the run log here. Attach this "
                                           "to a bug report if a step fails.")
    return parser


def _authenticate(args: argparse.Namespace, stdout: Any) -> Any:
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
    print("Authenticated.\n", file=stdout)
    return downloader


def main(argv: Optional[Sequence[str]] = None,
         stdout: Any = None,
         downloader: Any = None) -> int:
    """Run the validation.

    ``downloader`` is injectable so the whole command can be exercised in tests
    without credentials; the CLI builds a real one when it is not supplied.
    """
    stdout = stdout or sys.stdout
    args = build_parser().parse_args(argv)

    try:
        if downloader is None:
            downloader = _authenticate(args, stdout)
    except Exception as exc:
        print(f"Error: could not authenticate: {exc}", file=sys.stderr)
        print("Run 'ifetch auth doctor --online' first - a shared-folder failure "
              "and an authentication failure look alike in logs.", file=sys.stderr)
        return EXIT_ERROR

    print(f"Validating '{args.share_path}'...\n", file=stdout)
    checker = ShareChecker(
        downloader=downloader,
        share_path=args.share_path,
        nested=args.nested,
        deeper=args.deeper,
        workdir=Path(args.workdir).expanduser() if args.workdir else None,
        keep=args.keep,
        assume_shared=args.assume_shared,
        log=lambda message: print(message, file=stdout),
    )
    report = checker.run()
    payload = report.to_dict()

    if args.report:
        Path(args.report).expanduser().write_text(json.dumps(payload, indent=2))

    print("", file=stdout)
    print(json.dumps(payload, indent=2) if args.as_json else render_report(report),
          file=stdout)

    return _EXIT_FOR_VERDICT.get(report.verdict, EXIT_ERROR)


if __name__ == "__main__":
    raise SystemExit(main())
