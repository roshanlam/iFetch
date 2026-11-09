"""``ifetch adpcheck`` - run the Advanced Data Protection validation live.

The procedure in ``docs/adp-validation.md`` needs a real Apple ID with ADP
turned on, which needs a physical device and a recovery key. That part is
unavoidably manual. Everything after it - ``auth doctor`` -> list -> download ->
unattended renew - is this command.

Read-only: it lists and downloads into a temporary directory and removes it
afterwards. Nothing is written to iCloud.
"""

from __future__ import annotations

import argparse
import io
import json
import os
import sys
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence

from .adpcheck import ADPChecker, render_report
from .sharecheck import BROKEN, INCOMPLETE, VALIDATED

#: Every step passed against a live ADP account.
EXIT_OK = 0
#: A step failed. Something is genuinely wrong.
EXIT_BROKEN = 1
#: The run could not be started - bad credentials, bad arguments.
EXIT_ERROR = 2
#: Steps were skipped (usually: the account is not shown to be under ADP), so
#: the run proves less than a pass.
EXIT_INCOMPLETE = 3

_EXIT_FOR_VERDICT = {
    VALIDATED: EXIT_OK,
    BROKEN: EXIT_BROKEN,
    INCOMPLETE: EXIT_INCOMPLETE,
}

#: How long the unattended renew may wait for PCS. Short on purpose: a renew
#: that has to wait for a human is the failure step 8 exists to catch.
RENEW_ADP_TIMEOUT = 30.0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ifetch adpcheck",
        description=(
            "Validate iFetch against an Apple ID with Advanced Data Protection "
            "turned on: auth doctor, list, download, offline verify, re-run, "
            "and an unattended renew. Read-only: nothing is written to iCloud. "
            "See docs/adp-validation.md for the account setup this needs."
        ),
    )
    parser.add_argument(
        "folder",
        help="An iCloud Drive folder with a few files in it (e.g. 'ADPTest')",
    )
    parser.add_argument(
        "--assume-adp", action="store_true",
        help="Continue even when Apple does not report ADP as on. Without this, "
             "an account without ADP stops the run, because every later step "
             "would pass without testing the PCS flow at all.",
    )
    parser.add_argument(
        "--adp-service", choices=["drive", "photos"], default="drive",
        help="Which per-service PCS cookie to check and renew (default: drive)",
    )
    parser.add_argument("--keep", action="store_true",
                        help="Keep the downloaded files instead of deleting them")
    parser.add_argument("--workdir", metavar="DIR",
                        help="Download into this directory instead of a temporary one")
    parser.add_argument("--json", dest="as_json", action="store_true",
                        help="Emit JSON instead of a text report")
    parser.add_argument("--report",
                        help="Write the full result to this JSON path. The account "
                             "is masked, so the file is safe to attach to #31.")
    parser.add_argument("--email", help="iCloud account email (or set $ICLOUD_EMAIL)")
    parser.add_argument("--region", choices=["global", "china"], default=None)
    parser.add_argument("--password-command", dest="password_command")
    parser.add_argument("--max-workers", type=int, default=4)
    parser.add_argument("--log-file", help="Write the run log here. Attach this "
                                           "to a bug report if a step fails.")
    return parser


def _resolve_email(args: argparse.Namespace) -> str:
    email = (args.email or os.environ.get("ICLOUD_EMAIL") or "").strip()
    if not email:
        raise ValueError("an Apple ID is required: pass --email or set $ICLOUD_EMAIL")
    return email


def _authenticate(args: argparse.Namespace, email: str, stdout: Any) -> Any:
    from .auth import resolve_password
    from .downloader import DownloadManager
    from .logger import setup_logging

    downloader = DownloadManager(
        email=email,
        max_workers=args.max_workers,
        region=args.region,
        password=resolve_password(getattr(args, "password_command", None)),
    )
    if args.log_file:
        downloader.logger = setup_logging(args.log_file)
    print("Authenticating with iCloud...", file=stdout)
    downloader.authenticate()
    print("Authenticated.\n", file=stdout)
    return downloader


def _auth_flags(args: argparse.Namespace, email: str) -> List[str]:
    flags = ["--email", email]
    if args.region:
        flags += ["--region", args.region]
    if args.password_command:
        flags += ["--password-command", args.password_command]
    return flags


def _default_doctor(args: argparse.Namespace, email: str) -> Callable[[], Any]:
    def run() -> Any:
        from .auth import AuthDoctor, resolve_password, resolve_region

        return AuthDoctor(
            account=email,
            region=resolve_region(args.region),
            online=True,
            adp=True,
            pcs_service_name=args.adp_service,
            password=resolve_password(getattr(args, "password_command", None)),
        ).run()
    return run


def _default_cookies(args: argparse.Namespace, email: str) -> Callable[[], Any]:
    def run() -> Any:
        from .auth import pcs_cookie_state, read_session_snapshot

        return pcs_cookie_state(read_session_snapshot(email), args.adp_service)
    return run


def _default_renew(args: argparse.Namespace, email: str) -> Callable[[], Dict[str, Any]]:
    def run() -> Dict[str, Any]:
        from . import auth_cli

        buffer = io.StringIO()
        argv = ["renew", "--adp", "--non-interactive", "--no-stdin", "--json",
                "--adp-service", args.adp_service,
                "--adp-timeout", str(RENEW_ADP_TIMEOUT),
                *_auth_flags(args, email)]
        auth_cli.main(argv, stdout=buffer)
        text = buffer.getvalue().strip()
        try:
            return json.loads(text[text.index("{"):]) if "{" in text else {}
        except ValueError:
            return {"renewed": False, "error": text[:300] or "no output from renew"}
    return run


def main(argv: Optional[Sequence[str]] = None,
         stdout: Any = None,
         downloader: Any = None,
         doctor: Optional[Callable[[], Any]] = None,
         cookies: Optional[Callable[[], Any]] = None,
         renew: Optional[Callable[[], Dict[str, Any]]] = None) -> int:
    """Run the validation.

    Every live dependency is injectable so the whole command can be exercised
    in tests without an Apple ID; the CLI builds real ones when they are absent.
    """
    stdout = stdout or sys.stdout
    args = build_parser().parse_args(argv)

    try:
        email = _resolve_email(args)
        if downloader is None:
            downloader = _authenticate(args, email, stdout)
    except Exception as exc:
        print(f"Error: could not authenticate: {exc}", file=sys.stderr)
        print("Run 'ifetch auth renew --adp' first and approve the prompt on a "
              "trusted device, then re-run this check.", file=sys.stderr)
        return EXIT_ERROR

    print(f"Validating Advanced Data Protection with '{args.folder}'...\n", file=stdout)
    checker = ADPChecker(
        folder=args.folder,
        doctor=doctor or _default_doctor(args, email),
        downloader=downloader,
        cookies=cookies or _default_cookies(args, email),
        renew=renew or _default_renew(args, email),
        account=email,
        adp_service=args.adp_service,
        workdir=Path(args.workdir).expanduser() if args.workdir else None,
        keep=args.keep,
        assume_adp=args.assume_adp,
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
