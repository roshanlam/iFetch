"""``ifetch auth`` - diagnose, renew and report on iCloud authentication.

Three subcommands:

``doctor``
    Say which precondition failed, in words, instead of relaying an HTTP status.
``renew``
    Sign in and trust the session, using a non-interactive 2FA source so it can
    run from cron, Docker or a NAS.
``status``
    A one-line, script-friendly expiry report; exits non-zero *before* the
    session dies so a scheduled job can renew while there is still time.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Sequence

from .auth import (
    CHECK_FAIL,
    DEFAULT_WARN_DAYS,
    REGIONS,
    STATUS_EXPIRED,
    STATUS_OK,
    STATUS_UNKNOWN,
    STATUS_WARN,
    AuthDoctor,
    TwoFactorResolver,
    TwoFactorUnavailable,
    classify_auth_error,
    evaluate_expiry,
    resolve_password,
    read_session_snapshot,
    region_service_kwargs,
    render_diagnosis,
    resolve_region,
)

EXIT_OK = 0
EXIT_WARN = 1
EXIT_FAIL = 2
EXIT_USAGE = 64


def _add_common(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--email",
        help="iCloud account email (defaults to $ICLOUD_EMAIL)",
    )
    parser.add_argument(
        "--region",
        choices=list(REGIONS),
        help=(
            "iCloud region. 'china' switches every endpoint to iCloud.com.cn, "
            "which China Mainland Apple IDs require. Defaults to $ICLOUD_REGION, "
            "then the legacy ICLOUD_CHINA=true, then 'global'."
        ),
    )
    parser.add_argument(
        "--cookie-directory",
        help="Directory holding the stored pyicloud session (defaults to pyicloud's)",
    )
    parser.add_argument(
        "--password-command",
        dest="password_command",
        help=(
            "Command printing the Apple ID password on stdout, for machines with "
            "no system keyring (or set $IFETCH_PASSWORD_COMMAND)"
        ),
    )
    parser.add_argument(
        "--json",
        dest="as_json",
        action="store_true",
        help="Emit machine-readable JSON instead of text",
    )


def _add_twofactor(parser: argparse.ArgumentParser) -> None:
    group = parser.add_argument_group(
        "non-interactive two-factor",
        "Ways to supply a 2FA code without a terminal. Tried in this order.",
    )
    group.add_argument("--2fa-code", dest="twofa_code", help="The six-digit code itself")
    group.add_argument(
        "--2fa-file",
        dest="twofa_file",
        help="Poll this file until it contains a six-digit code",
    )
    group.add_argument(
        "--2fa-webhook",
        dest="twofa_webhook",
        help="Poll this URL (GET) until the response contains a six-digit code",
    )
    group.add_argument(
        "--2fa-timeout",
        dest="twofa_timeout",
        type=float,
        default=300.0,
        help="Seconds to wait for a polled code (default: 300)",
    )
    group.add_argument(
        "--no-stdin",
        dest="allow_stdin",
        action="store_false",
        default=True,
        help="Never read a code from stdin",
    )
    group.add_argument(
        "--non-interactive",
        action="store_true",
        help="Fail instead of prompting a human for anything",
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ifetch auth",
        description="Diagnose and maintain iCloud authentication.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    doctor = sub.add_parser(
        "doctor",
        help="Report which authentication precondition failed, and how to fix it",
    )
    _add_common(doctor)
    doctor.add_argument(
        "--online",
        action="store_true",
        help="Also test the stored session against Apple (makes network requests)",
    )
    doctor.add_argument(
        "--warn-days",
        type=int,
        default=DEFAULT_WARN_DAYS,
        help=f"Warn when fewer than this many days remain (default: {DEFAULT_WARN_DAYS})",
    )

    renew = sub.add_parser("renew", help="Sign in and trust the session")
    _add_common(renew)
    _add_twofactor(renew)
    renew.add_argument(
        "--reset",
        action="store_true",
        help="Discard the stored session first (use after 'Invalid Session Token')",
    )
    renew.add_argument(
        "--if-expiring-within",
        type=int,
        metavar="DAYS",
        help="Do nothing unless the session expires within DAYS (for cron)",
    )

    status = sub.add_parser("status", help="One-line session expiry report")
    _add_common(status)
    status.add_argument(
        "--warn-days",
        type=int,
        default=DEFAULT_WARN_DAYS,
        help=f"Exit 1 when fewer than this many days remain (default: {DEFAULT_WARN_DAYS})",
    )

    return parser


def _resolve_email(args: argparse.Namespace) -> str:
    email = args.email or os.environ.get("ICLOUD_EMAIL")
    if not email:
        raise SystemExit(
            "No Apple ID given. Pass --email or set ICLOUD_EMAIL."
        )
    return email.strip()


def _cookie_directory(args: argparse.Namespace) -> Optional[Path]:
    value = getattr(args, "cookie_directory", None)
    return Path(value).expanduser() if value else None


def _emit(payload: Dict[str, Any], text: str, as_json: bool, stream: Any) -> None:
    print(json.dumps(payload, indent=2) if as_json else text, file=stream)


# ---------------------------------------------------------------------------
# doctor
# ---------------------------------------------------------------------------

def cmd_doctor(args: argparse.Namespace, stdout: Any) -> int:
    email = _resolve_email(args)
    region = resolve_region(args.region)
    doctor = AuthDoctor(
        account=email,
        region=region,
        cookie_directory=_cookie_directory(args),
        warn_days=args.warn_days,
        online=args.online,
        password=resolve_password(getattr(args, "password_command", None)),
    )
    diagnosis = doctor.run()
    _emit(diagnosis.to_dict(), render_diagnosis(diagnosis), args.as_json, stdout)
    return diagnosis.exit_code


# ---------------------------------------------------------------------------
# status
# ---------------------------------------------------------------------------

def cmd_status(args: argparse.Namespace, stdout: Any) -> int:
    email = _resolve_email(args)
    region = resolve_region(args.region)
    snapshot = read_session_snapshot(email, _cookie_directory(args))
    verdict = evaluate_expiry(snapshot, warn_days=args.warn_days)

    payload = {"account": email, "region": region, **verdict.to_dict()}
    _emit(payload, f"{email} [{region}]: {verdict.detail}", args.as_json, stdout)

    return {
        STATUS_OK: EXIT_OK,
        STATUS_WARN: EXIT_WARN,
        STATUS_EXPIRED: EXIT_FAIL,
        STATUS_UNKNOWN: EXIT_WARN,
    }[verdict.status]


# ---------------------------------------------------------------------------
# renew
# ---------------------------------------------------------------------------

def _build_resolver(args: argparse.Namespace) -> TwoFactorResolver:
    return TwoFactorResolver(
        code=getattr(args, "twofa_code", None),
        file=Path(args.twofa_file).expanduser() if getattr(args, "twofa_file", None) else None,
        webhook=getattr(args, "twofa_webhook", None),
        allow_stdin=getattr(args, "allow_stdin", True),
        timeout=getattr(args, "twofa_timeout", 300.0),
    )


def _clear_session(email: str, cookie_directory: Optional[Path], stdout: Any) -> None:
    """Delete stored session artefacts so the next sign-in starts clean.

    This is the documented cure for 'Invalid Session Token': the stale token is
    presented on every attempt and keeps being rejected until it is removed.
    """
    snapshot = read_session_snapshot(email, cookie_directory)
    for path in (snapshot.session_path, snapshot.cookiejar_path):
        try:
            if path.exists():
                path.unlink()
                print(f"Removed {path}", file=stdout)
        except OSError as exc:
            print(f"Could not remove {path}: {exc}", file=stdout)


def cmd_renew(
    args: argparse.Namespace,
    stdout: Any,
    service_factory: Optional[Callable[..., Any]] = None,
) -> int:
    email = _resolve_email(args)
    region = resolve_region(args.region)
    cookie_directory = _cookie_directory(args)

    if args.if_expiring_within is not None:
        snapshot = read_session_snapshot(email, cookie_directory)
        verdict = evaluate_expiry(snapshot, warn_days=args.if_expiring_within)
        if verdict.status == STATUS_OK:
            _emit(
                {"account": email, "renewed": False, **verdict.to_dict()},
                f"{email}: {verdict.detail} Nothing to do.",
                args.as_json,
                stdout,
            )
            return EXIT_OK

    if args.reset:
        _clear_session(email, cookie_directory, stdout)

    resolver = _build_resolver(args)
    factory = service_factory or _default_factory

    try:
        service = factory(
            apple_id=email,
            password=resolve_password(getattr(args, "password_command", None)),
            **region_service_kwargs(region),
        )
        _complete_two_factor(service, resolver, args, stdout)
    except TwoFactorUnavailable as exc:
        _emit(
            {"account": email, "renewed": False, "error": str(exc), "code": "no_2fa_code"},
            f"FAIL: {exc}",
            args.as_json,
            stdout,
        )
        return EXIT_FAIL
    except Exception as exc:
        failure = classify_auth_error(exc)
        # An unrecognised failure must show what actually went wrong. Replacing
        # it with the generic "not recognised" blurb would discard the only
        # information the user has - including our own specific messages, which
        # are deliberately not Apple error strings and so never classify.
        summary = failure.summary if failure.code != "unknown" else str(exc)
        _emit(
            {"account": email, "renewed": False, **failure.to_dict()},
            f"FAIL: {summary}\n  -> {failure.remedy}",
            args.as_json,
            stdout,
        )
        return EXIT_FAIL

    snapshot = read_session_snapshot(email, cookie_directory)
    verdict = evaluate_expiry(snapshot)
    _emit(
        {"account": email, "region": region, "renewed": True, **verdict.to_dict()},
        f"OK: session renewed for {email}. {verdict.detail}",
        args.as_json,
        stdout,
    )
    return EXIT_OK


def _complete_two_factor(
    service: Any,
    resolver: TwoFactorResolver,
    args: argparse.Namespace,
    stdout: Any,
) -> None:
    """Answer Apple's 2FA challenge without requiring a human, then trust.

    Trusting the session is what buys the next ~30 days; a renewal that
    validates a code but fails to trust would need a fresh code on every run, so
    a failure to trust is reported rather than swallowed.
    """
    if not getattr(service, "requires_2fa", False):
        if not getattr(service, "is_trusted_session", True):
            if not service.trust_session():
                print("Warning: session authenticated but could not be trusted.", file=stdout)
        return

    print("Apple requires a two-factor code.", file=stdout)
    sources = resolver.describe_sources()
    print(f"Looking for one from: {', '.join(sources) or 'no configured source'}", file=stdout)

    code = resolver.resolve()
    print("Submitting code...", file=stdout)

    if not service.validate_2fa_code(code):
        raise RuntimeError(
            "Apple rejected the two-factor code. If Apple reported it as valid "
            "and still refused (HTTP 409), request a fresh code and retry."
        )

    if not getattr(service, "is_trusted_session", False):
        if not service.trust_session():
            raise RuntimeError(
                "The code was accepted but the session could not be trusted, so "
                "the next run would need another code."
            )


def _default_factory(**kwargs: Any) -> Any:
    from pyicloud import PyiCloudService

    return PyiCloudService(**kwargs)


# ---------------------------------------------------------------------------
# entry point
# ---------------------------------------------------------------------------

_COMMANDS: Dict[str, Callable[..., int]] = {
    "doctor": cmd_doctor,
    "status": cmd_status,
    "renew": cmd_renew,
}


def main(argv: Optional[Sequence[str]] = None, stdout: Any = None) -> int:
    stream = stdout if stdout is not None else sys.stdout
    parser = build_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)

    handler = _COMMANDS[args.command]
    try:
        return handler(args, stream)
    except SystemExit as exc:
        message = str(exc)
        if message and not message.isdigit():
            print(message, file=sys.stderr)
            return EXIT_USAGE
        raise
    except ValueError as exc:  # e.g. an unknown --region from the environment
        print(f"Error: {exc}", file=sys.stderr)
        return EXIT_USAGE
    except KeyboardInterrupt:
        print("Cancelled.", file=sys.stderr)
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
