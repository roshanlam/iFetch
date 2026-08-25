"""Authentication reliability: diagnosis, non-interactive 2FA, proactive renewal.

Motivation
----------
The single largest source of pain reported against iCloud CLI tooling is not
missing features, it is authentication that fails opaquely and cannot be
completed without a human at a terminal.  Three distinct problems show up over
and over:

1. **Opaque failures.**  Apple answers with ``HTTP 423 Missing PCS cookies``,
   ``HTTP 400 Invalid Session Token`` or ``HTTP 409`` on a 2FA code that its own
   response body marks ``"valid": true``.  None of those strings tell a user
   what to change.  :class:`AuthDoctor` turns each of them into a named cause
   with a concrete remedy.

2. **No way to answer 2FA without a TTY.**  Docker, cron, NAS boxes and systemd
   units have no interactive stdin.  :class:`TwoFactorResolver` supplies a code
   from an explicit argument, an environment variable, stdin, a watched file, or
   an HTTP callback, so the same flow works headless.

3. **Silent expiry.**  Apple's trust token lasts ~30 days.  Tools normally
   discover this when a scheduled backup fails.  :func:`evaluate_expiry` reads
   the real cookie expiry off disk so a run can warn *days before* the token
   dies, and exit non-zero while there is still time to act.

4. **Advanced Data Protection.**  With ADP on, Apple encrypts iCloud Drive
   end-to-end and refuses every web-API request that does not carry a *PCS*
   (Per-Service Encryption) cookie.  Obtaining one is a separate gate from 2FA:
   it needs "Access iCloud Data on the Web" enabled and an approval tapped on a
   trusted device.  :func:`ensure_pcs_cookies` performs that request/approve/
   poll exchange with a bounded, injectable clock, and every way it can fail is
   turned into a named cause by :func:`classify_auth_error`.

Everything here is deliberately usable without a network connection: the
diagnosis of local session state, the expiry arithmetic and the error
classification are pure functions over on-disk data and exception text, which is
also what makes them testable.

What is *verified*, and what is not
-----------------------------------
The ADP code below has **never been run against a live ADP-enabled Apple ID**,
because provisioning one for CI is not possible.  It is validated in
``tests/test_adp.py`` against *replayed* responses - the payload shapes taken
from Apple's own web client and from two independent working implementations
(pyicloud's ``_request_pcs_for_service`` and rclone's ``acquirePCSCookiesFor``).
Those tests prove that iFetch takes the right branch on the payloads we believe
Apple sends, and nothing more.  Anywhere the observed field names are not a
documented API this module reports "could not determine" rather than guessing;
that is deliberate, and it is the only honest position available.
"""

from __future__ import annotations

import json
import os
import re
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from http.cookiejar import LWPCookieJar
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

# ---------------------------------------------------------------------------
# Regions
# ---------------------------------------------------------------------------

REGION_GLOBAL = "global"
REGION_CHINA = "china"
REGIONS = (REGION_GLOBAL, REGION_CHINA)

#: Apple's trust token lifetime.  Apple does not publish this as an API value;
#: 30 days is the documented and universally observed figure.  It is only used
#: as a *fallback* when the real cookie expiry cannot be read off disk, and any
#: value derived from it is reported as an estimate.
TRUST_TOKEN_LIFETIME_DAYS = 30

#: Default number of days before expiry at which we start warning.
DEFAULT_WARN_DAYS = 7

#: The cookie whose expiry actually governs how long the session keeps working.
WEBAUTH_COOKIE = "X-APPLE-WEBAUTH-TOKEN"


def resolve_region(
    explicit: Optional[str] = None,
    env: Optional[Dict[str, str]] = None,
) -> str:
    """Resolve the iCloud region from an explicit value then the environment.

    Precedence: explicit argument > ``ICLOUD_REGION`` > legacy ``ICLOUD_CHINA``
    > ``global``.  ``ICLOUD_CHINA=true`` predates the ``--region`` flag and is
    still honoured so existing deployments do not break.
    """
    environ = os.environ if env is None else env

    if explicit:
        value = explicit.strip().lower()
        if value not in REGIONS:
            raise ValueError(
                f"unknown region {explicit!r}; expected one of {', '.join(REGIONS)}"
            )
        return value

    named = (environ.get("ICLOUD_REGION") or "").strip().lower()
    if named:
        if named not in REGIONS:
            raise ValueError(
                f"unknown ICLOUD_REGION {named!r}; expected one of {', '.join(REGIONS)}"
            )
        return named

    if (environ.get("ICLOUD_CHINA") or "").strip().lower() == "true":
        return REGION_CHINA

    return REGION_GLOBAL


def region_service_kwargs(region: str) -> Dict[str, Any]:
    """Translate a region name into ``PyiCloudService`` keyword arguments.

    pyicloud switches every endpoint (``idmsa``, ``www``, ``setup``) to its
    ``.cn`` counterpart from this single flag, which is why supporting China
    Mainland costs one keyword rather than an endpoint table.
    """
    if region == REGION_CHINA:
        return {"china_mainland": True}
    return {}


# ---------------------------------------------------------------------------
# Password sourcing
# ---------------------------------------------------------------------------

class PasswordCommandError(Exception):
    """The configured password command could not produce a password."""


def resolve_password(
    command: Optional[str] = None,
    env_var: str = "IFETCH_PASSWORD_COMMAND",
    env: Optional[Dict[str, str]] = None,
    timeout: float = 60.0,
    runner: Optional[Callable[[List[str]], str]] = None,
) -> Optional[str]:
    """Obtain the Apple ID password by running a command, or return ``None``.

    ``None`` means "not configured", and the caller should fall back to
    pyicloud's system keyring - which stays the recommended path on a desktop.
    But a keyring is exactly what a Docker container, a systemd unit or a
    headless NAS does not have, which otherwise leaves the password as the one
    part of authentication that still needs a human. This closes that gap by
    letting the password come from `pass`, `1password-cli`, `age`, a mounted
    secret, or anything else that prints it.

    The command is split with :mod:`shlex` and executed **without a shell**.
    That is deliberate on two counts: it avoids handing an injection vector to
    anything that can influence the string, and it means a quoted path
    containing spaces works correctly - the failure mode reported against other
    tools that split on whitespace.

    Only the first line of stdout is used, so a helper that prints diagnostics
    afterwards still works. The password is never logged or included in an
    exception message.
    """
    import shlex
    import subprocess

    environ = os.environ if env is None else env
    spec = command or environ.get(env_var)
    if not spec or not spec.strip():
        return None

    try:
        argv = shlex.split(spec)
    except ValueError as exc:
        raise PasswordCommandError(
            f"could not parse --password-command ({exc}). Quote paths that "
            'contain spaces, e.g. --password-command \'"/opt/my dir/get.sh" arg\''
        ) from exc

    if not argv:
        raise PasswordCommandError("--password-command is empty")

    if runner is not None:
        output = runner(argv)
    else:
        try:
            completed = subprocess.run(
                argv,
                capture_output=True,
                timeout=timeout,
                check=False,
                text=True,
            )
        except FileNotFoundError as exc:
            raise PasswordCommandError(
                f"password command not found: {argv[0]!r}"
            ) from exc
        except subprocess.TimeoutExpired as exc:
            raise PasswordCommandError(
                f"password command timed out after {timeout:.0f}s. A command that "
                "prompts for input cannot work here; it must print the password "
                "and exit."
            ) from exc
        except OSError as exc:
            raise PasswordCommandError(f"could not run password command: {exc}") from exc

        if completed.returncode != 0:
            # stderr may carry a useful reason ("locked", "not found"); stdout
            # may carry the secret, so it is never echoed.
            detail = (completed.stderr or "").strip().splitlines()
            hint = f": {detail[0]}" if detail else ""
            raise PasswordCommandError(
                f"password command exited {completed.returncode}{hint}"
            )
        output = completed.stdout

    password = (output or "").splitlines()[0].strip() if (output or "").strip() else ""
    if not password:
        raise PasswordCommandError(
            "password command produced no output. It must print the password on "
            "stdout."
        )
    return password


# ---------------------------------------------------------------------------
# Per-service encryption (PCS) vocabulary and secret redaction
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class PCSService:
    """One iCloud service and the PCS cookies Apple issues for it.

    ``app_name`` is the value Apple's ``requestPCS`` endpoint expects; it is not
    the same string as the web-service key, and getting it wrong yields a
    perfectly successful-looking response that sets no cookie at all.  Both
    values are taken from Apple's own web client and match the two independent
    implementations we cross-checked (pyicloud and rclone).
    """

    key: str
    app_name: str
    webservice: str
    cookies: Tuple[str, ...]


#: The services iFetch can obtain PCS cookies for.  Photos is listed because the
#: cookie set differs (Apple issues two, and a client that checks only one
#: reports success while sharing still fails); ``ifetch-photos`` can use it.
PCS_SERVICES: Dict[str, PCSService] = {
    "drive": PCSService(
        key="drive",
        app_name="iclouddrive",
        webservice="drivews",
        cookies=("X-APPLE-WEBAUTH-PCS-Documents",),
    ),
    "photos": PCSService(
        key="photos",
        app_name="photos",
        webservice="ckdatabasews",
        cookies=("X-APPLE-WEBAUTH-PCS-Photos", "X-APPLE-WEBAUTH-PCS-Sharing"),
    ),
}

DEFAULT_PCS_SERVICE = "drive"

#: Every cookie name that is a credential in its own right.
PCS_COOKIE_NAMES = frozenset(
    name for service in PCS_SERVICES.values() for name in service.cookies
)

REDACTED = "<redacted>"

#: ``NAME=value`` for any Apple web-auth cookie.  Matching on the *name* rather
#: than on a remembered value is what makes redaction work for a secret we have
#: never seen - a value that arrives in an error body we merely pass through.
_COOKIE_ASSIGNMENT = re.compile(
    r"(X-APPLE-WEBAUTH-(?:PCS-[A-Za-z]+|TOKEN|VALIDATE))\s*=\s*[^;,\s\"'\\]+",
    re.IGNORECASE,
)


def pcs_service(name: Optional[str] = None) -> PCSService:
    """Look up a :class:`PCSService` by key, rejecting unknown names loudly."""
    key = (name or DEFAULT_PCS_SERVICE).strip().lower()
    if key not in PCS_SERVICES:
        raise ValueError(
            f"unknown iCloud service {name!r}; expected one of "
            f"{', '.join(sorted(PCS_SERVICES))}"
        )
    return PCS_SERVICES[key]


def redact_secrets(text: Any, values: Sequence[str] = ()) -> str:
    """Strip cookie values out of arbitrary text.

    A PCS cookie is a bearer credential for end-to-end-encrypted data: anything
    holding one can read the account's files.  It must therefore never reach a
    log file, a ``--json`` report, or an exception message that a user will
    paste into a bug tracker.  Rather than trusting every call site to remember
    that, every string that leaves this module through an error or a summary is
    passed through here first.

    ``values`` lets a caller name secrets whose *format* is not recognisable -
    the raw cookie values it happens to hold - so they are removed even when
    they appear without their cookie name attached.
    """
    result = str(text or "")
    for value in values:
        if value and len(str(value)) >= 8:
            # A short "value" is far more likely to be a status word than a
            # secret; blanking it would corrupt the message it appears in.
            result = result.replace(str(value), REDACTED)
    return _COOKIE_ASSIGNMENT.sub(lambda m: f"{m.group(1)}={REDACTED}", result)


# ---------------------------------------------------------------------------
# Failure classification
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class AuthFailure:
    """A named, actionable interpretation of an authentication error."""

    code: str
    summary: str
    remedy: str
    raw: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "code": self.code,
            "summary": self.summary,
            "remedy": self.remedy,
            "raw": self.raw,
        }


_UNKNOWN_FAILURE = AuthFailure(
    code="unknown",
    summary="Authentication failed for a reason iFetch does not recognise.",
    remedy=(
        "Re-run with --verbose and open an issue at "
        "https://github.com/roshanlam/iFetch/issues including the full error."
    ),
)


def classify_auth_error(error: Any) -> AuthFailure:
    """Map an exception (or error string) onto a named cause and a remedy.

    This is the function that replaces ``HTTP 423 Missing PCS cookies from the
    request`` with a sentence a person can act on.  Matching is done on lowered
    substrings because the underlying libraries surface these conditions as
    unstructured text in several different shapes.

    An exception that already carries a classification (:class:`ADPError`) is
    passed through untouched: re-deriving a cause from our own prose would be a
    strictly worse answer than the one we already computed.
    """
    carried = getattr(error, "failure", None)
    if isinstance(carried, AuthFailure):
        return carried

    text = redact_secrets(str(error or ""))
    low = text.lower()

    # Ordered most-specific first: several of these conditions can co-occur in
    # one message and the first match must be the one that is actionable.  The
    # three ADP shapes come before the generic PCS-cookie rule because each of
    # them contains the substrings that rule matches on.
    if WEBAUTH_COOKIE.lower() in low:
        return AuthFailure(
            code="adp_webauth_token_missing",
            summary=(
                f"The session has no {WEBAUTH_COOKIE} cookie, so Apple treats it "
                "as signed out. Nothing was encrypted or refused - the request "
                "never carried a usable web session in the first place."
            ),
            remedy=(
                "Run 'ifetch auth renew --reset' to sign in again and rebuild the "
                "cookie jar. If this recurs on every run, the stored cookie jar is "
                "not being written: check that the --cookie-directory is writable "
                "and is not a container layer that is discarded on exit."
            ),
            raw=text,
        )

    if "requestpcs" in low or "unable to request pcs access" in low:
        return AuthFailure(
            code="adp_request_pcs_failed",
            summary=(
                "Apple's requestPCS exchange did not complete, so no per-service "
                "encryption cookie was issued for this account. This is the "
                "Advanced Data Protection approval step, not two-factor sign-in."
            ),
            remedy=(
                "Approve the 'iCloud Data on the Web' prompt on a trusted Apple "
                "device (it appears within a few seconds of the request), then "
                "re-run 'ifetch auth renew --adp'. If no prompt ever appears, "
                "turn Settings > [your name] > iCloud > 'Access iCloud Data on "
                "the Web' off and on again to re-arm it."
            ),
            raw=text,
        )

    if "access icloud data on the web" in low and (
        "off" in low or "disabled" in low or "not enabled" in low
    ):
        return AuthFailure(
            code="adp_web_access_disabled",
            summary=(
                "Apple reports that 'Access iCloud Data on the Web' is turned off "
                "for this account. With Advanced Data Protection enabled that "
                "setting is what authorises any web/API client at all, so no "
                "amount of re-authentication will help until it is on."
            ),
            remedy=(
                "On a trusted Apple device: Settings > [your name] > iCloud > "
                "'Access iCloud Data on the Web' -> ON (macOS: System Settings > "
                "[your name] > iCloud > Access iCloud Data on the Web). Then run "
                "'ifetch auth renew --adp'. The change can take a few minutes to "
                "propagate."
            ),
            raw=text,
        )

    if "pcs" in low and "cookie" in low or "423" in low and "locked" in low:
        return AuthFailure(
            code="adp_pcs_cookies",
            summary=(
                "Apple refused the request for lack of PCS cookies. This account "
                "has Advanced Data Protection enabled, or 'Access iCloud Data on "
                "the Web' is turned off."
            ),
            remedy=(
                "On an Apple device: Settings > [your name] > iCloud > "
                "'Access iCloud Data on the Web' must be ON. If Advanced Data "
                "Protection is on, run 'ifetch auth renew --adp' and approve the "
                "prompt that appears on a trusted device. Changes can take a few "
                "minutes to propagate."
            ),
            raw=text,
        )

    if "invalid session token" in low:
        return AuthFailure(
            code="invalid_session_token",
            summary=(
                "Apple rejected the stored session token. It has expired or was "
                "invalidated (password change, remote sign-out, or a token older "
                f"than {TRUST_TOKEN_LIFETIME_DAYS} days)."
            ),
            remedy=(
                "Run 'ifetch auth renew --reset' to discard the stored session "
                "and sign in again. If you are in China Mainland, also pass "
                "--region china."
            ),
            raw=text,
        )

    if "domaintouse" in low or "icloud.com.cn" in low:
        return AuthFailure(
            code="wrong_region",
            summary=(
                "Apple redirected to the China Mainland service (iCloud.com.cn); "
                "this account is not served by the global endpoints."
            ),
            remedy="Re-run with --region china (or set ICLOUD_REGION=china).",
            raw=text,
        )

    if "409" in low and ("edp" in low or "valid" in low):
        return AuthFailure(
            code="code_accepted_but_rejected",
            summary=(
                "Apple confirmed the 2FA code is valid but still returned 409. "
                "This affects accounts with a recovery key or extended device "
                "protection."
            ),
            remedy=(
                "Request a fresh code and submit it within its validity window. "
                "If it keeps failing, generate an offline code on a trusted "
                "device (enable Airplane Mode, then Settings > [your name] > "
                "Sign-In & Security > Two-Factor Authentication > Get "
                "Verification Code)."
            ),
            raw=text,
        )

    if "app-specific" in low or "app specific password" in low:
        return AuthFailure(
            code="app_specific_password",
            summary="App-specific passwords are not accepted by the iCloud web API.",
            remedy="Use your regular Apple ID password plus 2FA.",
            raw=text,
        )

    if "no stored password" in low or "nostoredpassword" in low:
        return AuthFailure(
            code="no_stored_password",
            summary="No password is stored in the system keyring for this Apple ID.",
            remedy=(
                "Run 'icloud auth login --username you@example.com' "
                '(requires: pip install "ifetch[auth]") to store it.'
            ),
            raw=text,
        )

    if "invalid credentials" in low or "failedlogin" in low or "401" in low:
        return AuthFailure(
            code="bad_credentials",
            summary="Apple rejected the Apple ID or password.",
            remedy=(
                "Confirm the Apple ID, and re-store the password with "
                "'icloud auth login --username you@example.com'."
            ),
            raw=text,
        )

    if "too many" in low or "429" in low or "throttl" in low:
        return AuthFailure(
            code="rate_limited",
            summary="Apple is throttling authentication attempts for this account.",
            remedy="Wait at least an hour before retrying; repeated attempts extend the lockout.",
            raw=text,
        )

    return AuthFailure(
        code=_UNKNOWN_FAILURE.code,
        summary=_UNKNOWN_FAILURE.summary,
        remedy=_UNKNOWN_FAILURE.remedy,
        raw=text,
    )


# ---------------------------------------------------------------------------
# On-disk session inspection
# ---------------------------------------------------------------------------

def default_cookie_directory() -> Path:
    """Mirror pyicloud's default session directory without importing it.

    pyicloud stores session state in ``<tmp>/pyicloud/<username>``.  Reproducing
    that here keeps ``ifetch auth doctor`` usable even when pyicloud cannot be
    imported, which is precisely the situation a diagnostic tool must survive.
    """
    import getpass
    import tempfile

    return Path(tempfile.gettempdir()) / "pyicloud" / getpass.getuser()


def session_slug(account_name: str) -> str:
    """pyicloud's filename slug for an account: word characters only."""
    return "".join(ch for ch in account_name if re.match(r"\w", ch))


@dataclass
class SessionSnapshot:
    """What is knowable about a stored session from disk alone, with no network."""

    account: str
    cookie_directory: Path
    session_path: Path
    cookiejar_path: Path
    exists: bool = False
    has_session_token: bool = False
    has_trust_token: bool = False
    #: Absolute expiry of the web-auth cookie, epoch seconds, when readable.
    webauth_expires_at: Optional[float] = None
    #: When the session file was last written; the best available proxy for
    #: "when was this token issued" if the cookie expiry is unreadable.
    session_written_at: Optional[float] = None
    #: Expiry of each stored PCS cookie, by name.  A name maps to ``None`` when
    #: the cookie is present but carries no expiry, which is not the same thing
    #: as being absent and must not be collapsed into it.
    pcs_expiries: Dict[str, Optional[float]] = field(default_factory=dict)
    read_errors: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "account": self.account,
            "cookie_directory": str(self.cookie_directory),
            "session_path": str(self.session_path),
            "cookiejar_path": str(self.cookiejar_path),
            "exists": self.exists,
            "has_session_token": self.has_session_token,
            "has_trust_token": self.has_trust_token,
            "webauth_expires_at": self.webauth_expires_at,
            "session_written_at": self.session_written_at,
            # Names and expiries only.  The values are credentials and are never
            # read off disk by this module, let alone serialised.
            "pcs_cookies": dict(self.pcs_expiries),
            "read_errors": list(self.read_errors),
        }


def read_session_snapshot(
    account: str,
    cookie_directory: Optional[Path] = None,
) -> SessionSnapshot:
    """Inspect the stored pyicloud session for ``account`` without authenticating.

    Every failure mode is captured rather than raised: a diagnostic that dies on
    a corrupt cookie jar is useless exactly when it is needed.
    """
    directory = Path(cookie_directory) if cookie_directory else default_cookie_directory()
    slug = session_slug(account)
    snapshot = SessionSnapshot(
        account=account,
        cookie_directory=directory,
        session_path=directory / f"{slug}.session",
        cookiejar_path=directory / f"{slug}.cookiejar",
    )

    if snapshot.session_path.exists():
        snapshot.exists = True
        try:
            snapshot.session_written_at = snapshot.session_path.stat().st_mtime
        except OSError as exc:
            snapshot.read_errors.append(f"stat session file: {exc}")
        try:
            payload = json.loads(snapshot.session_path.read_text(encoding="utf-8"))
        except (OSError, ValueError, UnicodeDecodeError) as exc:
            snapshot.read_errors.append(f"parse session file: {exc}")
            payload = {}
        if isinstance(payload, dict):
            snapshot.has_session_token = bool(payload.get("session_token"))
            snapshot.has_trust_token = bool(payload.get("trust_token"))

    if snapshot.cookiejar_path.exists():
        snapshot.exists = True
        expiries = _read_cookie_expiries(snapshot.cookiejar_path, snapshot.read_errors)
        snapshot.webauth_expires_at = expiries.get(WEBAUTH_COOKIE)
        snapshot.pcs_expiries = {
            name: value for name, value in expiries.items() if name in PCS_COOKIE_NAMES
        }

    return snapshot


def _read_cookie_expiries(
    path: Path, errors: List[str]
) -> Dict[str, Optional[float]]:
    """Read the expiry of every cookie iFetch cares about from an LWP jar.

    ``ignore_expires=True`` is essential: an already-expired cookie must still
    be loaded, because reporting *how long ago* a session died is the whole
    point of the diagnostic.

    A cookie present but empty is reported as an error *and* omitted from the
    result.  That combination is deliberate: a truncated write leaves exactly
    that shape, and treating it as "present" would make the next run skip
    re-acquisition and fail against Apple instead.
    """
    interesting = {WEBAUTH_COOKIE} | set(PCS_COOKIE_NAMES)
    jar = LWPCookieJar(str(path))
    try:
        jar.load(ignore_discard=True, ignore_expires=True)
    except (OSError, ValueError) as exc:
        errors.append(f"load cookie jar: {exc}")
        return {}

    found: Dict[str, Optional[float]] = {}
    for cookie in jar:
        if cookie.name not in interesting:
            continue
        if not cookie.value:
            errors.append(
                f"stored {cookie.name} cookie has no value; treating it as absent"
            )
            continue
        expires = float(cookie.expires) if cookie.expires else None
        if cookie.name in found:
            previous = found[cookie.name]
            # Several cookies can share a name across domains; the session
            # survives as long as the longest-lived one.  A cookie with no
            # expiry outlives every dated one, so it wins.
            if previous is None or expires is None:
                found[cookie.name] = None
            else:
                found[cookie.name] = max(previous, expires)
        else:
            found[cookie.name] = expires
    return found


# ---------------------------------------------------------------------------
# Expiry evaluation
# ---------------------------------------------------------------------------

STATUS_OK = "ok"
STATUS_WARN = "warn"
STATUS_EXPIRED = "expired"
STATUS_UNKNOWN = "unknown"


@dataclass(frozen=True)
class ExpiryVerdict:
    """How much life is left in the stored session."""

    status: str
    days_remaining: Optional[float]
    expires_at: Optional[float]
    estimated: bool
    detail: str

    @property
    def needs_attention(self) -> bool:
        return self.status in (STATUS_WARN, STATUS_EXPIRED)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "status": self.status,
            "days_remaining": (
                None if self.days_remaining is None else round(self.days_remaining, 2)
            ),
            "expires_at": (
                None
                if self.expires_at is None
                else datetime.fromtimestamp(self.expires_at, timezone.utc).isoformat()
            ),
            "estimated": self.estimated,
            "detail": self.detail,
        }


def evaluate_expiry(
    snapshot: SessionSnapshot,
    warn_days: int = DEFAULT_WARN_DAYS,
    now: Optional[float] = None,
) -> ExpiryVerdict:
    """Turn a snapshot into an ok / warn / expired verdict.

    The authoritative signal is the web-auth cookie's own expiry.  When that is
    unreadable we fall back to "session file mtime + 30 days" and mark the
    verdict ``estimated`` so a caller never presents a guess as a fact.
    """
    current = time.time() if now is None else now

    expires_at = snapshot.webauth_expires_at
    estimated = False

    if expires_at is None and snapshot.session_written_at is not None:
        expires_at = snapshot.session_written_at + TRUST_TOKEN_LIFETIME_DAYS * 86400
        estimated = True

    if expires_at is None:
        return ExpiryVerdict(
            status=STATUS_UNKNOWN,
            days_remaining=None,
            expires_at=None,
            estimated=False,
            detail=(
                "No stored session found for this account, so no expiry is known. "
                "Sign in once to create one."
                if not snapshot.exists
                else "A session exists but neither a cookie expiry nor a file "
                "timestamp could be read from it."
            ),
        )

    days_remaining = (expires_at - current) / 86400.0
    source = "estimated from the session file timestamp" if estimated else "read from the stored session cookie"

    if days_remaining <= 0:
        return ExpiryVerdict(
            status=STATUS_EXPIRED,
            days_remaining=days_remaining,
            expires_at=expires_at,
            estimated=estimated,
            detail=(
                f"Session expired {abs(days_remaining):.1f} days ago ({source}). "
                "Run 'ifetch auth renew' to sign in again."
            ),
        )

    if days_remaining <= warn_days:
        return ExpiryVerdict(
            status=STATUS_WARN,
            days_remaining=days_remaining,
            expires_at=expires_at,
            estimated=estimated,
            detail=(
                f"Session expires in {days_remaining:.1f} days ({source}). "
                "Renew it now so the next scheduled run does not fail."
            ),
        )

    return ExpiryVerdict(
        status=STATUS_OK,
        days_remaining=days_remaining,
        expires_at=expires_at,
        estimated=estimated,
        detail=f"Session valid for another {days_remaining:.1f} days ({source}).",
    )


# ---------------------------------------------------------------------------
# Non-interactive two-factor authentication
# ---------------------------------------------------------------------------

class TwoFactorUnavailable(Exception):
    """No 2FA code could be obtained from any configured source."""


_CODE_PATTERN = re.compile(r"\b(\d{6})\b")


def extract_code(raw: Any) -> Optional[str]:
    """Pull a six-digit code out of arbitrary text, or return ``None``.

    Sources are messy: a watched file may hold a whole SMS, a webhook may return
    JSON.  Rather than demand exact formatting from every integration, we accept
    anything that contains exactly one six-digit run.  Ambiguity (two different
    candidate codes) is rejected instead of guessed, because submitting the
    wrong code to Apple consumes a rate-limited attempt.
    """
    if raw is None:
        return None
    text = raw.decode("utf-8", "replace") if isinstance(raw, (bytes, bytearray)) else str(raw)

    stripped = text.strip()
    if stripped.isdigit() and len(stripped) == 6:
        return stripped

    candidates = set(_CODE_PATTERN.findall(text))
    if len(candidates) == 1:
        return candidates.pop()
    return None


@dataclass
class TwoFactorResolver:
    """Obtain a 2FA code without requiring an interactive terminal.

    Sources are tried in the order below; the first that yields a well-formed
    six-digit code wins:

    ``code``
        A literal value, from ``--2fa-code``.
    ``env_var``
        An environment variable, default ``IFETCH_2FA_CODE``.
    ``file``
        A path polled until it appears and contains a code.  This is the shape
        that suits a NAS or a phone-shortcut dropping a file into a share.
    ``webhook``
        A URL polled with GET; the body may be the bare code or JSON containing
        one.  Suits a service that receives the code out of band.
    ``stdin``
        A single line read from stdin, used only when stdin is not a TTY (a
        piped code) or when explicitly enabled.

    Interactive prompting is *not* handled here; the caller decides whether
    falling back to a human is acceptable, which keeps this class safe to use
    from a daemon.
    """

    code: Optional[str] = None
    env_var: str = "IFETCH_2FA_CODE"
    file: Optional[Path] = None
    webhook: Optional[str] = None
    allow_stdin: bool = True
    timeout: float = 300.0
    poll_interval: float = 2.0
    #: Injected for testing; defaults to the real clock and a real HTTP GET.
    now: Callable[[], float] = time.time
    sleep: Callable[[float], None] = time.sleep
    http_get: Optional[Callable[[str], Optional[str]]] = None
    stdin: Any = None
    env: Optional[Dict[str, str]] = None

    def describe_sources(self) -> List[str]:
        """Human-readable list of configured sources, for error messages."""
        sources = []
        if self.code:
            sources.append("--2fa-code")
        if self._env().get(self.env_var):
            sources.append(f"${self.env_var}")
        if self.file:
            sources.append(f"file {self.file}")
        if self.webhook:
            sources.append(f"webhook {self.webhook}")
        if self.allow_stdin:
            sources.append("stdin")
        return sources

    def _env(self) -> Dict[str, str]:
        return os.environ if self.env is None else self.env

    def resolve(self) -> str:
        """Return a six-digit code or raise :class:`TwoFactorUnavailable`.

        Immediate sources are consulted once; the polling sources (file and
        webhook) are retried until ``timeout`` so a code that arrives seconds
        after the run starts is still picked up.
        """
        immediate = self._resolve_immediate()
        if immediate:
            return immediate

        if not (self.file or self.webhook):
            raise TwoFactorUnavailable(
                "No two-factor code available. Provide one with --2fa-code, "
                f"${self.env_var}, --2fa-file, --2fa-webhook, or pipe it on stdin."
            )

        deadline = self.now() + self.timeout
        # A do/while shape: poll at least once even with a zero timeout, so a
        # file that already exists is never missed by an unlucky clock.
        while True:
            polled = self._poll_once()
            if polled:
                return polled
            if self.now() >= deadline:
                break
            self.sleep(self.poll_interval)

        raise TwoFactorUnavailable(
            f"Timed out after {self.timeout:.0f}s waiting for a two-factor code from "
            f"{', '.join(self.describe_sources()) or 'no configured source'}."
        )

    def _resolve_immediate(self) -> Optional[str]:
        for candidate in (self.code, self._env().get(self.env_var)):
            found = extract_code(candidate)
            if found:
                return found
        return self._read_stdin()

    def _read_stdin(self) -> Optional[str]:
        if not self.allow_stdin:
            return None
        stream = self.stdin if self.stdin is not None else sys.stdin
        if stream is None:
            return None
        # Reading a TTY here would block a daemon forever; only consume stdin
        # when it is a pipe or an injected test double.
        try:
            if hasattr(stream, "isatty") and stream.isatty():
                return None
        except (ValueError, OSError):
            return None
        try:
            line = stream.readline()
        except (OSError, ValueError):
            return None
        return extract_code(line)

    def _poll_once(self) -> Optional[str]:
        if self.file:
            try:
                path = Path(self.file)
                if path.exists():
                    found = extract_code(path.read_text(encoding="utf-8", errors="replace"))
                    if found:
                        return found
            except OSError:
                pass  # Unreadable right now (partial write); try again next tick.

        if self.webhook:
            try:
                body = self._get(self.webhook)
                found = extract_code(body)
                if found:
                    return found
            except Exception:
                pass  # Transient webhook failures must not abort the wait.

        return None

    def _get(self, url: str) -> Optional[str]:
        if self.http_get is not None:
            return self.http_get(url)
        import requests  # Imported lazily: unused unless a webhook is configured.

        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return response.text


# ---------------------------------------------------------------------------
# Advanced Data Protection: status, cookie state, acquisition
# ---------------------------------------------------------------------------
#
# Division of labour with pyicloud
# --------------------------------
# pyicloud already owns: SRP sign-in, 2FA, the trust token, the requests
# session, and the cookie jar on disk - *including* the PCS cookies, which Apple
# sets with ordinary ``Set-Cookie`` headers on the shared session.  It also
# performs its own ``requestPCS`` attempt whenever ``api.drive`` is touched.
#
# What it does not do is anything a person can act on: its loop sleeps on the
# real clock for a fixed 5s x 10, it cannot be told to wait longer for someone
# to walk to their phone, and every failure exits as
# ``PyiCloudAPIResponseException("Unable to request PCS access!")``, which is
# the opaque-error problem this module exists to fix.
#
# So iFetch drives the exchange itself - through pyicloud's authenticated
# session, so the cookies land in pyicloud's jar and are persisted by pyicloud's
# existing mechanism - and adds: a bounded wait with backoff and an injectable
# clock, a service-scoped cookie check, three distinct named diagnoses, and
# redaction.  No second credential store is introduced anywhere.

#: How long, by default, to keep waiting for someone to approve the prompt on a
#: trusted device.  Five minutes matches rclone; it is long enough to fetch a
#: phone from another room and short enough that a cron job does not wedge.
DEFAULT_PCS_TIMEOUT = 300.0

#: A hard cap on requests, independent of the clock.  Both bounds are enforced:
#: an injected clock that never advances must still terminate the loop.
DEFAULT_PCS_MAX_ATTEMPTS = 30

#: Polling starts fast (approval is often instant) and backs off, so a five
#: minute wait costs a handful of requests rather than one every two seconds.
PCS_INITIAL_INTERVAL = 2.0
PCS_MAX_INTERVAL = 15.0
PCS_BACKOFF = 1.5

#: Apple's setup endpoints, relative to the account's setup web service.
PCS_WEB_ACCESS_STATE_PATH = "requestWebAccessState"
PCS_ENABLE_CONSENT_PATH = "enableDeviceConsentForPCS"
PCS_REQUEST_PATH = "requestPCS"

#: ``requestPCS`` answers with one of these while the trusted device has not
#: finished uploading the keys.  They mean "not ready", not "failed".
PCS_PENDING_MESSAGES = (
    "requested the device to upload cookies",
    "cookies not available yet on server",
    "waiting for device",
)

#: Field names observed in Apple's ``requestWebAccessState`` payload.  This is
#: not a documented API, so several spellings are accepted and a payload that
#: carries none of them yields "undetermined" rather than a guess.
_WEB_ACCESS_KEYS = ("isWebAccessAllowed", "isWebAccessEnabled", "webAccessEnabled")

ADP_ON = "on"
ADP_OFF = "off"
ADP_UNDETERMINED = "undetermined"

WEB_ACCESS_ENABLED = "enabled"
WEB_ACCESS_DISABLED = "disabled"
WEB_ACCESS_UNDETERMINED = "undetermined"

CONSENT_GRANTED = "granted"
CONSENT_PENDING = "pending"
CONSENT_UNDETERMINED = "undetermined"

PCS_ACQUIRED = "acquired"
PCS_ALREADY_PRESENT = "already_present"
PCS_NOT_REQUIRED = "not_required"
PCS_UNDETERMINED = "undetermined"


class ADPError(Exception):
    """A named, redacted ADP failure.

    Carries the :class:`AuthFailure` that explains it, so callers report the
    cause and the remedy rather than re-deriving them from the message text.
    The message itself is redacted at construction: an ADP error is raised in
    the middle of handling a credential, and this is the one exception in the
    program most likely to be pasted into a public bug report.
    """

    def __init__(self, failure: AuthFailure):
        self.failure = failure
        super().__init__(redact_secrets(f"{failure.summary} {failure.remedy}"))


@dataclass(frozen=True)
class ADPStatus:
    """What is known - and, explicitly, what is not - about ADP on an account.

    Three fields are tri-state on purpose.  "Could not determine" is a real
    answer here: Apple exposes no documented "is ADP on" flag, and reporting
    ``off`` because a field was missing would be the single most damaging lie
    this module could tell, since it would send a user hunting for a
    non-existent problem while their real one goes unnamed.
    """

    state: str = ADP_UNDETERMINED
    web_access: str = WEB_ACCESS_UNDETERMINED
    device_consent: str = CONSENT_UNDETERMINED
    detail: str = ""
    #: What we looked at to reach this conclusion.
    evidence: Tuple[str, ...] = ()
    #: What we could *not* look at.  Never empty when anything was skipped:
    #: "no problem found" must not be able to mean "I could not look".
    unchecked: Tuple[str, ...] = ()

    @property
    def undetermined(self) -> bool:
        return self.state == ADP_UNDETERMINED

    def to_dict(self) -> Dict[str, Any]:
        return {
            "state": self.state,
            "web_access": self.web_access,
            "device_consent": self.device_consent,
            "detail": self.detail,
            "evidence": list(self.evidence),
            "unchecked": list(self.unchecked),
        }


def interpret_web_access_state(payload: Any) -> ADPStatus:
    """Read Apple's ``requestWebAccessState`` answer.

    ``isICDRSDisabled`` is the signal both pyicloud and Apple's own web client
    key off: ICDRS is the iCloud Data Recovery Service, and turning on Advanced
    Data Protection is precisely what disables it.  So ``isICDRSDisabled: true``
    means "this account's data is end-to-end encrypted and a PCS cookie is
    required"; ``false`` means an ordinary account that needs none.

    A payload that is not a dict, or that carries none of the fields we know,
    produces an undetermined status naming what was missing.
    """
    if not isinstance(payload, dict):
        return ADPStatus(
            detail=(
                "Apple's web-access state response was not a JSON object, so "
                "nothing could be concluded from it."
            ),
            unchecked=("requestWebAccessState returned an unreadable payload",),
        )

    evidence: List[str] = []
    unchecked: List[str] = []

    state = ADP_UNDETERMINED
    icdrs = payload.get("isICDRSDisabled")
    if isinstance(icdrs, bool):
        state = ADP_ON if icdrs else ADP_OFF
        evidence.append(f"isICDRSDisabled={icdrs}")
    else:
        unchecked.append(
            "isICDRSDisabled was absent from Apple's web-access state response, "
            "so whether Advanced Data Protection is enabled is unknown"
        )

    web_access = WEB_ACCESS_UNDETERMINED
    for key in _WEB_ACCESS_KEYS:
        value = payload.get(key)
        if isinstance(value, bool):
            web_access = WEB_ACCESS_ENABLED if value else WEB_ACCESS_DISABLED
            evidence.append(f"{key}={value}")
            break
    else:
        unchecked.append(
            "Apple's web-access state response named no web-access field "
            f"({', '.join(_WEB_ACCESS_KEYS)}), so whether 'Access iCloud Data on "
            "the Web' is enabled could not be read directly"
        )

    consent = CONSENT_UNDETERMINED
    consented = payload.get("isDeviceConsentedForPCS")
    if isinstance(consented, bool):
        consent = CONSENT_GRANTED if consented else CONSENT_PENDING
        evidence.append(f"isDeviceConsentedForPCS={consented}")
    else:
        unchecked.append(
            "isDeviceConsentedForPCS was absent, so whether a trusted device has "
            "already consented is unknown"
        )

    if state == ADP_ON:
        detail = (
            "Apple reports iCloud Data Recovery Service disabled, which is what "
            "Advanced Data Protection does; this account needs a PCS cookie."
        )
    elif state == ADP_OFF:
        detail = (
            "Apple reports iCloud Data Recovery Service enabled, so Advanced "
            "Data Protection is off and no PCS cookie is needed."
        )
    else:
        detail = (
            "Apple answered the web-access state request but not with a field "
            "that says whether Advanced Data Protection is enabled."
        )

    return ADPStatus(
        state=state,
        web_access=web_access,
        device_consent=consent,
        detail=detail,
        evidence=tuple(evidence),
        unchecked=tuple(unchecked),
    )


def adp_status_from_webservices(
    webservices: Any,
    service: Optional[PCSService] = None,
) -> ADPStatus:
    """Derive ADP status from the account payload iFetch already holds.

    ``pcsRequired`` rides along on the web-service descriptor Apple returns from
    ``accountLogin``/``validate``, which pyicloud has already fetched by the
    time any of this runs.  Reading it therefore costs **no additional
    request**, which is what lets a non-ADP account take exactly the path it
    took before: we look at what we have, see ``false``, and stop.
    """
    target = service or pcs_service()
    if webservices is None:
        return ADPStatus(
            detail=(
                "iFetch has no account web-service description for this session, "
                "so it cannot tell whether Apple requires a PCS cookie."
            ),
            unchecked=(
                "the account's webservices payload was not available "
                "(sign in first, or pass --adp to attempt the flow regardless)",
            ),
        )
    if not isinstance(webservices, dict):
        return ADPStatus(
            detail="The account's web-service description was not a JSON object.",
            unchecked=("webservices payload was unreadable",),
        )

    descriptor = webservices.get(target.webservice)
    if not isinstance(descriptor, dict):
        return ADPStatus(
            detail=(
                f"Apple's account description lists no '{target.webservice}' web "
                f"service, so whether {target.key} needs a PCS cookie is unknown."
            ),
            unchecked=(
                f"webservices['{target.webservice}'] was absent from the account "
                "payload",
            ),
        )

    required = descriptor.get("pcsRequired")
    if not isinstance(required, bool):
        return ADPStatus(
            detail=(
                f"Apple's '{target.webservice}' descriptor carries no pcsRequired "
                "flag, so whether a PCS cookie is needed is unknown."
            ),
            unchecked=(
                f"webservices['{target.webservice}'].pcsRequired was absent",
            ),
        )

    if required:
        return ADPStatus(
            state=ADP_ON,
            detail=(
                f"Apple marks the {target.key} service pcsRequired, which it does "
                "for accounts with Advanced Data Protection enabled."
            ),
            evidence=(f"webservices['{target.webservice}'].pcsRequired=True",),
        )
    return ADPStatus(
        state=ADP_OFF,
        detail=(
            f"Apple does not mark the {target.key} service pcsRequired, so this "
            "account does not use per-service encryption for it."
        ),
        evidence=(f"webservices['{target.webservice}'].pcsRequired=False",),
    )


def _merge_status(first: ADPStatus, second: ADPStatus) -> ADPStatus:
    """Combine two partial readings, preferring a determined answer to neither.

    Contradictions are not smoothed over: if the two sources disagree the result
    is undetermined and both readings are kept as evidence, because a client
    that picks a winner between two disagreeing signals is guessing.
    """
    if first.state == ADP_UNDETERMINED:
        state = second.state
    elif second.state == ADP_UNDETERMINED or second.state == first.state:
        state = first.state
    else:
        state = ADP_UNDETERMINED

    def pick(a: str, b: str, undetermined: str) -> str:
        return b if a == undetermined else a

    return ADPStatus(
        state=state,
        web_access=pick(second.web_access, first.web_access, WEB_ACCESS_UNDETERMINED),
        device_consent=pick(
            second.device_consent, first.device_consent, CONSENT_UNDETERMINED
        ),
        detail=second.detail or first.detail,
        evidence=tuple(first.evidence) + tuple(second.evidence),
        unchecked=tuple(first.unchecked) + tuple(second.unchecked),
    )


# -- stored cookie state ----------------------------------------------------

@dataclass(frozen=True)
class PCSCookieState:
    """Which PCS cookies are on disk for a service, and whether they are alive."""

    service: str
    present: Tuple[str, ...] = ()
    missing: Tuple[str, ...] = ()
    expires_at: Optional[float] = None
    expired: bool = False
    read_errors: Tuple[str, ...] = ()

    @property
    def usable(self) -> bool:
        """True when a re-run can proceed without asking for approval again."""
        return not self.missing and not self.expired

    @property
    def detail(self) -> str:
        if self.missing and not self.present:
            return (
                f"No PCS cookie is stored for {self.service}. If this account "
                "has Advanced Data Protection enabled, the next run will need "
                "approval on a trusted device."
            )
        if self.missing:
            return (
                f"Only part of the PCS cookie set for {self.service} is stored "
                f"(missing: {', '.join(self.missing)}). Apple issues them "
                "together, so this session will be refused."
            )
        if self.expired:
            return (
                f"The stored PCS cookie for {self.service} has expired; the next "
                "run will re-request it and may need approval on a trusted device."
            )
        return f"A live PCS cookie for {self.service} is stored and will be reused."

    def to_dict(self) -> Dict[str, Any]:
        return {
            "service": self.service,
            "present": list(self.present),
            "missing": list(self.missing),
            "expires_at": (
                None
                if self.expires_at is None
                else datetime.fromtimestamp(self.expires_at, timezone.utc).isoformat()
            ),
            "expired": self.expired,
            "read_errors": list(self.read_errors),
            "detail": self.detail,
        }


def pcs_cookie_state(
    snapshot: SessionSnapshot,
    service: Optional[str] = None,
    now: Optional[float] = None,
) -> PCSCookieState:
    """Report the stored PCS cookies for ``service`` from a session snapshot.

    This is what makes an unattended re-run possible: the cookie Apple issued
    last time is already in pyicloud's jar, so a run that finds a live one never
    asks anybody to approve anything.  Expiry needs no enforcement of ours -
    ``http.cookiejar`` drops expired cookies when the jar is loaded, so the next
    run simply does not see one and re-requests it.  This function reads the jar
    with ``ignore_expires=True`` for the opposite reason: a *diagnostic* must be
    able to say "it expired two days ago" rather than "there isn't one".

    A corrupt or empty entry was dropped by :func:`_read_cookie_expiries` and
    surfaces here as missing plus a named read error - discarded cleanly, never
    crashed on.
    """
    target = pcs_service(service)
    current = time.time() if now is None else now

    present: List[str] = []
    missing: List[str] = []
    expiries: List[float] = []
    unbounded = False
    for name in target.cookies:
        if name in snapshot.pcs_expiries:
            present.append(name)
            value = snapshot.pcs_expiries[name]
            if value is None:
                unbounded = True
            else:
                expiries.append(float(value))
        else:
            missing.append(name)

    # The set expires when its earliest member does; a cookie without an expiry
    # never forces a refresh on its own.
    expires_at = min(expiries) if expiries else None
    expired = bool(expires_at is not None and expires_at <= current)
    if unbounded and not expiries:
        expires_at = None

    relevant = tuple(
        error
        for error in snapshot.read_errors
        if any(name in error for name in target.cookies)
    )
    return PCSCookieState(
        service=target.key,
        present=tuple(present),
        missing=tuple(missing),
        expires_at=expires_at,
        expired=expired,
        read_errors=relevant,
    )


# -- acquisition ------------------------------------------------------------

@dataclass
class PCSResult:
    """The outcome of an attempt to make PCS cookies available for a service.

    ``requests_made`` is part of the contract, not debug output: "a non-ADP
    account issues no extra requests" is a promise that can only be kept if it
    can be asserted, and this is what the regression test asserts on.
    """

    service: str
    status: str
    detail: str = ""
    attempts: int = 0
    requests_made: int = 0
    waited_seconds: float = 0.0
    cookies_present: Tuple[str, ...] = ()
    cookies_missing: Tuple[str, ...] = ()
    adp: ADPStatus = field(default_factory=ADPStatus)

    @property
    def acquired(self) -> bool:
        return self.status == PCS_ACQUIRED

    def to_dict(self) -> Dict[str, Any]:
        """A summary safe to print, log and serialise.

        Cookie *names* only.  No value ever enters this dictionary, which is why
        the JSON report can be pasted into an issue.
        """
        return {
            "service": self.service,
            "status": self.status,
            "detail": redact_secrets(self.detail),
            "attempts": self.attempts,
            "requests_made": self.requests_made,
            "waited_seconds": round(self.waited_seconds, 2),
            "cookies_present": list(self.cookies_present),
            "cookies_missing": list(self.cookies_missing),
            "adp": self.adp.to_dict(),
        }


class PyiCloudPCSTransport:
    """Adapts a ``PyiCloudService`` to the three calls the PCS flow needs.

    Deliberately thin.  It borrows pyicloud's *authenticated* session, so the
    cookies Apple returns land in pyicloud's jar and are persisted by pyicloud's
    own save-after-request path; iFetch stores nothing of its own.  The
    alternative - a second HTTP client with a second cookie store - would mean
    two places to expire, two places to corrupt and two places to leak from.
    """

    def __init__(self, service: Any):
        self._service = service

    # -- reads ---------------------------------------------------------
    def cookie_names(self) -> List[str]:
        """Names of the cookies the live session currently holds.

        Values are never read.  There is no reason for this module to touch
        one, and not touching it is the cheapest possible guarantee that it
        cannot be logged.
        """
        jar = getattr(getattr(self._service, "session", None), "cookies", None)
        if jar is None:
            return []
        try:
            return [cookie.name for cookie in jar]
        except TypeError:
            return list(getattr(jar, "keys", list)())

    def webservices(self) -> Optional[Dict[str, Any]]:
        """The account's web-service map, or ``None`` when it is not known yet."""
        data = getattr(self._service, "data", None)
        if not isinstance(data, dict):
            return None
        services = data.get("webservices")
        return services if isinstance(services, dict) else None

    # -- writes --------------------------------------------------------
    def post(self, path: str, payload: Optional[Dict[str, Any]] = None) -> Any:
        session = getattr(self._service, "session", None)
        if session is None:
            raise RuntimeError(
                "This iCloud session cannot make requests, so the Advanced Data "
                "Protection flow could not be attempted."
            )
        endpoint = getattr(self._service, "_setup_endpoint", None)
        if not endpoint:
            raise RuntimeError(
                "This iCloud session exposes no setup endpoint, so the Advanced "
                "Data Protection flow could not be attempted."
            )
        params = getattr(self._service, "params", None)
        response = session.post(f"{endpoint}/{path}", json=payload, params=params)
        return response.json()

    def persist(self) -> None:
        """Flush the cookie jar.

        pyicloud already saves after every request; this is belt-and-braces for
        the case where a caller supplied a bare session, and it must never turn
        a successful acquisition into a failure.
        """
        jar = getattr(getattr(self._service, "session", None), "cookies", None)
        save = getattr(jar, "save", None)
        if save is None:
            return
        try:
            save()
        except (OSError, ValueError):
            pass


def _is_pending(message: str) -> bool:
    low = (message or "").lower()
    return any(marker in low for marker in PCS_PENDING_MESSAGES)


def _pcs_failure(code: str, summary: str, remedy: str, raw: str = "") -> AuthFailure:
    return AuthFailure(
        code=code,
        summary=redact_secrets(summary),
        remedy=redact_secrets(remedy),
        raw=redact_secrets(raw),
    )


_APPROVE_ON_DEVICE = (
    "Approve the 'iCloud Data on the Web' prompt on a trusted Apple device, "
    "then re-run 'ifetch auth renew --adp'."
)


def ensure_pcs_cookies(
    transport: Any,
    service: Optional[str] = None,
    *,
    force: bool = False,
    timeout: float = DEFAULT_PCS_TIMEOUT,
    max_attempts: int = DEFAULT_PCS_MAX_ATTEMPTS,
    initial_interval: float = PCS_INITIAL_INTERVAL,
    max_interval: float = PCS_MAX_INTERVAL,
    backoff: float = PCS_BACKOFF,
    now: Callable[[], float] = time.time,
    sleep: Callable[[float], None] = time.sleep,
    log: Optional[Callable[[str], None]] = None,
) -> PCSResult:
    """Make PCS cookies available for ``service``, or explain why they are not.

    The flow, in the order Apple requires it:

    1. **Do nothing if nothing is needed.**  If the session already carries the
       cookies, or Apple's own ``pcsRequired`` flag - which we already hold, at
       no request cost - says this service does not use per-service encryption,
       this returns immediately having issued *zero* requests.  Without ``force``
       we also refuse to proceed on no evidence at all: an account we cannot
       classify is left exactly as it was, and told so.
    2. **Check web access.**  ``requestWebAccessState`` is what distinguishes
       "Access iCloud Data on the Web is off" from every other ADP failure, and
       it is the one condition no amount of retrying will fix.
    3. **Ask for device consent** when Apple says it has not been given.
    4. **Poll ``requestPCS``** until Apple says success, bounded by both a
       deadline and an attempt cap, sleeping with backoff in between.

    ``derivedFromUserAction`` is sent only on the first request.  That flag is
    what makes Apple push the approval prompt to trusted devices; repeating it
    on every poll would re-notify the user once per attempt.

    Raises :class:`ADPError` - never a bare Apple error - for every failure,
    with a distinct code per cause.  Returns a :class:`PCSResult` otherwise.
    """
    target = pcs_service(service)
    emit = log or (lambda message: None)

    def held() -> Tuple[List[str], List[str]]:
        names = set(transport.cookie_names())
        return (
            [c for c in target.cookies if c in names],
            [c for c in target.cookies if c not in names],
        )

    present, missing = held()
    if not missing and not force:
        return PCSResult(
            service=target.key,
            status=PCS_ALREADY_PRESENT,
            detail=(
                f"The session already carries the PCS cookies for {target.key}; "
                "no approval was needed."
            ),
            cookies_present=tuple(present),
            adp=ADPStatus(
                detail=(
                    "A PCS cookie is already held. That does not by itself say "
                    "whether Advanced Data Protection is enabled."
                ),
                evidence=("session already holds the PCS cookie set",),
                unchecked=(
                    "Apple was not asked for the account's web-access state, "
                    "because nothing needed to be acquired",
                ),
            ),
        )

    status = adp_status_from_webservices(transport.webservices(), target)

    if not force:
        if status.state == ADP_OFF:
            return PCSResult(
                service=target.key,
                status=PCS_NOT_REQUIRED,
                detail=(
                    f"Apple does not require a PCS cookie for {target.key} on "
                    "this account, so nothing was requested."
                ),
                cookies_present=tuple(present),
                cookies_missing=tuple(missing),
                adp=status,
            )
        if status.state == ADP_UNDETERMINED:
            return PCSResult(
                service=target.key,
                status=PCS_UNDETERMINED,
                detail=(
                    "iFetch could not determine whether this account needs a PCS "
                    "cookie, so it changed nothing and made no request. Pass "
                    "--adp to attempt the Advanced Data Protection flow anyway."
                ),
                cookies_present=tuple(present),
                cookies_missing=tuple(missing),
                adp=status,
            )

    requests_made = 0
    emit(
        f"Advanced Data Protection: requesting per-service encryption access for "
        f"{target.key}."
    )

    web_state = _post(transport, PCS_WEB_ACCESS_STATE_PATH, None, target)
    requests_made += 1
    status = _merge_status(status, interpret_web_access_state(web_state))

    if status.web_access == WEB_ACCESS_DISABLED:
        raise ADPError(
            _pcs_failure(
                code="adp_web_access_disabled",
                summary=(
                    "Apple reports that 'Access iCloud Data on the Web' is turned "
                    "off for this account. With Advanced Data Protection enabled "
                    "that setting is what authorises any web or API client, so no "
                    "credential iFetch can obtain will work until it is on."
                ),
                remedy=(
                    "On a trusted Apple device: Settings > [your name] > iCloud > "
                    "'Access iCloud Data on the Web' -> ON (macOS: System Settings "
                    "> [your name] > iCloud > Access iCloud Data on the Web). Then "
                    "run 'ifetch auth renew --adp'. Allow a few minutes for the "
                    "change to propagate."
                ),
                raw=json.dumps(web_state, default=str)[:500],
            )
        )

    if not force and status.state == ADP_OFF:
        # Apple's authoritative answer arrived only now; believe it over the
        # webservices flag and stop rather than requesting a cookie nothing needs.
        return PCSResult(
            service=target.key,
            status=PCS_NOT_REQUIRED,
            detail=(
                "Apple reports Advanced Data Protection is not in force for this "
                "account, so no PCS cookie was requested."
            ),
            requests_made=requests_made,
            cookies_present=tuple(present),
            cookies_missing=tuple(missing),
            adp=status,
        )

    if status.device_consent == CONSENT_PENDING:
        emit("Advanced Data Protection: asking a trusted device for consent.")
        consent = _post(transport, PCS_ENABLE_CONSENT_PATH, None, target)
        requests_made += 1
        if not (isinstance(consent, dict) and consent.get("isDeviceConsentNotificationSent")):
            raise ADPError(
                _pcs_failure(
                    code="adp_consent_not_sent",
                    summary=(
                        "Apple declined to send the Advanced Data Protection "
                        "consent request to a trusted device, so approval can "
                        "never arrive and waiting for it would be pointless."
                    ),
                    remedy=(
                        "Confirm at least one Apple device is signed in to this "
                        "Apple ID, online, and running a current OS; then re-run "
                        "'ifetch auth renew --adp'. Devices too old to support "
                        "Advanced Data Protection cannot approve this request."
                    ),
                    raw=json.dumps(consent, default=str)[:500],
                )
            )

    deadline = now() + max(0.0, timeout)
    attempts = 0
    waited = 0.0
    last_message = ""

    while True:
        attempts += 1
        payload = _post(
            transport,
            PCS_REQUEST_PATH,
            {
                "appName": target.app_name,
                # Only the first request claims a user action; see docstring.
                "derivedFromUserAction": attempts == 1,
            },
            target,
        )
        requests_made += 1

        state = str((payload or {}).get("status") or "").lower() if isinstance(payload, dict) else ""
        message = str((payload or {}).get("message") or "") if isinstance(payload, dict) else ""

        if state == "success":
            transport.persist()
            present, missing = held()
            if missing:
                raise ADPError(
                    _pcs_failure(
                        code="adp_pcs_cookies_not_set",
                        summary=(
                            "Apple reported the per-service encryption request "
                            "succeeded but set no "
                            f"{', '.join(missing)} cookie, so iCloud Drive "
                            "requests will still be refused."
                        ),
                        remedy=(
                            "This usually means the session's cookie jar is not "
                            "being kept: check that --cookie-directory is "
                            "writable and shared between runs. Then re-run "
                            "'ifetch auth renew --adp'."
                        ),
                    )
                )
            emit(f"Advanced Data Protection: PCS access granted for {target.key}.")
            return PCSResult(
                service=target.key,
                status=PCS_ACQUIRED,
                detail=(
                    f"Apple granted per-service encryption access for "
                    f"{target.key} after {attempts} request(s)."
                ),
                attempts=attempts,
                requests_made=requests_made,
                waited_seconds=waited,
                cookies_present=tuple(present),
                adp=_merge_status(
                    status,
                    ADPStatus(
                        state=ADP_ON,
                        detail=(
                            "Apple issued a per-service encryption cookie, which "
                            "it only does for accounts that require one."
                        ),
                        evidence=("requestPCS returned success",),
                    ),
                ),
            )

        if not _is_pending(message):
            raise ADPError(
                _pcs_failure(
                    code="adp_request_pcs_failed",
                    summary=(
                        "Apple's requestPCS exchange returned a state iFetch does "
                        f"not recognise: {message or state or 'no status'}. No "
                        "per-service encryption cookie was issued."
                    ),
                    remedy=(
                        _APPROVE_ON_DEVICE
                        + " If no prompt appears, confirm 'Access iCloud Data on "
                        "the Web' is ON in iCloud settings. Unrecognised states "
                        "are treated as failures rather than retried, because "
                        "retrying an error is how a backup job hangs forever - "
                        "please report this message so it can be named properly."
                    ),
                    raw=json.dumps(payload, default=str)[:500],
                )
            )

        last_message = message
        interval = min(initial_interval * (backoff ** (attempts - 1)), max_interval)
        # Both bounds, checked before sleeping: an injected clock that never
        # advances still terminates on the attempt cap, and a slow approval
        # still stops at the deadline instead of overshooting it by an interval.
        if attempts >= max_attempts or now() + interval > deadline:
            break
        emit(
            "Advanced Data Protection: waiting for approval on a trusted device "
            f"({message or 'not ready yet'})."
        )
        sleep(interval)
        waited += interval

    raise ADPError(
        _pcs_failure(
            code="adp_approval_timeout",
            summary=(
                f"Apple never granted per-service encryption access for "
                f"{target.key}: it was still answering "
                f"'{last_message or 'not ready'}' after {attempts} attempts over "
                f"{waited:.0f}s. The most likely cause is that the approval "
                "prompt on a trusted device was not tapped; the next most likely "
                "is that 'Access iCloud Data on the Web' is off."
            ),
            remedy=(
                "Unlock a trusted Apple device, approve the 'iCloud Data on the "
                "Web' prompt, and re-run 'ifetch auth renew --adp'. If no prompt "
                "arrives, turn Settings > [your name] > iCloud > 'Access iCloud "
                "Data on the Web' off and on again, then retry. Use "
                "--adp-timeout to wait longer than the default "
                f"{DEFAULT_PCS_TIMEOUT:.0f}s. This is a separate approval from "
                "two-factor sign-in; a 2FA code cannot satisfy it."
            ),
        )
    )


def _post(
    transport: Any,
    path: str,
    payload: Optional[Dict[str, Any]],
    target: PCSService,
) -> Any:
    """POST one setup-endpoint call, converting any failure into a named cause.

    A raw ``requests`` traceback or a pyicloud exception reaching the user is
    the exact failure mode this module exists to remove, so nothing escapes
    unclassified - and an error body that happens to quote a cookie is redacted
    on the way past.
    """
    try:
        return transport.post(path, payload)
    except ADPError:
        raise
    except Exception as exc:  # noqa: BLE001 - deliberately total
        failure = classify_auth_error(exc)
        if failure.code == "unknown":
            failure = _pcs_failure(
                code="adp_request_pcs_failed",
                summary=(
                    f"Apple's {path} request failed while setting up per-service "
                    f"encryption for {target.key}, so no cookie was issued."
                ),
                remedy=(
                    _APPROVE_ON_DEVICE
                    + " If the error above looks like a network problem, retry; "
                    "if it repeats, run 'ifetch auth doctor --online --adp'."
                ),
                raw=str(exc),
            )
        raise ADPError(failure) from exc


# ---------------------------------------------------------------------------
# Diagnosis
# ---------------------------------------------------------------------------

CHECK_OK = "ok"
CHECK_WARN = "warn"
CHECK_FAIL = "fail"
CHECK_INFO = "info"
CHECK_SKIP = "skip"

#: Ordering used to reduce many check statuses to one overall status.
_SEVERITY = {CHECK_SKIP: 0, CHECK_INFO: 0, CHECK_OK: 0, CHECK_WARN: 1, CHECK_FAIL: 2}


@dataclass
class Check:
    """One diagnostic finding."""

    name: str
    status: str
    detail: str
    remedy: str = ""
    data: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        payload = {"name": self.name, "status": self.status, "detail": self.detail}
        if self.remedy:
            payload["remedy"] = self.remedy
        if self.data:
            payload["data"] = self.data
        return payload


@dataclass
class Diagnosis:
    """The complete result of ``ifetch auth doctor``."""

    account: str
    region: str
    checks: List[Check] = field(default_factory=list)

    @property
    def status(self) -> str:
        """Worst status across all checks."""
        worst = CHECK_OK
        for check in self.checks:
            if _SEVERITY.get(check.status, 0) > _SEVERITY.get(worst, 0):
                worst = check.status
        return worst

    @property
    def exit_code(self) -> int:
        """0 healthy, 1 needs attention soon, 2 broken now.

        Distinct codes let a cron job treat "expires in 3 days" differently from
        "cannot authenticate", which is the whole point of warning early.
        """
        return {CHECK_OK: 0, CHECK_INFO: 0, CHECK_SKIP: 0, CHECK_WARN: 1, CHECK_FAIL: 2}[
            self.status
        ]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "account": self.account,
            "region": self.region,
            "status": self.status,
            "checks": [c.to_dict() for c in self.checks],
        }


class AuthDoctor:
    """Diagnose iCloud authentication and report *which precondition failed*.

    The local half (session presence, expiry, region) needs no network and
    always runs.  The live half (does authentication actually succeed, is the
    session trusted, does Drive respond) runs only when ``online`` is set, and
    every Apple error it encounters is passed through
    :func:`classify_auth_error` so the output names a cause rather than an HTTP
    status.
    """

    def __init__(
        self,
        account: str,
        region: str = REGION_GLOBAL,
        cookie_directory: Optional[Path] = None,
        warn_days: int = DEFAULT_WARN_DAYS,
        online: bool = False,
        service_factory: Optional[Callable[..., Any]] = None,
        drive_probe: Optional[Callable[[Any], Any]] = None,
        now: Optional[float] = None,
        password: Optional[str] = None,
        adp: Optional[bool] = None,
        pcs_service_name: str = DEFAULT_PCS_SERVICE,
        adp_transport: Optional[Callable[[Any], Any]] = None,
    ):
        self.account = account
        self.region = region
        self.cookie_directory = cookie_directory
        self.warn_days = warn_days
        self.online = online
        self.service_factory = service_factory
        self.drive_probe = drive_probe
        self.now = now
        self.password = password
        #: ``True`` ask Apple about ADP even without evidence, ``False`` never
        #: ask, ``None`` ask only when the account payload says a PCS cookie is
        #: required.  ``None`` is what keeps a non-ADP account's request count
        #: identical to what it was before ADP support existed.
        self.adp = adp
        self.pcs_service = pcs_service(pcs_service_name)
        self.adp_transport = adp_transport or PyiCloudPCSTransport

    # -- local checks ---------------------------------------------------
    def run(self) -> Diagnosis:
        diagnosis = Diagnosis(account=self.account, region=self.region)
        snapshot = read_session_snapshot(self.account, self.cookie_directory)

        diagnosis.checks.append(self._check_region())
        diagnosis.checks.append(self._check_session_present(snapshot))
        diagnosis.checks.append(self._check_expiry(snapshot))
        diagnosis.checks.append(self._check_pcs_cookies(snapshot))

        if self.online:
            diagnosis.checks.extend(self._live_checks())
        else:
            diagnosis.checks.append(
                Check(
                    name="live_authentication",
                    status=CHECK_SKIP,
                    detail="Offline mode; no request was made to Apple.",
                    remedy="Re-run with --online to test the session against Apple.",
                )
            )
            diagnosis.checks.append(
                Check(
                    name="advanced_data_protection",
                    status=CHECK_INFO,
                    detail=(
                        "Whether Advanced Data Protection is enabled could not be "
                        "determined: Apple was not asked, because this run is "
                        "offline. This is not a report that it is off."
                    ),
                    remedy=(
                        "Re-run with --online (add --adp to force the check even "
                        "when the account payload says no PCS cookie is needed)."
                    ),
                    data=ADPStatus(
                        detail="Offline; no request was made to Apple.",
                        unchecked=(
                            "Apple's requestWebAccessState (needs --online)",
                            "the account's pcsRequired flag (needs a live session)",
                        ),
                    ).to_dict(),
                )
            )

        return diagnosis

    def _check_region(self) -> Check:
        if self.region == REGION_CHINA:
            detail = "Using China Mainland endpoints (iCloud.com.cn)."
        else:
            detail = "Using global endpoints (icloud.com)."
        return Check(
            name="region",
            status=CHECK_INFO,
            detail=detail,
            remedy=(
                ""
                if self.region == REGION_CHINA
                else "If your Apple ID is registered in China Mainland, pass --region china."
            ),
            data={"region": self.region},
        )

    def _check_session_present(self, snapshot: SessionSnapshot) -> Check:
        if not snapshot.exists:
            return Check(
                name="stored_session",
                status=CHECK_FAIL,
                detail=f"No stored session found under {snapshot.cookie_directory}.",
                remedy="Run 'ifetch auth renew' to sign in and create one.",
                data=snapshot.to_dict(),
            )

        if snapshot.read_errors:
            return Check(
                name="stored_session",
                status=CHECK_WARN,
                detail=(
                    "A stored session exists but parts of it could not be read: "
                    + "; ".join(snapshot.read_errors)
                ),
                remedy="Run 'ifetch auth renew --reset' to discard and recreate it.",
                data=snapshot.to_dict(),
            )

        if not snapshot.has_session_token:
            return Check(
                name="stored_session",
                status=CHECK_FAIL,
                detail="The stored session file contains no session token.",
                remedy="Run 'ifetch auth renew --reset' to sign in again.",
                data=snapshot.to_dict(),
            )

        trust = "with" if snapshot.has_trust_token else "without"
        return Check(
            name="stored_session",
            status=CHECK_OK if snapshot.has_trust_token else CHECK_WARN,
            detail=f"Stored session found {trust} a trust token.",
            remedy=(
                ""
                if snapshot.has_trust_token
                else "Without a trust token every run needs a fresh 2FA code. "
                "Run 'ifetch auth renew' and let it trust the session."
            ),
            data=snapshot.to_dict(),
        )

    def _check_expiry(self, snapshot: SessionSnapshot) -> Check:
        verdict = evaluate_expiry(snapshot, warn_days=self.warn_days, now=self.now)
        status = {
            STATUS_OK: CHECK_OK,
            STATUS_WARN: CHECK_WARN,
            STATUS_EXPIRED: CHECK_FAIL,
            STATUS_UNKNOWN: CHECK_WARN,
        }[verdict.status]
        return Check(
            name="session_expiry",
            status=status,
            detail=verdict.detail,
            remedy="" if status == CHECK_OK else "Run 'ifetch auth renew'.",
            data=verdict.to_dict(),
        )

    def _check_pcs_cookies(self, snapshot: SessionSnapshot) -> Check:
        """Report the stored per-service encryption cookies, with no network.

        This runs for every account, ADP or not, because its *absence* is not a
        finding: a non-ADP account legitimately has no PCS cookie, and saying so
        plainly is what stops the check being read as a fault.
        """
        state = pcs_cookie_state(snapshot, self.pcs_service.key, now=self.now)

        if state.read_errors:
            status = CHECK_WARN
            remedy = (
                "Run 'ifetch auth renew --adp' to request a fresh one; the stored "
                "entry is unusable and will be replaced."
            )
        elif state.missing and not state.present:
            status = CHECK_INFO
            remedy = (
                "Nothing to do unless this account has Advanced Data Protection "
                "enabled, in which case run 'ifetch auth renew --adp'."
            )
        elif state.missing or state.expired:
            status = CHECK_WARN
            remedy = "Run 'ifetch auth renew --adp' before the next scheduled run."
        else:
            status = CHECK_OK
            remedy = ""

        detail = state.detail
        if state.read_errors:
            detail = f"{detail} Unreadable: {'; '.join(state.read_errors)}"

        return Check(
            name="pcs_cookies",
            status=status,
            detail=detail,
            remedy=remedy,
            data=state.to_dict(),
        )

    # -- live checks ----------------------------------------------------
    def _check_adp(self, service: Any) -> Check:
        """Ask what is knowable about ADP for this account, and say what is not.

        Costs no request on the happy path: ``pcsRequired`` is already in the
        account payload pyicloud fetched during sign-in.  Apple is only asked
        the follow-up question when that flag says a PCS cookie is required, or
        when ``--adp`` demands it.
        """
        if self.adp is False:
            return Check(
                name="advanced_data_protection",
                status=CHECK_SKIP,
                detail=(
                    "Advanced Data Protection was not checked because --no-adp "
                    "was given. Its state is therefore unknown, not off."
                ),
                data=ADPStatus(unchecked=("--no-adp was given",)).to_dict(),
            )

        try:
            transport = self.adp_transport(service)
            status = adp_status_from_webservices(
                transport.webservices(), self.pcs_service
            )
            if self.adp is True or status.state == ADP_ON:
                status = _merge_status(
                    status,
                    interpret_web_access_state(
                        transport.post(PCS_WEB_ACCESS_STATE_PATH, None)
                    ),
                )
        except Exception as exc:  # noqa: BLE001 - a diagnostic must not die here
            failure = classify_auth_error(exc)
            return Check(
                name="advanced_data_protection",
                status=CHECK_WARN,
                detail=(
                    "Whether Advanced Data Protection is enabled could not be "
                    f"determined: {failure.summary}"
                ),
                remedy=failure.remedy,
                data=ADPStatus(
                    unchecked=(redact_secrets(str(exc))[:300],),
                ).to_dict(),
            )

        if status.web_access == WEB_ACCESS_DISABLED:
            return Check(
                name="advanced_data_protection",
                status=CHECK_FAIL,
                detail=(
                    "Apple reports 'Access iCloud Data on the Web' is turned off. "
                    "With Advanced Data Protection enabled, that blocks every "
                    "web/API client - including iFetch - regardless of sign-in."
                ),
                remedy=(
                    "On a trusted Apple device: Settings > [your name] > iCloud > "
                    "'Access iCloud Data on the Web' -> ON, then run "
                    "'ifetch auth renew --adp'."
                ),
                data=status.to_dict(),
            )

        if status.state == ADP_ON:
            return Check(
                name="advanced_data_protection",
                status=CHECK_INFO,
                detail=(
                    "Advanced Data Protection is in force for this account: "
                    f"{status.detail} iFetch will request a per-service "
                    "encryption cookie, which needs approval on a trusted device "
                    "the first time."
                ),
                remedy=(
                    "Run 'ifetch auth renew --adp' and approve the prompt on a "
                    "trusted device."
                ),
                data=status.to_dict(),
            )

        if status.state == ADP_OFF:
            return Check(
                name="advanced_data_protection",
                status=CHECK_OK,
                detail=(
                    "Advanced Data Protection is not in force for this account, "
                    "so no per-service encryption cookie is needed."
                ),
                data=status.to_dict(),
            )

        return Check(
            name="advanced_data_protection",
            status=CHECK_INFO,
            detail=(
                "Whether Advanced Data Protection is enabled could not be "
                f"determined. {status.detail} Not checked: "
                f"{'; '.join(status.unchecked) or 'nothing'}."
            ),
            remedy=(
                "Re-run with --adp to ask Apple directly. If Drive access fails "
                "with a PCS error, run 'ifetch auth renew --adp'."
            ),
            data=status.to_dict(),
        )

    def _live_checks(self) -> List[Check]:
        """Probe Apple. Every failure is classified, never surfaced raw."""
        factory = self.service_factory or self._default_service_factory
        try:
            service = factory(
                apple_id=self.account,
                password=self.password,
                **region_service_kwargs(self.region),
            )
        except Exception as exc:
            failure = classify_auth_error(exc)
            return [
                Check(
                    name="live_authentication",
                    status=CHECK_FAIL,
                    detail=failure.summary,
                    remedy=failure.remedy,
                    data=failure.to_dict(),
                )
            ]

        checks = [
            Check(
                name="live_authentication",
                status=CHECK_OK,
                detail="Apple accepted the stored session.",
            )
        ]

        requires_2fa = bool(getattr(service, "requires_2fa", False))
        trusted = bool(getattr(service, "is_trusted_session", False))
        if requires_2fa:
            checks.append(
                Check(
                    name="trusted_session",
                    status=CHECK_FAIL,
                    detail="Apple is asking for a 2FA code; this session is not trusted.",
                    remedy=(
                        "Run 'ifetch auth renew' and supply a code with --2fa-code, "
                        "--2fa-file or --2fa-webhook."
                    ),
                )
            )
        else:
            checks.append(
                Check(
                    name="trusted_session",
                    status=CHECK_OK if trusted else CHECK_WARN,
                    detail=(
                        "Session is trusted; no 2FA code is needed."
                        if trusted
                        else "Session authenticated but is not marked trusted."
                    ),
                    remedy=(
                        "" if trusted else "Run 'ifetch auth renew' to trust this session."
                    ),
                )
            )

        # ADP is a *separate* gate from 2FA: a session can be fully trusted and
        # still be refused for want of a PCS cookie, so this is reported on its
        # own rather than folded into the trust check above.
        checks.append(self._check_adp(service))
        checks.append(self._probe_drive(service))
        return checks

    def _probe_drive(self, service: Any) -> Check:
        """List the Drive root. This is where ADP/PCS failures actually surface.

        Authentication can succeed while every Drive request returns 423, which
        is exactly the confusing case reported against other tools; probing here
        is what lets the doctor name Advanced Data Protection as the cause.
        """
        probe = self.drive_probe or (lambda svc: svc.drive.dir())
        try:
            listing = probe(service)
        except Exception as exc:
            failure = classify_auth_error(exc)
            return Check(
                name="drive_access",
                status=CHECK_FAIL,
                detail=failure.summary,
                remedy=failure.remedy,
                data=failure.to_dict(),
            )

        count = len(listing) if hasattr(listing, "__len__") else None
        return Check(
            name="drive_access",
            status=CHECK_OK,
            detail=(
                f"iCloud Drive responded with {count} top-level items."
                if count is not None
                else "iCloud Drive responded."
            ),
            data={"top_level_items": count},
        )

    @staticmethod
    def _default_service_factory(**kwargs: Any) -> Any:
        from pyicloud import PyiCloudService

        return PyiCloudService(**kwargs)


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------

_SYMBOLS = {
    CHECK_OK: "ok  ",
    CHECK_WARN: "warn",
    CHECK_FAIL: "FAIL",
    CHECK_INFO: "info",
    CHECK_SKIP: "skip",
}


def render_diagnosis(diagnosis: Diagnosis) -> str:
    """Render a diagnosis as plain text suitable for a terminal or a log."""
    lines = [
        f"iFetch auth doctor - {diagnosis.account} [{diagnosis.region}]",
        "=" * 70,
    ]
    for check in diagnosis.checks:
        lines.append(f"[{_SYMBOLS.get(check.status, check.status)}] {check.name}")
        lines.append(f"       {check.detail}")
        if check.remedy and check.status not in (CHECK_OK, CHECK_INFO):
            lines.append(f"       -> {check.remedy}")
    lines.append("=" * 70)
    lines.append(f"Overall: {diagnosis.status}")
    return "\n".join(lines)


def render_expiry_warning(verdict: ExpiryVerdict, account: str) -> Optional[str]:
    """One-line warning for a download run, or ``None`` when nothing is wrong."""
    if not verdict.needs_attention:
        return None
    prefix = "WARNING" if verdict.status == STATUS_WARN else "ERROR"
    return f"{prefix}: iCloud session for {account}: {verdict.detail}"
