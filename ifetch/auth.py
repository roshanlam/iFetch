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

Everything here is deliberately usable without a network connection: the
diagnosis of local session state, the expiry arithmetic and the error
classification are pure functions over on-disk data and exception text, which is
also what makes them testable.
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
    """
    text = str(error or "")
    low = text.lower()

    # Ordered most-specific first: several of these conditions can co-occur in
    # one message and the first match must be the one that is actionable.
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
                "Protection is on, approve the prompt that appears on a trusted "
                "device, then re-run 'ifetch auth renew'. Changes can take a few "
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
        snapshot.webauth_expires_at = _read_webauth_expiry(
            snapshot.cookiejar_path, snapshot.read_errors
        )

    return snapshot


def _read_webauth_expiry(path: Path, errors: List[str]) -> Optional[float]:
    """Read the expiry of the web-auth cookie from an LWP-format cookie jar.

    ``ignore_expires=True`` is essential: an already-expired cookie must still
    be loaded, because reporting *how long ago* a session died is the whole
    point of the diagnostic.
    """
    jar = LWPCookieJar(str(path))
    try:
        jar.load(ignore_discard=True, ignore_expires=True)
    except (OSError, ValueError) as exc:
        errors.append(f"load cookie jar: {exc}")
        return None

    expiries = [
        float(cookie.expires)
        for cookie in jar
        if cookie.name == WEBAUTH_COOKIE and cookie.expires
    ]
    if not expiries:
        return None
    # Several cookies can share the name across domains; the session survives
    # as long as the longest-lived one.
    return max(expiries)


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

    # -- local checks ---------------------------------------------------
    def run(self) -> Diagnosis:
        diagnosis = Diagnosis(account=self.account, region=self.region)
        snapshot = read_session_snapshot(self.account, self.cookie_directory)

        diagnosis.checks.append(self._check_region())
        diagnosis.checks.append(self._check_session_present(snapshot))
        diagnosis.checks.append(self._check_expiry(snapshot))

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

    # -- live checks ----------------------------------------------------
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
