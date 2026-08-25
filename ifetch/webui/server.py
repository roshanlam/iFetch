"""HTTP for the local web UI: routing, access control, and nothing else.

The work itself lives in the modules the CLI already uses. This file only
decides who is allowed to ask for it and turns the answers into JSON.
``docs/webui-api.md`` is the contract and this implements it as written.

The care taken below is because this process holds an authenticated iCloud
session. Anything that can reach it can list a person's files, start downloads
onto their disk, and read their Apple ID email. So:

* It binds 127.0.0.1. ``--host`` overrides that and says out loud what has been
  exposed and to whom.
* A random token authorises every request, compared with
  ``hmac.compare_digest``. It arrives once in the URL and then lives in an
  HttpOnly, SameSite=Strict cookie.
* A request without that token gets 404. Not 401 - a 401 tells a port scanner
  that something worth returning to is listening here.
* A request whose ``Host`` header is not one of a small set of accepted names
  is refused. This is the one that matters most. Without it, any page a user
  visits can point a hostname it controls at 127.0.0.1 and drive this server
  through their own browser, cookie and all.
* No ``Access-Control-Allow-Origin`` header is ever sent, so another origin
  cannot read a response even if it manages to get one sent.
* The Apple ID password is never sent to the browser, and a request that
  carries one is rejected. It is resolved here, from the OS keyring or
  ``--password-command``, exactly as the CLI does. The browser only ever sends
  the six-digit code.

Paths from the browser are treated as hostile. A remote path may not climb out
of the Drive root; a local path must be absolute and inside a folder this
server was told it may use. Anything else is refused by name rather than
quietly corrected into something that looked close enough.
"""

from __future__ import annotations

import hmac
import http.cookies
import json
import logging
import mimetypes
import os
import secrets
import threading
import unicodedata
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple
from urllib.parse import parse_qs, urlsplit

from ..packages import is_package_name
from ..utils import can_read_file
from .jobs import JobConflict, JobRunner, attach

logger = logging.getLogger(__name__)

__all__ = [
    "ApiError",
    "AuthSession",
    "WebUIApp",
    "WebUIServer",
    "create_server",
    "host_is_allowed",
    "clean_local_path",
    "clean_remote_path",
]

#: Bodies are small by design - an email, a code, two paths. Anything larger is
#: refused before it is read rather than buffered to find out what it was.
MAX_BODY_BYTES = 64 * 1024

COOKIE_NAME = "ifetch_webui"

#: The only ``Host`` values accepted by default. A name that resolves to
#: 127.0.0.1 is not enough; the name itself has to be one of these.
LOOPBACK_HOSTS = frozenset({"localhost", "localhost.localdomain", "127.0.0.1", "::1"})

#: Addresses that mean "every interface", so they name no host of their own.
WILDCARD_HOSTS = frozenset({"", "0.0.0.0", "::", "*"})

SIGNED_OUT = "signed_out"
NEEDS_2FA = "needs_2fa"
SIGNED_IN = "signed_in"
AUTH_ERROR = "error"

STATIC_DIR = Path(__file__).resolve().parent / "static"

#: An endpoint: a decoded body (or query) in, ``(status, payload)`` out.
Endpoint = Callable[[Dict[str, Any]], Tuple[int, Dict[str, Any]]]

MISSING_PAGE = (
    "The iFetch web UI is running, but its page assets are not installed.\n\n"
    "Expected an index.html in:\n"
    f"  {STATIC_DIR}\n\n"
    "The API itself is up; only the page is missing. Reinstall iFetch, or use\n"
    "the command line in the meantime.\n"
)

#: Said up front rather than discovered by pressing Cancel and watching nothing
#: happen: a scan walks a tree with no per-item callback to check a flag in.
SCAN_NOTE = "A scan cannot be stopped once started; cancelling discards its result."


class ApiError(Exception):
    """A refusal with a status and one sentence explaining what to do."""

    def __init__(self, status: int, message: str) -> None:
        super().__init__(message)
        self.status = status
        self.message = message


# ---------------------------------------------------------------------------
# Access control helpers
# ---------------------------------------------------------------------------

def host_is_allowed(header: Optional[str], allowed: Iterable[str]) -> bool:
    """True when the ``Host`` header names something this server answers to.

    DNS rebinding works by giving a browser a hostname the attacker controls
    that resolves to 127.0.0.1. The connection is genuinely local, so nothing
    about the socket gives it away - but the browser still sends the attacker's
    hostname, and that is the part checked here.
    """
    if not header:
        return False  # HTTP/1.1 requires one; a request without it gets nothing.
    name = header.strip()
    if name.startswith("["):
        end = name.find("]")
        if end == -1:
            return False
        name = name[1:end]
    elif name.count(":") == 1:
        name = name.split(":", 1)[0]
    return name.lower() in {h.lower() for h in allowed}


def clean_remote_path(raw: Any, field: str = "icloud_path") -> str:
    """Validate a browser-supplied iCloud Drive path, or refuse it.

    Checking happens on an NFKC-folded copy so that look-alike spellings of
    ``..`` - fullwidth dots and the like - are caught, while the value returned
    keeps the exact characters Apple will be asked for, because Apple's own
    names are NFD and re-spelling them makes them 404.
    """
    if raw is None:
        return ""
    if not isinstance(raw, str):
        raise ApiError(400, f"{field} must be a string.")
    text = raw.strip()
    if not text or text == "/":
        return ""
    if any(ord(ch) < 32 or ord(ch) == 127 for ch in text):
        raise ApiError(400, f"{field} contains a control character.")

    probe = unicodedata.normalize("NFKC", text).replace("\\", "/")
    if probe.startswith("/"):
        raise ApiError(400, f"{field} must be relative to the iCloud Drive root.")
    if len(probe) > 1 and probe[1] == ":":
        raise ApiError(400, f"{field} must be relative to the iCloud Drive root.")
    if any(part in (".", "..") for part in probe.split("/")):
        raise ApiError(400, f"{field} may not contain '.' or '..' segments.")

    return "/".join(part for part in text.replace("\\", "/").split("/") if part)


def default_allowed_roots() -> List[Path]:
    """Where a browser-supplied destination may point without an explicit opt-in.

    The home directory plus the usual mount points. Everything else - ``/etc``,
    ``/usr``, another user's home - has to be named on the command line, so a
    page that talks this server into a download cannot choose where it lands.
    """
    roots = [Path.home()]
    for candidate in ("/Volumes", "/mnt", "/media"):
        path = Path(candidate)
        if path.is_dir():
            roots.append(path)
    return [_resolve(root) for root in roots]


def _resolve(path: Path) -> Path:
    try:
        return path.resolve()
    except (OSError, RuntimeError):
        return Path(os.path.normpath(str(path)))


def clean_local_path(raw: Any, allowed_roots: Iterable[Path], field: str = "local_path") -> Path:
    """Validate a browser-supplied local path, or refuse it.

    Symlinks are resolved before the containment check, so a link inside an
    allowed folder cannot be used as a door out of it.
    """
    if not isinstance(raw, str) or not raw.strip():
        raise ApiError(400, f"{field} is required and must be a non-empty string.")
    text = raw.strip()
    if "\x00" in text:
        raise ApiError(400, f"{field} contains a null byte.")

    candidate = Path(text).expanduser()
    if not candidate.is_absolute():
        raise ApiError(
            400,
            f"{field} must be an absolute path, for example '{Path.home() / 'icloud-backup'}'.",
        )

    resolved = _resolve(candidate)
    roots = list(allowed_roots)
    for root in roots:
        if resolved == root or root in resolved.parents:
            break
    else:
        listed = ", ".join(str(root) for root in roots) or "(none configured)"
        raise ApiError(
            400,
            f"'{text}' is outside the folders this server may use. Allowed: {listed}. "
            "Start 'ifetch serve' with --allow-path to add another.",
        )

    if resolved.exists() and not resolved.is_dir():
        raise ApiError(400, f"'{text}' is a file, not a folder.")
    return resolved


def shorten(path: Path) -> str:
    """``~``-relative display text, used only in job labels."""
    home = Path.home()
    try:
        return f"~/{path.relative_to(home)}" if path != home else "~"
    except ValueError:
        return str(path)


# ---------------------------------------------------------------------------
# Defaults that reach the rest of iFetch (imported late, so a test never does)
# ---------------------------------------------------------------------------

def build_manager_factory(
    region: Optional[str] = None, max_workers: int = 4
) -> Callable[..., Any]:
    def factory(email: str, password: Optional[str]) -> Any:
        from ..downloader import DownloadManager

        return DownloadManager(
            email=email, password=password, region=region, max_workers=max_workers
        )

    return factory


def build_password_provider(command: Optional[str] = None) -> Callable[[], Optional[str]]:
    """Resolve the password the way the CLI does, and never anywhere else.

    ``None`` means "not configured here", which leaves pyicloud to read the
    system keyring - the normal desktop path.
    """
    def provider() -> Optional[str]:
        from ..auth import resolve_password

        return resolve_password(command)

    return provider


def default_expiry_probe(email: str) -> Optional[float]:
    from ..auth import evaluate_expiry, read_session_snapshot

    verdict = evaluate_expiry(read_session_snapshot(email))
    if verdict.days_remaining is None:
        return None
    return round(verdict.days_remaining, 2)


def default_guard_scan(root: Path) -> Dict[str, Any]:
    from ..guard import GuardScanner

    return GuardScanner(root).scan().to_dict()


def default_vanish_check(root: Path) -> Dict[str, Any]:
    from ..index import open_index
    from ..recovery import PlaceholderDetector
    from ..vanished import analyse

    store = open_index(root)
    try:
        if store.latest_scan() is None:
            raise RuntimeError(
                "no iCloud scan exists for this folder yet, so there is nothing to "
                "compare against. Run 'ifetch plan' or 'ifetch vanish check --refresh' once first."
            )
        return analyse(store, root, placeholders=PlaceholderDetector(root).scan()).to_dict()
    finally:
        store.close()


def app_version() -> str:
    try:
        from importlib.metadata import version

        return version("ifetch")
    except Exception:  # noqa: BLE001 - a missing distribution is not a failure
        return "0.0.0"


# ---------------------------------------------------------------------------
# Sign-in
# ---------------------------------------------------------------------------

class _BrowserTwoFactor:
    """Stands in for the terminal prompt ``authenticate`` would otherwise use.

    It blocks where a person would be typing, which is what lets one long
    ``authenticate()`` call be driven by two short HTTP requests.
    """

    def __init__(self, session: "AuthSession", wait: float) -> None:
        self._session = session
        self._wait = wait

    def describe_sources(self) -> List[str]:
        return ["the iFetch web UI"]

    def resolve(self) -> str:
        return self._session.await_code(self._wait)


class AuthSession:
    """Owns the iCloud sign-in, including the part that has to block.

    ``DownloadManager.authenticate`` is a single call that either returns signed
    in or stops partway through to ask for a code; it was written for a
    terminal, not for a web request. So it runs on its own thread and a resolver
    stands in for the prompt: when the downloader asks, the resolver blocks,
    this class reports ``needs_2fa``, and the browser's POST releases it.

    Apple's flow cannot be rewound, so a rejected code cannot simply be retried
    inside the same call. A retry therefore starts ``authenticate`` again behind
    the scenes; the person at the browser sees only the code box a second time,
    which is what the contract promises.

    The password never crosses the HTTP boundary in either direction. It is
    resolved here and handed straight to the downloader.
    """

    def __init__(
        self,
        manager_factory: Callable[..., Any],
        password_provider: Callable[[], Optional[str]],
        expiry_probe: Optional[Callable[[str], Optional[float]]] = None,
        auth_timeout: float = 180.0,
        code_timeout: float = 300.0,
    ) -> None:
        self._factory = manager_factory
        self._password = password_provider
        self._expiry_probe = expiry_probe if expiry_probe is not None else default_expiry_probe
        self._auth_timeout = auth_timeout
        self._code_timeout = code_timeout

        self._lock = threading.RLock()
        self.state = SIGNED_OUT
        self.email: Optional[str] = None
        self.message = ""
        self.manager: Any = None

        self._settled = threading.Event()
        self._outcome: Optional[str] = None
        self._error = ""
        self._code_ready = threading.Event()
        self._code: Optional[str] = None
        self._thread: Optional[threading.Thread] = None

    # -- what the page sees -------------------------------------------------
    def describe(self) -> Dict[str, Any]:
        with self._lock:
            state, email, message = self.state, self.email, self.message
        return {
            "state": state,
            "email": email,
            "message": message,
            "expires_in_days": self._expires_in_days(state, email),
        }

    def _expires_in_days(self, state: str, email: Optional[str]) -> Optional[float]:
        if not email or state == SIGNED_OUT:
            return None
        try:
            return self._expiry_probe(email)
        except Exception:  # noqa: BLE001 - a diagnostic must never break the page
            return None

    def require_manager(self) -> Any:
        with self._lock:
            if self.state != SIGNED_IN or self.manager is None:
                raise ApiError(409, "Not signed in to iCloud yet. Sign in first.")
            return self.manager

    # -- the 2FA rendezvous -------------------------------------------------
    def await_code(self, wait: float) -> str:
        """Called on the auth thread. Announces ``needs_2fa``, then blocks."""
        with self._lock:
            self._outcome = NEEDS_2FA
        self._settled.set()
        if not self._code_ready.wait(wait):
            # Deliberately not TwoFactorUnavailable: the downloader treats that
            # as permission to fall back to input(), which on a server started
            # from a terminal would hang the thread on a prompt nobody sees.
            raise RuntimeError(
                f"no two-factor code was entered in the web UI within {wait:.0f}s"
            )
        self._code_ready.clear()
        with self._lock:
            code = self._code or ""
            self._code = None
        return code

    # -- driving it ---------------------------------------------------------
    def start(self, email: str) -> Dict[str, Any]:
        with self._lock:
            if self.state == SIGNED_IN and self.email == email:
                return {"ok": True, "state": SIGNED_IN, "message": "Already signed in."}
            if self._thread is not None and self._thread.is_alive():
                raise ApiError(409, "A sign-in is already in progress.")
            self.email = email
            self.manager = None
            self.state = SIGNED_OUT
            self.message = ""

        outcome = self._launch()
        with self._lock:
            if outcome == SIGNED_IN:
                self.state = SIGNED_IN
                self.message = f"Signed in as {email}."
                return {"ok": True, "state": SIGNED_IN, "message": self.message}
            if outcome == NEEDS_2FA:
                self.state = NEEDS_2FA
                self.message = "Apple sent a six-digit code to your trusted devices."
                return {"ok": True, "state": NEEDS_2FA, "message": self.message}
            self.state = AUTH_ERROR
            self.message = self._error
        raise ApiError(400, self._error)

    def submit_code(self, code: str) -> Dict[str, Any]:
        with self._lock:
            if self.state != NEEDS_2FA:
                raise ApiError(409, "No two-factor code is expected right now.")
            email = self.email or ""

        self._settled.clear()
        with self._lock:
            self._outcome = None
            self._code = code
        self._code_ready.set()

        outcome = self._await_outcome()
        if outcome == SIGNED_IN:
            with self._lock:
                self.state = SIGNED_IN
                self.message = f"Signed in as {email}."
                return {"ok": True, "state": SIGNED_IN, "message": self.message}
        if outcome == NEEDS_2FA:
            # The run asked for a second code without failing; nothing to report
            # beyond "still waiting".
            return {"ok": True, "state": NEEDS_2FA, "message": "Another code is needed."}

        rejected = self._error or "Apple rejected that code."
        retry = self._launch()
        with self._lock:
            if retry == SIGNED_IN:
                self.state = SIGNED_IN
                self.message = f"Signed in as {email}."
                return {"ok": True, "state": SIGNED_IN, "message": self.message}
            if retry == NEEDS_2FA:
                self.state = NEEDS_2FA
                self.message = rejected
                raise ApiError(400, f"{rejected} Enter the code again.")
            self.state = AUTH_ERROR
            self.message = self._error
        raise ApiError(400, self._error)

    def sign_out(self) -> Dict[str, Any]:
        with self._lock:
            thread = self._thread
            self._code = None
        # An auth thread parked in await_code would otherwise sit there for
        # minutes holding a half-finished sign-in; releasing it with no code
        # lets it unwind now.
        self._code_ready.set()
        if thread is not None and thread.is_alive():
            thread.join(2.0)
        with self._lock:
            self.state = SIGNED_OUT
            self.manager = None
            self.message = ""
            self._outcome = None
            self._error = ""
            self._thread = None
        self._code_ready.clear()
        self._settled.clear()
        return {"ok": True}

    # -- internals ----------------------------------------------------------
    def _launch(self) -> str:
        """Run ``authenticate`` on a fresh thread and wait for its first word."""
        try:
            password = self._password()
        except Exception as exc:  # noqa: BLE001 - the reason belongs in the UI
            with self._lock:
                self._error = f"Could not obtain the Apple ID password: {exc}"
            return AUTH_ERROR

        self._settled.clear()
        self._code_ready.clear()
        with self._lock:
            self._outcome = None
            self._error = ""
            email = self.email or ""
        resolver = _BrowserTwoFactor(self, self._code_timeout)
        thread = threading.Thread(
            target=self._authenticate,
            args=(email, password, resolver),
            name="ifetch-webui-auth",
            daemon=True,
        )
        with self._lock:
            self._thread = thread
        thread.start()
        return self._await_outcome()

    def _await_outcome(self) -> str:
        if not self._settled.wait(self._auth_timeout):
            with self._lock:
                self._error = (
                    f"Signing in did not finish within {self._auth_timeout:.0f}s."
                )
            return AUTH_ERROR
        with self._lock:
            return self._outcome or AUTH_ERROR

    def _authenticate(self, email: str, password: Optional[str], resolver: Any) -> None:
        try:
            manager = self._factory(email=email, password=password)
            try:
                manager.authenticate(two_factor=resolver)
            except TypeError as exc:
                if "two_factor" not in str(exc):
                    raise
                # An override written against the older signature. It cannot be
                # handed a code, so say that rather than appearing to hang.
                raise RuntimeError(
                    "this downloader does not accept a two-factor resolver, so a "
                    "code cannot be relayed from the browser"
                ) from exc
        except Exception as exc:  # noqa: BLE001 - every failure is reported
            with self._lock:
                self._outcome = AUTH_ERROR
                self._error = str(exc) or exc.__class__.__name__
            self._settled.set()
            return

        with self._lock:
            self.manager = manager
            self._outcome = SIGNED_IN
        self._settled.set()


# ---------------------------------------------------------------------------
# The endpoints
# ---------------------------------------------------------------------------

class WebUIApp:
    """Every endpoint in ``docs/webui-api.md``, with no HTTP in sight.

    Each method takes a dict - a decoded JSON body, or the flattened query
    string - and returns ``(status, payload)``. Refusals are raised as
    :class:`ApiError`, so there is exactly one place that turns a problem into
    a response.
    """

    def __init__(
        self,
        token: Optional[str] = None,
        manager_factory: Optional[Callable[..., Any]] = None,
        password_provider: Optional[Callable[[], Optional[str]]] = None,
        default_email: Optional[str] = None,
        allowed_roots: Optional[Iterable[Path]] = None,
        allowed_hosts: Optional[Iterable[str]] = None,
        default_local: Optional[Path] = None,
        expiry_probe: Optional[Callable[[str], Optional[float]]] = None,
        guard_scan: Optional[Callable[[Path], Dict[str, Any]]] = None,
        vanish_check: Optional[Callable[[Path], Dict[str, Any]]] = None,
        auth_timeout: float = 180.0,
        code_timeout: float = 300.0,
        static_dir: Optional[Path] = None,
    ) -> None:
        self.token = token or secrets.token_urlsafe(32)
        self.allowed_hosts = frozenset(allowed_hosts or LOOPBACK_HOSTS)
        self.allowed_roots = [
            _resolve(Path(root)) for root in (allowed_roots or default_allowed_roots())
        ]
        self.default_email = default_email
        self.default_local = _resolve(
            Path(default_local) if default_local else Path.home() / "icloud-backup"
        )
        self.static_dir = _resolve(Path(static_dir) if static_dir else STATIC_DIR)
        self.version = app_version()
        self.jobs = JobRunner()
        self.auth = AuthSession(
            manager_factory or build_manager_factory(),
            password_provider or build_password_provider(),
            expiry_probe=expiry_probe,
            auth_timeout=auth_timeout,
            code_timeout=code_timeout,
        )
        self._guard_scan = guard_scan or default_guard_scan
        self._vanish_check = vanish_check or default_vanish_check

        self.routes: Dict[str, Tuple[str, Endpoint]] = {
            "/api/state": ("GET", self.get_state),
            "/api/browse": ("GET", self.browse),
            "/api/auth/start": ("POST", self.auth_start),
            "/api/auth/2fa": ("POST", self.auth_2fa),
            "/api/auth/signout": ("POST", self.auth_signout),
            "/api/download": ("POST", self.start_download),
            "/api/guard": ("POST", self.start_guard),
            "/api/vanish": ("POST", self.start_vanish),
            "/api/cancel": ("POST", self.cancel),
        }

    def matches_token(self, candidate: Optional[str]) -> bool:
        if not candidate:
            return False
        return hmac.compare_digest(candidate, self.token)

    # -- page ---------------------------------------------------------------
    def page(self) -> Tuple[bytes, str]:
        """The single page, or a plain-text note that it was never installed."""
        try:
            body = self.static_dir.joinpath("index.html").read_bytes()
            return body, "text/html; charset=utf-8"
        except OSError:
            return MISSING_PAGE.encode("utf-8"), "text/plain; charset=utf-8"

    def asset(self, name: str) -> Tuple[bytes, str]:
        """A file the page asks for alongside itself, from the static folder only."""
        if not name or any(part in ("", ".", "..") for part in name.split("/")):
            raise ApiError(404, "No such asset.")
        target = _resolve(self.static_dir / name)
        if self.static_dir not in target.parents:
            raise ApiError(404, "No such asset.")
        try:
            body = target.read_bytes()
        except OSError:
            raise ApiError(404, "No such asset.") from None
        guessed, _ = mimetypes.guess_type(target.name)
        return body, guessed or "application/octet-stream"

    # -- state --------------------------------------------------------------
    def get_state(self, payload: Dict[str, Any]) -> Tuple[int, Dict[str, Any]]:
        job = self.jobs.current
        last = self.jobs.last
        return 200, {
            "version": self.version,
            "auth": self.auth.describe(),
            "job": job.to_dict() if job else None,
            "last": last.to_dict() if last else None,
            "paths": {
                "default_local": str(self.default_local),
                "icloud_drive": self._icloud_drive(),
            },
        }

    @staticmethod
    def _icloud_drive() -> Optional[str]:
        from ..guard import default_icloud_folder

        folder = default_icloud_folder()
        return str(folder) if folder.is_dir() else None

    # -- sign in ------------------------------------------------------------
    def auth_start(self, payload: Dict[str, Any]) -> Tuple[int, Dict[str, Any]]:
        self._refuse_password(payload)
        raw = payload.get("email") or self.default_email or ""
        if not isinstance(raw, str) or not raw.strip():
            raise ApiError(400, "An Apple ID email address is required.")
        return 200, self.auth.start(raw.strip())

    def auth_2fa(self, payload: Dict[str, Any]) -> Tuple[int, Dict[str, Any]]:
        self._refuse_password(payload)
        from ..auth import extract_code

        code = extract_code(payload.get("code"))
        if not code:
            raise ApiError(400, "Enter the six-digit code Apple sent to your devices.")
        return 200, self.auth.submit_code(code)

    def auth_signout(self, payload: Dict[str, Any]) -> Tuple[int, Dict[str, Any]]:
        return 200, self.auth.sign_out()

    @staticmethod
    def _refuse_password(payload: Dict[str, Any]) -> None:
        """The browser has no business holding this, so it is not accepted."""
        if "password" in payload:
            raise ApiError(
                400,
                "The Apple ID password is never accepted from the browser. It is "
                "read on this machine from the keyring or --password-command.",
            )

    # -- browse -------------------------------------------------------------
    def browse(self, payload: Dict[str, Any]) -> Tuple[int, Dict[str, Any]]:
        manager = self.auth.require_manager()
        path = clean_remote_path(payload.get("path"), field="path")
        try:
            item = manager.get_drive_item(path)
        except Exception as exc:  # noqa: BLE001 - Apple's wording is the answer
            raise ApiError(404, str(exc) or f"Could not open '{path}'.") from None

        lister = getattr(item, "dir", None)
        if not callable(lister):
            raise ApiError(400, f"'{path}' is a file, not a folder.")

        entries = []
        for name in lister() or []:
            try:
                child = item[name]
            except Exception:  # noqa: BLE001 - one bad child is not a dead listing
                logger.warning("could not resolve '%s' inside '%s'", name, path)
                continue
            if can_read_file(child):
                kind = "package" if is_package_name(name) else "file"
                size = getattr(child, "size", None)
                entries.append({
                    "name": name,
                    "kind": kind,
                    "size": size if isinstance(size, int) else None,
                })
            else:
                entries.append({"name": name, "kind": "dir", "size": None})

        entries.sort(key=lambda e: (e["kind"] != "dir", e["name"].lower()))
        return 200, {"path": path, "parent": _parent_of(path), "entries": entries}

    # -- jobs ---------------------------------------------------------------
    def start_download(self, payload: Dict[str, Any]) -> Tuple[int, Dict[str, Any]]:
        manager = self.auth.require_manager()
        icloud_path = clean_remote_path(payload.get("icloud_path"))
        local_path = clean_local_path(payload.get("local_path"), self.allowed_roots)

        def work(job):
            with attach(manager, job) as notes:
                if notes:
                    job.message = "; ".join(notes)
                manager.download(icloud_path, str(local_path))
                report = getattr(manager, "generate_summary_report", None)
                return report() if callable(report) else None

        label = f"{icloud_path or '/'} -> {shorten(local_path)}"
        return self._start("download", label, work)

    def start_guard(self, payload: Dict[str, Any]) -> Tuple[int, Dict[str, Any]]:
        raw = payload.get("local_path")
        if raw is None:
            from ..guard import default_icloud_folder

            raw = str(default_icloud_folder())
        root = clean_local_path(raw, self.allowed_roots)
        if not root.is_dir():
            raise ApiError(400, f"'{root}' does not exist, so there is nothing to check.")
        return self._start(
            "guard", f"guard {shorten(root)}", lambda job: self._guard_scan(root),
            note=SCAN_NOTE,
        )

    def start_vanish(self, payload: Dict[str, Any]) -> Tuple[int, Dict[str, Any]]:
        root = clean_local_path(payload.get("local_path"), self.allowed_roots)
        if not root.is_dir():
            raise ApiError(400, f"'{root}' does not exist, so there is nothing to compare.")
        return self._start(
            "vanish", f"vanish {shorten(root)}", lambda job: self._vanish_check(root),
            note=SCAN_NOTE,
        )

    def _start(self, kind: str, label: str, work, note: str = "") -> Tuple[int, Dict[str, Any]]:
        try:
            job = self.jobs.start(kind, label, work)
        except JobConflict as exc:
            raise ApiError(409, str(exc)) from None
        if note:
            job.message = note
        return 200, {"ok": True, "job_id": job.id}

    def cancel(self, payload: Dict[str, Any]) -> Tuple[int, Dict[str, Any]]:
        if self.jobs.cancel() is None:
            raise ApiError(409, "No job is running, so there is nothing to cancel.")
        return 200, {"ok": True}


def _parent_of(path: str) -> Optional[str]:
    """``None`` at the root - the page uses it to hide the "up" control."""
    if not path:
        return None
    head, _, _ = path.rpartition("/")
    return head


# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------

class _Handler(BaseHTTPRequestHandler):
    """Access control first, routing second, and one place that writes headers."""

    server_version = "iFetch"
    sys_version = ""
    protocol_version = "HTTP/1.1"

    def do_GET(self):
        self._dispatch("GET")

    def do_HEAD(self):
        self._dispatch("HEAD")

    def do_POST(self):
        self._dispatch("POST")

    def do_PUT(self):
        self._dispatch("PUT")

    def do_PATCH(self):
        self._dispatch("PATCH")

    def do_DELETE(self):
        self._dispatch("DELETE")

    def do_OPTIONS(self):
        self._dispatch("OPTIONS")

    # -- logging: never the query string, which is where the token arrives ---
    def log_request(self, code="-", size="-"):
        path = urlsplit(getattr(self, "path", "") or "").path
        logger.info(
            '%s "%s %s" %s %s', self.address_string(), self.command or "-", path, code, size
        )

    def log_message(self, fmt, *args):
        logger.info("%s %s", self.address_string(), fmt % args)

    def log_error(self, fmt, *args):
        logger.warning("%s %s", self.address_string(), fmt % args)

    # -- the request --------------------------------------------------------
    def _dispatch(self, method: str) -> None:
        self._suppress_body = method == "HEAD"
        app: WebUIApp = self.server.app  # type: ignore[attr-defined]
        try:
            if not host_is_allowed(self.headers.get("Host"), app.allowed_hosts):
                self._deny()
                return

            parts = urlsplit(self.path or "/")
            query = {k: v[0] for k, v in parse_qs(parts.query).items() if v}
            from_url = app.matches_token(query.get("t"))
            if not (from_url or app.matches_token(self._cookie_token())):
                self._deny()
                return

            route = parts.path.rstrip("/") or "/"
            if route == "/":
                self._serve_root(method, from_url, app)
            elif route.startswith("/static/"):
                self._serve_asset(method, route[len("/static/"):], app)
            else:
                self._serve_api(method, route, query, app)
        except ApiError as exc:
            self._send_json(exc.status, {"ok": False, "error": exc.message})
        except (BrokenPipeError, ConnectionResetError):
            self.close_connection = True
        except Exception:  # noqa: BLE001 - a traceback is not an API response
            logger.exception("unhandled error serving %s", urlsplit(self.path or "").path)
            self._send_json(500, {
                "ok": False,
                "error": "The server hit an unexpected error; its log has the details.",
            })

    def _serve_root(self, method: str, from_url: bool, app: WebUIApp) -> None:
        if method not in ("GET", "HEAD"):
            raise ApiError(405, "The page is fetched with GET.")
        if from_url:
            # Move the token out of the URL bar and into a cookie the page
            # cannot read, then reload without it so it stops being copyable.
            cookie = (
                f"{COOKIE_NAME}={app.token}; Path=/; HttpOnly; SameSite=Strict"
            )
            self._send_bytes(302, b"", "text/plain; charset=utf-8",
                             [("Location", "/"), ("Set-Cookie", cookie)])
            return
        body, content_type = app.page()
        self._send_bytes(200, body, content_type)

    def _serve_asset(self, method: str, name: str, app: WebUIApp) -> None:
        if method not in ("GET", "HEAD"):
            raise ApiError(405, "Assets are fetched with GET.")
        body, content_type = app.asset(name)
        self._send_bytes(200, body, content_type)

    def _serve_api(self, method: str, route: str, query: Dict[str, str], app: WebUIApp) -> None:
        entry = app.routes.get(route)
        body = self._read_json_body() if method == "POST" else {}
        if entry is None:
            raise ApiError(404, f"No such endpoint: {route}.")
        wanted, handler = entry
        if method != wanted:
            self._send_json(
                405,
                {"ok": False, "error": f"{route} accepts {wanted} requests only."},
                [("Allow", wanted)],
            )
            return
        status, payload = handler(body if method == "POST" else query)
        self._send_json(status, payload)

    def _cookie_token(self) -> Optional[str]:
        raw = self.headers.get("Cookie")
        if not raw:
            return None
        jar = http.cookies.SimpleCookie()
        try:
            jar.load(raw)
        except http.cookies.CookieError:
            return None
        morsel = jar.get(COOKIE_NAME)
        return morsel.value if morsel else None

    def _read_json_body(self) -> Dict[str, Any]:
        if self.headers.get("Transfer-Encoding"):
            self.close_connection = True
            raise ApiError(400, "Chunked request bodies are not accepted; send Content-Length.")
        raw = self.headers.get("Content-Length")
        if raw is None:
            return {}
        try:
            length = int(raw)
        except ValueError:
            self.close_connection = True
            raise ApiError(400, "Content-Length is not a number.") from None
        if length < 0:
            self.close_connection = True
            raise ApiError(400, "Content-Length is negative.")
        if length > MAX_BODY_BYTES:
            # Refused before it is read; buffering it to find out what it was is
            # the whole problem.
            self.close_connection = True
            raise ApiError(413, f"Request body is larger than {MAX_BODY_BYTES} bytes.")
        if length == 0:
            return {}
        data = self.rfile.read(length)
        if len(data) != length:
            self.close_connection = True
            raise ApiError(400, "Request body was shorter than Content-Length said.")
        try:
            parsed = json.loads(data.decode("utf-8"))
        except (UnicodeDecodeError, ValueError):
            raise ApiError(400, "Request body is not valid JSON.") from None
        if not isinstance(parsed, dict):
            raise ApiError(400, "Request body must be a JSON object.")
        return parsed

    # -- responses ----------------------------------------------------------
    def _deny(self) -> None:
        """What an unauthorised request gets: the same nothing a bare port gives."""
        self.close_connection = True
        self._send_bytes(404, b"Not Found\n", "text/plain; charset=utf-8")

    def _send_json(self, status: int, payload: Dict[str, Any],
                   extra: Optional[List[Tuple[str, str]]] = None) -> None:
        body = json.dumps(payload).encode("utf-8")
        self._send_bytes(status, body, "application/json", extra)

    def _send_bytes(self, status: int, body: bytes, content_type: str,
                    extra: Optional[List[Tuple[str, str]]] = None) -> None:
        # The only place a header is written. There is no branch here that adds
        # Access-Control-Allow-Origin, which is how "never" is enforced.
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        # Without this the token in the first URL would be handed to every site
        # the page ever links to.
        self.send_header("Referrer-Policy", "no-referrer")
        for name, value in extra or []:
            self.send_header(name, value)
        self.end_headers()
        if body and not getattr(self, "_suppress_body", False):
            self.wfile.write(body)


class _Server(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, address: Tuple[str, int], app: WebUIApp) -> None:
        self.app = app
        super().__init__(address, _Handler)


class WebUIServer:
    """Owns the socket and the thread, so callers can start and stop cleanly."""

    def __init__(self, app: WebUIApp, host: str = "127.0.0.1", port: int = 8765) -> None:
        self.app = app
        self.requested_host = host
        self._http = _Server((host, port), app)
        self._thread: Optional[threading.Thread] = None
        self._serving = threading.Event()

    @property
    def port(self) -> int:
        return int(self._http.server_address[1])

    @property
    def token(self) -> str:
        return self.app.token

    @property
    def url(self) -> str:
        """The URL to open, token and all. Printed once; never logged."""
        host = self.requested_host
        if host in WILDCARD_HOSTS:
            host = "127.0.0.1"
        if ":" in host:
            host = f"[{host}]"
        return f"http://{host}:{self.port}/?t={self.app.token}"

    def start(self) -> "WebUIServer":
        self._serving.set()
        self._thread = threading.Thread(
            target=self._http.serve_forever, name="ifetch-webui", daemon=True
        )
        self._thread.start()
        return self

    def serve_forever(self) -> None:
        self._serving.set()
        try:
            self._http.serve_forever()
        finally:
            self._serving.clear()

    def stop(self, timeout: float = 5.0) -> None:
        # shutdown() waits for the accept loop to acknowledge it, so asking a
        # server that was bound but never started would wait for ever.
        if self._serving.is_set():
            self._http.shutdown()
            self._serving.clear()
        self._http.server_close()
        if self._thread is not None:
            self._thread.join(timeout)
            self._thread = None
        # A job still writing after the socket closes would keep writing to a
        # destination nobody is watching, so it is asked to stop too.
        self.app.jobs.cancel()


def create_server(
    host: str = "127.0.0.1",
    port: int = 8765,
    token: Optional[str] = None,
    allow_hosts: Optional[Iterable[str]] = None,
    **app_kwargs: Any,
) -> WebUIServer:
    """Build the app and bind the socket.

    ``allowed_hosts`` is the loopback set plus the bind address when that is a
    real address, because a server told to listen on 192.168.1.5 must accept a
    ``Host`` of 192.168.1.5 or it answers nobody at all. A wildcard bind names
    no host, so only loopback is accepted unless more are given explicitly.
    """
    hosts = set(LOOPBACK_HOSTS)
    if host not in WILDCARD_HOSTS:
        hosts.add(host)
    hosts.update(allow_hosts or ())
    app = WebUIApp(token=token, allowed_hosts=hosts, **app_kwargs)
    return WebUIServer(app, host=host, port=port)
