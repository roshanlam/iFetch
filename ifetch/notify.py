"""Telling somebody the run happened - without ever being the reason it didn't.

Why this exists
---------------
iFetch is meant to be scheduled: a cron entry on a NAS, a systemd timer, a
container that wakes at 02:30. The way scheduled backups fail is not a crash,
it is *silence*. The job stops running in March and nobody notices until July,
when the file is needed. A log nobody reads does not fix that. Two things do:

* a **dead-man's switch** (Healthchecks.io), which alerts on the run that did
  *not* happen - the only signal that survives the machine being off, the disk
  being full, the cron entry being deleted, or Python failing to start;
* a **push** (ntfy, or any webhook), which puts the result on a phone.

What it refuses to do
---------------------
**A notification never fails a run.** A dead endpoint, a wrong URL, a 500, an
HTML error page where JSON was expected, a hung socket, or a bug in a backend
all become one logged warning and a recorded :class:`DeliveryResult`.
:meth:`Notifier.notify` has no path that raises at the caller.
``KeyboardInterrupt`` and ``SystemExit`` are deliberately not caught, because
swallowing a Ctrl-C is worse than a missed ping.

**It never invents a state.** A run that finished with some files failing is
neither a failed run nor a clean one. Treating it as either is how people learn
to ignore alerts, so ``anomaly`` is its own event. Healthchecks only has up and
down, so an anomaly is posted to ``/log``, which records the text without
turning the check red. Set ``IFETCH_HEALTHCHECKS_ANOMALY_FAILS=1`` to be paged
instead - that mapping is your choice, not a default made quietly for you.

**Nothing undelivered is dropped quietly.** Every attempt produces a
:class:`DeliveryResult` with the backend, attempt count, status code and a
redacted error, and :meth:`Notifier.report_snapshot` returns the lot, so a run's
summary can say "the ntfy push did not go out" rather than implying it did.

Secrets
-------
A Healthchecks ping URL *is* the credential: anyone holding it can mark the
check up and suppress your alerts forever. So is an ntfy topic URL, a webhook
URL, or a bearer token. None may appear in a log line, a JSON report, or an
exception message - and ``requests`` puts the full URL into the text of nearly
every exception it raises, so exceptions are where this actually leaks.
Everything logged or returned goes through a :class:`Redactor`, and URLs that
need to stay diagnosable go through :func:`safe_url`, which keeps the scheme and
host (so "my server is unreachable" still reads) and drops the path.

Configuration
-------------
Flags and environment variables both work and an explicit flag wins. With
nothing configured this is a silent no-op, not a warning: someone who never
asked for notifications should not be reminded of them every run. Containers are
configured by environment, so that path carries the weight - see
:data:`NOTIFY_ENV_VARS`.
"""

from __future__ import annotations

import json
import logging
import os
import re
import socket
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple
from urllib.parse import urlsplit

from .render import human_bytes, human_duration

__all__ = [
    "EVENT_START",
    "EVENT_SUCCESS",
    "EVENT_FAILURE",
    "EVENT_ANOMALY",
    "EVENTS",
    "REDACTED",
    "NOTIFY_ENV_VARS",
    "SECRET_ENV_VARS",
    "NotifyError",
    "NotifyConfigError",
    "Redactor",
    "safe_url",
    "RunEvent",
    "DeliveryResult",
    "HttpTransport",
    "NotificationBackend",
    "HealthchecksBackend",
    "NtfyBackend",
    "WebhookBackend",
    "NotifyConfig",
    "Notifier",
    "NullNotifier",
    "build_notifier",
    "add_notification_arguments",
    "format_run_summary",
    "anomalies_from_report",
]

logger = logging.getLogger("ifetch.notify")

#: Events a run can report. ``anomaly`` is deliberately not ``failure``: a run
#: that completed and lost three files is a different fact from a run that did
#: not complete, and an alerting system that cannot tell them apart is one
#: people stop reading.
EVENT_START = "start"
EVENT_SUCCESS = "success"
EVENT_FAILURE = "failure"
EVENT_ANOMALY = "anomaly"
EVENTS: Tuple[str, ...] = (EVENT_START, EVENT_SUCCESS, EVENT_FAILURE, EVENT_ANOMALY)

#: What a redacted secret is replaced with. Short, and obviously not a value.
REDACTED = "***"

#: Short by design. A notification endpoint that has not answered in five
#: seconds is not going to; waiting longer only delays the backup.
DEFAULT_TIMEOUT = 5.0
#: Retries *after* the first attempt, so three requests at most by default.
DEFAULT_RETRIES = 2
DEFAULT_BACKOFF = 1.0
MAX_BACKOFF = 10.0

#: Healthchecks truncates ping bodies server-side (10 KB by default); we
#: truncate first so the request itself stays small.
MAX_BODY_BYTES = 10_000
#: ntfy message bodies are shown on a phone. Anything longer is not read.
MAX_NTFY_BODY_BYTES = 4_000

#: Strings shorter than this are not treated as secrets: redacting "1" or
#: "abc" would corrupt every log line in the process.
MIN_SECRET_LENGTH = 8

_TRUTHY = {"1", "true", "yes", "on"}
_FALSEY = {"0", "false", "no", "off"}

# Suffixes a user may have pasted along with a Healthchecks ping URL. The base
# URL is what we need; appending "/start" to ".../fail" pings nothing.
_HC_SUFFIXES = ("/start", "/fail", "/log", "/0", "/1")

_UUID_RE = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")


# ---------------------------------------------------------------------------
# Environment variables
# ---------------------------------------------------------------------------

ENV_DISABLED = "IFETCH_NOTIFY_DISABLED"
ENV_HC_URL = "IFETCH_HEALTHCHECKS_URL"
ENV_HC_UUID = "IFETCH_HEALTHCHECKS_UUID"
ENV_HC_BASE_URL = "IFETCH_HEALTHCHECKS_BASE_URL"
ENV_HC_ANOMALY_FAILS = "IFETCH_HEALTHCHECKS_ANOMALY_FAILS"
ENV_NTFY_URL = "IFETCH_NTFY_URL"
ENV_NTFY_TOPIC = "IFETCH_NTFY_TOPIC"
ENV_NTFY_SERVER = "IFETCH_NTFY_SERVER"
ENV_NTFY_TOKEN = "IFETCH_NTFY_TOKEN"
ENV_NTFY_PRIORITY = "IFETCH_NTFY_PRIORITY"
ENV_NTFY_TAGS = "IFETCH_NTFY_TAGS"
ENV_WEBHOOK_URL = "IFETCH_WEBHOOK_URL"
ENV_WEBHOOK_HEADERS = "IFETCH_WEBHOOK_HEADERS"
ENV_WEBHOOK_METHOD = "IFETCH_WEBHOOK_METHOD"
ENV_TIMEOUT = "IFETCH_NOTIFY_TIMEOUT"
ENV_RETRIES = "IFETCH_NOTIFY_RETRIES"
ENV_BACKOFF = "IFETCH_NOTIFY_BACKOFF"

#: Every variable this module reads, for documentation and for tests that must
#: keep docs and code in step.
NOTIFY_ENV_VARS: Tuple[str, ...] = (
    ENV_DISABLED,
    ENV_HC_URL,
    ENV_HC_UUID,
    ENV_HC_BASE_URL,
    ENV_HC_ANOMALY_FAILS,
    ENV_NTFY_URL,
    ENV_NTFY_TOPIC,
    ENV_NTFY_SERVER,
    ENV_NTFY_TOKEN,
    ENV_NTFY_PRIORITY,
    ENV_NTFY_TAGS,
    ENV_WEBHOOK_URL,
    ENV_WEBHOOK_HEADERS,
    ENV_WEBHOOK_METHOD,
    ENV_TIMEOUT,
    ENV_RETRIES,
    ENV_BACKOFF,
)

#: The subset whose *values* are credentials. Anything read from one of these
#: is registered with the redactor before it can reach a log line.
SECRET_ENV_VARS: Tuple[str, ...] = (
    ENV_HC_URL,
    ENV_HC_UUID,
    ENV_NTFY_URL,
    ENV_NTFY_TOPIC,
    ENV_NTFY_TOKEN,
    ENV_WEBHOOK_URL,
    ENV_WEBHOOK_HEADERS,
)


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------

class NotifyError(Exception):
    """A delivery failed.

    Raised inside the notification subsystem and caught by
    :meth:`Notifier.notify`; it is never allowed to reach a caller. Its message
    is redacted at construction sites, because ``requests`` embeds the full
    request URL - which is the credential - in almost every exception it raises.
    """

    def __init__(
        self,
        message: str,
        *,
        attempts: int = 1,
        status_code: Optional[int] = None,
        retryable: bool = False,
    ):
        super().__init__(message)
        self.attempts = attempts
        self.status_code = status_code
        self.retryable = retryable


class NotifyConfigError(NotifyError):
    """A backend was asked for with configuration it cannot work from."""


# ---------------------------------------------------------------------------
# Redaction
# ---------------------------------------------------------------------------

class Redactor:
    """Replaces known secret substrings wherever they appear in text.

    Deliberately dumb: substring replacement, longest first. A cleverer scheme
    (regex per credential shape) would miss the case that actually matters -
    a ``requests`` exception carrying a ping URL that is not shaped like
    anything in particular. Substrings catch it because the exact string is
    known.
    """

    def __init__(self, secrets: Iterable[Any] = ()):
        self._secrets: List[str] = []
        for secret in secrets:
            self.add(secret)

    def add(self, secret: Any) -> None:
        """Register a value that must never be printed.

        Values shorter than :data:`MIN_SECRET_LENGTH` are ignored: redacting a
        four-character token would mangle unrelated text far more often than it
        would protect anything.
        """
        if secret is None:
            return
        text = str(secret).strip()
        if len(text) < MIN_SECRET_LENGTH or text in self._secrets:
            return
        self._secrets.append(text)
        # Longest first: a ping URL contains its own UUID, and redacting the
        # UUID first would leave "https://hc-ping.com/***" *plus* a partially
        # rewritten longer secret that no longer matches.
        self._secrets.sort(key=len, reverse=True)

    def merge(self, other: "Redactor") -> "Redactor":
        """Absorb another redactor's secrets.

        Needed because a backend can be constructed on its own - with its own
        URL and token - and then handed to a :class:`Notifier` that would
        otherwise log that backend's failures unscrubbed.
        """
        if isinstance(other, Redactor) and other is not self:
            for secret in other._secrets:
                self.add(secret)
        return self

    def redact(self, value: Any) -> str:
        text = value if isinstance(value, str) else str(value)
        for secret in self._secrets:
            if secret in text:
                text = text.replace(secret, REDACTED)
        return text

    def __call__(self, value: Any) -> str:
        return self.redact(value)

    def __len__(self) -> int:  # the values themselves are never exposed
        return len(self._secrets)


def safe_url(url: Any) -> str:
    """Render a URL with its scheme and host intact and its path destroyed.

    The host is the diagnostic half ("my self-hosted Healthchecks is
    unreachable") and the path is the credential half. Userinfo
    (``https://user:pass@host/``) is dropped entirely.
    """
    if not url:
        return REDACTED
    try:
        parts = urlsplit(str(url))
    except (ValueError, TypeError):
        return REDACTED
    if not parts.scheme or not parts.hostname:
        return REDACTED
    host = parts.hostname
    if parts.port:
        host = f"{host}:{parts.port}"
    if parts.path.strip("/") or parts.query:
        return f"{parts.scheme}://{host}/{REDACTED}"
    return f"{parts.scheme}://{host}"


# ---------------------------------------------------------------------------
# Events
# ---------------------------------------------------------------------------

@dataclass
class RunEvent:
    """One thing worth telling somebody about.

    ``details`` is carried verbatim into the webhook JSON body and rendered
    into the human text, so callers must not put credentials in it.
    """

    event: str
    title: str
    message: str = ""
    details: Dict[str, Any] = field(default_factory=dict)
    run_id: str = ""
    timestamp: float = field(default_factory=time.time)
    host: str = ""

    def __post_init__(self) -> None:
        if not self.host:
            try:
                self.host = socket.gethostname()
            except OSError:  # containers with no resolvable hostname
                self.host = "unknown-host"

    @property
    def is_failure(self) -> bool:
        return self.event == EVENT_FAILURE

    def to_dict(self) -> Dict[str, Any]:
        """JSON-safe payload. This is the generic webhook body."""
        return {
            "source": "ifetch",
            "event": self.event,
            "title": self.title,
            "message": self.message,
            "run_id": self.run_id,
            "host": self.host,
            "timestamp": self.timestamp,
            "timestamp_iso": time.strftime(
                "%Y-%m-%dT%H:%M:%S", time.gmtime(self.timestamp)
            ) + "Z",
            "details": _jsonable(self.details),
        }

    def body_text(self) -> str:
        """The text a human reads in Healthchecks or on a phone."""
        lines = [self.title]
        if self.message:
            lines.append("")
            lines.append(self.message)
        if self.details:
            lines.append("")
            for key, value in self.details.items():
                lines.append(f"{_humanise_key(key)}: {_render_value(value)}")
        lines.append("")
        lines.append(f"host: {self.host}")
        if self.run_id:
            lines.append(f"run: {self.run_id}")
        return "\n".join(lines)


@dataclass
class DeliveryResult:
    """What happened to one notification on one backend.

    ``error`` is already redacted; it is safe to log and safe to serialise into
    the run's JSON report.
    """

    backend: str
    event: str
    delivered: bool
    attempts: int = 0
    status_code: Optional[int] = None
    error: Optional[str] = None
    target: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "backend": self.backend,
            "event": self.event,
            "delivered": self.delivered,
            "attempts": self.attempts,
            "status_code": self.status_code,
            "error": self.error,
            "target": self.target,
        }


@dataclass
class TransportResult:
    """A successful HTTP exchange, and how much work it took."""

    status_code: int
    attempts: int
    response: Any = None


# ---------------------------------------------------------------------------
# Transport
# ---------------------------------------------------------------------------

@dataclass
class HttpTransport:
    """A tiny, always-timing-out, boundedly-retrying HTTP caller.

    ``requests`` is imported lazily and only ever reached through
    :attr:`session`, so tests inject a double and no test in this repository
    touches the network. ``sleep`` is injected for the same reason: backoff is
    verified against a fake clock rather than by actually waiting.
    """

    timeout: float = DEFAULT_TIMEOUT
    retries: int = DEFAULT_RETRIES
    backoff: float = DEFAULT_BACKOFF
    max_backoff: float = MAX_BACKOFF
    session: Any = None
    sleep: Callable[[float], None] = time.sleep
    redactor: Redactor = field(default_factory=Redactor)

    def request(self, method: str, url: str, **kwargs: Any) -> TransportResult:
        """Perform the request, retrying only what is worth retrying.

        Retried: connection errors, timeouts, HTTP 5xx and 429 - conditions
        that are plausibly transient. Not retried: 4xx (a 404 means the check
        does not exist; asking three times does not create it) and unexpected
        exceptions, which are far more likely to be a bug or bad configuration
        than a blip. Raises :class:`NotifyError` when every attempt is spent.
        """
        # A timeout is not optional and not overridable-to-None. A hung
        # notification endpoint holding a backup open indefinitely is the exact
        # failure this module exists to avoid causing.
        kwargs["timeout"] = kwargs.get("timeout") or self.timeout

        total = max(1, int(self.retries) + 1)
        attempts = 0
        failure: NotifyError = NotifyError("no attempt was made")

        while attempts < total:
            attempts += 1
            try:
                response = self._send(method, url, **kwargs)
            except Exception as exc:  # noqa: BLE001 - classified below
                failure = NotifyError(
                    f"{type(exc).__name__}: {self.redactor(exc)}",
                    attempts=attempts,
                    retryable=_is_retryable_exception(exc),
                )
            else:
                status = _status_of(response)
                if status is None:
                    # A response object we cannot read a status from is a
                    # malformed answer, not a transient one. Say so.
                    failure = NotifyError(
                        "response carried no usable status code",
                        attempts=attempts,
                        retryable=False,
                    )
                elif 200 <= status < 300:
                    return TransportResult(
                        status_code=status, attempts=attempts, response=response
                    )
                else:
                    failure = NotifyError(
                        f"HTTP {status}",
                        attempts=attempts,
                        status_code=status,
                        retryable=status >= 500 or status == 429,
                    )

            if not failure.retryable or attempts >= total:
                break
            self.sleep(self._delay(attempts))

        failure.attempts = attempts
        raise failure

    def _delay(self, attempt: int) -> float:
        """Exponential backoff, capped. No jitter: with at most three requests
        to a single endpoint there is no thundering herd to spread out, and a
        deterministic delay is a testable one."""
        return min(self.max_backoff, self.backoff * (2 ** (attempt - 1)))

    def _send(self, method: str, url: str, **kwargs: Any) -> Any:
        session = self.session
        if session is None:
            import requests  # lazy: unused until something is configured

            session = requests
        return session.request(method, url, **kwargs)


def _is_retryable_exception(exc: BaseException) -> bool:
    try:
        import requests
    except Exception:  # pragma: no cover - requests is a hard dependency
        return False
    retryable = (
        requests.exceptions.Timeout,
        requests.exceptions.ConnectionError,
        requests.exceptions.ChunkedEncodingError,
    )
    return isinstance(exc, retryable)


def _status_of(response: Any) -> Optional[int]:
    """Read ``status_code`` from anything, or admit we could not."""
    try:
        return int(getattr(response, "status_code"))
    except (AttributeError, TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# Backends
# ---------------------------------------------------------------------------

class NotificationBackend:
    """One delivery mechanism.

    Subclasses implement :meth:`deliver` and may raise :class:`NotifyError`;
    containment is :class:`Notifier`'s job, so that every backend gets the same
    treatment and none can forget to be safe.
    """

    name = "backend"

    def __init__(
        self,
        transport: Optional[HttpTransport] = None,
        redactor: Optional[Redactor] = None,
    ):
        self.redactor = redactor if redactor is not None else Redactor()
        self.transport = transport if transport is not None else HttpTransport()
        # A transport built for us shares our redactor so that exception text
        # produced deep inside it is scrubbed at the point it is stringified.
        if transport is None:
            self.transport.redactor = self.redactor

    def describe(self) -> str:
        """A one-line, redaction-safe description for logs and reports."""
        return self.name

    def deliver(self, event: RunEvent) -> DeliveryResult:  # pragma: no cover
        raise NotImplementedError


class HealthchecksBackend(NotificationBackend):
    """Healthchecks.io (or any self-hosted instance) as a dead-man's switch.

    ``hc-ping.com`` is only the default. Self-hosting Healthchecks is common in
    exactly the homelab population this targets, so a full ping URL on any host
    is the primary input and the UUID form is the convenience.

    Event mapping::

        start    POST <url>/start
        success  POST <url>
        failure  POST <url>/fail
        anomaly  POST <url>/log      (or /fail with anomaly_is_failure=True)

    ``/log`` records the message against the check without changing its state.
    That is the honest mapping for "the run finished and you should look at
    it": Healthchecks has two states and this is a third fact.
    """

    name = "healthchecks"
    DEFAULT_BASE_URL = "https://hc-ping.com"

    def __init__(
        self,
        url: Optional[str] = None,
        uuid_: Optional[str] = None,
        base_url: Optional[str] = None,
        anomaly_is_failure: bool = False,
        transport: Optional[HttpTransport] = None,
        redactor: Optional[Redactor] = None,
    ):
        super().__init__(transport=transport, redactor=redactor)
        self.anomaly_is_failure = bool(anomaly_is_failure)
        self.ping_url = self._resolve(url, uuid_, base_url)
        self.redactor.add(self.ping_url)
        # The trailing segment is the credential proper; it can appear on its
        # own in a log line the URL never reaches.
        self.redactor.add(self.ping_url.rsplit("/", 1)[-1])
        if uuid_:
            self.redactor.add(uuid_)

    @classmethod
    def _resolve(
        cls, url: Optional[str], uuid_: Optional[str], base_url: Optional[str]
    ) -> str:
        if url:
            candidate = str(url).strip().rstrip("/")
            # Accept a pasted ".../start" or ".../fail" rather than silently
            # pinging a URL that means something other than it looks like.
            lowered = candidate.lower()
            for suffix in _HC_SUFFIXES:
                if lowered.endswith(suffix):
                    candidate = candidate[: -len(suffix)]
                    break
        elif uuid_:
            base = (base_url or cls.DEFAULT_BASE_URL).strip().rstrip("/")
            candidate = f"{base}/{str(uuid_).strip().strip('/')}"
        else:
            raise NotifyConfigError(
                "Healthchecks needs either a full ping URL or a check UUID"
            )

        parts = urlsplit(candidate)
        if parts.scheme not in ("http", "https") or not parts.hostname:
            # The URL is a secret, so the complaint cannot quote it.
            raise NotifyConfigError(
                "Healthchecks ping URL must be an absolute http(s) URL "
                f"(got {safe_url(candidate)})"
            )
        return candidate

    def describe(self) -> str:
        return f"healthchecks -> {safe_url(self.ping_url)}"

    def endpoint_for(self, event: str) -> str:
        if event == EVENT_START:
            return f"{self.ping_url}/start"
        if event == EVENT_FAILURE:
            return f"{self.ping_url}/fail"
        if event == EVENT_ANOMALY:
            return f"{self.ping_url}/fail" if self.anomaly_is_failure else f"{self.ping_url}/log"
        return self.ping_url

    def deliver(self, event: RunEvent) -> DeliveryResult:
        url = self.endpoint_for(event.event)
        body = _truncate(event.body_text(), MAX_BODY_BYTES)
        params = {}
        # rid ties the /start ping to the finishing ping so Healthchecks can
        # report a duration. It must be a UUID or Healthchecks rejects it.
        if event.run_id and _UUID_RE.match(event.run_id):
            params["rid"] = event.run_id
        result = self.transport.request(
            "POST",
            url,
            data=body.encode("utf-8"),
            headers={"Content-Type": "text/plain; charset=utf-8"},
            params=params or None,
        )
        return DeliveryResult(
            backend=self.name,
            event=event.event,
            delivered=True,
            attempts=result.attempts,
            status_code=result.status_code,
            target=safe_url(url),
        )


class NtfyBackend(NotificationBackend):
    """ntfy push, on ntfy.sh or a self-hosted server.

    The topic URL is treated as a credential: on a public server, knowing the
    topic is the whole of the authorisation to publish to it.

    Priority and tags follow the event unless the operator pins them, because
    a start ping and a failure should not buzz a phone the same way.
    """

    name = "ntfy"
    DEFAULT_SERVER = "https://ntfy.sh"

    #: Per-event defaults. Failures are ``urgent`` so they bypass do-not-disturb
    #: on most clients; starts are ``min`` so a nightly job is not a nightly
    #: buzz.
    PRIORITIES = {
        EVENT_START: "min",
        EVENT_SUCCESS: "default",
        EVENT_ANOMALY: "high",
        EVENT_FAILURE: "urgent",
    }
    TAGS = {
        EVENT_START: ("hourglass_flowing_sand",),
        EVENT_SUCCESS: ("white_check_mark",),
        EVENT_ANOMALY: ("warning",),
        EVENT_FAILURE: ("rotating_light",),
    }

    def __init__(
        self,
        url: Optional[str] = None,
        topic: Optional[str] = None,
        server: Optional[str] = None,
        token: Optional[str] = None,
        priority: Optional[str] = None,
        tags: Sequence[str] = (),
        transport: Optional[HttpTransport] = None,
        redactor: Optional[Redactor] = None,
    ):
        super().__init__(transport=transport, redactor=redactor)
        self.token = (token or "").strip() or None
        self.priority = (priority or "").strip() or None
        self.tags = tuple(t for t in (tags or ()) if t)
        self.topic_url = self._resolve(url, topic, server)
        self.redactor.add(self.topic_url)
        self.redactor.add(self.token)
        if topic:
            self.redactor.add(topic)

    @classmethod
    def _resolve(
        cls, url: Optional[str], topic: Optional[str], server: Optional[str]
    ) -> str:
        if url:
            candidate = str(url).strip().rstrip("/")
        elif topic:
            base = (server or cls.DEFAULT_SERVER).strip().rstrip("/")
            candidate = f"{base}/{str(topic).strip().strip('/')}"
        else:
            raise NotifyConfigError("ntfy needs either a topic URL or a topic name")

        parts = urlsplit(candidate)
        if parts.scheme not in ("http", "https") or not parts.hostname:
            raise NotifyConfigError(
                "ntfy URL must be an absolute http(s) URL "
                f"(got {safe_url(candidate)})"
            )
        if not parts.path.strip("/"):
            raise NotifyConfigError(
                f"ntfy URL {safe_url(candidate)} names a server but no topic"
            )
        return candidate

    def describe(self) -> str:
        return f"ntfy -> {safe_url(self.topic_url)}"

    def headers_for(self, event: RunEvent) -> Dict[str, str]:
        tags = list(self.TAGS.get(event.event, ())) + list(self.tags)
        headers = {
            "Title": _header_value(event.title),
            "Priority": self.priority or self.PRIORITIES.get(event.event, "default"),
            "Content-Type": "text/plain; charset=utf-8",
        }
        if tags:
            headers["Tags"] = _header_value(",".join(tags))
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        return headers

    def deliver(self, event: RunEvent) -> DeliveryResult:
        body = _truncate(event.message or event.body_text(), MAX_NTFY_BODY_BYTES)
        if event.details:
            extra = "\n".join(
                f"{_humanise_key(k)}: {_render_value(v)}" for k, v in event.details.items()
            )
            body = _truncate(f"{body}\n\n{extra}" if body else extra, MAX_NTFY_BODY_BYTES)
        result = self.transport.request(
            "POST",
            self.topic_url,
            data=body.encode("utf-8"),
            headers=self.headers_for(event),
        )
        return DeliveryResult(
            backend=self.name,
            event=event.event,
            delivered=True,
            attempts=result.attempts,
            status_code=result.status_code,
            target=safe_url(self.topic_url),
        )


class WebhookBackend(NotificationBackend):
    """POST the event as JSON anywhere.

    The escape hatch for Discord relays, Gotify, Home Assistant, n8n, or a
    three-line Flask app. The URL and every header value are registered as
    secrets, because webhook URLs routinely *are* the token (Slack, Discord)
    and headers routinely carry one.
    """

    name = "webhook"

    def __init__(
        self,
        url: str,
        headers: Optional[Mapping[str, str]] = None,
        method: str = "POST",
        transport: Optional[HttpTransport] = None,
        redactor: Optional[Redactor] = None,
    ):
        super().__init__(transport=transport, redactor=redactor)
        candidate = str(url or "").strip()
        parts = urlsplit(candidate)
        if parts.scheme not in ("http", "https") or not parts.hostname:
            raise NotifyConfigError(
                f"webhook URL must be an absolute http(s) URL (got {safe_url(candidate)})"
            )
        self.url = candidate
        self.headers = {str(k): str(v) for k, v in (headers or {}).items()}
        self.method = (method or "POST").strip().upper() or "POST"
        self.redactor.add(self.url)
        for value in self.headers.values():
            self.redactor.add(value)

    def describe(self) -> str:
        return f"webhook -> {self.method} {safe_url(self.url)}"

    def deliver(self, event: RunEvent) -> DeliveryResult:
        headers = {"Content-Type": "application/json"}
        headers.update(self.headers)
        result = self.transport.request(
            self.method, self.url, json=event.to_dict(), headers=headers
        )
        return DeliveryResult(
            backend=self.name,
            event=event.event,
            delivered=True,
            attempts=result.attempts,
            status_code=result.status_code,
            target=safe_url(self.url),
        )


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

@dataclass
class NotifyConfig:
    """Resolved notification settings.

    Precedence is explicit-argument, then environment, then default. Flags win
    because a person typing one is making a decision about *this* run;
    environment carries the weight because that is how a container is
    configured and containers are the point.
    """

    enabled: bool = True
    healthchecks_url: Optional[str] = None
    healthchecks_uuid: Optional[str] = None
    healthchecks_base_url: Optional[str] = None
    healthchecks_anomaly_is_failure: bool = False
    ntfy_url: Optional[str] = None
    ntfy_topic: Optional[str] = None
    ntfy_server: Optional[str] = None
    ntfy_token: Optional[str] = None
    ntfy_priority: Optional[str] = None
    ntfy_tags: Tuple[str, ...] = ()
    webhook_url: Optional[str] = None
    webhook_headers: Dict[str, str] = field(default_factory=dict)
    webhook_method: str = "POST"
    timeout: float = DEFAULT_TIMEOUT
    retries: int = DEFAULT_RETRIES
    backoff: float = DEFAULT_BACKOFF

    @classmethod
    def from_env(cls, env: Optional[Mapping[str, str]] = None, **overrides: Any) -> "NotifyConfig":
        """Build a config from the environment, with non-``None`` overrides winning."""
        environ = os.environ if env is None else env

        def pick(name: str, key: str) -> Optional[str]:
            value = overrides.get(name)
            if value is not None:
                return str(value)
            raw = environ.get(key)
            return raw.strip() if raw and raw.strip() else None

        enabled = overrides.get("enabled")
        if enabled is None:
            enabled = not _as_bool(environ.get(ENV_DISABLED), default=False)

        tags = overrides.get("ntfy_tags")
        if tags is None:
            tags = _split_list(environ.get(ENV_NTFY_TAGS))
        else:
            tags = tuple(tags)

        headers = overrides.get("webhook_headers")
        if headers is None:
            headers = _parse_headers(environ.get(ENV_WEBHOOK_HEADERS))
        else:
            headers = _parse_headers(headers)

        anomaly_fails = overrides.get("healthchecks_anomaly_is_failure")
        if anomaly_fails is None:
            anomaly_fails = _as_bool(environ.get(ENV_HC_ANOMALY_FAILS), default=False)

        return cls(
            enabled=bool(enabled),
            healthchecks_url=pick("healthchecks_url", ENV_HC_URL),
            healthchecks_uuid=pick("healthchecks_uuid", ENV_HC_UUID),
            healthchecks_base_url=pick("healthchecks_base_url", ENV_HC_BASE_URL),
            healthchecks_anomaly_is_failure=bool(anomaly_fails),
            ntfy_url=pick("ntfy_url", ENV_NTFY_URL),
            ntfy_topic=pick("ntfy_topic", ENV_NTFY_TOPIC),
            ntfy_server=pick("ntfy_server", ENV_NTFY_SERVER),
            ntfy_token=pick("ntfy_token", ENV_NTFY_TOKEN),
            ntfy_priority=pick("ntfy_priority", ENV_NTFY_PRIORITY),
            ntfy_tags=tuple(tags),
            webhook_url=pick("webhook_url", ENV_WEBHOOK_URL),
            webhook_headers=dict(headers),
            webhook_method=(pick("webhook_method", ENV_WEBHOOK_METHOD) or "POST"),
            timeout=_as_float(
                overrides.get("timeout"), environ.get(ENV_TIMEOUT), DEFAULT_TIMEOUT
            ),
            retries=int(
                _as_float(overrides.get("retries"), environ.get(ENV_RETRIES), DEFAULT_RETRIES)
            ),
            backoff=_as_float(
                overrides.get("backoff"), environ.get(ENV_BACKOFF), DEFAULT_BACKOFF
            ),
        )

    @classmethod
    def from_args(
        cls, args: Any = None, env: Optional[Mapping[str, str]] = None
    ) -> "NotifyConfig":
        """Merge an ``argparse.Namespace`` (see :func:`add_notification_arguments`)
        over the environment. Attributes that are absent or ``None`` fall through."""
        overrides: Dict[str, Any] = {}
        if args is not None:
            for name in (
                "healthchecks_url",
                "healthchecks_uuid",
                "healthchecks_base_url",
                "ntfy_url",
                "ntfy_topic",
                "ntfy_server",
                "ntfy_token",
                "ntfy_priority",
                "webhook_url",
                "webhook_method",
                "timeout",
                "retries",
                "backoff",
            ):
                value = getattr(args, f"notify_{name}", None)
                if value is None:
                    value = getattr(args, name, None)
                if value is not None:
                    overrides[name] = value
            tags = getattr(args, "ntfy_tags", None)
            if tags:
                overrides["ntfy_tags"] = tuple(_split_list(tags) if isinstance(tags, str) else tags)
            headers = getattr(args, "webhook_header", None)
            if headers:
                overrides["webhook_headers"] = _parse_headers(headers)
            if getattr(args, "healthchecks_anomaly_fails", None):
                overrides["healthchecks_anomaly_is_failure"] = True
            notify = getattr(args, "notify", None)
            if notify is False:
                overrides["enabled"] = False
        return cls.from_env(env, **overrides)

    def is_configured(self) -> bool:
        """True when at least one backend has somewhere to send to."""
        return self.enabled and bool(
            self.healthchecks_url
            or self.healthchecks_uuid
            or self.ntfy_url
            or self.ntfy_topic
            or self.webhook_url
        )


def add_notification_arguments(parser: Any) -> Any:
    """Attach the notification flags to an ``argparse`` parser.

    Every flag documents its environment variable, because the environment is
    how these get set in practice and a ``--help`` that hides that is a
    ``--help`` that sends people to the source.
    """
    group = parser.add_argument_group(
        "notifications",
        "Ping a dead-man's switch and push run outcomes. All of these can be "
        "set from the environment instead; see docs/monitoring.md.",
    )
    group.add_argument(
        "--healthchecks-url",
        dest="healthchecks_url",
        help=f"Full Healthchecks ping URL, self-hosted or not (${ENV_HC_URL})",
    )
    group.add_argument(
        "--healthchecks-uuid",
        dest="healthchecks_uuid",
        help=f"Healthchecks check UUID, combined with the base URL (${ENV_HC_UUID})",
    )
    group.add_argument(
        "--healthchecks-base-url",
        dest="healthchecks_base_url",
        help=(
            "Base URL for --healthchecks-uuid (default https://hc-ping.com; "
            f"${ENV_HC_BASE_URL})"
        ),
    )
    group.add_argument(
        "--healthchecks-anomaly-fails",
        dest="healthchecks_anomaly_fails",
        action="store_true",
        default=None,
        help=(
            "Treat an anomaly as a failed check instead of logging it against "
            f"the check (${ENV_HC_ANOMALY_FAILS})"
        ),
    )
    group.add_argument(
        "--ntfy-url", dest="ntfy_url", help=f"Full ntfy topic URL (${ENV_NTFY_URL})"
    )
    group.add_argument(
        "--ntfy-topic", dest="ntfy_topic", help=f"ntfy topic name (${ENV_NTFY_TOPIC})"
    )
    group.add_argument(
        "--ntfy-server",
        dest="ntfy_server",
        help=f"ntfy server for --ntfy-topic (default https://ntfy.sh; ${ENV_NTFY_SERVER})",
    )
    group.add_argument(
        "--ntfy-token",
        dest="ntfy_token",
        help=f"ntfy access token, sent as a bearer token (${ENV_NTFY_TOKEN})",
    )
    group.add_argument(
        "--ntfy-priority",
        dest="ntfy_priority",
        help=f"Pin every ntfy message to this priority (${ENV_NTFY_PRIORITY})",
    )
    group.add_argument(
        "--ntfy-tags",
        dest="ntfy_tags",
        help=f"Extra comma-separated ntfy tags (${ENV_NTFY_TAGS})",
    )
    group.add_argument(
        "--webhook-url",
        dest="webhook_url",
        help=f"POST the run event as JSON to this URL (${ENV_WEBHOOK_URL})",
    )
    group.add_argument(
        "--webhook-header",
        dest="webhook_header",
        action="append",
        metavar="KEY: VALUE",
        help=f"Extra webhook header; repeatable (${ENV_WEBHOOK_HEADERS} takes JSON)",
    )
    group.add_argument(
        "--notify-timeout",
        dest="notify_timeout",
        type=float,
        help=f"Seconds before a notification request is abandoned (${ENV_TIMEOUT})",
    )
    group.add_argument(
        "--notify-retries",
        dest="notify_retries",
        type=int,
        help=f"Retries after the first attempt (${ENV_RETRIES})",
    )
    group.add_argument(
        "--no-notify",
        dest="notify",
        action="store_false",
        default=None,
        help="Send no notifications even if the environment configures them",
    )
    return group


# ---------------------------------------------------------------------------
# The notifier
# ---------------------------------------------------------------------------

class Notifier:
    """Fans a run event out to every configured backend, and swallows the fallout.

    Construct with :func:`build_notifier` (or :meth:`from_config`); an instance
    with no backends is a working no-op, so callers never need a conditional.
    """

    def __init__(
        self,
        backends: Sequence[NotificationBackend] = (),
        *,
        logger: Optional[logging.Logger] = None,
        redactor: Optional[Redactor] = None,
        run_id: Optional[str] = None,
        clock: Callable[[], float] = time.time,
    ):
        self._backends: List[NotificationBackend] = list(backends)
        self._logger = logger if logger is not None else globals()["logger"]
        self._redactor = redactor if redactor is not None else Redactor()
        self.run_id = run_id or str(uuid.uuid4())
        self._clock = clock
        self._started_at: Optional[float] = None
        self._results: List[DeliveryResult] = []
        # Backends built independently carry their own secrets; pull them in so
        # this notifier's own log lines are scrubbed too.
        for backend in self._backends:
            self._redactor.merge(getattr(backend, "redactor", None))

    # -- construction --------------------------------------------------
    @classmethod
    def from_config(
        cls,
        config: NotifyConfig,
        *,
        logger: Optional[logging.Logger] = None,
        transport: Optional[HttpTransport] = None,
        run_id: Optional[str] = None,
        clock: Callable[[], float] = time.time,
    ) -> "Notifier":
        """Build the backends the config asks for.

        A backend that cannot be constructed (a malformed URL, say) is reported
        as a warning and dropped; the others still run. Refusing to notify at
        all because one of three settings is wrong would be the subsystem
        deciding the run's fate, which is precisely what it must not do.
        """
        log = logger if logger is not None else globals()["logger"]
        redactor = Redactor()
        for value in (
            config.healthchecks_url,
            config.healthchecks_uuid,
            config.ntfy_url,
            config.ntfy_token,
            config.webhook_url,
            *config.webhook_headers.values(),
        ):
            redactor.add(value)

        backends: List[NotificationBackend] = []
        if not config.enabled:
            return cls([], logger=log, redactor=redactor, run_id=run_id, clock=clock)

        def make_transport() -> HttpTransport:
            if transport is not None:
                # An injected transport adopts the shared redactor rather than
                # merging into it: backends register their secrets *after*
                # construction, and only a shared object sees those.
                transport.redactor = redactor
                return transport
            return HttpTransport(
                timeout=config.timeout,
                retries=config.retries,
                backoff=config.backoff,
                redactor=redactor,
            )

        specs: List[Tuple[str, Callable[[], NotificationBackend]]] = []
        if config.healthchecks_url or config.healthchecks_uuid:
            specs.append(
                (
                    "healthchecks",
                    lambda: HealthchecksBackend(
                        url=config.healthchecks_url,
                        uuid_=config.healthchecks_uuid,
                        base_url=config.healthchecks_base_url,
                        anomaly_is_failure=config.healthchecks_anomaly_is_failure,
                        transport=make_transport(),
                        redactor=redactor,
                    ),
                )
            )
        if config.ntfy_url or config.ntfy_topic:
            specs.append(
                (
                    "ntfy",
                    lambda: NtfyBackend(
                        url=config.ntfy_url,
                        topic=config.ntfy_topic,
                        server=config.ntfy_server,
                        token=config.ntfy_token,
                        priority=config.ntfy_priority,
                        tags=config.ntfy_tags,
                        transport=make_transport(),
                        redactor=redactor,
                    ),
                )
            )
        if config.webhook_url:
            specs.append(
                (
                    "webhook",
                    lambda: WebhookBackend(
                        url=config.webhook_url or "",
                        headers=config.webhook_headers,
                        method=config.webhook_method,
                        transport=make_transport(),
                        redactor=redactor,
                    ),
                )
            )

        for name, factory in specs:
            try:
                backends.append(factory())
            except Exception as exc:  # noqa: BLE001 - configuration must not kill the run
                log.warning(
                    "Notification backend %s could not be configured and will be "
                    "skipped: %s",
                    name,
                    redactor(exc),
                )

        return cls(backends, logger=log, redactor=redactor, run_id=run_id, clock=clock)

    @classmethod
    def from_env(
        cls,
        env: Optional[Mapping[str, str]] = None,
        *,
        logger: Optional[logging.Logger] = None,
        **overrides: Any,
    ) -> "Notifier":
        return cls.from_config(NotifyConfig.from_env(env, **overrides), logger=logger)

    # -- introspection -------------------------------------------------
    @property
    def enabled(self) -> bool:
        """True when there is at least one backend to deliver to."""
        return bool(self._backends)

    @property
    def backends(self) -> List[NotificationBackend]:
        return list(self._backends)

    @property
    def results(self) -> List[DeliveryResult]:
        return list(self._results)

    def describe(self) -> List[str]:
        """Redaction-safe one-liners, suitable for printing at startup."""
        return [backend.describe() for backend in self._backends]

    def redact(self, value: Any) -> str:
        """Scrub every configured secret out of ``value``."""
        return self._redactor.redact(value)

    # -- events --------------------------------------------------------
    def start(self, message: str = "", **details: Any) -> List[DeliveryResult]:
        """Signal that a run has begun. This is what arms the dead-man's switch."""
        self._started_at = self._clock()
        return self.notify(
            RunEvent(
                event=EVENT_START,
                title="iFetch run started",
                message=message,
                details=dict(details),
                run_id=self.run_id,
                timestamp=self._started_at,
            )
        )

    def success(
        self,
        message: str = "",
        *,
        report: Optional[Mapping[str, Any]] = None,
        **details: Any,
    ) -> List[DeliveryResult]:
        """Signal a run that finished and had nothing to complain about."""
        payload = self._run_details(report, details)
        return self.notify(
            RunEvent(
                event=EVENT_SUCCESS,
                title="iFetch run finished",
                message=message or format_run_summary(report),
                details=payload,
                run_id=self.run_id,
                timestamp=self._clock(),
            )
        )

    def failure(self, error: Any, **details: Any) -> List[DeliveryResult]:
        """Signal a run that did not complete. The error text is redacted."""
        payload = self._run_details(details.pop("report", None), details)
        return self.notify(
            RunEvent(
                event=EVENT_FAILURE,
                title="iFetch run FAILED",
                message=self.redact(error),
                details=payload,
                run_id=self.run_id,
                timestamp=self._clock(),
            )
        )

    def anomaly(
        self,
        reason: Any = "",
        *,
        findings: Sequence[str] = (),
        report: Optional[Mapping[str, Any]] = None,
        **details: Any,
    ) -> List[DeliveryResult]:
        """Signal a run that *completed* but found something worth reading.

        Individual file errors, a failed integrity check, placeholders found by
        a recovery scan. Not a failure - and reported as such, so that a
        failure alert keeps meaning what it says.
        """
        payload = self._run_details(report, details)
        if findings:
            payload["findings"] = [self.redact(f) for f in findings]
        message = self.redact(reason) if reason else "; ".join(payload.get("findings", []))
        return self.notify(
            RunEvent(
                event=EVENT_ANOMALY,
                title="iFetch run finished with findings",
                message=message,
                details=payload,
                run_id=self.run_id,
                timestamp=self._clock(),
            )
        )

    def run_finished(
        self,
        report: Optional[Mapping[str, Any]] = None,
        *,
        error: Any = None,
        findings: Sequence[str] = (),
    ) -> List[DeliveryResult]:
        """The one call a run's ``finally`` block needs.

        Failure when ``error`` is given; otherwise success, followed by a
        separate anomaly event when the report or ``findings`` say the run
        completed with something in it. Success is sent first deliberately: on
        Healthchecks with ``anomaly_is_failure`` the anomaly must land last for
        the check to end up red.
        """
        if error is not None:
            return self.failure(error, report=report)
        results = list(self.success(report=report))
        found = list(findings) + anomalies_from_report(report)
        if found:
            results.extend(self.anomaly(findings=found, report=report))
        return results

    # -- delivery ------------------------------------------------------
    def notify(self, event: RunEvent) -> List[DeliveryResult]:
        """Deliver ``event`` to every backend. Never raises.

        ``KeyboardInterrupt`` and ``SystemExit`` are not caught: they are not
        notification failures, and swallowing a Ctrl-C to finish sending a push
        would be a worse bug than the one this guard prevents.
        """
        results: List[DeliveryResult] = []
        if not self._backends:
            return results

        for backend in self._backends:
            try:
                result = backend.deliver(event)
            except Exception as exc:  # noqa: BLE001 - the whole point of this method
                result = DeliveryResult(
                    backend=getattr(backend, "name", type(backend).__name__),
                    event=event.event,
                    delivered=False,
                    attempts=int(getattr(exc, "attempts", 1) or 1),
                    status_code=getattr(exc, "status_code", None),
                    error=self.redact(exc) or type(exc).__name__,
                    target=self._safe_describe(backend),
                )
                self._logger.warning(
                    "Notification not delivered (%s, event=%s, attempts=%d): %s",
                    result.backend,
                    event.event,
                    result.attempts,
                    result.error,
                )
            else:
                self._logger.debug(
                    "Notification delivered (%s, event=%s, status=%s)",
                    result.backend,
                    event.event,
                    result.status_code,
                )
            results.append(result)
            self._results.append(result)
        return results

    def report_snapshot(self) -> Dict[str, Any]:
        """A JSON-safe record of what was and was not delivered.

        Fully redacted, so it can be embedded in ``download_report.json``
        without turning that file into a credential.
        """
        undelivered = [r for r in self._results if not r.delivered]
        return {
            "enabled": self.enabled,
            "run_id": self.run_id,
            "backends": self.describe(),
            "deliveries": [r.to_dict() for r in self._results],
            "undelivered": len(undelivered),
        }

    # -- internals -----------------------------------------------------
    def _run_details(
        self, report: Optional[Mapping[str, Any]], extra: Mapping[str, Any]
    ) -> Dict[str, Any]:
        details: Dict[str, Any] = {}
        summary = (report or {}).get("summary") if isinstance(report, Mapping) else None
        if isinstance(summary, Mapping):
            for key in (
                "total_files",
                "successful",
                "failed",
                "skipped",
                "total_bytes_transferred",
            ):
                if key in summary:
                    details[key] = summary[key]
        if self._started_at is not None:
            details["duration"] = human_duration(self._clock() - self._started_at)
        for key, value in extra.items():
            if value is not None:
                details[key] = value
        return details

    def _safe_describe(self, backend: Any) -> Optional[str]:
        try:
            return self.redact(backend.describe())
        except Exception:  # noqa: BLE001 - describing must not fail either
            return None


class NullNotifier(Notifier):
    """A notifier that is explicitly nothing.

    ``Notifier([])`` already behaves this way; this name exists so a caller can
    say what it means (``notifier or NullNotifier()``) instead of constructing
    an empty list of backends and hoping the reader understands why.
    """

    def __init__(self, **kwargs: Any):
        super().__init__((), **kwargs)

    def notify(self, event: RunEvent) -> List[DeliveryResult]:
        return []


def build_notifier(
    args: Any = None,
    env: Optional[Mapping[str, str]] = None,
    *,
    logger: Optional[logging.Logger] = None,
    run_id: Optional[str] = None,
) -> Notifier:
    """The wiring one-liner: flags plus environment in, ready notifier out.

    Returns a working no-op when nothing is configured, so no caller needs an
    ``if``.
    """
    config = NotifyConfig.from_args(args, env)
    if not config.is_configured():
        return NullNotifier(logger=logger, run_id=run_id)
    return Notifier.from_config(config, logger=logger, run_id=run_id)


# ---------------------------------------------------------------------------
# Summarising a run
# ---------------------------------------------------------------------------

def format_run_summary(report: Optional[Mapping[str, Any]]) -> str:
    """One short line describing a run, for the ping body.

    Healthchecks shows the body in its UI and in the alert e-mail. A ping with
    an empty body tells you a run happened; a ping with this line tells you
    whether you need to do anything, which is the difference between an alert
    that is read and one that is filtered.
    """
    if not isinstance(report, Mapping):
        return ""
    summary = report.get("summary")
    if not isinstance(summary, Mapping):
        return ""
    total = summary.get("total_files", 0)
    ok = summary.get("successful", 0)
    failed = summary.get("failed", 0)
    skipped = summary.get("skipped", 0)
    transferred = summary.get("total_bytes_transferred")
    parts = [
        f"{total} files seen",
        f"{ok} downloaded",
        f"{skipped} unchanged",
        f"{failed} failed",
    ]
    if isinstance(transferred, (int, float)):
        parts.append(f"{human_bytes(int(transferred))} transferred")
    return ", ".join(parts)


def anomalies_from_report(report: Optional[Mapping[str, Any]]) -> List[str]:
    """Findings a completed run should still tell somebody about.

    Only things the summary can actually prove. A run with zero failures is not
    asserted to be *correct* here - proving that is ``ifetch-verify``'s job, and
    claiming it from a download summary would be exactly the kind of overstated
    confidence the planner refuses to make.
    """
    findings: List[str] = []
    if not isinstance(report, Mapping):
        return findings
    summary = report.get("summary")
    if not isinstance(summary, Mapping):
        return findings
    try:
        failed = int(summary.get("failed", 0) or 0)
    except (TypeError, ValueError):
        failed = 0
    if failed > 0:
        findings.append(f"{failed} file(s) failed to download")
    return findings


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------

def _truncate(text: str, limit: int) -> str:
    """Truncate on a byte budget without splitting a UTF-8 sequence."""
    raw = text.encode("utf-8")
    if len(raw) <= limit:
        return text
    return raw[: max(0, limit - 3)].decode("utf-8", "ignore") + "..."


def _header_value(value: Any) -> str:
    """HTTP headers are latin-1 on the wire and ntfy titles come from filenames.

    Non-ASCII is replaced rather than allowed to raise inside ``requests``:
    a mangled title is a far better outcome than an undelivered notification.
    """
    text = str(value).encode("ascii", "replace").decode("ascii")
    return "".join(ch for ch in text if 32 <= ord(ch) < 127).strip()


def _humanise_key(key: str) -> str:
    return str(key).replace("_", " ")


def _render_value(value: Any) -> str:
    if isinstance(value, bool):
        return "yes" if value else "no"
    if isinstance(value, (list, tuple)):
        return "; ".join(str(v) for v in value)
    return str(value)


def _jsonable(value: Any) -> Any:
    """Coerce to something ``json.dumps`` accepts, without ever raising."""
    try:
        json.dumps(value)
        return value
    except (TypeError, ValueError):
        if isinstance(value, Mapping):
            return {str(k): _jsonable(v) for k, v in value.items()}
        if isinstance(value, (list, tuple, set)):
            return [_jsonable(v) for v in value]
        return str(value)


def _as_bool(raw: Any, default: bool = False) -> bool:
    if raw is None:
        return default
    if isinstance(raw, bool):
        return raw
    text = str(raw).strip().lower()
    if text in _TRUTHY:
        return True
    if text in _FALSEY:
        return False
    return default


def _as_float(override: Any, raw: Any, default: float) -> float:
    for candidate in (override, raw):
        if candidate is None:
            continue
        try:
            return float(candidate)
        except (TypeError, ValueError):
            # A malformed IFETCH_NOTIFY_TIMEOUT must not stop a backup; fall
            # through to the default, which is safe by construction.
            logger.warning(
                "Ignoring unparseable notification setting %r; using %s",
                candidate,
                default,
            )
    return float(default)


def _split_list(raw: Any) -> Tuple[str, ...]:
    if not raw:
        return ()
    if isinstance(raw, (list, tuple)):
        return tuple(str(item).strip() for item in raw if str(item).strip())
    return tuple(part.strip() for part in str(raw).split(",") if part.strip())


def _parse_headers(raw: Any) -> Dict[str, str]:
    """Accept a mapping, a JSON object, or repeated ``Key: Value`` strings.

    A header that cannot be parsed is dropped with a warning rather than
    guessed at: sending an authorization header the user did not write is worse
    than sending none.
    """
    if not raw:
        return {}
    if isinstance(raw, Mapping):
        return {str(k): str(v) for k, v in raw.items()}

    items: List[str] = []
    if isinstance(raw, (list, tuple)):
        items = [str(item) for item in raw]
    else:
        text = str(raw).strip()
        if text.startswith("{"):
            try:
                parsed = json.loads(text)
            except ValueError:
                logger.warning("Ignoring webhook headers: not valid JSON")
                return {}
            if isinstance(parsed, Mapping):
                return {str(k): str(v) for k, v in parsed.items()}
            logger.warning("Ignoring webhook headers: JSON was not an object")
            return {}
        items = [line for line in text.replace("\n", ",").split(",") if line.strip()]

    headers: Dict[str, str] = {}
    for item in items:
        if ":" not in item:
            logger.warning("Ignoring webhook header with no ':' separator")
            continue
        key, _, value = item.partition(":")
        key = key.strip()
        value = value.strip()
        if key and value:
            headers[key] = value
        else:
            logger.warning("Ignoring webhook header with an empty name or value")
    return headers
