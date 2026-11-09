"""Retry pacing shared by every network retry loop in the downloader.

Two rules, both aimed at Apple's throttling behaviour:

* **Full jitter.** A pure ``2 ** n`` sleep makes every worker that failed in
  the same burst wake up in the same instant and re-trip the same throttle.
  Drawing each wait uniformly from ``[0, min(cap, base * 2 ** n)]`` (the "full
  jitter" scheme) keeps the expected back-off growth while spreading the
  retries out.

* **The server's word wins.** When Apple says how long to wait - an HTTP
  ``Retry-After`` header, or a ``retryAfter`` field in a JSON error body - that
  value is used instead of the computed one, still subject to the cap so a
  hostile or buggy value cannot park a worker for an hour.

Nothing here sleeps; callers do, so tests can observe the chosen waits.
"""

from __future__ import annotations

import random
import re
from email.utils import parsedate_to_datetime
from datetime import datetime, timezone
from typing import Any, Callable, Optional

#: Statuses that mean "slow down and try again", on any endpoint.
THROTTLE_STATUSES = frozenset({429, 503})

#: Upper bound on any single wait, computed or server-supplied.
DEFAULT_CAP_SECONDS = 60.0

#: Base of the exponential: attempt 0 waits up to 1s, attempt 1 up to 2s, ...
DEFAULT_BASE_SECONDS = 1.0

_RETRY_AFTER_TEXT = re.compile(r'retry[_-]?after["\'\s:=]+(\d+(?:\.\d+)?)', re.IGNORECASE)


def backoff_delay(
    attempt: int,
    *,
    base: float = DEFAULT_BASE_SECONDS,
    cap: float = DEFAULT_CAP_SECONDS,
    rng: Optional[Callable[[], float]] = None,
) -> float:
    """Full-jitter exponential back-off for zero-based ``attempt``."""
    attempt = max(0, int(attempt))
    # Bound the exponent before computing it: 2 ** 1000 is a perfectly good int
    # but a useless float, and the cap is reached long before that anyway.
    ceiling = min(float(cap), float(base) * (2 ** min(attempt, 32)))
    draw = rng if rng is not None else random.random
    return max(0.0, ceiling * draw())


def parse_retry_after(value: Any, *, now: Optional[datetime] = None) -> Optional[float]:
    """Interpret a ``Retry-After`` value: delta-seconds or an HTTP-date.

    Returns ``None`` for anything absent or unparseable, never raises.
    """
    if value is None:
        return None
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return max(0.0, float(value))
    text = str(value).strip()
    if not text:
        return None
    try:
        return max(0.0, float(text))
    except ValueError:
        pass
    try:
        when = parsedate_to_datetime(text)
    except (TypeError, ValueError, IndexError):
        return None
    if when is None:
        return None
    if when.tzinfo is None:
        when = when.replace(tzinfo=timezone.utc)
    current = now or datetime.now(timezone.utc)
    return max(0.0, (when - current).total_seconds())


def _response_of(error: Any) -> Any:
    return getattr(error, "response", None)


def status_from_error(error: Any) -> Optional[int]:
    """Best-effort HTTP status for a requests or pyicloud exception."""
    response = _response_of(error)
    status = getattr(response, "status_code", None) if response is not None else None
    if isinstance(status, int):
        return status
    # pyicloud's PyiCloudAPIResponseException carries ``code`` (int or str).
    code = getattr(error, "code", None)
    if isinstance(code, int) and not isinstance(code, bool):
        return code
    if isinstance(code, str) and code.isdigit():
        return int(code)
    return None


def retry_after_from_error(error: Any) -> Optional[float]:
    """The server-requested wait carried by ``error``, if any.

    Looks, in order of trust, at the response's ``Retry-After`` header, a
    ``retryAfter`` field in a JSON body, and finally the error text itself
    (pyicloud folds Apple's JSON body into the exception message).
    """
    response = _response_of(error)
    if response is not None:
        headers = getattr(response, "headers", None) or {}
        try:
            header = headers.get("Retry-After")
        except Exception:
            header = None
        parsed = parse_retry_after(header)
        if parsed is not None:
            return parsed
        try:
            body = response.json()
        except Exception:
            body = None
        if isinstance(body, dict):
            for key in ("retryAfter", "retry_after", "Retry-After"):
                parsed = parse_retry_after(body.get(key))
                if parsed is not None:
                    return parsed
    match = _RETRY_AFTER_TEXT.search(str(error or ""))
    if match:
        return parse_retry_after(match.group(1))
    return None


def retry_wait(
    attempt: int,
    error: Any = None,
    *,
    base: float = DEFAULT_BASE_SECONDS,
    cap: float = DEFAULT_CAP_SECONDS,
    rng: Optional[Callable[[], float]] = None,
) -> float:
    """How long to wait before retry ``attempt`` after ``error``.

    A server-supplied delay is honoured (capped); otherwise full jitter.
    """
    requested = retry_after_from_error(error) if error is not None else None
    if requested is not None:
        return min(float(cap), requested)
    return backoff_delay(attempt, base=base, cap=cap, rng=rng)


def is_throttle(error: Any) -> bool:
    """True when ``error`` is a 429/503 throttle, by status or by Apple's text."""
    status = status_from_error(error)
    if status in THROTTLE_STATUSES:
        return True
    text = str(error or "").lower()
    return any(marker in text for marker in (
        "429", "too many requests", "503", "service unavailable",
    ))
