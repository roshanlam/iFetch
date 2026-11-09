"""Jittered backoff and first-class 429 / Retry-After handling (#46)."""

import random
import sys
from datetime import datetime, timedelta, timezone
from email.utils import format_datetime
from pathlib import Path

import pytest
import requests

sys.path.append(str(Path(__file__).resolve().parents[1]))

from ifetch import backoff  # noqa: E402
from ifetch.downloader import DownloadManager  # noqa: E402


class FakeResponse:
    def __init__(self, status, content=b"", headers=None, body=None):
        self.status_code = status
        self.content = content
        self.headers = headers or {}
        self._body = body

    def json(self):
        if self._body is None:
            raise ValueError("no json")
        return self._body

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"{self.status_code} Error", response=self)


class ApiError(Exception):
    """Shaped like pyicloud's PyiCloudAPIResponseException."""

    def __init__(self, reason, code=None, response=None):
        self.code = code
        self.response = response
        super().__init__(f"{reason} ({code})" if code else reason)


@pytest.fixture
def sleeps(monkeypatch):
    recorded = []
    monkeypatch.setattr("ifetch.downloader.time.sleep", recorded.append)
    return recorded


def _manager(**kwargs):
    kwargs.setdefault("max_retries", 3)
    return DownloadManager(email="user@example.com", **kwargs)


# -- the helper itself -------------------------------------------------------

def test_backoff_grows_exponentially_and_is_capped():
    top = lambda: 1.0  # noqa: E731 - the largest value jitter can draw
    assert [backoff.backoff_delay(n, rng=top) for n in range(4)] == [1, 2, 4, 8]
    assert backoff.backoff_delay(20, rng=top) == backoff.DEFAULT_CAP_SECONDS
    assert backoff.backoff_delay(3, cap=5, rng=top) == 5
    assert backoff.backoff_delay(10_000, rng=top) == backoff.DEFAULT_CAP_SECONDS


def test_full_jitter_spreads_waits_across_the_window():
    rng = random.Random(1234)
    waits = [backoff.backoff_delay(4, rng=rng.random) for _ in range(500)]
    assert all(0.0 <= w <= 16.0 for w in waits)
    # Spread, not a stampede: values cover the window instead of clustering.
    assert len({round(w, 3) for w in waits}) > 400
    assert min(waits) < 2.0 and max(waits) > 14.0


def test_parse_retry_after_seconds_and_http_date():
    assert backoff.parse_retry_after("7") == 7.0
    assert backoff.parse_retry_after(3) == 3.0
    assert backoff.parse_retry_after("") is None
    assert backoff.parse_retry_after("soon") is None
    now = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    later = format_datetime(now + timedelta(seconds=30), usegmt=True)
    assert backoff.parse_retry_after(later, now=now) == pytest.approx(30.0)
    earlier = format_datetime(now - timedelta(seconds=30), usegmt=True)
    assert backoff.parse_retry_after(earlier, now=now) == 0.0


def test_retry_after_read_from_header_body_and_text():
    header = ApiError("Throttled", 429, FakeResponse(429, headers={"Retry-After": "9"}))
    assert backoff.retry_after_from_error(header) == 9.0
    body = ApiError("Throttled", 503, FakeResponse(503, body={"retryAfter": 4}))
    assert backoff.retry_after_from_error(body) == 4.0
    text = Exception('Service Unavailable {"reason":"RETRY_NEEDED","retryAfter": 12}')
    assert backoff.retry_after_from_error(text) == 12.0
    assert backoff.retry_after_from_error(Exception("plain failure")) is None


def test_retry_wait_prefers_server_value_and_caps_it():
    error = ApiError("Throttled", 429, FakeResponse(429, headers={"Retry-After": "5"}))
    assert backoff.retry_wait(0, error, rng=lambda: 0.0) == 5.0
    huge = ApiError("Throttled", 429, FakeResponse(429, headers={"Retry-After": "3600"}))
    assert backoff.retry_wait(0, huge) == backoff.DEFAULT_CAP_SECONDS
    assert backoff.retry_wait(2, Exception("boom"), rng=lambda: 0.5) == 2.0


def test_is_throttle_by_status_and_text():
    assert backoff.is_throttle(ApiError("x", 429))
    assert backoff.is_throttle(ApiError("x", 503))
    assert backoff.is_throttle(Exception("429 Too Many Requests"))
    assert not backoff.is_throttle(ApiError("x", 404))
    assert not backoff.is_throttle(Exception("Invalid credentials"))


# -- range GET ----------------------------------------------------------------

def test_range_get_retries_429_and_honours_retry_after(sleeps):
    dm = _manager()
    responses = [
        FakeResponse(429, headers={"Retry-After": "3"}),
        FakeResponse(206, content=b"abcd"),
    ]
    dm.http.get = lambda *a, **k: responses.pop(0)
    assert dm.download_chunk("https://x.invalid", 0, 3) == b"abcd"
    assert sleeps == [3.0]


def test_range_get_429_without_header_uses_jittered_backoff(sleeps, monkeypatch):
    dm = _manager()
    responses = [FakeResponse(429), FakeResponse(503), FakeResponse(206, content=b"ok")]
    dm.http.get = lambda *a, **k: responses.pop(0)
    monkeypatch.setattr("ifetch.backoff.random.random", lambda: 0.5)
    assert dm.download_chunk("https://x.invalid", 0, 1) == b"ok"
    # 0.5 of the 1s then 2s windows: jittered, not the old fixed 2s/4s.
    assert sleeps == [0.5, 1.0]


def test_range_get_caps_a_huge_retry_after(sleeps):
    dm = _manager()
    responses = [FakeResponse(429, headers={"Retry-After": "86400"}), FakeResponse(206, content=b"z")]
    dm.http.get = lambda *a, **k: responses.pop(0)
    dm.download_chunk("https://x.invalid", 0, 0)
    assert sleeps == [backoff.DEFAULT_CAP_SECONDS]


def test_range_get_gives_up_after_max_retries_without_trailing_sleep(sleeps):
    dm = _manager(max_retries=3)
    dm.http.get = lambda *a, **k: FakeResponse(429)
    with pytest.raises(Exception, match="after 3 retries"):
        dm.download_chunk("https://x.invalid", 0, 9)
    assert len(sleeps) == 2  # between attempts only


# -- open ---------------------------------------------------------------------

class FlakyNode:
    name = "file.bin"

    def __init__(self, errors):
        self.errors = list(errors)
        self.opens = 0

    def open(self, stream=True):
        self.opens += 1
        if self.errors:
            raise self.errors.pop(0)
        return "stream"


def test_open_retries_429_like_503(sleeps, monkeypatch):
    monkeypatch.setattr("ifetch.backoff.random.random", lambda: 1.0)
    dm = _manager()
    node = FlakyNode([ApiError("Too Many Requests", 429), ApiError("Service Unavailable", 503)])
    assert dm._open_with_retry(node) == "stream"
    assert node.opens == 3
    assert sleeps == [2.0, 4.0]


def test_open_reads_retry_after_from_response_headers(sleeps):
    dm = _manager()
    response = FakeResponse(429, headers={"Retry-After": "11"})
    node = FlakyNode([ApiError("Throttled", 429, response)])
    assert dm._open_with_retry(node) == "stream"
    assert sleeps == [11.0]


def test_open_still_scrapes_retry_after_from_apple_payload(sleeps):
    dm = _manager()
    node = FlakyNode([Exception('503 {"errorCode":"RETRY_NEEDED","retryAfter": 6}')])
    assert dm._open_with_retry(node) == "stream"
    assert sleeps == [6.0]


def test_non_retryable_auth_error_is_raised_at_once(sleeps):
    dm = _manager()
    node = FlakyNode([Exception("Invalid credentials")])
    with pytest.raises(Exception, match="Invalid credentials"):
        dm._open_with_retry(node)
    assert node.opens == 1
    assert sleeps == []
