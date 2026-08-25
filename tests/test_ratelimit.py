"""Tests for ``--bwlimit``: the shared token bucket and its rclone-style schedule.

Two contracts are under test.

*The bucket is shared.*  iFetch downloads through a thread pool, so the thing
that must be true is that ``N`` workers together consume no more than the
configured rate - not that each of them does.  The concurrency test drives real
threads and asserts the aggregate property directly.

*Nothing sleeps and nothing touches the network.*  The clock and the sleep
function are injected into the limiter precisely so this file can run in
milliseconds and still assert exact delays.  A test here that took a real
second would be a bug in the design, not in the test.
"""

import sys
import threading
from datetime import datetime
from pathlib import Path

import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))

from ifetch.downloader import DownloadManager, SyncState  # noqa: E402
from ifetch.ratelimit import (  # noqa: E402
    BURST_SECONDS,
    BandwidthLimitError,
    BandwidthLimiter,
    BandwidthSchedule,
    TokenBucket,
    create_limiter,
    format_rate,
    parse_bwlimit,
    parse_size,
)


KIB = 1024
MIB = 1024 * 1024


class FakeClock:
    """A monotonic clock that only moves when something sleeps on it.

    ``sleep`` advances the clock instead of blocking, which is what lets a test
    assert "this call waited exactly 1.5 seconds" without waiting at all.  It is
    lock-protected because the concurrency tests share one instance across real
    threads.
    """

    def __init__(self, start=0.0):
        self._now = float(start)
        self._lock = threading.Lock()
        self.sleeps = []

    def time(self):
        with self._lock:
            return self._now

    def sleep(self, seconds):
        if seconds <= 0:
            return
        with self._lock:
            self.sleeps.append(float(seconds))
            self._now += float(seconds)

    def advance(self, seconds):
        with self._lock:
            self._now += float(seconds)


class FakeWallClock:
    """A settable wall clock, for schedule boundaries."""

    def __init__(self, moment):
        self.moment = moment

    def __call__(self):
        return self.moment


def _bucket(rate, clock, capacity=None):
    return TokenBucket(rate, capacity=capacity, clock=clock.time, sleep=clock.sleep)


# ---------------------------------------------------------------------------
# 1. Token accounting: exact delays under a known rate and a fake clock
# ---------------------------------------------------------------------------
def test_exact_delays_for_a_sequence_of_requests():
    clock = FakeClock()
    bucket = _bucket(1000, clock)  # capacity == 1000 bytes (1s of burst)

    # The burst is free.
    assert bucket.consume(1000) == 0.0
    assert clock.time() == 0.0

    # Nothing banked: a second full second of traffic costs a second.
    assert bucket.consume(1000) == pytest.approx(1.0)
    assert clock.time() == pytest.approx(1.0)

    # Half a second's worth costs half a second.
    assert bucket.consume(500) == pytest.approx(0.5)
    assert clock.time() == pytest.approx(1.5)

    assert bucket.consume(250) == pytest.approx(0.25)
    assert clock.time() == pytest.approx(1.75)


def test_accrued_tokens_are_spent_before_any_wait():
    clock = FakeClock()
    bucket = _bucket(1000, clock)
    bucket.consume(1000)  # drain the burst

    clock.advance(0.4)  # 400 bytes accrue while nothing is downloading
    assert bucket.consume(400) == 0.0
    assert bucket.consume(600) == pytest.approx(0.6)


def test_idle_time_cannot_bank_more_than_the_burst():
    """An hour of idling must not buy an hour of unthrottled traffic."""
    clock = FakeClock()
    bucket = _bucket(1000, clock)
    clock.advance(3600)

    assert bucket.tokens == pytest.approx(1000)  # capped at capacity, not 3.6M
    assert bucket.consume(1000) == 0.0
    assert bucket.consume(1000) == pytest.approx(1.0)


def test_zero_and_negative_amounts_are_free_and_do_not_move_the_clock():
    clock = FakeClock()
    bucket = _bucket(1000, clock)
    assert bucket.consume(0) == 0.0
    assert bucket.consume(-5) == 0.0
    assert clock.time() == 0.0
    assert bucket.tokens == pytest.approx(1000)


def test_a_backwards_clock_cannot_mint_tokens():
    clock = FakeClock(start=100.0)
    bucket = _bucket(1000, clock)
    bucket.consume(1000)

    clock.advance(-50)  # NTP step, suspended laptop, whatever
    assert bucket.tokens == pytest.approx(0.0)
    # And the bucket is not wedged: time moving forward again still refills.
    clock.advance(0.5)
    assert bucket.tokens == pytest.approx(500)


# ---------------------------------------------------------------------------
# 2. Burst allowance at the start of a transfer
# ---------------------------------------------------------------------------
def test_burst_is_one_second_of_the_configured_rate():
    clock = FakeClock()
    bucket = _bucket(4096, clock)
    assert bucket.capacity == pytest.approx(4096 * BURST_SECONDS)


def test_first_chunk_of_a_transfer_is_not_delayed():
    """A 1 MiB chunk at a 1 MiB/s limit starts immediately, not a second late."""
    clock = FakeClock()
    bucket = _bucket(MIB, clock)
    assert bucket.consume(MIB) == 0.0
    assert clock.time() == 0.0


def test_explicit_capacity_overrides_the_default_burst():
    clock = FakeClock()
    bucket = _bucket(1000, clock, capacity=250)
    assert bucket.capacity == pytest.approx(250)
    assert bucket.consume(250) == 0.0
    assert bucket.consume(250) == pytest.approx(0.25)


# ---------------------------------------------------------------------------
# 3. Degenerate cases: forward progress, never a deadlock
# ---------------------------------------------------------------------------
def test_request_larger_than_the_bucket_capacity_still_completes():
    clock = FakeClock()
    bucket = _bucket(1000, clock)  # capacity 1000, request 5000

    waited = bucket.consume(5000)

    assert waited == pytest.approx(4.0)  # 1000 free, 4000 at 1000 B/s
    assert clock.time() == pytest.approx(4.0)
    # The debt is settled, not carried: the bucket is at zero, not negative.
    assert bucket.tokens == pytest.approx(0.0)


def test_limit_smaller_than_one_chunk_makes_forward_progress():
    """10 B/s with 1000-byte chunks: slow, but every chunk gets through."""
    clock = FakeClock()
    bucket = _bucket(10, clock)

    for _ in range(3):
        bucket.consume(1000)

    # 10 bytes of burst, 2990 bytes to pay for at 10 B/s.
    assert clock.time() == pytest.approx(299.0)


def test_a_sleep_that_does_not_advance_the_clock_does_not_spin_forever():
    """Degrades to "no throttling" rather than to an infinite loop."""
    calls = []

    def frozen_clock():
        return 0.0

    def noop_sleep(seconds):
        calls.append(seconds)

    bucket = TokenBucket(1000, clock=frozen_clock, sleep=noop_sleep)
    bucket.consume(1000)
    assert bucket.consume(10_000) == 0.0
    assert len(calls) == 1  # tried once, gave up rather than spinning


def test_configure_rejects_a_non_positive_rate():
    clock = FakeClock()
    bucket = _bucket(1000, clock)
    with pytest.raises(ValueError):
        bucket.configure(0)
    with pytest.raises(ValueError):
        bucket.configure(-1)
    with pytest.raises(ValueError):
        bucket.configure(1000, capacity=0)


def test_switching_to_unlimited_makes_consume_free():
    clock = FakeClock()
    bucket = _bucket(1000, clock)
    bucket.consume(5000)

    bucket.configure(None)
    assert bucket.rate is None
    assert bucket.consume(10 ** 9) == 0.0
    assert clock.time() == pytest.approx(4.0)  # unchanged by the free consume


def test_debt_survives_a_rate_change_but_credit_is_clamped():
    clock = FakeClock()
    bucket = _bucket(1000, clock)
    clock.advance(10)
    assert bucket.tokens == pytest.approx(1000)

    bucket.configure(100)  # capacity is now 100
    assert bucket.tokens == pytest.approx(100)


# ---------------------------------------------------------------------------
# 4. The core property: aggregate limiting across concurrent workers
# ---------------------------------------------------------------------------
def test_concurrent_workers_share_one_limit_in_aggregate():
    """Eight threads must together stay under the rate, not each of them."""
    rate = 1000
    workers = 8
    rounds = 10
    per_request = 250
    total = workers * rounds * per_request  # 20000 bytes

    clock = FakeClock()
    bucket = _bucket(rate, clock)
    capacity = bucket.capacity

    record_lock = threading.Lock()
    released = 0
    samples = []
    errors = []

    def worker():
        nonlocal released
        try:
            for _ in range(rounds):
                bucket.consume(per_request)
                with record_lock:
                    released += per_request
                    samples.append((clock.time(), released))
        except Exception as exc:  # pragma: no cover - a failure is the signal
            errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(workers)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=30)
        assert not thread.is_alive(), "a worker never finished: the bucket starved it"

    assert not errors
    assert released == total

    # The invariant: at the moment any byte is released, the bytes released so
    # far never exceed the burst plus what the rate has earned by then.  This is
    # what "the limit applies to the pool, not to each worker" means.
    for moment, cumulative in samples:
        assert cumulative <= capacity + rate * moment + 1e-6

    # And the run really was throttled - it could not have finished sooner.
    assert clock.time() >= (total - capacity) / rate - 1e-6


def test_no_tokens_are_lost_under_contention():
    """Every byte is charged exactly once, however the threads interleave."""
    clock = FakeClock()
    limiter = BandwidthLimiter(
        parse_bwlimit("64k"), clock=clock.time, sleep=clock.sleep,
        wall_clock=lambda: datetime(2026, 7, 28, 3, 0),
    )
    workers, rounds, size = 6, 25, 4096

    def worker():
        for _ in range(rounds):
            limiter.consume(size)

    threads = [threading.Thread(target=worker) for _ in range(workers)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=30)
        assert not thread.is_alive()

    assert limiter.total_bytes == workers * rounds * size


def test_every_worker_completes_its_rounds_within_a_bounded_horizon():
    """Bounded rounds, bounded work: no worker is left behind indefinitely."""
    clock = FakeClock()
    bucket = _bucket(2000, clock)
    workers, rounds = 5, 8
    completed = {}

    def worker(index):
        for round_index in range(rounds):
            bucket.consume(500)
            completed[(index, round_index)] = clock.time()

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(workers)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=30)
        assert not thread.is_alive()

    assert len(completed) == workers * rounds


# ---------------------------------------------------------------------------
# 5. Size parsing - binary units, rclone spellings
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("text,expected", [
    ("off", None),
    ("OFF", None),
    ("unlimited", None),
    ("0", None),
    ("512", 512 * KIB),        # a bare number is KiB/s, exactly as in rclone
    ("512k", 512 * KIB),
    ("512K", 512 * KIB),
    ("10M", 10 * MIB),
    ("10m", 10 * MIB),
    ("1G", 1024 * MIB),
    ("1T", 1024 * 1024 * MIB),
    ("512b", 512),
    ("512B", 512),
    ("10MiB", 10 * MIB),
    ("10MB", 10 * MIB),
    ("1.5M", int(1.5 * MIB)),
    ("  512k  ", 512 * KIB),
])
def test_parse_size_accepts_rclone_spellings(text, expected):
    assert parse_size(text) == expected


def test_units_are_binary_not_decimal():
    assert parse_size("1k") == 1024
    assert parse_size("1M") == 1048576


def test_format_rate_round_trips_the_common_cases():
    assert format_rate(None) == "unlimited"
    assert format_rate(512 * KIB) == "512 KiB/s"
    assert format_rate(10 * MIB) == "10 MiB/s"
    assert format_rate(512) == "512 B/s"


# ---------------------------------------------------------------------------
# 6. Schedule parsing - the valid forms
# ---------------------------------------------------------------------------
def test_constant_limit_applies_at_every_moment():
    schedule = parse_bwlimit("512k")
    assert schedule.is_constant
    assert schedule.is_unlimited is False
    for hour in range(0, 24, 3):
        assert schedule.limit_at(datetime(2026, 7, 28, hour, 17)) == 512 * KIB


def test_off_is_a_schedule_that_never_limits():
    schedule = parse_bwlimit("off")
    assert schedule.is_unlimited
    assert schedule.limit_at(datetime(2026, 7, 28, 3, 0)) is None


def test_full_rclone_timetable():
    schedule = parse_bwlimit(
        "08:00,512k 12:00,10M 13:00,off 18:00,30M 23:00,off"
    )
    assert not schedule.is_constant
    assert not schedule.is_unlimited
    # 5 change points, expanded over 7 days.
    assert len(schedule.entries) == 35

    day = 28  # a Tuesday
    assert schedule.limit_at(datetime(2026, 7, day, 9, 0)) == 512 * KIB
    assert schedule.limit_at(datetime(2026, 7, day, 12, 30)) == 10 * MIB
    assert schedule.limit_at(datetime(2026, 7, day, 13, 0)) is None
    assert schedule.limit_at(datetime(2026, 7, day, 17, 59)) is None
    assert schedule.limit_at(datetime(2026, 7, day, 18, 0)) == 30 * MIB
    assert schedule.limit_at(datetime(2026, 7, day, 22, 59)) == 30 * MIB
    assert schedule.limit_at(datetime(2026, 7, day, 23, 30)) is None


def test_boundary_is_inclusive_at_its_own_minute():
    schedule = parse_bwlimit("08:00,512k 20:00,off")
    assert schedule.limit_at(datetime(2026, 7, 28, 7, 59)) is None
    assert schedule.limit_at(datetime(2026, 7, 28, 8, 0)) == 512 * KIB


def test_weekday_prefixed_entries():
    schedule = parse_bwlimit("Mon-08:00,512k Fri-18:00,10M Sun-20:00,off")
    assert len(schedule.entries) == 3

    monday = datetime(2026, 7, 27, 9, 0)
    friday = datetime(2026, 7, 31, 19, 0)
    saturday = datetime(2026, 8, 1, 12, 0)

    assert schedule.limit_at(monday) == 512 * KIB
    assert schedule.limit_at(friday) == 10 * MIB
    assert schedule.limit_at(saturday) == 10 * MIB  # still Friday's rule


def test_weekday_names_are_case_insensitive():
    schedule = parse_bwlimit("MON-08:00,1M")
    assert schedule.limit_at(datetime(2026, 7, 27, 9, 0)) == MIB


def test_a_named_day_overrides_an_every_day_entry_at_the_same_minute():
    schedule = parse_bwlimit("08:00,1M Wed-08:00,off")
    assert schedule.limit_at(datetime(2026, 7, 28, 9, 0)) == MIB       # Tuesday
    assert schedule.limit_at(datetime(2026, 7, 29, 9, 0)) is None      # Wednesday
    assert schedule.limit_at(datetime(2026, 7, 30, 9, 0)) == MIB       # Thursday


def test_single_digit_hours_are_accepted():
    schedule = parse_bwlimit("8:00,1M 20:00,off")
    assert schedule.limit_at(datetime(2026, 7, 28, 9, 0)) == MIB


def test_describe_names_the_source_and_the_active_rate():
    clock = FakeClock()
    limiter = BandwidthLimiter(
        parse_bwlimit("08:00,512k 20:00,off"),
        clock=clock.time, sleep=clock.sleep,
        wall_clock=lambda: datetime(2026, 7, 28, 9, 0),
    )
    described = limiter.describe()
    assert "08:00,512k" in described
    assert "512 KiB/s" in described


# ---------------------------------------------------------------------------
# 7. Midnight and week wrap-around
# ---------------------------------------------------------------------------
def test_wraps_around_midnight():
    """At 02:00 the rule still in force is the one 23:00 set yesterday."""
    schedule = parse_bwlimit("08:00,512k 23:00,off")
    assert schedule.limit_at(datetime(2026, 7, 28, 2, 0)) is None
    assert schedule.limit_at(datetime(2026, 7, 28, 0, 0)) is None
    assert schedule.limit_at(datetime(2026, 7, 28, 7, 59)) is None


def test_wraps_around_the_start_of_the_week():
    """Before the only change-point of the week, last week's rule applies."""
    schedule = parse_bwlimit("Mon-08:00,1M")
    # Sunday night, and Monday morning before 08:00, both wrap to the last
    # (only) entry of the week.
    assert schedule.limit_at(datetime(2026, 8, 2, 23, 0)) == MIB   # Sunday
    assert schedule.limit_at(datetime(2026, 7, 27, 7, 59)) == MIB  # Monday early


def test_midnight_entry_is_honoured_exactly():
    schedule = parse_bwlimit("00:00,1M 12:00,off")
    assert schedule.limit_at(datetime(2026, 7, 28, 0, 0)) == MIB
    assert schedule.limit_at(datetime(2026, 7, 28, 11, 59)) == MIB
    assert schedule.limit_at(datetime(2026, 7, 28, 12, 0)) is None


# ---------------------------------------------------------------------------
# 8. Invalid schedules - rejected, with the offending token named
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("spec,offender", [
    ("", "empty bandwidth limit"),
    ("   ", "empty bandwidth limit"),
    ("512x", "512x"),
    ("-1M", "-1M"),
    ("abc", "abc"),
    ("1M:2M", "1M:2M"),
    ("08:00", "08:00"),
    ("08:00,", "08:00,"),
    ("25:00,1M", "25:00"),
    ("08:60,1M", "08:60"),
    ("8:0,1M", "8:0"),
    ("Xyz-08:00,1M", "Xyz"),
    ("08:00,1M,2M", "08:00,1M,2M"),
    ("08:00,512k 512k", "512k"),
    ("08:00,512k 09:00", "09:00"),
    ("08:00,1M 08:00,2M", "08:00,2M"),
    ("Mon-08:00,1M Mon-08:00,2M", "Mon-08:00,2M"),
    ("08:00,512k 12:00,10Q", "10Q"),
    ("512k 10M", "10M"),
    ("08:00,0.0001b", "0.0001b"),
])
def test_invalid_bwlimit_is_rejected_and_names_the_token(spec, offender):
    with pytest.raises(BandwidthLimitError) as excinfo:
        parse_bwlimit(spec)
    assert offender in str(excinfo.value)


def test_a_bad_value_is_never_silently_treated_as_unlimited():
    for spec in ("512x", "nonsense", "08:00,oops"):
        with pytest.raises(BandwidthLimitError):
            parse_bwlimit(spec)
        with pytest.raises(BandwidthLimitError):
            create_limiter(spec)


# ---------------------------------------------------------------------------
# 9. The unlimited path allocates nothing
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("spec", [None, "off", "OFF", "0", "unlimited",
                                  "08:00,off 20:00,off"])
def test_unlimited_specs_allocate_no_limiter(spec):
    assert create_limiter(spec) is None


def test_an_existing_limiter_is_passed_through():
    clock = FakeClock()
    limiter = BandwidthLimiter(
        parse_bwlimit("1M"), clock=clock.time, sleep=clock.sleep,
    )
    assert create_limiter(limiter) is limiter


# ---------------------------------------------------------------------------
# 10. Boundary crossing mid-transfer
# ---------------------------------------------------------------------------
def test_crossing_a_boundary_changes_the_limit_without_a_restart():
    clock = FakeClock()
    wall = FakeWallClock(datetime(2026, 7, 28, 8, 30))
    limiter = BandwidthLimiter(
        parse_bwlimit("08:00,1k 12:00,10k 13:00,off"),
        clock=clock.time, sleep=clock.sleep, wall_clock=wall,
    )

    assert limiter.current_limit() == KIB
    limiter.consume(KIB)                    # burst, free
    assert limiter.consume(KIB) == pytest.approx(1.0)

    # The clock crosses noon in the middle of the run.
    wall.moment = datetime(2026, 7, 28, 12, 30)
    assert limiter.current_limit() == 10 * KIB
    assert limiter.bucket.rate == pytest.approx(10 * KIB)

    before = clock.time()
    limiter.consume(20 * KIB)
    # Crossing a boundary does NOT hand out a fresh burst - the balance carries
    # over, and it was zero - so all 20 KiB is paid for at the new 10 KiB/s.
    assert clock.time() - before == pytest.approx(2.0)
    assert limiter.changes == 1


def test_crossing_into_off_stops_throttling_entirely():
    clock = FakeClock()
    wall = FakeWallClock(datetime(2026, 7, 28, 8, 30))
    limiter = BandwidthLimiter(
        parse_bwlimit("08:00,1k 23:00,off"),
        clock=clock.time, sleep=clock.sleep, wall_clock=wall,
    )
    limiter.consume(4 * KIB)
    assert clock.time() > 0

    wall.moment = datetime(2026, 7, 28, 23, 30)
    marker = clock.time()
    assert limiter.consume(100 * MIB) == 0.0
    assert clock.time() == pytest.approx(marker)
    assert limiter.current_limit() is None


def test_boundary_is_re_evaluated_on_every_consume_not_only_at_startup():
    clock = FakeClock()
    wall = FakeWallClock(datetime(2026, 7, 28, 8, 30))
    limiter = BandwidthLimiter(
        parse_bwlimit("08:00,1k 09:00,2k 10:00,4k"),
        clock=clock.time, sleep=clock.sleep, wall_clock=wall,
    )
    seen = []
    for hour in (8, 9, 10):
        wall.moment = datetime(2026, 7, 28, hour, 30)
        limiter.consume(1)
        seen.append(limiter.bucket.rate)
    assert seen == [KIB, 2 * KIB, 4 * KIB]
    assert limiter.changes == 2


# ---------------------------------------------------------------------------
# 11. Integration with DownloadManager
# ---------------------------------------------------------------------------
MTIME = datetime(2026, 1, 2, 3, 4, 5)


class _StreamCtx:
    """A response body that can only be read sequentially (no content-length)."""

    def __init__(self, content, headers, chunk_bytes=4):
        self.headers = headers
        self.url = "https://example.invalid/download"
        self._content = content
        self._chunk_bytes = chunk_bytes

    def iter_content(self, chunk_size=None):
        size = self._chunk_bytes
        for offset in range(0, len(self._content), size):
            yield self._content[offset:offset + size]

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class StreamNode:
    """DriveNode stand-in whose download response has no content-length."""

    type = "file"

    def __init__(self, name="setup.app", content=b"0123456789abcdef"):
        self.name = name
        self._content = content
        self.size = len(content)
        self.date_modified = MTIME
        self.date_changed = None
        self.url = "https://example.invalid/download"
        self.headers = {"content-type": "application/octet-stream"}

    def open(self, stream=True):
        return _StreamCtx(self._content, dict(self.headers))


class _FakeResp:
    def __init__(self, content, status_code=206):
        self.content = content
        self.status_code = status_code
        self.url = "http://example.com"

    def raise_for_status(self):
        if self.status_code >= 400:
            raise Exception("HTTP error")


def _manager(tmp_path, **kwargs):
    kwargs.setdefault("max_retries", 1)
    dm = DownloadManager(email="user@example.com", chunk_size=4, **kwargs)
    dm.root_path = tmp_path
    dm.sync_state = SyncState(tmp_path)
    return dm


def _limiter(rate="1M", moment=datetime(2026, 7, 28, 3, 0)):
    clock = FakeClock()
    return clock, BandwidthLimiter(
        parse_bwlimit(rate), clock=clock.time, sleep=clock.sleep,
        wall_clock=lambda: moment,
    )


def test_manager_without_bwlimit_behaves_exactly_as_before(tmp_path):
    dm = _manager(tmp_path)
    assert dm.bwlimit is None
    assert dm.limiter is None

    node = StreamNode(content=b"0123456789abcdef")
    local_path = tmp_path / "setup.app"
    assert dm.download_drive_item(node, local_path) is True

    assert local_path.read_bytes() == b"0123456789abcdef"
    summary = dm.generate_summary_report()["summary"]
    assert summary["successful"] == 1
    assert summary["failed"] == 0
    assert summary["total_bytes_transferred"] == 16
    # And the throttle hook is a no-op with no limiter allocated.
    assert dm._throttle(10_000_000) is None


def test_manager_with_a_limit_routes_streamed_bytes_through_the_limiter(tmp_path):
    clock, limiter = _limiter("1M")
    dm = _manager(tmp_path, bwlimit=limiter)
    assert dm.limiter is limiter
    assert dm.bwlimit == "1M"

    node = StreamNode(content=b"x" * 64)
    local_path = tmp_path / "setup.app"
    assert dm.download_drive_item(node, local_path) is True

    assert local_path.read_bytes() == b"x" * 64
    assert limiter.total_bytes == 64


def test_manager_with_a_limit_routes_ranged_chunks_through_the_limiter(monkeypatch):
    clock, limiter = _limiter("1M")
    dm = DownloadManager(email="user@example.com", max_retries=1, bwlimit=limiter)

    monkeypatch.setattr(
        dm.http, "get",
        lambda url, headers, stream, timeout: _FakeResp(b"abc"),
    )
    assert dm.download_chunk("http://example.com/file", 0, 2) == b"abc"
    assert limiter.total_bytes == 3


def test_a_slow_limit_actually_delays_a_streamed_download(tmp_path):
    """1 KiB/s and a 4 KiB file: the run takes three seconds of clock time."""
    clock, limiter = _limiter("1k")
    dm = _manager(tmp_path, bwlimit=limiter)
    dm.chunker.chunk_size = 1024

    node = StreamNode(content=b"y" * 4096)
    local_path = tmp_path / "big.app"
    assert dm.download_drive_item(node, local_path) is True

    assert local_path.stat().st_size == 4096
    # 1 KiB of burst, 3 KiB paid for at 1 KiB/s. The download still completed.
    assert clock.time() == pytest.approx(3.0)


def test_manager_accepts_a_bwlimit_string(tmp_path):
    dm = _manager(tmp_path, bwlimit="512k")
    assert dm.bwlimit == "512k"
    assert dm.limiter is not None
    assert dm.limiter.current_limit() == 512 * KIB


def test_manager_with_an_off_bwlimit_allocates_no_limiter(tmp_path):
    dm = _manager(tmp_path, bwlimit="off")
    assert dm.limiter is None


def test_manager_rejects_a_bad_bwlimit_at_construction(tmp_path):
    with pytest.raises(BandwidthLimitError) as excinfo:
        _manager(tmp_path, bwlimit="512x")
    assert "512x" in str(excinfo.value)


def test_a_failing_limiter_disables_itself_instead_of_failing_the_download(tmp_path):
    class ExplodingLimiter(BandwidthLimiter):
        def consume(self, amount):
            raise RuntimeError("boom")

    clock = FakeClock()
    limiter = ExplodingLimiter(
        parse_bwlimit("1k"), clock=clock.time, sleep=clock.sleep,
        wall_clock=lambda: datetime(2026, 7, 28, 3, 0),
    )
    dm = _manager(tmp_path, bwlimit=limiter)

    node = StreamNode(content=b"z" * 32)
    local_path = tmp_path / "setup.app"
    assert dm.download_drive_item(node, local_path) is True
    assert local_path.read_bytes() == b"z" * 32
    assert dm.limiter is None  # dropped, loudly, rather than retried forever


# ---------------------------------------------------------------------------
# 12. Schedule construction helpers
# ---------------------------------------------------------------------------
def test_schedule_constant_helper():
    schedule = BandwidthSchedule.constant(1000, "1000b")
    assert schedule.is_constant
    assert schedule.limit_at(datetime(2026, 7, 28, 5, 0)) == 1000
    assert schedule.describe() == "1000 B/s"


def test_schedule_entry_describe_names_the_day_and_time():
    schedule = parse_bwlimit("Wed-13:45,512k")
    assert schedule.entries[0].describe() == "Wed 13:45 -> 512 KiB/s"
