"""Bandwidth limiting: a ceiling on what the whole run may pull.

Why this exists
---------------
An unattended mirror on a NAS with ``--max-workers 8`` will take every bit of a
home uplink at 3am, which everyone else in the house notices before the person
who wrote the cron entry does. rclone has ``--bwlimit`` and a timetable for
this; iFetch had nothing. This is that piece, and it speaks rclone's syntax so
nobody has to learn a second one.

What it actually guarantees
---------------------------
**It limits how fast this process reads from its sockets, which is not the same
as limiting what the network delivers.** The kernel keeps filling buffers
whether we read them or not, so:

* over a few seconds, the average is held to the configured rate;
* at any instant it is not - a chunk already in the socket buffer is read at
  memory speed and the pause happens after it, not during it;
* nothing here stops bytes arriving. It stops us *asking for more*, which is
  what eventually applies back-pressure. A limit well below one chunk therefore
  behaves as "one chunk, then a long wait", not as a smooth trickle.

The honest claim is an average over a window, not an instantaneous cap. For a
hard cap on the wire, shape it on the router.

Design notes
------------
*One bucket for the whole pool.* iFetch downloads through a
:class:`~concurrent.futures.ThreadPoolExecutor`, and a per-stream limiter would
multiply the limit by ``--max-workers`` - the easiest way to get this wrong. The
bucket is shared and every byte-consuming path charges the same one.

*The clock and the sleep are injected*, so tests drive real threads against a
fake clock without sleeping.

*It can never fail a download.* Parse errors are raised at startup, naming the
offending token - never swallowed, never quietly downgraded to "unlimited".
Once running, the limiter can only delay a caller; it has no path that raises.
"""

from __future__ import annotations

import re
import threading
import time
from bisect import bisect_right
from dataclasses import dataclass
from datetime import datetime
from typing import Callable, Dict, List, Optional, Sequence, Tuple, Union

__all__ = [
    "BandwidthLimitError",
    "BandwidthLimiter",
    "BandwidthSchedule",
    "ScheduleEntry",
    "TokenBucket",
    "create_limiter",
    "format_rate",
    "parse_bwlimit",
    "parse_size",
    "BURST_SECONDS",
]


KIB = 1024
MIB = 1024 * KIB
GIB = 1024 * MIB
TIB = 1024 * GIB

#: Size suffixes, matching rclone: they are **binary**.  ``1k`` is 1024 bytes
#: per second, ``1M`` is 1048576, and a bare number - again as in rclone - is
#: read as KiB/s, so ``--bwlimit 512`` and ``--bwlimit 512k`` mean the same
#: thing.  Write ``512b`` if you really meant 512 bytes per second.
_SUFFIX_FACTORS: Dict[str, int] = {
    "b": 1,
    "k": KIB,
    "m": MIB,
    "g": GIB,
    "t": TIB,
}

#: Multiplier used when a value carries no suffix at all (rclone compatibility).
_DEFAULT_FACTOR = KIB

#: How much budget the bucket may bank while idle, expressed in seconds of the
#: currently active rate.
#:
#: One second is a deliberate middle: it is enough that the first read of a
#: transfer is not delayed for any limit at or above the chunk size (the
#: default chunk is 1 MiB, so ``--bwlimit 1M`` and up start instantly), and it
#: is small enough that the pattern this feature exists to prevent - a limiter
#: that has been idle for an hour releasing an hour of credit the moment a
#: transfer starts - cannot happen.  A larger burst would make the limit true
#: on average and false exactly when someone is watching a video call.
BURST_SECONDS = 1.0

_WEEKDAYS: Dict[str, int] = {
    "mon": 0, "tue": 1, "wed": 2, "thu": 3, "fri": 4, "sat": 5, "sun": 6,
}

_MINUTES_PER_DAY = 24 * 60
_MINUTES_PER_WEEK = 7 * _MINUTES_PER_DAY

_SIZE_RE = re.compile(r"^(\d+(?:\.\d+)?)\s*([bkmgt])?(?:i?b)?$")
_TIME_RE = re.compile(r"^(\d{1,2}):(\d{2})$")


class BandwidthLimitError(ValueError):
    """A ``--bwlimit`` value that cannot be understood.

    Always carries the offending token verbatim.  A bandwidth limit that is
    silently ignored is worse than no bandwidth limit, because the user
    believes they are protected and they are not.
    """


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------
def parse_size(text: str, *, context: Optional[str] = None) -> Optional[int]:
    """Parse one rate value into bytes per second; ``None`` means unlimited.

    Accepts ``off``, ``0``, and a number with an optional binary suffix
    (``b``/``k``/``M``/``G``/``T``, case-insensitive, with an optional trailing
    ``i``/``b`` so ``10MiB`` and ``10MB`` also work).  A bare number is KiB/s,
    which is what rclone does.
    """
    where = f" in {context!r}" if context else ""
    raw = (text or "").strip()
    if not raw:
        raise BandwidthLimitError(
            f"missing bandwidth value{where}: expected a rate like '512k' or 'off'"
        )

    lowered = raw.lower()
    if lowered in ("off", "unlimited"):
        return None
    if ":" in raw:
        raise BandwidthLimitError(
            f"invalid bandwidth value {raw!r}{where}: rclone's 'upload:download' "
            "form is not accepted because iFetch only downloads; give a single rate"
        )

    match = _SIZE_RE.match(lowered)
    if not match:
        raise BandwidthLimitError(
            f"invalid bandwidth value {raw!r}{where}: expected a number with an "
            "optional b/k/M/G/T suffix (binary units), or 'off'"
        )

    number = float(match.group(1))
    factor = _SUFFIX_FACTORS[match.group(2)] if match.group(2) else _DEFAULT_FACTOR
    if number == 0:
        return None

    value = int(round(number * factor))
    if value < 1:
        raise BandwidthLimitError(
            f"bandwidth value {raw!r}{where} rounds to less than one byte per "
            "second; use 'off' for unlimited or a larger rate"
        )
    return value


@dataclass(frozen=True)
class ScheduleEntry:
    """One change-point in a timetable.

    ``minute_of_week`` is ``weekday * 1440 + hour * 60 + minute`` with Monday as
    day 0, which is what makes both midnight and end-of-week wrap-around fall
    out of a single sorted list instead of needing special cases.
    """

    minute_of_week: int
    limit: Optional[int]
    token: str
    weekday_specific: bool = False

    def describe(self) -> str:
        day = list(_WEEKDAYS)[self.minute_of_week // _MINUTES_PER_DAY]
        minutes = self.minute_of_week % _MINUTES_PER_DAY
        return f"{day.title()} {minutes // 60:02d}:{minutes % 60:02d} -> {format_rate(self.limit)}"


@dataclass(frozen=True)
class BandwidthSchedule:
    """A parsed ``--bwlimit`` value: either one constant rate or a timetable.

    Timetable semantics follow rclone: each entry is the moment a limit *starts*
    applying, and it applies until the next entry.  The limit in force at some
    instant is therefore the entry with the greatest start time at or before it,
    wrapping backwards past midnight - and past Sunday night - to the last entry
    of the week when there is none earlier.

    Times are **local time**, read from the injected wall clock, because that is
    what "don't saturate the line during the day" means to the person writing
    the flag.
    """

    source: str
    entries: Tuple[ScheduleEntry, ...] = ()
    constant_limit: Optional[int] = None
    is_constant: bool = False

    @classmethod
    def constant(cls, limit: Optional[int], source: str) -> "BandwidthSchedule":
        return cls(source=source, constant_limit=limit, is_constant=True)

    @classmethod
    def timetable(
        cls, entries: Sequence[ScheduleEntry], source: str
    ) -> "BandwidthSchedule":
        return cls(source=source, entries=tuple(entries))

    @property
    def is_unlimited(self) -> bool:
        """True when this schedule never imposes a limit at any moment."""
        if self.is_constant:
            return self.constant_limit is None
        return all(entry.limit is None for entry in self.entries)

    def limit_at(self, when: Optional[datetime] = None) -> Optional[int]:
        """Bytes per second in force at ``when`` (``None`` -> unlimited)."""
        if self.is_constant:
            return self.constant_limit
        if not self.entries:
            return None

        moment = when if when is not None else datetime.now()
        minute = (
            moment.weekday() * _MINUTES_PER_DAY
            + moment.hour * 60
            + moment.minute
        )
        starts = [entry.minute_of_week for entry in self.entries]
        index = bisect_right(starts, minute) - 1
        if index < 0:
            # Before the first change-point of the week: the limit still in
            # force is the one the last entry of the week set.
            index = len(self.entries) - 1
        return self.entries[index].limit

    def describe(self) -> str:
        if self.is_constant:
            return format_rate(self.constant_limit)
        return "; ".join(entry.describe() for entry in self.entries)


def parse_bwlimit(spec: str) -> BandwidthSchedule:
    """Parse an rclone-compatible ``--bwlimit`` value.

    Two forms::

        512k
        "08:00,512k 12:00,10M 13:00,off 18:00,30M 23:00,off"

    Timetable entries may carry a single weekday prefix (``Mon-08:00,512k``);
    an entry without one applies on every day, and a weekday-specific entry
    wins over a day-agnostic one that starts at the same minute.

    Raises :class:`BandwidthLimitError` - naming the token at fault - for
    anything it cannot read.  It never returns "unlimited" as a way of coping
    with input it did not understand.
    """
    text = (spec or "").strip()
    if not text:
        raise BandwidthLimitError(
            "empty bandwidth limit: give a rate like '512k', a timetable like "
            "'08:00,512k 23:00,off', or 'off'"
        )

    tokens = text.split()
    timed = [token for token in tokens if "," in token]

    if not timed:
        if len(tokens) > 1:
            raise BandwidthLimitError(
                f"unexpected extra value {tokens[1]!r} in {text!r}: a timetable "
                "entry looks like 'HH:MM,RATE'"
            )
        if _TIME_RE.match(tokens[0].partition("-")[2] or tokens[0]):
            raise BandwidthLimitError(
                f"timetable entry {tokens[0]!r} is missing its rate: write "
                f"'{tokens[0]},512k' (or '{tokens[0]},off')"
            )
        return BandwidthSchedule.constant(parse_size(tokens[0]), text)

    generic: Dict[int, ScheduleEntry] = {}
    specific: Dict[int, ScheduleEntry] = {}

    for token in tokens:
        if "," not in token:
            raise BandwidthLimitError(
                f"bare rate {token!r} cannot be mixed with a timetable in "
                f"{text!r}: every entry needs a 'HH:MM,RATE' time"
            )
        if token.count(",") != 1:
            raise BandwidthLimitError(
                f"invalid timetable entry {token!r}: expected exactly one comma, "
                "as in '08:00,512k'"
            )

        head, _, size_text = token.partition(",")
        weekday: Optional[int] = None
        time_text = head
        if "-" in head:
            day_text, _, time_text = head.partition("-")
            weekday = _WEEKDAYS.get(day_text.strip().lower())
            if weekday is None:
                raise BandwidthLimitError(
                    f"unknown weekday {day_text!r} in {token!r}: use one of "
                    "Mon, Tue, Wed, Thu, Fri, Sat, Sun"
                )

        match = _TIME_RE.match(time_text.strip())
        if not match:
            raise BandwidthLimitError(
                f"invalid time {time_text!r} in {token!r}: expected HH:MM"
            )
        hour, minute = int(match.group(1)), int(match.group(2))
        if hour > 23 or minute > 59:
            raise BandwidthLimitError(
                f"invalid time {time_text!r} in {token!r}: hour must be 00-23 "
                "and minute 00-59"
            )

        limit = parse_size(size_text, context=token)
        offset = hour * 60 + minute

        if weekday is None:
            for day in range(7):
                start = day * _MINUTES_PER_DAY + offset
                existing = generic.get(start)
                if existing is not None:
                    raise BandwidthLimitError(
                        f"duplicate timetable entry {token!r}: {existing.token!r} "
                        "already sets a limit at that time"
                    )
                generic[start] = ScheduleEntry(start, limit, token)
        else:
            start = weekday * _MINUTES_PER_DAY + offset
            existing = specific.get(start)
            if existing is not None:
                raise BandwidthLimitError(
                    f"duplicate timetable entry {token!r}: {existing.token!r} "
                    "already sets a limit at that time"
                )
            specific[start] = ScheduleEntry(start, limit, token, weekday_specific=True)

    merged: Dict[int, ScheduleEntry] = dict(generic)
    merged.update(specific)  # a named day beats "every day" at the same minute
    entries: List[ScheduleEntry] = [merged[key] for key in sorted(merged)]
    return BandwidthSchedule.timetable(entries, text)


def format_rate(limit: Optional[int]) -> str:
    """Render a byte-per-second rate the way it was most likely written."""
    if limit is None:
        return "unlimited"
    for suffix, factor in (("GiB", GIB), ("MiB", MIB), ("KiB", KIB)):
        if limit >= factor and limit % factor == 0:
            return f"{limit // factor} {suffix}/s"
    if limit >= MIB:
        return f"{limit / MIB:.2f} MiB/s"
    if limit >= KIB:
        return f"{limit / KIB:.2f} KiB/s"
    return f"{limit} B/s"


# ---------------------------------------------------------------------------
# The bucket
# ---------------------------------------------------------------------------
class TokenBucket:
    """A thread-safe token bucket shared by every worker in the pool.

    Tokens accrue at ``rate`` bytes per second up to ``capacity`` bytes, and a
    caller that cannot be paid immediately is put into *debt*: the tokens are
    deducted at admission time, the balance is allowed to go negative, and the
    caller sleeps until the balance would be back at zero.  Three things follow
    from that, all of them deliberate:

    * a request larger than the whole capacity works, and simply waits
      proportionally - it can never deadlock waiting for a bucket that will
      never hold enough;
    * the price of a request is fixed when it is admitted, so a caller is never
      re-quoted a higher price because other threads arrived while it slept;
    * over any window the bytes released total at most
      ``capacity + rate * window``, whatever the number of threads.

    Fairness, stated precisely
    --------------------------
    Callers are served in the order they acquire the internal lock, and each
    one's wait is settled at that moment, so **no admitted caller can be
    overtaken in a way that extends its own wait**.  What is *not* guaranteed is
    the admission order itself: :class:`threading.Lock` is not FIFO, so a thread
    can in principle lose several races to acquire it.  There is no unbounded
    starvation - each admitted request holds a finite, already-fixed wait, and
    debt is monotonically paid down - but iFetch does not promise strict FIFO
    fairness, because it cannot deliver it on top of a non-FIFO lock.

    The clock and the sleep are injected; the core logic never touches the
    module-level ones.
    """

    def __init__(
        self,
        rate: Optional[float],
        capacity: Optional[float] = None,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._clock = clock
        self._sleep = sleep
        self._lock = threading.Lock()
        self._rate: Optional[float] = None
        self._capacity = 0.0
        self._tokens = 0.0
        self._last = clock()
        self.configure(rate, capacity)

    # -- introspection ------------------------------------------------
    @property
    def rate(self) -> Optional[float]:
        with self._lock:
            return self._rate

    @property
    def capacity(self) -> float:
        with self._lock:
            return self._capacity

    @property
    def tokens(self) -> float:
        """Current balance, refilled to now. Negative means outstanding debt."""
        with self._lock:
            self._refill_locked()
            return self._tokens

    # -- configuration ------------------------------------------------
    def configure(
        self, rate: Optional[float], capacity: Optional[float] = None
    ) -> None:
        """Switch to a new rate, crediting whatever accrued under the old one.

        ``rate=None`` means unlimited, and :meth:`consume` becomes a no-op until
        a rate is set again.  Any debt outstanding at that moment is forgiven -
        it was denominated in a limit that no longer applies.
        """
        with self._lock:
            self._refill_locked()
            if rate is None:
                self._rate = None
                self._capacity = 0.0
                self._tokens = 0.0
                return

            rate = float(rate)
            if rate <= 0:
                raise ValueError("rate must be positive, or None for unlimited")

            if capacity is None:
                new_capacity = max(1.0, rate * BURST_SECONDS)
            else:
                new_capacity = float(capacity)
                if new_capacity <= 0:
                    raise ValueError("capacity must be positive")

            was_unlimited = self._rate is None
            self._capacity = new_capacity
            if was_unlimited:
                # Arriving from "off" (or from construction): start with a full
                # burst so the first read of a transfer is not delayed.
                self._tokens = new_capacity
            else:
                # Debt survives a rate change - the bytes were really consumed -
                # but banked credit is clamped to what the new bucket can hold.
                self._tokens = min(self._tokens, new_capacity)
            self._rate = rate

    # -- the hot path -------------------------------------------------
    def consume(self, amount: int) -> float:
        """Charge ``amount`` bytes, sleeping if they are not yet affordable.

        Returns the number of seconds actually waited, measured on the injected
        clock (``0.0`` when the bucket is unlimited or the tokens were there).
        """
        amount = int(amount)
        if amount <= 0:
            return 0.0

        with self._lock:
            if self._rate is None:
                return 0.0
            self._refill_locked()
            self._tokens -= amount
            if self._tokens >= 0:
                return 0.0
            deadline = self._last + (-self._tokens) / self._rate

        return self._wait_until(deadline)

    # -- internals ----------------------------------------------------
    def _refill_locked(self) -> None:
        now = self._clock()
        elapsed = now - self._last
        if elapsed <= 0:
            # A clock that went backwards grants nothing; it must not be able to
            # mint tokens, and it must not stall the bucket forever either.
            if elapsed < 0:
                self._last = now
            return
        if self._rate is not None:
            self._tokens = min(self._capacity, self._tokens + elapsed * self._rate)
        self._last = now

    def _wait_until(self, deadline: float) -> float:
        """Sleep until ``deadline`` on the injected clock; return time waited.

        The loop re-reads the clock because a sleep is a lower bound, not an
        exact one.  It also gives up if the clock does not move, so a sleep
        function that does nothing (a test double, a suspended VM) degrades to
        "no throttling" rather than to an infinite spin.  Forward progress is
        the invariant that matters here.
        """
        start = self._clock()
        remaining = deadline - start
        while remaining > 0:
            self._sleep(remaining)
            updated = deadline - self._clock()
            if updated >= remaining:
                break  # the clock is not advancing; do not spin on it
            remaining = updated
        return max(0.0, self._clock() - start)


# ---------------------------------------------------------------------------
# Schedule-aware limiter
# ---------------------------------------------------------------------------
class BandwidthLimiter:
    """A token bucket that re-reads its schedule as the clock crosses a boundary.

    One instance is shared by the whole worker pool.  :meth:`consume` checks the
    wall clock against the schedule on every call, so a timetable boundary takes
    effect within one chunk of a long transfer - no restart, no reconnect.
    """

    def __init__(
        self,
        schedule: BandwidthSchedule,
        *,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
        wall_clock: Callable[[], datetime] = datetime.now,
        capacity: Optional[float] = None,
    ) -> None:
        self.schedule = schedule
        self._wall_clock = wall_clock
        self._capacity = capacity
        self._state_lock = threading.RLock()
        self._current_limit: Optional[int] = None
        self._configured = False
        self.total_bytes = 0
        self.total_wait = 0.0
        self.changes = 0
        self._bucket = TokenBucket(None, clock=clock, sleep=sleep)
        self._refresh()

    @property
    def bucket(self) -> TokenBucket:
        return self._bucket

    def current_limit(self) -> Optional[int]:
        """The limit in force right now, in bytes per second (None: off)."""
        return self._refresh()

    def describe(self) -> str:
        return (
            f"{self.schedule.source} (currently {format_rate(self.current_limit())})"
        )

    def consume(self, amount: int) -> float:
        """Charge ``amount`` bytes against the active limit; return seconds waited."""
        amount = int(amount)
        if amount <= 0:
            return 0.0
        self._refresh()
        waited = self._bucket.consume(amount)
        with self._state_lock:
            self.total_bytes += amount
            self.total_wait += waited
        return waited

    def _refresh(self) -> Optional[int]:
        limit = self.schedule.limit_at(self._wall_clock())
        with self._state_lock:
            if self._configured and limit == self._current_limit:
                return limit
            if self._configured:
                self.changes += 1
            self._current_limit = limit
            self._configured = True
            # Held across configure() so two threads crossing the boundary
            # together cannot leave the bucket on the losing thread's rate.
            self._bucket.configure(limit, self._capacity)
        return limit


def create_limiter(
    spec: Union[str, "BandwidthLimiter", None],
    *,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
    wall_clock: Callable[[], datetime] = datetime.now,
    capacity: Optional[float] = None,
) -> Optional[BandwidthLimiter]:
    """Build the limiter for a ``--bwlimit`` value, or ``None`` for unlimited.

    ``None`` is returned - meaning **no object is allocated and the download
    path keeps its original zero-overhead shape** - only when the value is
    absent or the schedule imposes no limit at any moment of the week.  An
    already-built limiter is passed through unchanged, which is how tests inject
    a fake clock.

    A malformed value raises :class:`BandwidthLimitError` here, at startup,
    rather than being discovered halfway through a transfer.
    """
    if spec is None:
        return None
    if isinstance(spec, BandwidthLimiter):
        return spec

    schedule = parse_bwlimit(spec)
    if schedule.is_unlimited:
        return None
    return BandwidthLimiter(
        schedule, clock=clock, sleep=sleep, wall_clock=wall_clock, capacity=capacity
    )
