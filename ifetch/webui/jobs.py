"""Background work for the local web UI, and the counters the page renders.

A download, a guard scan or a vanish check takes minutes. An HTTP request that
waits for one is a request that times out, so the work runs on its own thread,
the endpoint answers with a job id, and the page polls ``/api/state`` for what
has happened since.

Only one job runs at a time. That is not a simplification for its own sake: two
downloads writing the same destination corrupt each other's partial files, and
a guard scan of a tree that a download is still writing produces a number that
was never true. A second start is refused with an explanation rather than
queued, because a queue would hide the refusal until much later.

Three things this module refuses to do:

* It will not invent a denominator. ``download_progress`` reports bytes for one
  file, and the full file list only exists once the walk has finished, so
  ``files_total`` and ``bytes_total`` stay ``None`` until the run reports its
  own summary - and ``bytes_total`` stays ``None`` even then, because a total
  of "what we managed to transfer" is not a total of what there was. An
  indeterminate bar is honest; a percentage of a guess is not.
* It will not count what it cannot see. Nothing is dispatched for a file proven
  unchanged, so ``skipped`` reads 0 for the length of the run and only becomes
  exact when the closing summary arrives.
* It will not claim to have stopped work it cannot stop. Cancelling sets a flag
  that the downloader consults between files; a transfer already in flight
  finishes writing what it has, which is what keeps it resumable. When a
  manager offers nothing to wrap, :func:`attach` says so in the job message
  instead of pretending the cancel button works.
"""

from __future__ import annotations

import logging
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, Iterator, List, Optional

from ..plugin import BasePlugin

logger = logging.getLogger(__name__)

__all__ = [
    "Job",
    "JobCancelled",
    "JobConflict",
    "JobRunner",
    "Progress",
    "ProgressPlugin",
    "attach",
    "STATE_CANCELLED",
    "STATE_DONE",
    "STATE_FAILED",
    "STATE_RUNNING",
]

STATE_RUNNING = "running"
STATE_DONE = "done"
STATE_FAILED = "failed"
STATE_CANCELLED = "cancelled"


class JobConflict(Exception):
    """A job was already running, so a second one was refused."""


class JobCancelled(Exception):
    """Raised by work that noticed the cancel flag and stopped early."""


class Progress:
    """Counts for one run, written from worker threads and read from handlers.

    Bytes are tracked per file because that is all the event stream offers: the
    last figure seen for each in-flight file, plus the final figure for each
    file that has finished. Summing those gives a true running total without
    ever needing to know how much there was to begin with.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._files_done = 0
        self._files_total: Optional[int] = None
        self._bytes_total: Optional[int] = None
        self._skipped = 0
        self._failed = 0
        self._current: Optional[str] = None
        self._settled_bytes = 0
        self._in_flight: Dict[str, int] = {}

    def note_current(self, label: Optional[str]) -> None:
        if label:
            with self._lock:
                self._current = label

    def note_bytes(self, key: str, downloaded: int) -> None:
        with self._lock:
            self._in_flight[key] = int(downloaded)

    def note_finished(self, key: str, success: bool) -> None:
        with self._lock:
            self._settled_bytes += self._in_flight.pop(key, 0)
            if success:
                self._files_done += 1
            else:
                self._failed += 1

    def adopt_summary(self, summary: Dict[str, Any]) -> None:
        """Replace the running estimates with the figures the run reported.

        ``files_total`` becomes known here and nowhere earlier. ``bytes_total``
        deliberately does not: the summary reports bytes transferred, and a run
        with failures transferred fewer bytes than it set out to.
        """
        with self._lock:
            self._files_total = _as_int(summary.get("total_files"), self._files_total)
            self._files_done = _as_int(summary.get("successful"), self._files_done)
            self._failed = _as_int(summary.get("failed"), self._failed)
            self._skipped = _as_int(summary.get("skipped"), self._skipped)
            transferred = _as_int(summary.get("total_bytes_transferred"), None)
            if transferred is not None:
                self._settled_bytes = transferred
                self._in_flight.clear()
            self._current = None

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "files_done": self._files_done,
                "files_total": self._files_total,
                "bytes_done": self._settled_bytes + sum(self._in_flight.values()),
                "bytes_total": self._bytes_total,
                "skipped": self._skipped,
                "failed": self._failed,
                "current": self._current,
            }


def _as_int(value: Any, fallback: Optional[int]) -> Optional[int]:
    return int(value) if isinstance(value, int) and not isinstance(value, bool) else fallback


class ProgressPlugin(BasePlugin):
    """Turns DownloadManager events into the counts ``/api/state`` returns.

    Registered on the manager's own plugin manager for the length of one job,
    which is why nothing in ``downloader.py`` had to change to support a UI.
    """

    def __init__(self, progress: Progress) -> None:
        self.progress = progress

    def before_download(self, remote_item, local_path, **kwargs):
        self.progress.note_current(_label(remote_item, local_path))

    def after_download(self, remote_item, local_path, success: bool, **kwargs):
        self.progress.note_finished(str(local_path), bool(success))

    def on_event(self, name: str, **payload):
        if name == "download_progress":
            local_path = payload.get("local_path")
            downloaded = payload.get("downloaded")
            if local_path is None or not isinstance(downloaded, int):
                return
            self.progress.note_bytes(str(local_path), downloaded)
            self.progress.note_current(_label(payload.get("remote_item"), local_path))
        elif name == "download_session_completed":
            report = payload.get("summary")
            summary = report.get("summary") if isinstance(report, dict) else None
            if isinstance(summary, dict):
                self.progress.adopt_summary(summary)


def _label(remote_item: Any, local_path: Any) -> Optional[str]:
    """The name to show as "currently working on".

    The event carries no remote path, so the item's own name is the best
    available answer and the local filename is the fallback.
    """
    name = getattr(remote_item, "name", None)
    if isinstance(name, str) and name:
        return name
    try:
        return Path(str(local_path)).name or None
    except (TypeError, ValueError):
        return None


@dataclass
class Job:
    """One unit of work and everything the page needs to draw it."""

    id: str
    kind: str
    label: str
    started_at: float
    state: str = STATE_RUNNING
    finished_at: Optional[float] = None
    message: str = ""
    result: Optional[Any] = None
    progress: Progress = field(default_factory=Progress)
    cancel_flag: threading.Event = field(default_factory=threading.Event)

    @property
    def cancelled(self) -> bool:
        return self.cancel_flag.is_set()

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "kind": self.kind,
            "state": self.state,
            "label": self.label,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "message": self.message,
            "progress": self.progress.snapshot(),
            # A result before the run is done would be a partial answer wearing
            # a finished answer's clothes.
            "result": self.result if self.state == STATE_DONE else None,
        }


@contextmanager
def attach(manager: Any, job: Job) -> Iterator[List[str]]:
    """Wire progress and cancellation onto ``manager`` for the length of a job.

    Yields a list of notes naming anything that could not be wired up, so the
    caller can say so in the job message rather than leaving a dead cancel
    button and a progress bar that never moves.
    """
    notes: List[str] = []
    registry = getattr(getattr(manager, "plugin_manager", None), "_plugins", None)
    plugin = ProgressPlugin(job.progress)
    if isinstance(registry, list):
        registry.append(plugin)
    else:
        registry = None
        notes.append("progress cannot be reported: this downloader has no plugin manager")

    original = getattr(manager, "process_item_parallel", None)
    if callable(original):
        def guarded(*args, **kwargs):
            # The one place every file and every subtree passes through, so a
            # set flag turns the rest of the walk into a series of no-ops.
            if job.cancelled:
                return None
            return original(*args, **kwargs)

        manager.process_item_parallel = guarded
    else:
        notes.append("this job cannot be stopped early: the downloader exposes no per-item step")

    try:
        yield notes
    finally:
        if registry is not None:
            try:
                registry.remove(plugin)
            except ValueError:
                pass
        if callable(original):
            try:
                del manager.process_item_parallel
            except AttributeError:
                manager.process_item_parallel = original


class JobRunner:
    """Runs one job at a time and remembers the one before it."""

    def __init__(self, clock: Callable[[], float] = time.time) -> None:
        self._lock = threading.RLock()
        self._clock = clock
        self._counter = 0
        self._current: Optional[Job] = None
        self._last: Optional[Job] = None
        self._thread: Optional[threading.Thread] = None

    @property
    def current(self) -> Optional[Job]:
        with self._lock:
            return self._current

    @property
    def last(self) -> Optional[Job]:
        with self._lock:
            return self._last

    def start(self, kind: str, label: str, work: Callable[[Job], Any]) -> Job:
        """Begin ``work`` on a thread, or raise :class:`JobConflict`."""
        with self._lock:
            running = self._current
            if running is not None:
                raise JobConflict(
                    f"a {running.kind} job ({running.id}) is already running. "
                    "Cancel it or wait for it to finish before starting another; "
                    "two runs writing the same folder would corrupt each other."
                )
            self._counter += 1
            job = Job(
                id=f"j{self._counter}",
                kind=kind,
                label=label,
                started_at=self._clock(),
            )
            self._current = job
            self._thread = threading.Thread(
                target=self._run,
                args=(job, work),
                name=f"ifetch-webui-{job.id}",
                daemon=True,
            )
        self._thread.start()
        return job

    def cancel(self) -> Optional[Job]:
        """Ask the running job to stop. Returns it, or ``None`` if idle."""
        job = self.current
        if job is None:
            return None
        job.cancel_flag.set()
        return job

    def wait(self, timeout: Optional[float] = None) -> bool:
        """Join the worker thread. Returns False if it is still going."""
        with self._lock:
            thread = self._thread
        if thread is None:
            return True
        thread.join(timeout)
        return not thread.is_alive()

    def _run(self, job: Job, work: Callable[[Job], Any]) -> None:
        try:
            result = work(job)
        except JobCancelled as exc:
            job.state = STATE_CANCELLED
            job.message = str(exc) or "Cancelled."
        except Exception as exc:  # noqa: BLE001 - a failed job is a reported job
            logger.warning("job %s (%s) failed: %s", job.id, job.kind, exc, exc_info=True)
            job.state = STATE_FAILED
            job.message = str(exc) or exc.__class__.__name__
        else:
            if job.cancelled:
                job.state = STATE_CANCELLED
                job.message = (
                    "Cancelled. Partial files are left in place and stay resumable."
                    if job.kind == "download"
                    else "Cancelled. The scan had already finished, so its result was discarded."
                )
            else:
                job.state = STATE_DONE
                job.result = result
        finally:
            job.finished_at = self._clock()
            with self._lock:
                self._last = job
                self._current = None
