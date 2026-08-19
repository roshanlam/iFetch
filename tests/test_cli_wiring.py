"""Tests for the wiring in ``ifetch.cli.main``.

Why this file exists
--------------------
``cli.py`` is where every subsystem is actually connected: the bandwidth limit
reaches ``DownloadManager`` from here, and the notifier's start/finish/failure
calls are made from here. A mistake at this layer does not fail loudly - it
silently disables a feature that all its own unit tests still pass. A limiter
that is never passed through throttles nothing while ``tests/test_ratelimit.py``
stays green; a notifier that is never called pages nobody while
``tests/test_notify.py`` stays green.

So these tests assert the connections, not the behaviour behind them. The
behaviour has its own files.

Everything here runs with ``DownloadManager`` replaced, so no credentials, no
network and no filesystem writes outside ``tmp_path``.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))

from ifetch import cli as cli_module  # noqa: E402

REPORT = {
    "summary": {
        "total_files": 3,
        "successful": 3,
        "failed": 0,
        "skipped": 0,
        "total_bytes_transferred": 1024,
        "total_changed_chunks": 0,
    }
}


class FakeDownloader:
    """Records how it was constructed and what was asked of it."""

    instances: list = []

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.calls = []
        FakeDownloader.instances.append(self)

    def authenticate(self, two_factor=None):
        self.calls.append("authenticate")

    def download(self, icloud_path, local_path, log_file=None):
        self.calls.append("download")

    def list_contents(self, path):
        self.calls.append("list_contents")

    def generate_summary_report(self):
        return REPORT


class RecordingNotifier:
    """A notifier that records the lifecycle calls made against it."""

    def __init__(self):
        self.events = []

    def start(self, **details):
        self.events.append(("start", details))

    def run_finished(self, report=None, *, error=None, findings=()):
        self.events.append(("run_finished", report))

    def failure(self, error, **details):
        self.events.append(("failure", str(error)))

    def __getattr__(self, name):
        # Anything else the CLI might reach for is a no-op, so a new call site
        # fails this file's assertions rather than crashing the run.
        return lambda *a, **k: None


@pytest.fixture
def harness(monkeypatch, tmp_path):
    """Run ``main`` with the downloader and notifier replaced."""
    FakeDownloader.instances = []
    notifier = RecordingNotifier()
    monkeypatch.setattr(cli_module, "DownloadManager", FakeDownloader)
    monkeypatch.setattr(cli_module, "build_notifier", lambda args: notifier)
    monkeypatch.setenv("ICLOUD_EMAIL", "you@example.com")

    def run(*extra):
        argv = ["Documents", str(tmp_path / "dest"), "--email", "you@example.com"]
        argv.extend(extra)
        try:
            cli_module.main(argv)
            return 0
        except SystemExit as exc:
            return exc.code if exc.code is not None else 0

    return run, notifier


# ---------------------------------------------------------------------------
# --bwlimit
# ---------------------------------------------------------------------------


class TestBandwidthFlag:
    def test_a_valid_limit_reaches_the_download_manager(self, harness):
        run, _ = harness
        assert run("--bwlimit", "512k") == 0
        assert FakeDownloader.instances[0].kwargs["bwlimit"] == "512k"

    def test_a_timetable_reaches_the_download_manager_verbatim(self, harness):
        run, _ = harness
        table = "08:00,512k 13:00,off 18:00,30M"
        assert run("--bwlimit", table) == 0
        assert FakeDownloader.instances[0].kwargs["bwlimit"] == table

    def test_the_default_is_unlimited(self, harness):
        run, _ = harness
        assert run() == 0
        assert FakeDownloader.instances[0].kwargs["bwlimit"] is None

    def test_a_malformed_limit_exits_before_anything_is_constructed(self, harness, capsys):
        """Rejected at startup, not at the first byte.

        An overnight job with a typo in its bandwidth flag should fail in the
        first second, having contacted nobody - not four hours in.
        """
        run, notifier = harness
        assert run("--bwlimit", "notarate") == 2
        assert FakeDownloader.instances == []
        assert notifier.events == []

    def test_the_rejection_names_the_offending_token(self, harness, capsys):
        run, _ = harness
        run("--bwlimit", "08:00,512k 99:99,10M")
        assert "99:99" in capsys.readouterr().err

    @pytest.mark.parametrize("spec", ["off", "0"])
    def test_explicit_unlimited_values_are_accepted(self, harness, spec):
        run, _ = harness
        assert run("--bwlimit", spec) == 0


# ---------------------------------------------------------------------------
# Notifier lifecycle
# ---------------------------------------------------------------------------


class TestNotifierLifecycle:
    def test_a_successful_run_reports_start_then_finish(self, harness):
        run, notifier = harness
        assert run() == 0
        assert [name for name, _ in notifier.events] == ["start", "run_finished"]

    def test_the_start_event_precedes_authentication(self, harness):
        """A dead-man's switch has to be armed before the step that can hang.

        Authentication is where an expired session stalls, so a start ping sent
        afterwards is a start ping that never arrives for the run that most
        needed it.
        """
        run, notifier = harness
        run()
        downloader = FakeDownloader.instances[0]
        assert notifier.events[0][0] == "start"
        assert downloader.calls[0] == "authenticate"

    def test_the_start_event_carries_both_paths(self, harness, tmp_path):
        run, notifier = harness
        run()
        _, details = notifier.events[0]
        assert details["icloud_path"] == "Documents"
        assert str(tmp_path / "dest") in details["local_path"]

    def test_the_finish_event_carries_the_full_report_not_just_the_summary(self, harness):
        """``run_finished`` decides success-vs-anomaly from the whole report.

        Handing it ``report["summary"]`` would look identical here and would
        quietly cost every anomaly notification.
        """
        run, notifier = harness
        run()
        _, payload = notifier.events[1]
        assert payload == REPORT
        assert "summary" in payload

    def test_a_failing_run_reports_the_failure(self, harness, monkeypatch):
        def explode(self, icloud_path, local_path, log_file=None):
            raise RuntimeError("Apple said 503")

        monkeypatch.setattr(FakeDownloader, "download", explode)
        run, notifier = harness
        assert run() == 1
        names = [name for name, _ in notifier.events]
        assert names == ["start", "failure"]
        assert "503" in notifier.events[-1][1]

    def test_a_failing_run_does_not_also_report_success(self, harness, monkeypatch):
        def explode(self, icloud_path, local_path, log_file=None):
            raise RuntimeError("boom")

        monkeypatch.setattr(FakeDownloader, "download", explode)
        run, notifier = harness
        run()
        assert "run_finished" not in [name for name, _ in notifier.events]

    def test_a_listing_only_run_notifies_start_but_not_completion(self, harness):
        """``--list`` transfers nothing, so it must not report a finished backup.

        A dead-man's switch that a listing run can satisfy stops proving that
        any data moved.
        """
        run, notifier = harness
        assert run("--list") == 0
        assert [name for name, _ in notifier.events] == ["start"]

    def test_an_unconfigured_notifier_does_not_break_the_run(self, monkeypatch, tmp_path):
        """The real ``build_notifier`` with nothing configured must be a no-op."""
        FakeDownloader.instances = []
        monkeypatch.setattr(cli_module, "DownloadManager", FakeDownloader)
        for var in list(cli_module.__dict__):  # no notification env in this process
            pass
        monkeypatch.delenv("IFETCH_HEALTHCHECKS_URL", raising=False)
        monkeypatch.delenv("IFETCH_NTFY_URL", raising=False)
        monkeypatch.delenv("IFETCH_WEBHOOK_URL", raising=False)

        cli_module.main([
            "Documents", str(tmp_path / "dest"), "--email", "you@example.com",
        ])
        assert FakeDownloader.instances[0].calls == ["authenticate", "download"]
