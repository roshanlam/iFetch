"""Up-front validation of worker-count options in ``ifetch.cli.main``.

``--max-workers 0`` (or ``--range-concurrency 0``) used to sail through
argument parsing and die with ``ValueError`` inside ThreadPoolExecutor deep
in the first download, after the banner printed and authentication was
attempted. Both are now rejected by the parser (exit 2) before any output
or network I/O, mirroring the existing ``--bwlimit`` validation.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))

from ifetch import cli as cli_module  # noqa: E402


class FakeDownloader:
    """Accepts any construction kwargs; records the run."""

    instances: list = []

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        FakeDownloader.instances.append(self)

    def authenticate(self, two_factor=None):
        pass

    def download(self, icloud_path, local_path, log_file=None):
        pass

    def generate_summary_report(self):
        return {
            "summary": {
                "total_files": 0,
                "successful": 0,
                "failed": 0,
                "skipped": 0,
                "total_bytes_transferred": 0,
                "total_changed_chunks": 0,
            }
        }


class NoopNotifier:
    def __getattr__(self, name):
        return lambda *a, **k: None


@pytest.fixture
def run_cli(monkeypatch, tmp_path):
    FakeDownloader.instances = []
    monkeypatch.setattr(cli_module, "DownloadManager", FakeDownloader)
    monkeypatch.setattr(cli_module, "build_notifier", lambda args: NoopNotifier())
    monkeypatch.setenv("ICLOUD_EMAIL", "you@example.com")

    def run(*extra):
        argv = ["Documents", str(tmp_path / "dest"), "--email", "you@example.com"]
        argv.extend(extra)
        try:
            cli_module.main(argv)
            return 0
        except SystemExit as exc:
            return exc.code if exc.code is not None else 0

    return run


class TestWorkerCountValidation:
    @pytest.mark.parametrize("flag", ["--max-workers", "--range-concurrency"])
    @pytest.mark.parametrize("bad", ["0", "-3"])
    def test_non_positive_counts_exit_2(self, run_cli, bad, flag, capsys):
        assert run_cli(flag, bad) == 2
        err = capsys.readouterr().err
        assert "positive integer" in err
        assert FakeDownloader.instances == []

    def test_positive_counts_still_reach_downloader(self, run_cli):
        assert run_cli("--max-workers", "2", "--range-concurrency", "2") == 0
        assert FakeDownloader.instances[0].kwargs["max_workers"] == 2
        assert FakeDownloader.instances[0].kwargs["range_concurrency"] == 2
