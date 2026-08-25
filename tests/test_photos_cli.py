"""Tests for the ifetch-photos CLI (ifetch.photos_cli).

The engine is stubbed out entirely, so nothing here touches the network.
"""

import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))

from ifetch import photos_cli  # noqa: E402


class StubDownloader:
    """Records how the CLI drove the engine."""

    instances = []

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.authenticated = False
        self.download_calls = []
        self.albums = [
            {"name": "Library", "fullname": "Library", "count": 3},
            {"name": "Trip", "fullname": "Folder/Trip", "count": None},
        ]
        self.report = {
            "summary": {
                "total_assets": 3,
                "downloaded": 3,
                "skipped": 0,
                "failed": 0,
                "total_bytes_transferred": 2048,
            },
            "details": [],
        }
        StubDownloader.instances.append(self)

    def authenticate(self):
        self.authenticated = True

    def list_albums(self):
        return self.albums

    def download(self, **kwargs):
        self.download_calls.append(kwargs)
        return self.report


@pytest.fixture(autouse=True)
def _stub_engine(monkeypatch):
    StubDownloader.instances = []
    monkeypatch.setattr(photos_cli, "PhotosDownloader", StubDownloader)
    return StubDownloader


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------
def test_help_exits_zero(capsys):
    with pytest.raises(SystemExit) as exc:
        photos_cli.parse_args(["--help"])
    assert exc.value.code == 0
    assert "ifetch-photos" in capsys.readouterr().out


def test_defaults():
    args = photos_cli.parse_args([])

    assert args.local_path == "."
    assert args.folder_structure == "date"
    assert args.version == "original"
    assert args.max_workers == 4
    assert args.max_retries == 3
    assert args.live_photos is False
    assert args.dry_run is False
    assert args.set_mtime is True


def test_flag_parsing():
    args = photos_cli.parse_args(
        [
            "/tmp/photos",
            "--email", "user@example.com",
            "--album", "Family",
            "--since", "2024-01-01",
            "--until", "2024-12-31",
            "--folder-structure", "flat",
            "--live-photos",
            "--max-workers", "8",
            "--max-retries", "5",
            "--log-file", "/tmp/log.json",
            "--dry-run",
        ]
    )

    assert args.local_path == "/tmp/photos"
    assert args.album == "Family"
    assert args.since == "2024-01-01"
    assert args.folder_structure == "flat"
    assert args.live_photos is True
    assert args.max_workers == 8
    assert args.max_retries == 5
    assert args.log_file == "/tmp/log.json"
    assert args.dry_run is True


def test_invalid_folder_structure_rejected_by_argparse():
    with pytest.raises(SystemExit):
        photos_cli.parse_args(["--folder-structure", "sideways"])


# ---------------------------------------------------------------------------
# main()
# ---------------------------------------------------------------------------
def test_main_success_returns_zero(capsys):
    rc = photos_cli.main(["/tmp/photos", "--email", "user@example.com"])

    assert rc == 0
    stub = StubDownloader.instances[0]
    assert stub.authenticated is True
    call = stub.download_calls[0]
    assert call["destination"] == "/tmp/photos"
    assert call["album"] is None
    assert call["since"] is None and call["until"] is None
    out = capsys.readouterr().out
    assert "Downloaded: 3" in out


def test_main_passes_options_through():
    photos_cli.main(
        [
            "/tmp/photos",
            "--email", "user@example.com",
            "--album", "Family",
            "--folder-structure", "flat",
            "--version", "medium",
            "--live-photos",
            "--include-deleted",
            "--no-set-mtime",
            "--max-workers", "7",
            "--max-retries", "2",
            "--since", "2024-02-03",
            "--until", "2024-04-05",
            "--log-file", "/tmp/log.json",
        ]
    )

    stub = StubDownloader.instances[0]
    assert stub.kwargs["email"] == "user@example.com"
    assert stub.kwargs["folder_structure"] == "flat"
    assert stub.kwargs["version"] == "medium"
    assert stub.kwargs["live_photos"] is True
    assert stub.kwargs["include_deleted"] is True
    assert stub.kwargs["set_mtime"] is False
    assert stub.kwargs["max_workers"] == 7
    assert stub.kwargs["max_retries"] == 2

    call = stub.download_calls[0]
    assert call["album"] == "Family"
    assert call["since"] == datetime(2024, 2, 3, tzinfo=timezone.utc)
    assert call["until"] == datetime(2024, 4, 5, tzinfo=timezone.utc)
    assert call["log_file"] == "/tmp/log.json"


def test_main_returns_one_when_an_asset_failed():
    rc_holder = {}

    class Failing(StubDownloader):
        def __init__(self, **kwargs):
            super().__init__(**kwargs)
            self.report["summary"]["failed"] = 2

    StubDownloader.instances = []
    import ifetch.photos_cli as mod
    original = mod.PhotosDownloader
    mod.PhotosDownloader = Failing
    try:
        rc_holder["rc"] = photos_cli.main(["/tmp/photos", "--email", "u@e.com"])
    finally:
        mod.PhotosDownloader = original

    assert rc_holder["rc"] == 1


def test_list_albums_output(capsys):
    rc = photos_cli.main(["--list-albums", "--email", "user@example.com"])

    assert rc == 0
    out = capsys.readouterr().out
    assert "Library (3 items)" in out
    assert "Folder/Trip" in out
    assert "2 album(s)" in out
    # Listing must not trigger a download.
    assert StubDownloader.instances[0].download_calls == []


def test_dry_run_reports_would_download(capsys):
    class Dry(StubDownloader):
        def __init__(self, **kwargs):
            super().__init__(**kwargs)
            self.report["summary"]["dry_run"] = True
            self.report["summary"]["would_download"] = 3
            self.report["summary"]["downloaded"] = 0

    import ifetch.photos_cli as mod
    StubDownloader.instances = []
    original = mod.PhotosDownloader
    mod.PhotosDownloader = Dry
    try:
        rc = photos_cli.main(["/tmp/photos", "--email", "u@e.com", "--dry-run"])
    finally:
        mod.PhotosDownloader = original

    assert rc == 0
    out = capsys.readouterr().out
    assert "Would download: 3" in out
    assert StubDownloader.instances[0].kwargs["dry_run"] is True


def test_keyboard_interrupt_returns_130(monkeypatch, capsys):
    class Interrupting(StubDownloader):
        def authenticate(self):
            raise KeyboardInterrupt

    monkeypatch.setattr(photos_cli, "PhotosDownloader", Interrupting)

    rc = photos_cli.main(["/tmp/photos", "--email", "u@e.com"])

    assert rc == 130
    assert "cancelled by user" in capsys.readouterr().err


def test_engine_error_returns_one(monkeypatch, capsys):
    class Broken(StubDownloader):
        def authenticate(self):
            raise Exception("boom")

    monkeypatch.setattr(photos_cli, "PhotosDownloader", Broken)

    rc = photos_cli.main(["/tmp/photos", "--email", "u@e.com"])

    assert rc == 1
    assert "boom" in capsys.readouterr().err


def test_bad_date_returns_one(capsys):
    rc = photos_cli.main(["/tmp/photos", "--email", "u@e.com", "--since", "yesterday"])

    assert rc == 1
    assert "Unrecognised date" in capsys.readouterr().err


def test_module_entrypoint_help_exits_zero():
    """`python -m ifetch.photos_cli --help` must exit 0."""
    import subprocess

    result = subprocess.run(
        [sys.executable, "-m", "ifetch.photos_cli", "--help"],
        cwd=str(Path(__file__).resolve().parents[1]),
        capture_output=True,
    )

    assert result.returncode == 0
    assert b"--live-photos" in result.stdout
