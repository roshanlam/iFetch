import sys
import types
from pathlib import Path

import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))

from ifetch.downloader import DownloadManager  # noqa: E402
from ifetch import cli  # noqa: E402
from ifetch.versioning import VersionManager  # noqa: E402


class DummyItem:
    """Mimics an iCloud file object with minimal API for download_drive_item."""

    def __init__(self, name: str, size: int, content: bytes):
        self.name = name
        self.size = size
        self.type = "file"
        self._content = content
        self.url = "https://dummy.download/url"

    def open(self, stream: bool = True):  # noqa: D401 – context manager
        class _Ctx:
            def __init__(self, outer):
                self.outer = outer
                self.headers = {"content-length": str(outer.size)}
                self.url = outer.url
                self.raw = types.SimpleNamespace(tell=lambda: 0, seek=lambda *a: None, read=lambda n=-1: outer._content)

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

        return _Ctx(self)


def test_download_drive_item_success(tmp_path, monkeypatch):
    dm = DownloadManager(email="user@example.com", max_retries=1)

    # Patch helpers to simplify logic
    monkeypatch.setattr(dm.chunker, "compute_download_ranges", lambda resp, local_path=None, force=False: [(0, 4), (5, 9)])
    monkeypatch.setattr(dm, "download_chunk", lambda url, start, end, item=None: b"0123456789")
    monkeypatch.setattr(dm, "calculate_checksum", lambda p: "dummy")

    item = DummyItem("test.txt", 10, b"0123456789")
    local_path = tmp_path / "test.txt"

    ok = dm.download_drive_item(item, local_path)
    assert ok is True
    # File should now exist and be 10 bytes long
    assert local_path.exists() and local_path.stat().st_size == 10


def test_download_drive_item_merges_contiguous_ranges(tmp_path, monkeypatch):
    dm = DownloadManager(email="user@example.com", max_retries=1)
    monkeypatch.setattr(dm.chunker, "compute_download_ranges", lambda resp, local_path=None, force=False: [(0, 4), (5, 9)])
    monkeypatch.setattr(dm, "calculate_checksum", lambda p: "dummy")

    calls = []

    def _download_chunk(url, start, end, item=None):
        calls.append((start, end))
        return b"0123456789"

    monkeypatch.setattr(dm, "download_chunk", _download_chunk)

    item = DummyItem("test.txt", 10, b"0123456789")
    local_path = tmp_path / "test.txt"

    assert dm.download_drive_item(item, local_path) is True
    assert calls == [(0, 9)]


def test_download_drive_item_recovers_from_initial_open_404(tmp_path, monkeypatch):
    dm = DownloadManager(email="user@example.com", max_retries=2)
    monkeypatch.setattr(dm.chunker, "compute_download_ranges", lambda resp, local_path=None, force=False: [(0, 9)])
    monkeypatch.setattr(dm, "download_chunk", lambda url, start, end, item=None: b"0123456789")
    monkeypatch.setattr(dm, "calculate_checksum", lambda p: "dummy")

    class _ResponseCtx:
        def __init__(self):
            self.headers = {"content-length": "10"}
            self.url = "https://dummy.download/url"

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    class _StaleItem:
        name = "test.txt"
        type = "file"
        size = 10

        def __init__(self):
            self.calls = 0

        def open(self, stream=True):
            self.calls += 1
            # Generic 404 (e.g. signed-URL expiry) — NOT WSObjectNotFound.
            # WSObjectNotFound triggers the shared-file fast-fail path instead.
            raise Exception("Not Found (404): signed URL expired")

    class _FreshItem:
        name = "test.txt"
        type = "file"
        size = 10

        def open(self, stream=True):
            return _ResponseCtx()

    stale = _StaleItem()
    fresh = _FreshItem()
    resolved = []

    def _refresh(remote_path):
        resolved.append(remote_path)
        return fresh

    monkeypatch.setattr(dm, "get_drive_item", _refresh)

    local_path = tmp_path / "test.txt"

    ok = dm.download_drive_item(stale, local_path, remote_path="Documents/test.txt")

    assert ok is True
    assert resolved == ["Documents/test.txt"]
    assert local_path.exists() and local_path.read_bytes() == b"0123456789"


def test_download_drive_item_preserves_resumed_prefix_with_versioning(tmp_path, monkeypatch):
    dm = DownloadManager(email="user@example.com", max_retries=1)
    dm.root_path = tmp_path
    dm.version_manager = VersionManager(tmp_path)

    local_path = tmp_path / "test.txt"
    local_path.write_bytes(b"abcde")

    monkeypatch.setattr(dm.chunker, "compute_download_ranges", lambda resp, local_path=None, force=False: [(5, 9)])
    monkeypatch.setattr(dm, "download_chunk", lambda url, start, end, item=None: b"fghij")
    monkeypatch.setattr(dm, "calculate_checksum", lambda p: "dummy")

    item = DummyItem("test.txt", 10, b"abcdefghij")

    ok = dm.download_drive_item(item, local_path, remote_path="Documents/test.txt")

    assert ok is True
    assert local_path.read_bytes() == b"abcdefghij"
    assert any(tmp_path.joinpath(".versions").rglob("test.txt.v1_*"))


def test_should_process_keeps_directories_traversable_for_include_globs():
    dm = DownloadManager(email="user@example.com", include_patterns=["*.pdf"])

    assert dm._should_process(Path("Documents"), is_dir=True) is True
    assert dm._should_process(Path("Documents/file.pdf"), is_dir=False) is True
    assert dm._should_process(Path("Documents/file.txt"), is_dir=False) is False


def test_list_contents_and_cli(monkeypatch, tmp_path):
    """Exercise list_contents as well as the CLI entry point in list mode."""
    # Build fake directory structure
    class DirItem(dict):
        def __init__(self):
            super().__init__()
        def dir(self):
            return self

    root = DirItem()
    root["file.txt"] = DummyItem("file.txt", 4, b"test")

    monkeypatch.setattr(DownloadManager, "authenticate", lambda self: None)
    monkeypatch.setattr(DownloadManager, "get_drive_item", lambda self, path: root)
    # Suppress actual downloading
    monkeypatch.setattr(DownloadManager, "download", lambda *a, **kw: None)

    # Run list_contents directly
    dm = DownloadManager(email="user@example.com")
    dm.list_contents("/")  # should log but not raise

    # Simulate CLI invocation: list mode
    test_args = ["cli.py", "/", "--email", "user@example.com", "--list"]
    monkeypatch.setattr(sys, "argv", test_args)

    # Patch DownloadManager inside cli to our dummy that captures calls
    calls = {}
    class _FakeDM(DownloadManager):
        def __init__(self, *a, **kw):
            super().__init__(*a, **kw)
            calls["init"] = True
        def list_contents(self, p):
            calls["list"] = p
        def authenticate(self):
            calls["auth"] = True

    monkeypatch.setattr(cli, "DownloadManager", _FakeDM)

    cli.main()  # should run without error
    assert calls.get("list") == "/"  # list_contents called 
