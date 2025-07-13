import sys
import types
from pathlib import Path

import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))

from ifetch.downloader import DownloadManager  # noqa: E402
from ifetch import cli  # noqa: E402


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


@pytest.fixture(autouse=True)
def _patch_tqdm(monkeypatch):
    """Replace tqdm with a noop to keep tests quiet."""
    import builtins

    class _NoTqdm(list):
        def __init__(self, *args, **kwargs):
            super().__init__()
        def __enter__(self):
            return self
        def __exit__(self, exc_type, exc, tb):
            return False
        def update(self, n):
            pass

    monkeypatch.setattr("ifetch.downloader.tqdm", lambda *a, **kw: _NoTqdm())
    yield


def test_download_drive_item_success(tmp_path, monkeypatch):
    dm = DownloadManager(email="user@example.com", max_retries=1)

    # Patch helpers to simplify logic
    monkeypatch.setattr(dm.chunker, "get_file_chunks", lambda p: {})  # pretend no local chunks
    monkeypatch.setattr(dm.chunker, "find_changed_chunks", lambda resp, chunks: [(0, 9)])
    monkeypatch.setattr(dm, "download_chunk", lambda url, start, end: b"0123456789")
    monkeypatch.setattr(dm, "calculate_checksum", lambda p: "dummy")

    item = DummyItem("test.txt", 10, b"0123456789")
    local_path = tmp_path / "test.txt"

    ok = dm.download_drive_item(item, local_path)
    assert ok is True
    # File should now exist and be 10 bytes long
    assert local_path.exists() and local_path.stat().st_size == 10


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