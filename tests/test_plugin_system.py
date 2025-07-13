import os
import sys
from pathlib import Path

import pytest

# Ensure package is importable when running tests from repo root
sys.path.append(str(Path(__file__).resolve().parents[1]))

from ifetch.downloader import DownloadManager  # noqa: E402


def create_test_plugin(tmp_path: Path):
    """Write a minimal plugin that records received events in a global list."""
    plugin_code = (
        "from ifetch.plugin import BasePlugin\n"
        "events = []\n"
        "class TestPlugin(BasePlugin):\n"
        "    def on_authenticated(self, downloader, **kwargs):\n"
        "        events.append(\"on_authenticated\")\n"
        "    def before_download(self, remote_item, local_path, **kwargs):\n"
        "        events.append(\"before_download\")\n"
        "    def after_download(self, remote_item, local_path, success, **kwargs):\n"
        "        events.append(f'after_download_{success}')\n"
        "    def on_list_contents(self, path, contents, **kwargs):\n"
        "        events.append('on_list_contents')\n"
    )

    plugin_file = tmp_path / "test_plugin.py"
    plugin_file.write_text(plugin_code)
    return plugin_file


class FakePyiCloudService:  # pragma: no cover – simple stub
    """Minimal stub of PyiCloudService to bypass network calls."""

    requires_2fa = False
    requires_2sa = False

    def __init__(self, **kwargs):
        self.drive = FakeDrive()


class FakeItem:
    """Dummy iCloud Drive item representing a file."""

    type = "file"
    size = 1
    name = "dummy.txt"

    def open(self, stream: bool = True):  # noqa: D401 – pretend context manager
        from contextlib import nullcontext
        return nullcontext(b"dummy")


class FakeDrive:  # pragma: no cover
    """Stub that returns FakeItem for any key access."""

    def __getitem__(self, key):
        return self  # Return self to allow chaining for any depth

    def dir(self):
        # Return dict-like keys for directory listing
        return {"dummy.txt": FakeItem()}


@pytest.fixture(autouse=True)
def _patch_pyicloud(monkeypatch):
    """Automatically patch PyiCloudService used in DownloadManager."""
    monkeypatch.setattr("ifetch.downloader.PyiCloudService", FakePyiCloudService)
    yield


def test_plugin_hooks(tmp_path, monkeypatch):
    """Ensure that plugin callbacks are triggered by DownloadManager."""
    # Create plugin file and point IFETCH_PLUGIN_PATH to it
    create_test_plugin(tmp_path)
    monkeypatch.setenv("IFETCH_PLUGIN_PATH", str(tmp_path))

    dm = DownloadManager(email="user@example.com")

    # Monkeypatch time-intensive function: still dispatch success hook so plugin receives it
    def _fake_download(item, local_path):
        dm.plugin_manager.dispatch(
            "after_download", remote_item=item, local_path=local_path, success=True
        )
        return True

    monkeypatch.setattr(dm, "download_drive_item", _fake_download)

    # Authenticate (should trigger on_authenticated)
    dm.authenticate()

    # List directory contents (should trigger on_list_contents)
    dm.list_contents("/")

    # Process a fake file (triggers before_download and after_download)
    item = FakeItem()
    dm.process_item_parallel(item, tmp_path / "dummy.txt")

    # Retrieve events recorded by plugin
    events_mod = sys.modules.get("ifetch_plugin_test_plugin")
    assert events_mod is not None, "Plugin module not loaded"

    expected = {
        "on_authenticated",
        "on_list_contents",
        "before_download",
        "after_download_True",
    }
    assert set(events_mod.events) >= expected 