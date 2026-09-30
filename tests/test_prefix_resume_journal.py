"""Journal-row-only provenance for prefix resume (follow-up to #29/#36)."""

import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from ifetch.downloader import DownloadManager, SyncState  # noqa: E402
from ifetch.index import IndexStore  # noqa: E402
from ifetch.tracker import DownloadTracker  # noqa: E402
from ifetch.transfers import TransferJournal  # noqa: E402


MTIME_A = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)


class FakeNode:
    type = "file"

    def __init__(self, name, content=b"0123456789", date_modified=MTIME_A, size=None):
        self.name = name
        self._content = content
        self.size = len(content) if size is None else size
        self.date_modified = date_modified
        self.date_changed = None
        self.url = "https://example.invalid/download"
        self.open_calls = 0

    def open(self, stream=True):
        self.open_calls += 1
        outer = self

        class _Ctx:
            def __init__(self):
                self.headers = {"content-length": str(len(outer._content))}
                self.url = outer.url

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

        return _Ctx()


def _manager(tmp_path, monkeypatch, **kwargs):
    dm = DownloadManager(email="user@example.com", max_retries=1, **kwargs)
    dm.root_path = tmp_path
    dm.sync_state = SyncState(tmp_path)
    monkeypatch.setattr(
        dm, "download_chunk",
        lambda url, start, end, item=None: item._content[start:end + 1],
    )
    monkeypatch.setattr(dm, "calculate_checksum", lambda p: "dummy")
    return dm


def test_prefix_resume_proven_by_journal_row_only(tmp_path, monkeypatch):
    """Journal row alone (no .download tracker) may prove a prefix resume."""
    node = FakeNode("file.bin")
    local_path = tmp_path / "file.bin"
    local_path.write_bytes(b"01234")

    dm = _manager(tmp_path, monkeypatch)
    with IndexStore(tmp_path) as store:
        dm.journal = TransferJournal(store, tmp_path)
        dm.journal.begin(local_path, total_bytes=10, resume_from=5)
        assert DownloadTracker(local_path).current_position == 0

        calls = []

        def _download_chunk(url, start, end, item=None):
            calls.append((start, end))
            return node._content[start:end + 1]

        monkeypatch.setattr(dm, "download_chunk", _download_chunk)

        assert dm.download_drive_item(node, local_path) is True
        assert calls == [(5, 9)]
        assert local_path.read_bytes() == b"0123456789"
