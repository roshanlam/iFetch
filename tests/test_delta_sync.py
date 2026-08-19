"""Tests for the metadata fast path (.ifetch_state.json delta sync).

The contract under test: a file that is *provably* unchanged is skipped with
ZERO network calls, and anything less than proof falls back to the old
open-the-stream-and-compare-sizes behaviour.
"""

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))

from ifetch.downloader import DownloadManager, SyncState, remote_metadata  # noqa: E402


MTIME_A = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
MTIME_B = datetime(2026, 6, 7, 8, 9, 10, tzinfo=timezone.utc)


class FakeNode:
    """Minimal stand-in for a pyicloud DriveNode from a folder listing."""

    type = "file"

    def __init__(self, name, content=b"0123456789", date_modified=MTIME_A,
                 size=None, date_changed=None):
        self.name = name
        self._content = content
        self.size = len(content) if size is None else size
        self.date_modified = date_modified
        self.date_changed = date_changed
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


class NoMetadataNode(FakeNode):
    """A node whose size/date properties blow up or are absent."""

    def __init__(self, name, content=b"0123456789"):
        super().__init__(name, content)
        self.date_modified = None
        self.date_changed = None


def _manager(tmp_path, monkeypatch, **kwargs):
    dm = DownloadManager(email="user@example.com", max_retries=1, **kwargs)
    dm.root_path = tmp_path
    dm.sync_state = SyncState(tmp_path)
    monkeypatch.setattr(
        dm, "download_chunk",
        lambda url, start, end, item=None: item._content[start:end + 1]
    )
    monkeypatch.setattr(dm, "calculate_checksum", lambda p: "dummy")
    return dm


def _seed(tmp_path, monkeypatch, node, name="file.bin"):
    """Run one successful download so state is populated, return (dm, path)."""
    dm = _manager(tmp_path, monkeypatch)
    local_path = tmp_path / name
    assert dm.download_drive_item(node, local_path) is True
    assert local_path.read_bytes() == node._content
    dm.sync_state.save()
    return dm, local_path


# ---------------------------------------------------------------------------
# remote_metadata / pyicloud attribute handling
# ---------------------------------------------------------------------------
def test_remote_metadata_prefers_date_modified():
    node = FakeNode("a.bin", date_modified=MTIME_A, date_changed=MTIME_B)
    size, token = remote_metadata(node)
    assert size == 10
    assert token == "date_modified=" + MTIME_A.isoformat()


def test_remote_metadata_falls_back_to_date_changed():
    node = FakeNode("a.bin", date_modified=None, date_changed=MTIME_B)
    size, token = remote_metadata(node)
    assert size == 10
    assert token == "date_changed=" + MTIME_B.isoformat()


def test_remote_metadata_falls_back_to_raw_data_dict():
    node = NoMetadataNode("a.bin")
    node.data = {"dateModified": "2026-01-02T03:04:05Z"}
    _, token = remote_metadata(node)
    assert token == "dateModified=2026-01-02T03:04:05Z"


def test_remote_metadata_returns_none_when_absent():
    node = NoMetadataNode("a.bin")
    size, token = remote_metadata(node)
    assert size == 10
    assert token is None


def test_remote_metadata_survives_raising_properties():
    class Exploding:
        name = "boom.bin"

        @property
        def size(self):
            raise RuntimeError("no size")

        @property
        def date_modified(self):
            raise RuntimeError("no date")

    size, token = remote_metadata(Exploding())
    assert size is None and token is None


# ---------------------------------------------------------------------------
# The core promise: zero network calls for unchanged files
# ---------------------------------------------------------------------------
def test_fast_path_skip_never_opens_a_stream(tmp_path, monkeypatch):
    node = FakeNode("file.bin")
    dm, local_path = _seed(tmp_path, monkeypatch, node)

    # Second run: brand-new manager reading the persisted state.
    dm2 = _manager(tmp_path, monkeypatch)
    node.open_calls = 0

    assert dm2.download_drive_item(node, local_path) is True
    assert node.open_calls == 0, "fast path must not open an HTTPS stream"

    report = dm2.generate_summary_report()["summary"]
    assert report["skipped"] == 1
    assert report["failed"] == 0
    assert report["successful"] == 0
    assert report["total_bytes_transferred"] == 0


def test_state_file_is_written_to_destination_root(tmp_path, monkeypatch):
    node = FakeNode("file.bin")
    dm, local_path = _seed(tmp_path, monkeypatch, node)

    state_path = tmp_path / SyncState.STATE_FILENAME
    assert state_path.exists()
    payload = json.loads(state_path.read_text())
    entry = payload["files"]["file.bin"]
    assert entry["status"] == "completed"
    assert entry["remote_size"] == 10
    assert entry["local_size"] == 10
    assert entry["remote_modified"] == "date_modified=" + MTIME_A.isoformat()


# ---------------------------------------------------------------------------
# Anything that changes must fall through to the network path
# ---------------------------------------------------------------------------
def test_changed_remote_size_triggers_download(tmp_path, monkeypatch):
    node = FakeNode("file.bin")
    dm, local_path = _seed(tmp_path, monkeypatch, node)

    node._content = b"0123456789ABCDE"
    node.size = 15
    dm2 = _manager(tmp_path, monkeypatch)
    node.open_calls = 0

    assert dm2.download_drive_item(node, local_path) is True
    assert node.open_calls == 1
    assert local_path.read_bytes() == b"0123456789ABCDE"


def test_changed_remote_mtime_triggers_download(tmp_path, monkeypatch):
    """Same size, different remote timestamp: the fast path must not skip."""
    node = FakeNode("file.bin")
    dm, local_path = _seed(tmp_path, monkeypatch, node)

    node.date_modified = MTIME_B
    dm2 = _manager(tmp_path, monkeypatch)
    node.open_calls = 0

    assert dm2.download_drive_item(node, local_path) is True
    # Stream opened; the size-based comparison then decides nothing changed.
    assert node.open_calls == 1


def test_local_file_deleted_but_state_present_redownloads(tmp_path, monkeypatch):
    node = FakeNode("file.bin")
    dm, local_path = _seed(tmp_path, monkeypatch, node)
    local_path.unlink()

    dm2 = _manager(tmp_path, monkeypatch)
    node.open_calls = 0

    assert dm2.download_drive_item(node, local_path) is True
    assert node.open_calls == 1
    assert local_path.read_bytes() == node._content


def test_local_file_truncated_but_state_present_redownloads(tmp_path, monkeypatch):
    node = FakeNode("file.bin")
    dm, local_path = _seed(tmp_path, monkeypatch, node)
    local_path.write_bytes(b"012")

    dm2 = _manager(tmp_path, monkeypatch)
    node.open_calls = 0

    assert dm2.download_drive_item(node, local_path) is True
    assert node.open_calls == 1
    assert local_path.read_bytes() == node._content


def test_missing_remote_metadata_never_fast_skips(tmp_path, monkeypatch):
    node = FakeNode("file.bin")
    dm, local_path = _seed(tmp_path, monkeypatch, node)

    stripped = NoMetadataNode("file.bin")
    dm2 = _manager(tmp_path, monkeypatch)

    assert dm2.download_drive_item(stripped, local_path) is True
    assert stripped.open_calls == 1


def test_leftover_temp_file_blocks_fast_skip(tmp_path, monkeypatch):
    node = FakeNode("file.bin")
    dm, local_path = _seed(tmp_path, monkeypatch, node)
    local_path.with_suffix(local_path.suffix + ".temp").write_bytes(b"partial")

    dm2 = _manager(tmp_path, monkeypatch)
    node.open_calls = 0

    assert dm2.download_drive_item(node, local_path) is True
    assert node.open_calls == 1


def test_leftover_tracker_file_blocks_fast_skip(tmp_path, monkeypatch):
    node = FakeNode("file.bin")
    dm, local_path = _seed(tmp_path, monkeypatch, node)
    local_path.with_suffix(local_path.suffix + ".download").write_text('{"position": 4}')

    dm2 = _manager(tmp_path, monkeypatch)
    node.open_calls = 0

    assert dm2.download_drive_item(node, local_path) is True
    assert node.open_calls == 1


# ---------------------------------------------------------------------------
# State-file robustness
# ---------------------------------------------------------------------------
def test_first_run_without_state_file_falls_back(tmp_path, monkeypatch):
    node = FakeNode("file.bin")
    local_path = tmp_path / "file.bin"
    local_path.write_bytes(node._content)  # local copy already correct

    dm = _manager(tmp_path, monkeypatch)
    assert not (tmp_path / SyncState.STATE_FILENAME).exists()

    assert dm.download_drive_item(node, local_path) is True
    assert node.open_calls == 1  # no state -> must verify over the network


def test_corrupt_state_file_falls_back_safely(tmp_path, monkeypatch):
    node = FakeNode("file.bin")
    dm, local_path = _seed(tmp_path, monkeypatch, node)

    (tmp_path / SyncState.STATE_FILENAME).write_text("{not json at all")

    dm2 = _manager(tmp_path, monkeypatch)
    node.open_calls = 0

    assert dm2.download_drive_item(node, local_path) is True
    assert node.open_calls == 1


@pytest.mark.parametrize("payload", [
    "[]",
    '{"files": "nope"}',
    '{"version": 1}',
    '{"files": {"file.bin": "not-a-dict"}}',
])
def test_malformed_state_payloads_never_fast_skip(tmp_path, monkeypatch, payload):
    node = FakeNode("file.bin")
    local_path = tmp_path / "file.bin"
    local_path.write_bytes(node._content)
    (tmp_path / SyncState.STATE_FILENAME).write_text(payload)

    dm = _manager(tmp_path, monkeypatch)
    assert dm.download_drive_item(node, local_path) is True
    assert node.open_calls == 1


def test_partial_state_entry_does_not_cause_false_skip(tmp_path, monkeypatch):
    """An entry missing status/local_size (e.g. an interrupted write) is unusable."""
    node = FakeNode("file.bin")
    local_path = tmp_path / "file.bin"
    local_path.write_bytes(node._content)

    (tmp_path / SyncState.STATE_FILENAME).write_text(json.dumps({
        "version": 1,
        "files": {
            "file.bin": {
                "remote_size": 10,
                "remote_modified": "date_modified=" + MTIME_A.isoformat(),
            }
        },
    }))

    dm = _manager(tmp_path, monkeypatch)
    assert dm.download_drive_item(node, local_path) is True
    assert node.open_calls == 1


def test_failed_download_invalidates_state(tmp_path, monkeypatch):
    node = FakeNode("file.bin")
    dm, local_path = _seed(tmp_path, monkeypatch, node)
    assert dm.sync_state.is_unchanged(local_path, 10, remote_metadata(node)[1])

    # Remote grows, then the transfer blows up mid-flight.
    node._content = b"0123456789ABCDE"
    node.size = 15
    dm2 = _manager(tmp_path, monkeypatch)

    def _boom(url, start, end, item=None):
        raise Exception("network went away")

    monkeypatch.setattr(dm2, "download_chunk", _boom)

    assert dm2.download_drive_item(node, local_path) is False
    assert dm2.sync_state.is_unchanged(local_path, 15, remote_metadata(node)[1]) is False


def test_interrupted_run_still_persists_learned_state(tmp_path, monkeypatch):
    """download() persists learned state via its finally block.

    Under the shared bounded-pool traversal, a worker's exception is contained
    by the pool rather than propagated out of download(); per-item failures are
    logged and recorded, not re-raised. The finally still runs, so the sync
    state learned before the failure is saved regardless.
    """
    dm = DownloadManager(email="user@example.com")
    monkeypatch.setattr(dm, "authenticate", lambda: setattr(
        dm, "api", type("API", (), {"drive": object()})()))
    monkeypatch.setattr(dm, "get_drive_item", lambda path: "item")

    def _explode(item, local_path, remote_path=None):
        dm.sync_state.record_completed(
            Path(local_path) / "seen.bin", None, None
        )
        raise RuntimeError("interrupted")

    monkeypatch.setattr(dm, "process_item_parallel", _explode)

    # The pool contains the worker exception; download() returns normally and
    # the finally has still persisted whatever state was learned.
    dm.download("Documents", tmp_path)

    assert (tmp_path / SyncState.STATE_FILENAME).exists()


# ---------------------------------------------------------------------------
# Flags
# ---------------------------------------------------------------------------
def test_no_fast_scan_forces_the_network_check(tmp_path, monkeypatch):
    node = FakeNode("file.bin")
    dm, local_path = _seed(tmp_path, monkeypatch, node)

    dm2 = _manager(tmp_path, monkeypatch, fast_scan=False)
    node.open_calls = 0

    assert dm2.download_drive_item(node, local_path) is True
    assert node.open_calls == 1


def test_force_redownloads_even_when_everything_matches(tmp_path, monkeypatch):
    node = FakeNode("file.bin")
    dm, local_path = _seed(tmp_path, monkeypatch, node)
    local_path.write_bytes(b"XXXXXXXXXX")  # same size, wrong content

    dm2 = _manager(tmp_path, monkeypatch, force=True)
    node.open_calls = 0

    assert dm2.download_drive_item(node, local_path) is True
    assert node.open_calls == 1
    assert local_path.read_bytes() == b"0123456789"

    report = dm2.generate_summary_report()["summary"]
    assert report["successful"] == 1
    assert report["skipped"] == 0


def test_cli_wires_fast_scan_and_force_flags(monkeypatch, tmp_path):
    from ifetch import cli

    captured = {}

    class _FakeDM:
        def __init__(self, **kwargs):
            captured.update(kwargs)

        def authenticate(self):
            pass

        def download(self, *a, **kw):
            pass

        def generate_summary_report(self):
            return {"summary": {
                "total_files": 0, "successful": 0, "failed": 0, "skipped": 0,
                "total_bytes_transferred": 0, "total_changed_chunks": 0,
            }}

    monkeypatch.setattr(cli, "DownloadManager", _FakeDM)

    monkeypatch.setattr(sys, "argv", ["cli.py", "Docs", str(tmp_path),
                                      "--email", "u@example.com"])
    cli.main()
    assert captured["fast_scan"] is True and captured["force"] is False

    captured.clear()
    monkeypatch.setattr(sys, "argv", ["cli.py", "Docs", str(tmp_path),
                                      "--email", "u@example.com",
                                      "--no-fast-scan", "--force"])
    cli.main()
    assert captured["fast_scan"] is False and captured["force"] is True


# ---------------------------------------------------------------------------
# Regressions: no wasted hashing, resume still works
# ---------------------------------------------------------------------------
def test_download_path_never_hashes_the_local_file(tmp_path, monkeypatch):
    """get_file_chunks used to hash every byte on every run; it must not run."""
    node = FakeNode("file.bin")
    local_path = tmp_path / "file.bin"
    local_path.write_bytes(b"01234")

    dm = _manager(tmp_path, monkeypatch)

    def _forbidden(path):
        raise AssertionError("get_file_chunks must not run in the download path")

    monkeypatch.setattr(dm.chunker, "get_file_chunks", _forbidden)

    assert dm.download_drive_item(node, local_path) is True
    assert local_path.read_bytes() == b"0123456789"


def test_prefix_resume_downloads_only_the_missing_tail(tmp_path, monkeypatch):
    node = FakeNode("file.bin")
    local_path = tmp_path / "file.bin"
    local_path.write_bytes(b"01234")

    dm = _manager(tmp_path, monkeypatch)
    calls = []

    def _download_chunk(url, start, end, item=None):
        calls.append((start, end))
        return node._content[start:end + 1]

    monkeypatch.setattr(dm, "download_chunk", _download_chunk)

    assert dm.download_drive_item(node, local_path) is True
    assert calls == [(5, 9)]
    assert local_path.read_bytes() == b"0123456789"


def test_unchanged_via_network_is_reported_as_skipped(tmp_path, monkeypatch):
    """No state file, but sizes match -> counted as skipped, not failed."""
    node = FakeNode("file.bin")
    local_path = tmp_path / "file.bin"
    local_path.write_bytes(node._content)

    dm = _manager(tmp_path, monkeypatch)
    assert dm.download_drive_item(node, local_path) is True

    report = dm.generate_summary_report()["summary"]
    assert report["skipped"] == 1
    assert report["failed"] == 0
    # ...and it seeds the state so the *next* run needs no network at all.
    node.open_calls = 0
    dm2 = _manager(tmp_path, monkeypatch)
    dm.sync_state.save()
    dm2.sync_state = SyncState(tmp_path)
    assert dm2.download_drive_item(node, local_path) is True
    assert node.open_calls == 0


# ---------------------------------------------------------------------------
# SyncState unit-level guarantees
# ---------------------------------------------------------------------------
def test_sync_state_rejects_metadata_free_records(tmp_path):
    state = SyncState(tmp_path)
    target = tmp_path / "a.bin"
    target.write_bytes(b"abc")

    state.record_completed(target, None, None)
    assert state.is_unchanged(target, 3, "date_modified=x") is False


def test_sync_state_rejects_size_mismatch_at_record_time(tmp_path):
    state = SyncState(tmp_path)
    target = tmp_path / "a.bin"
    target.write_bytes(b"abc")

    # Claimed remote size disagrees with what is actually on disk.
    state.record_completed(target, 99, "date_modified=x")
    assert state.is_unchanged(target, 99, "date_modified=x") is False


def test_sync_state_is_thread_safe(tmp_path):
    import threading

    state = SyncState(tmp_path)
    paths = []
    for i in range(60):
        p = tmp_path / f"f{i}.bin"
        p.write_bytes(b"x" * 4)
        paths.append(p)

    def worker(p):
        for _ in range(20):
            state.record_completed(p, 4, "date_modified=x")
            state.is_unchanged(p, 4, "date_modified=x")
            state.save()

    threads = [threading.Thread(target=worker, args=(p,)) for p in paths]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    state.save()
    payload = json.loads((tmp_path / SyncState.STATE_FILENAME).read_text())
    assert len(payload["files"]) == 60
    assert all(state.is_unchanged(p, 4, "date_modified=x") for p in paths)


def test_sync_state_key_is_relative_posix(tmp_path):
    state = SyncState(tmp_path)
    nested = tmp_path / "a" / "b" / "c.bin"
    assert state.key_for(nested) == "a/b/c.bin"
