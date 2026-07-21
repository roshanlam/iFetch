"""Regressions for files Apple serves without a usable ``content-length``.

Confirmed data-loss bug: a 2.8 GiB folder of 416 remote files reported
``failed: 0`` and 416 skips, but only 414 files existed on disk.  The two
missing entries were macOS package bundles (``setup.app``, ``*.logicx``) which
Apple streams with chunked transfer-encoding and NO ``content-length`` header.
The old code did ``int(headers.get('content-length', 0))`` -> 0 -> "no byte
ranges to fetch" -> "file_unchanged" -> reported as a successful skip while
nothing was ever written.

The contract under test:

* header absent          -> stream the whole body sequentially (no resume)
* ``content-length: 0``  -> genuinely empty remote file, create an empty local
* "unchanged"/"skipped"  -> only ever legal when the file is really on disk
"""

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))

from ifetch.downloader import DownloadManager, SyncState  # noqa: E402
from ifetch.versioning import VersionManager  # noqa: E402


MTIME = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)


class _StreamCtx:
    """A response whose body can only be consumed sequentially."""

    def __init__(self, content, headers, chunk_bytes=4, fail_after=None):
        self.headers = headers
        self.url = "https://example.invalid/download"
        self._content = content
        self._chunk_bytes = chunk_bytes
        self._fail_after = fail_after
        self.iter_calls = 0

    def iter_content(self, chunk_size=None):
        self.iter_calls += 1
        size = self._chunk_bytes
        emitted = 0
        for offset in range(0, len(self._content), size):
            if self._fail_after is not None and emitted >= self._fail_after:
                raise ConnectionError("connection reset mid-stream")
            piece = self._content[offset:offset + size]
            emitted += len(piece)
            yield piece

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class StreamNode:
    """DriveNode stand-in whose download response has no content-length."""

    type = "file"

    def __init__(self, name="setup.app", content=b"0123456789abcdef",
                 listed_size=None, headers=None, fail_after=None,
                 date_modified=MTIME):
        self.name = name
        self._content = content
        self.size = len(content) if listed_size is None else listed_size
        self.date_modified = date_modified
        self.date_changed = None
        self.url = "https://example.invalid/download"
        self.open_calls = 0
        self.fail_after = fail_after
        # Apple sends these for package bundles: no content-length at all.
        self.headers = {"content-type": "application/octet-stream"} \
            if headers is None else headers

    def open(self, stream=True):
        self.open_calls += 1
        return _StreamCtx(self._content, dict(self.headers),
                          fail_after=self.fail_after)


class SizedNode(StreamNode):
    """DriveNode whose response *does* carry a content-length header."""

    def __init__(self, name="file.bin", content=b"", **kwargs):
        super().__init__(name=name, content=content, **kwargs)
        self.headers = {"content-length": str(len(content))}


def _manager(tmp_path, **kwargs):
    kwargs.setdefault("max_retries", 1)
    dm = DownloadManager(email="user@example.com", chunk_size=4, **kwargs)
    dm.root_path = tmp_path
    dm.sync_state = SyncState(tmp_path)
    return dm


def _summary(dm):
    return dm.generate_summary_report()["summary"]


# ---------------------------------------------------------------------------
# 1. No content-length header -> full sequential download
# ---------------------------------------------------------------------------
def test_missing_content_length_downloads_full_body(tmp_path):
    node = StreamNode(content=b"0123456789abcdef")
    dm = _manager(tmp_path)
    local_path = tmp_path / "setup.app"

    assert dm.download_drive_item(node, local_path) is True

    assert local_path.is_file()
    assert local_path.read_bytes() == b"0123456789abcdef"

    report = _summary(dm)
    assert report["successful"] == 1
    assert report["skipped"] == 0
    assert report["failed"] == 0
    assert report["total_bytes_transferred"] == 16


def test_missing_content_length_is_never_a_silent_skip(tmp_path):
    """The exact production failure: listing says 24 MB, header says nothing."""
    node = StreamNode(name="setup.app", content=b"P" * 512, listed_size=24068701)
    dm = _manager(tmp_path)
    local_path = tmp_path / "setup.app"

    result = dm.download_drive_item(node, local_path)

    assert result is True
    assert local_path.exists(), "file must exist on disk after a success"
    assert _summary(dm)["skipped"] == 0


def test_missing_content_length_never_asks_for_byte_ranges(tmp_path, monkeypatch):
    """Range requests are impossible without a total size; none may be issued."""
    node = StreamNode(content=b"abcdefgh")
    dm = _manager(tmp_path)

    monkeypatch.setattr(dm, "download_chunk", lambda *a, **kw: pytest.fail(
        "ranged download_chunk must not be used for unknown-length bodies"))

    assert dm.download_drive_item(node, tmp_path / "setup.app") is True


def test_missing_content_length_replaces_existing_file_and_archives_version(tmp_path):
    dm = _manager(tmp_path)
    dm.version_manager = VersionManager(tmp_path)
    local_path = tmp_path / "setup.app"
    local_path.write_bytes(b"old-content")

    node = StreamNode(content=b"brand-new-content")

    assert dm.download_drive_item(node, local_path) is True
    assert local_path.read_bytes() == b"brand-new-content"
    assert any(tmp_path.joinpath(".versions").rglob("setup.app.v1_*"))
    assert _summary(dm)["successful"] == 1


def test_missing_content_length_ignores_stale_resume_checkpoint(tmp_path):
    """Resume is impossible here, so an old range checkpoint must be dropped."""
    dm = _manager(tmp_path)
    local_path = tmp_path / "setup.app"
    status_path = local_path.with_suffix(local_path.suffix + ".download")
    status_path.write_text('{"position": 99999}')

    node = StreamNode(content=b"0123456789")

    assert dm.download_drive_item(node, local_path) is True
    assert local_path.read_bytes() == b"0123456789"
    assert not status_path.exists()


def test_missing_content_length_empty_body_with_nonzero_listing_fails(tmp_path):
    """A promised-but-absent body is a failure, never an empty 'success'."""
    node = StreamNode(content=b"", listed_size=24068701)
    dm = _manager(tmp_path)
    local_path = tmp_path / "setup.app"

    assert dm.download_drive_item(node, local_path) is False
    assert not local_path.exists()

    report = _summary(dm)
    assert report["failed"] == 1
    assert report["successful"] == 0
    assert report["skipped"] == 0


def test_missing_content_length_and_no_listed_size_yields_empty_file(tmp_path):
    """Nothing anywhere says the file has bytes -> an empty file is correct."""
    node = StreamNode(content=b"", listed_size=0)
    dm = _manager(tmp_path)
    local_path = tmp_path / "setup.app"

    assert dm.download_drive_item(node, local_path) is True
    assert local_path.is_file() and local_path.stat().st_size == 0


def test_missing_content_length_without_iter_content_is_a_failure(tmp_path):
    class _NoIter:
        headers = {"content-type": "application/octet-stream"}
        url = "https://example.invalid/download"

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    class _Node:
        name = "setup.app"
        type = "file"
        size = 10

        def open(self, stream=True):
            return _NoIter()

    dm = _manager(tmp_path)
    local_path = tmp_path / "setup.app"

    assert dm.download_drive_item(_Node(), local_path) is False
    assert not local_path.exists()
    assert _summary(dm)["failed"] == 1


# ---------------------------------------------------------------------------
# 2. Atomicity of the unknown-length path
# ---------------------------------------------------------------------------
def test_missing_content_length_failure_leaves_no_partial_file(tmp_path):
    node = StreamNode(content=b"0123456789abcdef", fail_after=8)
    dm = _manager(tmp_path)
    local_path = tmp_path / "setup.app"

    assert dm.download_drive_item(node, local_path) is False

    assert not local_path.exists(), "no partial file may appear at the destination"
    assert not local_path.with_suffix(local_path.suffix + ".temp").exists()
    assert _summary(dm)["failed"] == 1


def test_missing_content_length_failure_preserves_existing_file(tmp_path):
    dm = _manager(tmp_path)
    local_path = tmp_path / "setup.app"
    local_path.write_bytes(b"previous-good-backup")

    node = StreamNode(content=b"0123456789abcdef", fail_after=8)

    assert dm.download_drive_item(node, local_path) is False
    assert local_path.read_bytes() == b"previous-good-backup"
    assert not local_path.with_suffix(local_path.suffix + ".temp").exists()


def test_missing_content_length_retries_reopen_the_stream(tmp_path, monkeypatch):
    node = StreamNode(content=b"0123456789abcdef")
    dm = _manager(tmp_path, max_retries=3)
    monkeypatch.setattr("ifetch.downloader.time.sleep", lambda seconds: None)

    calls = {"n": 0}
    real_stream = dm._stream_body_to_temp

    def _flaky(response, temp_path, item, local_path):
        calls["n"] += 1
        if calls["n"] < 3:
            raise ConnectionError("connection reset")
        return real_stream(response, temp_path, item, local_path)

    monkeypatch.setattr(dm, "_stream_body_to_temp", _flaky)

    local_path = tmp_path / "setup.app"
    assert dm.download_drive_item(node, local_path) is True
    assert calls["n"] == 3
    # Attempt 1 uses the original stream; retries open fresh ones.
    assert node.open_calls == 3
    assert local_path.read_bytes() == b"0123456789abcdef"


# ---------------------------------------------------------------------------
# 3. content-length: 0 -> a genuinely empty remote file
# ---------------------------------------------------------------------------
def test_zero_length_remote_creates_empty_local_file(tmp_path):
    node = SizedNode(name="empty.txt", content=b"")
    dm = _manager(tmp_path)
    local_path = tmp_path / "empty.txt"

    assert dm.download_drive_item(node, local_path) is True
    assert local_path.is_file()
    assert local_path.stat().st_size == 0

    report = _summary(dm)
    assert report["successful"] == 1
    assert report["failed"] == 0
    assert report["skipped"] == 0


def test_zero_length_remote_with_existing_empty_file_is_skipped(tmp_path):
    node = SizedNode(name="empty.txt", content=b"")
    dm = _manager(tmp_path)
    local_path = tmp_path / "empty.txt"
    local_path.write_bytes(b"")

    assert dm.download_drive_item(node, local_path) is True
    assert local_path.stat().st_size == 0

    report = _summary(dm)
    assert report["skipped"] == 1
    assert report["successful"] == 0
    assert report["failed"] == 0


def test_zero_length_remote_never_deletes_a_non_empty_local_file(tmp_path):
    """A dubious zero from the server must not destroy an existing backup."""
    node = SizedNode(name="empty.txt", content=b"")
    dm = _manager(tmp_path)
    local_path = tmp_path / "empty.txt"
    local_path.write_bytes(b"real data")

    assert dm.download_drive_item(node, local_path) is True
    assert local_path.read_bytes() == b"real data"
    assert _summary(dm)["skipped"] == 1


def test_zero_length_creation_survives_a_second_run(tmp_path):
    node = SizedNode(name="empty.txt", content=b"")
    dm = _manager(tmp_path)
    local_path = tmp_path / "empty.txt"

    assert dm.download_drive_item(node, local_path) is True
    dm.sync_state.save()

    dm2 = _manager(tmp_path)
    assert dm2.download_drive_item(node, local_path) is True
    assert local_path.is_file() and local_path.stat().st_size == 0
    assert _summary(dm2)["failed"] == 0


# ---------------------------------------------------------------------------
# 4. The invariant: "unchanged" is only legal when the file is on disk
# ---------------------------------------------------------------------------
def test_unchanged_verdict_without_local_file_downloads_instead(tmp_path, monkeypatch):
    """A mocked 'nothing to fetch' verdict for an absent file must not succeed."""
    node = SizedNode(name="file.bin", content=b"0123456789")
    dm = _manager(tmp_path)
    local_path = tmp_path / "file.bin"

    monkeypatch.setattr(
        dm.chunker, "compute_download_ranges",
        lambda resp, local_path=None, force=False: []
    )
    monkeypatch.setattr(
        dm, "download_chunk",
        lambda url, start, end, item=None: node._content[start:end + 1]
    )

    assert dm.download_drive_item(node, local_path) is True

    report = _summary(dm)
    assert report["skipped"] == 0, "an absent file must never be reported skipped"
    assert report["successful"] == 1
    assert local_path.read_bytes() == b"0123456789"


def test_unchanged_verdict_without_local_file_reports_failure_if_undownloadable(
    tmp_path, monkeypatch
):
    node = SizedNode(name="file.bin", content=b"0123456789")
    dm = _manager(tmp_path)
    local_path = tmp_path / "file.bin"

    monkeypatch.setattr(
        dm.chunker, "compute_download_ranges",
        lambda resp, local_path=None, force=False: []
    )

    def _boom(url, start, end, item=None):
        raise Exception("network went away")

    monkeypatch.setattr(dm, "download_chunk", _boom)

    assert dm.download_drive_item(node, local_path) is False

    report = _summary(dm)
    assert report["failed"] == 1
    assert report["skipped"] == 0
    assert report["successful"] == 0


def test_stale_tracker_checkpoint_cannot_skip_an_absent_file(tmp_path, monkeypatch):
    """Checkpoint beyond EOF used to empty the range list and 'succeed'."""
    node = SizedNode(name="file.bin", content=b"0123456789")
    dm = _manager(tmp_path)
    local_path = tmp_path / "file.bin"
    local_path.with_suffix(local_path.suffix + ".download").write_text(
        '{"position": 999}'
    )

    monkeypatch.setattr(
        dm, "download_chunk",
        lambda url, start, end, item=None: node._content[start:end + 1]
    )

    assert dm.download_drive_item(node, local_path) is True
    assert local_path.read_bytes() == b"0123456789"
    assert _summary(dm)["skipped"] == 0


def test_fast_path_cannot_skip_a_file_that_is_not_on_disk(tmp_path, monkeypatch):
    node = SizedNode(name="file.bin", content=b"0123456789")
    dm = _manager(tmp_path)
    local_path = tmp_path / "file.bin"

    # Pretend the state file claims this path is complete.
    monkeypatch.setattr(dm, "_can_fast_skip", lambda item, path: True)
    monkeypatch.setattr(
        dm, "download_chunk",
        lambda url, start, end, item=None: node._content[start:end + 1]
    )

    assert dm.download_drive_item(node, local_path) is True
    assert node.open_calls == 1, "must fall through to the network check"
    assert local_path.read_bytes() == b"0123456789"
    assert _summary(dm)["skipped"] == 0


def test_local_file_exists_rejects_directories(tmp_path):
    (tmp_path / "adir").mkdir()
    assert DownloadManager._local_file_exists(tmp_path / "adir") is False
    assert DownloadManager._local_file_exists(tmp_path / "nope") is False
    (tmp_path / "a.bin").write_bytes(b"x")
    assert DownloadManager._local_file_exists(tmp_path / "a.bin") is True


# ---------------------------------------------------------------------------
# 5. The state file may never record a file that was never written
# ---------------------------------------------------------------------------
def test_state_never_records_a_file_that_was_never_written(tmp_path):
    """Unknown-length + failed transfer must leave no completed state entry."""
    node = StreamNode(name="setup.app", content=b"0123456789abcdef",
                      listed_size=16, fail_after=8)
    dm = _manager(tmp_path)
    local_path = tmp_path / "setup.app"

    assert dm.download_drive_item(node, local_path) is False
    dm.sync_state.save()

    payload = json.loads((tmp_path / SyncState.STATE_FILENAME).read_text())
    assert "setup.app" not in payload["files"]

    # A fresh run must therefore not fast-skip it.
    dm2 = _manager(tmp_path)
    good = StreamNode(name="setup.app", content=b"0123456789abcdef", listed_size=16)
    assert dm2.download_drive_item(good, local_path) is True
    assert good.open_calls == 1
    assert local_path.read_bytes() == b"0123456789abcdef"


def test_state_never_records_an_absent_path(tmp_path):
    state = SyncState(tmp_path)
    missing = tmp_path / "ghost.bin"

    state.record_completed(missing, 10, "date_modified=x")
    state.save()

    payload = json.loads((tmp_path / SyncState.STATE_FILENAME).read_text())
    assert payload["files"] == {}
    assert state.is_unchanged(missing, 10, "date_modified=x") is False


def test_state_never_records_a_directory(tmp_path):
    state = SyncState(tmp_path)
    target = tmp_path / "bundle.app"
    target.mkdir()

    state.record_completed(target, 0, "date_modified=x")
    assert state.is_unchanged(target, 0, "date_modified=x") is False


def test_state_ignores_bundles_whose_stream_size_differs_from_the_listing(tmp_path):
    """Server-side repackaging changes the byte count; never claim completion."""
    node = StreamNode(name="setup.app", content=b"zipped", listed_size=24068701)
    dm = _manager(tmp_path)
    local_path = tmp_path / "setup.app"

    assert dm.download_drive_item(node, local_path) is True
    dm.sync_state.save()

    payload = json.loads((tmp_path / SyncState.STATE_FILENAME).read_text())
    assert "setup.app" not in payload["files"]

    # ...so the next run re-fetches rather than fast-skipping a mismatch.
    dm2 = _manager(tmp_path)
    node.open_calls = 0
    assert dm2.download_drive_item(node, local_path) is True
    assert node.open_calls == 1


# ---------------------------------------------------------------------------
# 6. Run report arithmetic
# ---------------------------------------------------------------------------
def test_run_report_counts_the_mixed_case(tmp_path):
    dm = _manager(tmp_path)

    bundle = StreamNode(name="setup.app", content=b"0123456789abcdef")
    empty = SizedNode(name="empty.txt", content=b"")
    broken = StreamNode(name="broken.app", content=b"abcdefgh", fail_after=4)
    unchanged = SizedNode(name="same.bin", content=b"0123456789")
    (tmp_path / "same.bin").write_bytes(b"0123456789")

    assert dm.download_drive_item(bundle, tmp_path / "setup.app") is True
    assert dm.download_drive_item(empty, tmp_path / "empty.txt") is True
    assert dm.download_drive_item(broken, tmp_path / "broken.app") is False
    assert dm.download_drive_item(unchanged, tmp_path / "same.bin") is True

    report = _summary(dm)
    assert report["total_files"] == 4
    assert report["successful"] == 2
    assert report["failed"] == 1
    assert report["skipped"] == 1

    on_disk = {p.name for p in tmp_path.iterdir() if p.is_file()}
    # Every result reported as success or skip has a real file behind it.
    for result in dm.download_results:
        if result.status in ("completed", "skipped"):
            assert Path(result.path).name in on_disk
