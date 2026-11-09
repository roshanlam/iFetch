"""Parallel intra-file range GETs under the global concurrency budget (#45)."""

import os
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

import pytest
import requests

sys.path.append(str(Path(__file__).resolve().parents[1]))

from ifetch.downloader import DownloadManager, SyncState  # noqa: E402
from ifetch.index import IndexStore  # noqa: E402
from ifetch.tracker import DownloadTracker  # noqa: E402
from ifetch.transfers import TransferJournal  # noqa: E402

KIB = 1024
MTIME = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)


class Node:
    type = "file"

    def __init__(self, name, content):
        self.name = name
        self.content = content
        self.size = len(content)
        self.date_modified = MTIME
        self.date_changed = None

    def open(self, stream=True):
        outer = self

        class Ctx:
            headers = {"content-length": str(len(outer.content))}
            url = f"https://cdn.invalid/{outer.name}"

            def __enter__(self):
                return self

            def __exit__(self, *exc):
                return False

        return Ctx()


class FakeCDN:
    """Serves Range GETs for several files and measures concurrency."""

    def __init__(self, files, delay=0.01, fail=None):
        self.files = files
        self.delay = delay
        self.fail = fail or (lambda name, start: False)
        self.lock = threading.Lock()
        self.in_flight = 0
        self.peak = 0
        self.requests = []

    def get(self, url, headers=None, stream=True, timeout=None):
        name = url.rsplit("/", 1)[-1]
        start, end = map(int, re.match(r"bytes=(\d+)-(\d+)", headers["Range"]).groups())
        with self.lock:
            self.in_flight += 1
            self.peak = max(self.peak, self.in_flight)
            self.requests.append((name, start, end))
        try:
            time.sleep(self.delay)
            failing = self.fail(name, start)
        finally:
            with self.lock:
                self.in_flight -= 1

        class Resp:
            status_code = 500 if failing else 206
            content = b"" if failing else self.files[name][start:end + 1]

            def raise_for_status(self):
                if self.status_code >= 400:
                    raise requests.HTTPError("500 Server Error", response=self)

        resp = Resp()
        resp.headers = {}
        return resp


def _payload(size):
    return os.urandom(size)


def _manager(tmp_path, cdn, monkeypatch, **kwargs):
    kwargs.setdefault("max_workers", 4)
    kwargs.setdefault("max_retries", 1)
    kwargs.setdefault("chunk_size", 16 * KIB)
    kwargs.setdefault("range_segment_size", 16 * KIB)
    dm = DownloadManager(email="user@example.com", **kwargs)
    dm.root_path = tmp_path
    dm.sync_state = SyncState(tmp_path)
    dm.http.get = cdn.get
    monkeypatch.setattr(dm, "calculate_checksum", lambda p: "dummy")
    return dm


def test_one_large_file_uses_parallel_range_gets(tmp_path, monkeypatch):
    content = _payload(256 * KIB)
    cdn = FakeCDN({"big.bin": content})
    dm = _manager(tmp_path, cdn, monkeypatch)
    assert dm.download_drive_item(Node("big.bin", content), tmp_path / "big.bin")
    assert (tmp_path / "big.bin").read_bytes() == content
    assert len(cdn.requests) == 16
    assert 1 < cdn.peak <= dm.max_workers


def test_ranges_are_merged_then_split_only_for_parallel_fetch(tmp_path, monkeypatch):
    dm = _manager(tmp_path, FakeCDN({}), monkeypatch)
    merged = dm._merge_ranges([(0, 9), (10, 19), (15, 40 * KIB - 1)])
    assert merged == [(0, 40 * KIB - 1)]
    assert dm._plan_segments(merged) == [
        (0, 16 * KIB - 1), (16 * KIB, 32 * KIB - 1), (32 * KIB, 40 * KIB - 1),
    ]
    dm.range_concurrency = 1
    assert dm._plan_segments(merged) == merged  # off: one GET per merged range


def test_small_file_stays_a_single_get(tmp_path, monkeypatch):
    content = _payload(10 * KIB)
    cdn = FakeCDN({"small.bin": content})
    dm = _manager(tmp_path, cdn, monkeypatch)
    assert dm.download_drive_item(Node("small.bin", content), tmp_path / "small.bin")
    assert cdn.requests == [("small.bin", 0, len(content) - 1)]


def test_global_budget_caps_range_gets_across_files(tmp_path, monkeypatch):
    files = {f"f{i}.bin": _payload(128 * KIB) for i in range(4)}
    cdn = FakeCDN(files)
    dm = _manager(tmp_path, cdn, monkeypatch, max_workers=2, range_concurrency=8)
    with ThreadPoolExecutor(max_workers=4) as pool:
        ok = list(pool.map(
            lambda n: dm.download_drive_item(Node(n, files[n]), tmp_path / n), files
        ))
    assert ok == [True] * 4
    assert cdn.peak <= 2
    for name, content in files.items():
        assert (tmp_path / name).read_bytes() == content


def test_range_concurrency_one_is_sequential(tmp_path, monkeypatch):
    content = _payload(128 * KIB)
    cdn = FakeCDN({"big.bin": content})
    dm = _manager(tmp_path, cdn, monkeypatch, range_concurrency=1)
    assert dm.download_drive_item(Node("big.bin", content), tmp_path / "big.bin")
    assert cdn.peak == 1
    assert cdn.requests == [("big.bin", 0, len(content) - 1)]


def test_checkpoints_are_monotonic_and_backed_by_flushed_bytes(tmp_path, monkeypatch):
    content = _payload(256 * KIB)
    # Reverse-skewed latency: later segments finish first, so completions
    # arrive out of order and the frontier logic is actually exercised.
    cdn = FakeCDN({"big.bin": content})
    original_get = cdn.get

    def skewed(url, headers=None, **kw):
        start = int(re.match(r"bytes=(\d+)", headers["Range"]).group(1))
        time.sleep(0.02 if start % (64 * KIB) == 0 else 0)
        return original_get(url, headers=headers, **kw)

    local = tmp_path / "big.bin"
    temp = local.with_suffix(".bin.temp")
    positions, journal_positions = [], []
    real_save = DownloadTracker.save_status

    def spy_save(self, position):
        # Every claimed prefix must already be on disk, byte for byte.
        with temp.open("rb") as fh:
            assert fh.read(position) == content[:position]
        positions.append(position)
        real_save(self, position)

    monkeypatch.setattr(DownloadTracker, "save_status", spy_save)
    with IndexStore(tmp_path) as store:
        dm = _manager(tmp_path, cdn, monkeypatch)
        dm.http.get = skewed
        dm.journal = TransferJournal(store, tmp_path)
        real_progress = dm.journal.progress
        dm.journal.progress = lambda p, n: (journal_positions.append(n), real_progress(p, n))
        assert dm.download_drive_item(Node("big.bin", content), local)

    assert positions == sorted(positions)
    assert journal_positions == positions
    assert positions[-1] == len(content)
    assert local.read_bytes() == content


def test_failure_mid_file_leaves_a_true_prefix_and_stops_new_segments(tmp_path, monkeypatch):
    content = _payload(256 * KIB)
    bad = 5 * 16 * KIB
    cdn = FakeCDN({"big.bin": content}, fail=lambda name, start: start == bad)
    dm = _manager(tmp_path, cdn, monkeypatch, range_concurrency=2, max_workers=2)
    local = tmp_path / "big.bin"
    assert dm.download_drive_item(Node("big.bin", content), local) is False
    assert "Failed to download chunk" in dm.download_results[-1].error

    position = DownloadTracker(local).current_position
    assert position <= bad and position % (16 * KIB) == 0
    temp = local.with_suffix(".bin.temp")
    assert temp.read_bytes()[:position] == content[:position]
    # Workers stop picking up new segments once one has failed.
    assert len(cdn.requests) < 16


def test_proven_prefix_resume_fetches_only_the_tail_in_parallel(tmp_path, monkeypatch):
    content = _payload(256 * KIB)
    prefix = 96 * KIB
    local = tmp_path / "big.bin"
    local.write_bytes(content[:prefix])
    cdn = FakeCDN({"big.bin": content})
    with IndexStore(tmp_path) as store:
        dm = _manager(tmp_path, cdn, monkeypatch)
        dm.journal = TransferJournal(store, tmp_path)
        dm.journal.begin(local, total_bytes=len(content), resume_from=prefix)
        dm.journal.progress(local, prefix)
        assert dm.download_drive_item(Node("big.bin", content), local)

    assert min(start for _, start, _ in cdn.requests) == prefix
    assert sum(e - s + 1 for _, s, e in cdn.requests) == len(content) - prefix
    assert cdn.peak > 1
    assert local.read_bytes() == content


@pytest.mark.parametrize("value, expected", [(None, 4), (1, 1), (0, 1), (12, 12)])
def test_range_concurrency_defaults_to_max_workers(value, expected):
    dm = DownloadManager(email="u@example.com", max_workers=4, range_concurrency=value)
    assert dm.range_concurrency == expected
