import hashlib
import json
import sys
import threading
from pathlib import Path

import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))

from ifetch import verify as verify_mod  # noqa: E402
from ifetch.verify import (  # noqa: E402
    LEVEL_CHECKSUM,
    LEVEL_REDOWNLOAD,
    LEVEL_SIZE,
    STATUS_CHECKSUM_MISMATCH,
    STATUS_CHECKSUM_UNAVAILABLE,
    STATUS_ERROR,
    STATUS_EXTRA_LOCAL,
    STATUS_MISSING_LOCAL,
    STATUS_SIZE_MISMATCH,
    STATUS_VERIFIED,
    VerificationResult,
    Verifier,
    main,
    remote_checksum,
    sha256_file,
    sha256_stream,
)


# ---------------------------------------------------------------------------
# Mocked pyicloud drive objects (mirrors tests/test_downloader_behavior.py)
# ---------------------------------------------------------------------------

class _Stream:
    """Minimal stand-in for a streamed requests.Response."""

    def __init__(self, payload: bytes, chunks: int = 3):
        self.payload = payload
        self._chunks = chunks
        self.closed = False

    def iter_content(self, chunk_size=None):
        size = max(1, len(self.payload) // self._chunks or 1)
        for i in range(0, len(self.payload), size):
            yield self.payload[i:i + size]

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.closed = True
        return False


class _File:
    type = "file"

    def __init__(self, name, content=b"", size=None, data=None, open_error=None):
        self.name = name
        self.content = content
        self.size = len(content) if size is None else size
        self.data = data if data is not None else {}
        self.open_error = open_error
        self.opened = 0

    def open(self, stream=True):
        self.opened += 1
        if self.open_error:
            raise self.open_error
        return _Stream(self.content)


class _Folder:
    type = "folder"

    def __init__(self, name="folder", children=None, dir_error=None):
        self.name = name
        self.children = children or {}
        self.dir_error = dir_error

    def dir(self):
        if self.dir_error:
            raise self.dir_error
        return list(self.children.keys())

    def __getitem__(self, key):
        return self.children[key]


class _FakeDownloader:
    """Stands in for DownloadManager (composition target)."""

    def __init__(self, root_node):
        self.root_node = root_node
        self.api = object()  # already "authenticated"
        self.authenticated = 0
        self.max_retries = 1

    def authenticate(self):
        self.authenticated += 1
        self.api = object()

    def get_drive_item(self, path):
        return self.root_node

    def _open_with_retry(self, item, max_retries=None, remote_path=None):
        return item.open(stream=True)


def _make_verifier(tmp_path, root_node, **kwargs):
    return Verifier(
        local_root=tmp_path,
        downloader=_FakeDownloader(root_node),
        **kwargs,
    )


def _tree(tmp_path, files):
    for rel, content in files.items():
        path = tmp_path / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def test_sha256_file_matches_hashlib(tmp_path):
    path = tmp_path / "a.txt"
    path.write_bytes(b"hello world")
    assert sha256_file(path) == hashlib.sha256(b"hello world").hexdigest()


def test_sha256_stream_hashes_chunks_without_disk():
    payload = b"x" * 1000
    digest, total = sha256_stream(_Stream(payload))
    assert digest == hashlib.sha256(payload).hexdigest()
    assert total == 1000


def test_remote_checksum_absent_for_plain_icloud_node():
    node = _File("a.txt", b"abc", data={"etag": "3::5", "size": 3, "zone": "com.apple.CloudDocs"})
    assert remote_checksum(node) is None


def test_remote_checksum_picked_up_when_apple_publishes_one():
    node = _File("a.txt", b"abc", data={"fileChecksum": "deadbeef"})
    assert remote_checksum(node) == "deadbeef"


def test_invalid_level_rejected(tmp_path):
    with pytest.raises(ValueError):
        Verifier(local_root=tmp_path, downloader=_FakeDownloader(_Folder()), level="bogus")


# ---------------------------------------------------------------------------
# Size level
# ---------------------------------------------------------------------------

def test_all_files_match(tmp_path):
    _tree(tmp_path, {"a.txt": b"aaa", "sub/b.txt": b"bbbb"})
    root = _Folder("root", {
        "a.txt": _File("a.txt", b"aaa"),
        "sub": _Folder("sub", {"b.txt": _File("b.txt", b"bbbb")}),
    })

    summary = _make_verifier(tmp_path, root).verify("Documents")

    assert summary.ok is True
    assert summary.counts["total"] == 2
    assert summary.counts[STATUS_VERIFIED] == 2
    assert [r.path for r in summary.results] == ["a.txt", "sub/b.txt"]


def test_size_mismatch_detected(tmp_path):
    _tree(tmp_path, {"a.txt": b"aa"})
    root = _Folder("root", {"a.txt": _File("a.txt", b"aaaaa")})

    summary = _make_verifier(tmp_path, root).verify("Documents")

    result = summary.results[0]
    assert result.status == STATUS_SIZE_MISMATCH
    assert result.local_size == 2
    assert result.remote_size == 5
    assert "!=" in result.reason
    assert summary.ok is False


def test_missing_local_file_detected(tmp_path):
    root = _Folder("root", {"gone.txt": _File("gone.txt", b"abc")})

    summary = _make_verifier(tmp_path, root).verify("Documents")

    assert summary.counts[STATUS_MISSING_LOCAL] == 1
    assert summary.results[0].remote_size == 3
    assert summary.results[0].local_size is None
    assert summary.ok is False


def test_extra_local_file_detected(tmp_path):
    _tree(tmp_path, {"a.txt": b"aaa", "orphan.txt": b"zz"})
    root = _Folder("root", {"a.txt": _File("a.txt", b"aaa")})

    summary = _make_verifier(tmp_path, root).verify("Documents")

    extras = [r for r in summary.results if r.status == STATUS_EXTRA_LOCAL]
    assert [r.path for r in extras] == ["orphan.txt"]
    assert extras[0].local_size == 2
    # Extra local files are reported but are not a verification failure.
    assert summary.ok is True


def test_ifetch_artifacts_not_reported_as_extra(tmp_path):
    _tree(tmp_path, {
        "a.txt": b"aaa",
        "download_report.json": b"{}",
        ".ifetch_versions.json": b"{}",
        "a.txt.temp": b"x",
        "a.txt.download": b"{}",
        ".versions/a.txt/0": b"old",
    })
    root = _Folder("root", {"a.txt": _File("a.txt", b"aaa")})

    summary = _make_verifier(tmp_path, root).verify("Documents")

    assert summary.counts[STATUS_EXTRA_LOCAL] == 0
    assert summary.counts["total"] == 1


def test_single_remote_file_uses_its_own_name(tmp_path):
    _tree(tmp_path, {"solo.txt": b"abc"})
    root = _File("solo.txt", b"abc")

    summary = _make_verifier(tmp_path, root).verify("Documents/solo.txt")

    assert summary.counts[STATUS_VERIFIED] == 1
    assert summary.results[0].path == "solo.txt"


def test_include_exclude_patterns_filter_both_sides(tmp_path):
    _tree(tmp_path, {"keep.txt": b"aaa", "skip.log": b"z"})
    root = _Folder("root", {
        "keep.txt": _File("keep.txt", b"aaa"),
        "skip.log": _File("skip.log", b"different"),
    })

    verifier = _make_verifier(tmp_path, root, exclude_patterns=["*.log"])
    summary = verifier.verify("Documents")

    assert summary.counts["total"] == 1
    assert summary.results[0].path == "keep.txt"
    assert summary.ok is True


# ---------------------------------------------------------------------------
# Checksum level
# ---------------------------------------------------------------------------

def test_checksum_level_reports_unavailable_when_apple_has_no_hash(tmp_path):
    _tree(tmp_path, {"a.txt": b"abc"})
    root = _Folder("root", {"a.txt": _File("a.txt", b"abc", data={"etag": "1::1"})})

    summary = _make_verifier(tmp_path, root, level=LEVEL_CHECKSUM).verify("Documents")

    result = summary.results[0]
    assert result.status == STATUS_CHECKSUM_UNAVAILABLE
    assert result.local_checksum == hashlib.sha256(b"abc").hexdigest()
    assert result.remote_checksum is None
    # Not a hard failure -- size matched, we just cannot prove content.
    assert summary.ok is True


def test_checksum_level_matches_when_remote_hash_present(tmp_path):
    payload = b"abc"
    digest = hashlib.sha256(payload).hexdigest()
    _tree(tmp_path, {"a.txt": payload})
    root = _Folder("root", {"a.txt": _File("a.txt", payload, data={"fileChecksum": digest})})

    summary = _make_verifier(tmp_path, root, level=LEVEL_CHECKSUM).verify("Documents")

    assert summary.results[0].status == STATUS_VERIFIED
    assert summary.results[0].remote_checksum == digest
    assert summary.ok is True


def test_checksum_level_mismatch_when_remote_hash_differs(tmp_path):
    _tree(tmp_path, {"a.txt": b"abc"})
    root = _Folder("root", {"a.txt": _File("a.txt", b"abc", data={"fileChecksum": "0" * 64})})

    summary = _make_verifier(tmp_path, root, level=LEVEL_CHECKSUM).verify("Documents")

    assert summary.results[0].status == STATUS_CHECKSUM_MISMATCH
    assert summary.ok is False


def test_checksum_level_still_catches_size_mismatch_without_hashing(tmp_path):
    _tree(tmp_path, {"a.txt": b"a"})
    root = _Folder("root", {"a.txt": _File("a.txt", b"aaaa")})

    summary = _make_verifier(tmp_path, root, level=LEVEL_CHECKSUM).verify("Documents")

    assert summary.results[0].status == STATUS_SIZE_MISMATCH
    assert summary.results[0].local_checksum is None


# ---------------------------------------------------------------------------
# Redownload level
# ---------------------------------------------------------------------------

def test_redownload_level_verifies_matching_content(tmp_path):
    payload = b"the quick brown fox" * 10
    _tree(tmp_path, {"a.txt": payload})
    node = _File("a.txt", payload)
    root = _Folder("root", {"a.txt": node})

    summary = _make_verifier(tmp_path, root, level=LEVEL_REDOWNLOAD).verify("Documents")

    result = summary.results[0]
    assert result.status == STATUS_VERIFIED
    assert result.remote_checksum == hashlib.sha256(payload).hexdigest()
    assert node.opened == 1
    assert summary.ok is True


def test_redownload_level_detects_content_corruption_with_equal_size(tmp_path):
    local = b"A" * 32
    remote = b"B" * 32
    _tree(tmp_path, {"a.txt": local})
    root = _Folder("root", {"a.txt": _File("a.txt", remote)})

    summary = _make_verifier(tmp_path, root, level=LEVEL_REDOWNLOAD).verify("Documents")

    result = summary.results[0]
    assert result.status == STATUS_CHECKSUM_MISMATCH
    assert result.local_checksum == hashlib.sha256(local).hexdigest()
    assert result.remote_checksum == hashlib.sha256(remote).hexdigest()
    assert summary.ok is False


def test_redownload_never_writes_remote_bytes_to_disk(tmp_path):
    payload = b"C" * 64
    _tree(tmp_path, {"a.txt": payload})
    before = {p: p.read_bytes() for p in tmp_path.rglob("*") if p.is_file()}
    root = _Folder("root", {"a.txt": _File("a.txt", payload)})

    _make_verifier(tmp_path, root, level=LEVEL_REDOWNLOAD).verify("Documents")

    after = {p: p.read_bytes() for p in tmp_path.rglob("*") if p.is_file()}
    assert after == before


def test_size_level_never_opens_remote_files(tmp_path):
    _tree(tmp_path, {"a.txt": b"abc"})
    node = _File("a.txt", b"abc")
    root = _Folder("root", {"a.txt": node})

    _make_verifier(tmp_path, root, level=LEVEL_SIZE).verify("Documents")

    assert node.opened == 0


# ---------------------------------------------------------------------------
# Error containment / parallelism
# ---------------------------------------------------------------------------

def test_one_failing_file_does_not_abort_the_run(tmp_path):
    _tree(tmp_path, {"good.txt": b"abc", "bad.txt": b"abc"})
    root = _Folder("root", {
        "good.txt": _File("good.txt", b"abc"),
        "bad.txt": _File("bad.txt", b"abc", open_error=RuntimeError("boom")),
    })

    summary = _make_verifier(tmp_path, root, level=LEVEL_REDOWNLOAD).verify("Documents")

    statuses = {r.path: r.status for r in summary.results}
    assert statuses["good.txt"] == STATUS_VERIFIED
    assert statuses["bad.txt"] == STATUS_ERROR
    assert "boom" in next(r.reason for r in summary.results if r.path == "bad.txt")
    assert summary.ok is False


def test_listing_error_in_subtree_is_contained(tmp_path):
    _tree(tmp_path, {"a.txt": b"abc"})
    root = _Folder("root", {
        "a.txt": _File("a.txt", b"abc"),
        "broken": _Folder("broken", dir_error=RuntimeError("listing failed")),
    })

    summary = _make_verifier(tmp_path, root).verify("Documents")

    assert summary.counts["total"] == 1
    assert summary.results[0].status == STATUS_VERIFIED


def test_parallel_execution_uses_multiple_workers(tmp_path):
    files = {f"f{i}.txt": b"abc" for i in range(12)}
    _tree(tmp_path, files)
    root = _Folder("root", {name: _File(name, b"abc") for name in files})

    verifier = _make_verifier(tmp_path, root, max_workers=4)
    threads = set()
    original = verifier.verify_file

    def _tracking(rel_path, item):
        threads.add(threading.current_thread().name)
        return original(rel_path, item)

    verifier.verify_file = _tracking
    summary = verifier.verify("Documents")

    assert summary.counts[STATUS_VERIFIED] == 12
    assert len(threads) > 1


def test_progress_callback_reports_monotonic_progress(tmp_path):
    files = {f"f{i}.txt": b"abc" for i in range(5)}
    _tree(tmp_path, files)
    root = _Folder("root", {name: _File(name, b"abc") for name in files})

    seen = []
    lock = threading.Lock()

    def _cb(done, total, result):
        with lock:
            seen.append((done, total, result.path))

    _make_verifier(tmp_path, root, max_workers=3, progress_callback=_cb).verify("Documents")

    assert len(seen) == 5
    assert sorted(done for done, _, _ in seen) == [1, 2, 3, 4, 5]
    assert all(total == 5 for _, total, _ in seen)


def test_broken_progress_callback_does_not_break_verification(tmp_path):
    _tree(tmp_path, {"a.txt": b"abc"})
    root = _Folder("root", {"a.txt": _File("a.txt", b"abc")})

    def _cb(done, total, result):
        raise RuntimeError("renderer exploded")

    summary = _make_verifier(tmp_path, root, progress_callback=_cb).verify("Documents")

    assert summary.counts[STATUS_VERIFIED] == 1


def test_verify_authenticates_only_when_needed(tmp_path):
    root = _Folder("root", {})
    downloader = _FakeDownloader(root)
    downloader.api = None

    Verifier(local_root=tmp_path, downloader=downloader).verify("Documents")

    assert downloader.authenticated == 1


# ---------------------------------------------------------------------------
# Report shape
# ---------------------------------------------------------------------------

def test_summary_to_dict_shape(tmp_path):
    _tree(tmp_path, {"a.txt": b"abc", "orphan.txt": b"z"})
    root = _Folder("root", {
        "a.txt": _File("a.txt", b"abc"),
        "missing.txt": _File("missing.txt", b"zzz"),
    })

    summary = _make_verifier(tmp_path, root).verify("Documents")
    payload = summary.to_dict()

    assert set(payload) == {"summary", "details"}
    head = payload["summary"]
    assert head["level"] == LEVEL_SIZE
    assert head["icloud_path"] == "Documents"
    assert head["local_root"] == str(tmp_path.resolve())
    assert head["ok"] is False
    assert head["counts"]["total"] == 3
    assert head["counts"][STATUS_VERIFIED] == 1
    assert head["counts"][STATUS_MISSING_LOCAL] == 1
    assert head["counts"][STATUS_EXTRA_LOCAL] == 1
    assert isinstance(head["duration_seconds"], float)
    assert set(payload["details"][0]) == {
        "path", "status", "local_size", "remote_size",
        "local_checksum", "remote_checksum", "reason",
    }
    # Must be JSON-serialisable as-is.
    json.loads(json.dumps(payload))


def test_verification_result_ok_property():
    assert VerificationResult(path="a", status=STATUS_VERIFIED).ok is True
    assert VerificationResult(path="a", status=STATUS_EXTRA_LOCAL).ok is True
    assert VerificationResult(path="a", status=STATUS_CHECKSUM_UNAVAILABLE).ok is True
    assert VerificationResult(path="a", status=STATUS_SIZE_MISMATCH).ok is False
    assert VerificationResult(path="a", status=STATUS_ERROR).ok is False


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _patch_verifier(monkeypatch, tmp_path, root, captured=None):
    def _factory(**kwargs):
        if captured is not None:
            captured.update(kwargs)
        return Verifier(
            local_root=kwargs.get("local_root", tmp_path),
            downloader=_FakeDownloader(root),
            level=kwargs.get("level", LEVEL_SIZE),
            max_workers=kwargs.get("max_workers", 4),
            progress_callback=kwargs.get("progress_callback"),
        )

    monkeypatch.setattr(verify_mod, "Verifier", _factory)


def test_main_returns_zero_when_everything_verifies(monkeypatch, tmp_path, capsys):
    _tree(tmp_path, {"a.txt": b"abc"})
    root = _Folder("root", {"a.txt": _File("a.txt", b"abc")})
    _patch_verifier(monkeypatch, tmp_path, root)

    code = main(["Documents", str(tmp_path)])

    assert code == 0
    assert "Verified: 1" in capsys.readouterr().out


def test_main_returns_one_on_verification_failure(monkeypatch, tmp_path):
    _tree(tmp_path, {"a.txt": b"a"})
    root = _Folder("root", {"a.txt": _File("a.txt", b"abcdef")})
    _patch_verifier(monkeypatch, tmp_path, root)

    assert main(["Documents", str(tmp_path), "--quiet"]) == 1


def test_main_returns_two_on_operational_error(monkeypatch, tmp_path):
    def _boom(**kwargs):
        raise Exception("auth exploded")

    monkeypatch.setattr(verify_mod, "Verifier", _boom)

    assert main(["Documents", str(tmp_path), "--quiet"]) == 2


def test_main_writes_json_report(monkeypatch, tmp_path):
    local = tmp_path / "mirror"
    local.mkdir()
    _tree(local, {"a.txt": b"abc"})
    root = _Folder("root", {"a.txt": _File("a.txt", b"abc")})
    _patch_verifier(monkeypatch, local, root)
    report = tmp_path / "reports" / "verify.json"

    code = main(["Documents", str(local), "--report", str(report), "--quiet"])

    assert code == 0
    payload = json.loads(report.read_text())
    assert payload["summary"]["ok"] is True
    assert payload["summary"]["counts"][STATUS_VERIFIED] == 1
    assert payload["details"][0]["path"] == "a.txt"


def test_main_passes_level_and_workers_through(monkeypatch, tmp_path):
    _tree(tmp_path, {"a.txt": b"abc"})
    root = _Folder("root", {"a.txt": _File("a.txt", b"abc")})
    captured = {}
    _patch_verifier(monkeypatch, tmp_path, root, captured)

    main(["Documents", str(tmp_path), "--level", "redownload", "--max-workers", "7", "--quiet"])

    assert captured["level"] == LEVEL_REDOWNLOAD
    assert captured["max_workers"] == 7
    assert captured["local_root"] == str(tmp_path)


def test_main_quiet_suppresses_progress_output(monkeypatch, tmp_path, capsys):
    _tree(tmp_path, {"a.txt": b"abc"})
    root = _Folder("root", {"a.txt": _File("a.txt", b"abc")})
    _patch_verifier(monkeypatch, tmp_path, root)

    main(["Documents", str(tmp_path), "--quiet"])

    out = capsys.readouterr().out
    # The shared JSON logger still emits its completion line (project
    # convention), but no banner, per-file progress or summary is printed.
    assert "Verification Summary" not in out
    assert "iFetch Integrity Verification" not in out
    assert "[1/1]" not in out


def test_main_rejects_unknown_level(tmp_path):
    with pytest.raises(SystemExit):
        main(["Documents", str(tmp_path), "--level", "bogus"])
