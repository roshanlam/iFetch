import json
import sys
import threading
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from ifetch.versioning import (  # noqa: E402
    VersionManager,
    compute_checksum,
    entry_epoch,
    entry_size,
    format_timestamp,
    is_within,
    load_metadata,
    parse_timestamp,
)


def test_version_manager_backup_and_meta(tmp_path):
    root = tmp_path / "dl"
    root.mkdir()
    vm = VersionManager(root)

    # Create a file to archive
    file_path = root / "sample.txt"
    file_path.write_text("v1")

    rel = file_path.relative_to(root)
    checksum_v1 = "ck1"

    vm.record_version(rel, checksum_v1, file_path)

    # Original path should not exist (moved)
    assert not file_path.exists()

    # Metadata persisted
    meta = json.loads((root / vm.META_FILENAME).read_text())
    assert str(rel) in meta
    assert meta[str(rel)][0]["checksum"] == checksum_v1

    # New content (simulate write) and record again
    new_file = root / "sample.txt"
    new_file.write_text("v2")
    vm.record_version(rel, "ck2", new_file)

    # Two versions recorded
    meta2 = json.loads((root / vm.META_FILENAME).read_text())
    assert len(meta2[str(rel)]) == 2

def test_latest_checksum_empty(tmp_path):
    vm = VersionManager(tmp_path)
    assert vm.latest_checksum(Path("nonexistent.txt")) is None


def test_latest_checksum_after_records(tmp_path):
    vm = VersionManager(tmp_path)
    p = Path("a.txt")
    file_path = tmp_path / p
    file_path.write_text("x")
    vm.record_version(p, "abc", file_path)
    assert vm.latest_checksum(p) == "abc"


def test_persistence_after_reload(tmp_path):
    root = tmp_path
    vm1 = VersionManager(root)
    p = Path("b.txt")
    fp = root / p
    fp.write_text("1")
    vm1.record_version(p, "c1", fp)

    # Reload
    vm2 = VersionManager(root)
    assert vm2.latest_checksum(p) == "c1"


def test_move_failure_graceful(tmp_path, monkeypatch):
    vm = VersionManager(tmp_path)
    p = Path("c.txt")
    fp = tmp_path / p
    fp.write_text("content")

    # Patch shutil.move to raise
    monkeypatch.setattr("ifetch.versioning.shutil.move", lambda *a, **kw: (_ for _ in ()).throw(OSError("fail")))

    import json as _json
    before = _json.dumps(vm._data)
    vm.record_version(p, "xyz", fp)

    # Data should be unchanged due to move failure
    assert _json.dumps(vm._data) == before
    # Original file should still exist as move failed
    assert fp.exists()


def test_record_version_returns_entry_with_size_and_epoch(tmp_path):
    vm = VersionManager(tmp_path)
    p = Path("d.txt")
    fp = tmp_path / p
    fp.write_text("hello")

    entry = vm.record_version(p, "sum", fp)

    assert entry is not None
    assert entry["size"] == 5
    assert entry["epoch"] > 0
    assert entry_size(entry) == 5
    assert entry_epoch(entry) == entry["epoch"]
    assert Path(entry["archived_path"]).read_text() == "hello"


def test_record_version_failure_returns_none(tmp_path, monkeypatch):
    vm = VersionManager(tmp_path)
    fp = tmp_path / "e.txt"
    fp.write_text("x")
    monkeypatch.setattr(
        "ifetch.versioning.shutil.move",
        lambda *a, **kw: (_ for _ in ()).throw(OSError("fail")),
    )

    assert vm.record_version(Path("e.txt"), "sum", fp) is None


def test_legacy_entries_without_size_or_epoch(tmp_path):
    """Metadata written by older iFetch releases must still be readable."""
    archived = tmp_path / "old.bin"
    archived.write_text("legacy")
    legacy = {
        "version": 1,
        "checksum": "abc",
        "archived_path": str(archived),
        "timestamp": "20260101T101112",
    }

    assert entry_size(legacy) == 6  # falls back to the file on disk
    assert entry_epoch(legacy) == parse_timestamp("20260101T101112")
    assert format_timestamp(entry_epoch(legacy)) == "20260101T101112"


def test_legacy_metadata_file_loads(tmp_path):
    (tmp_path / VersionManager.META_FILENAME).write_text(json.dumps({
        "a.txt": [{"version": 1, "checksum": "c1",
                   "archived_path": "/x", "timestamp": "20260101T000000"}]
    }))

    vm = VersionManager(tmp_path)

    assert vm.latest_checksum(Path("a.txt")) == "c1"
    assert vm.list_paths() == ["a.txt"]
    assert vm.versions_for("a.txt")[0]["version"] == 1


def test_corrupt_metadata_shapes_are_tolerated(tmp_path):
    meta = tmp_path / VersionManager.META_FILENAME

    meta.write_text("[1, 2, 3]")
    assert load_metadata(meta) == {}

    meta.write_text("{not json")
    assert load_metadata(meta) == {}

    meta.write_text(json.dumps({"a.txt": "nope", "b.txt": [{"version": 1}]}))
    assert load_metadata(meta) == {"b.txt": [{"version": 1}]}

    assert load_metadata(tmp_path / "absent.json") == {}


def test_record_version_refuses_to_escape_versions_dir(tmp_path):
    root = tmp_path / "dl"
    root.mkdir()
    vm = VersionManager(root)
    victim = tmp_path / "victim.txt"
    victim.write_text("data")

    assert vm.record_version(Path("../../victim.txt"), "sum", victim) is None
    # Nothing moved, nothing recorded outside the tree.
    assert victim.read_text() == "data"
    assert vm.versions_for("../../victim.txt") == []


def test_concurrent_records_get_unique_versions(tmp_path):
    vm = VersionManager(tmp_path)
    rel = Path("busy.txt")
    barrier = threading.Barrier(4)

    def worker(n):
        src = tmp_path / f"src{n}"
        src.write_text(f"content-{n}")
        barrier.wait()
        vm.record_version(rel, f"ck{n}", src)

    threads = [threading.Thread(target=worker, args=(n,)) for n in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    versions = vm.versions_for(rel)
    assert len(versions) == 4
    assert sorted(v["version"] for v in versions) == [1, 2, 3, 4]
    # Every archive survived; none was clobbered by a duplicate version number.
    assert len({v["archived_path"] for v in versions}) == 4
    assert all(Path(v["archived_path"]).exists() for v in versions)


def test_key_for_normalises_paths():
    assert VersionManager.key_for(Path("a/b.txt")) == "a/b.txt"
    assert VersionManager.key_for("a\\b.txt") == "a/b.txt"


def test_compute_checksum_matches_hashlib(tmp_path):
    import hashlib

    fp = tmp_path / "f.bin"
    payload = b"x" * (1024 * 1024 + 7)
    fp.write_bytes(payload)

    assert compute_checksum(fp) == hashlib.sha256(payload).hexdigest()


def test_parse_timestamp_variants():
    assert parse_timestamp(None) is None
    assert parse_timestamp("") is None
    assert parse_timestamp("garbage") is None
    assert parse_timestamp(123.5) == 123.5
    assert parse_timestamp("123.5") == 123.5
    assert parse_timestamp("2026-01-01") == parse_timestamp("20260101")
    assert parse_timestamp("2026-01-01T10:11:12") == parse_timestamp("20260101T101112")
    assert format_timestamp(None) is None


def test_is_within(tmp_path):
    assert is_within(tmp_path / "a" / "b", tmp_path)
    assert is_within(tmp_path, tmp_path)
    assert not is_within(tmp_path.parent / "elsewhere", tmp_path)