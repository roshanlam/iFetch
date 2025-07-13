import json
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from ifetch.versioning import VersionManager  # noqa: E402


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


# ----------------------------------------------------------------------
# Additional tests merged from test_versioning_extra.py
# ----------------------------------------------------------------------


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