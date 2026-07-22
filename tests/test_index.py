"""Tests for the SQLite index that backs every recovery workflow.

The contract:

* remote and local are recorded **separately** and never merged - the whole
  point is being able to ask what differs;
* the diff distinguishes new / changed / unchanged / local-only / unjudgeable,
  and never claims to know something it cannot;
* migration from the legacy JSON files happens once, is non-destructive, and
  leaves the JSON readable by an older iFetch;
* transfer rows survive the process dying, which is what makes resume possible;
* snapshots are frozen copies, not live views.
"""

import json
import sqlite3
import sys
import threading
import time
from pathlib import Path

import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))

from ifetch.index import (  # noqa: E402
    DIFF_CHANGED,
    DIFF_LOCAL_ONLY,
    DIFF_NEW,
    DIFF_UNCHANGED,
    DIFF_UNKNOWN,
    INDEX_FILENAME,
    KIND_DIR,
    KIND_FILE,
    KIND_PACKAGE,
    TRANSFER_ACTIVE,
    TRANSFER_DONE,
    TRANSFER_FAILED,
    IndexStore,
    LocalItem,
    RemoteItem,
    open_index,
)


@pytest.fixture
def store(tmp_path):
    with IndexStore(tmp_path / "dest") as s:
        yield s


def statuses(entries):
    return {e.path: e.status for e in entries}


class TestSchema:
    def test_database_is_created_in_the_destination(self, tmp_path):
        with IndexStore(tmp_path / "dest") as s:
            assert s.path.name == INDEX_FILENAME
            assert s.path.exists()

    def test_schema_version_is_recorded(self, store):
        assert store.get_meta("schema_version") == "1"

    def test_reopening_preserves_data(self, tmp_path):
        root = tmp_path / "dest"
        with IndexStore(root) as s:
            s.record_local(LocalItem("a.txt", size=5, sha256="abc"))
        with IndexStore(root) as s:
            assert s.get_local("a.txt")["sha256"] == "abc"

    def test_wal_mode_is_enabled_for_crash_safety(self, store):
        mode = store._conn.execute("PRAGMA journal_mode").fetchone()[0]
        assert mode.lower() == "wal"

    def test_meta_round_trips(self, store):
        store.set_meta("k", "v")
        assert store.get_meta("k") == "v"
        store.set_meta("k", "v2")
        assert store.get_meta("k") == "v2"

    def test_unknown_meta_is_none(self, store):
        assert store.get_meta("nope") is None


class TestRemoteInventory:
    def test_records_and_reads_back(self, store):
        store.record_remote(RemoteItem("a.txt", size=100, modified_token="t1"))
        row = store.get_remote("a.txt")
        assert row["size"] == 100 and row["modified_token"] == "t1"

    def test_bulk_insert(self, store):
        n = store.record_remote_many(
            [RemoteItem(f"f{i}.txt", size=i) for i in range(500)]
        )
        assert n == 500
        assert store.remote_count() == 500

    def test_empty_bulk_insert_is_a_noop(self, store):
        assert store.record_remote_many([]) == 0

    def test_reinserting_a_path_updates_rather_than_duplicates(self, store):
        store.record_remote(RemoteItem("a.txt", size=1))
        store.record_remote(RemoteItem("a.txt", size=2))
        assert store.remote_count() == 1
        assert store.get_remote("a.txt")["size"] == 2

    def test_directories_are_excluded_from_counts_and_totals(self, store):
        """A directory has no bytes to download; counting it inflates the plan."""
        store.record_remote_many([
            RemoteItem("dir", kind=KIND_DIR),
            RemoteItem("dir/a.txt", size=10),
        ])
        assert store.remote_count() == 1
        assert store.remote_total_bytes() == 10

    def test_total_bytes_tolerates_missing_sizes(self, store):
        store.record_remote_many([
            RemoteItem("a.txt", size=10), RemoteItem("b.txt", size=None)
        ])
        assert store.remote_total_bytes() == 10

    def test_share_metadata_is_preserved(self, store):
        """Needed to download cross-account shared files later."""
        store.record_remote(RemoteItem("s.txt", share_id="SHARE-1", docwsid="D1"))
        row = store.get_remote("s.txt")
        assert row["share_id"] == "SHARE-1" and row["docwsid"] == "D1"

    def test_clear_remote_empties_the_inventory_only(self, store):
        store.record_remote(RemoteItem("a.txt", size=1))
        store.record_local(LocalItem("a.txt", size=1))
        store.clear_remote()
        assert store.remote_count() == 0
        assert store.local_count() == 1

    def test_iteration_is_ordered_by_path(self, store):
        store.record_remote_many([RemoteItem(p) for p in ("c", "a", "b")])
        assert [r["path"] for r in store.iter_remote()] == ["a", "b", "c"]


class TestScans:
    def test_scan_records_totals(self, store):
        scan = store.begin_scan("Documents")
        store.record_remote_many(
            [RemoteItem("a", size=10), RemoteItem("b", size=32)], scan_id=scan
        )
        result = store.finish_scan(scan)
        assert result["item_count"] == 2
        assert result["total_bytes"] == 42

    def test_latest_scan_returns_the_most_recent_finished_one(self, store):
        first = store.begin_scan("A")
        store.finish_scan(first)
        second = store.begin_scan("B")
        store.finish_scan(second)
        assert store.latest_scan()["icloud_path"] == "B"

    def test_an_unfinished_scan_is_not_reported_as_latest(self, store):
        """A scan that died half-way must not be presented as a complete picture."""
        done = store.begin_scan("done")
        store.finish_scan(done)
        store.begin_scan("interrupted")
        assert store.latest_scan()["icloud_path"] == "done"

    def test_no_scans_yet(self, store):
        assert store.latest_scan() is None


class TestDiff:
    def test_remote_only_is_new(self, store):
        store.record_remote(RemoteItem("a.txt", size=10))
        assert statuses(store.diff()) == {"a.txt": DIFF_NEW}

    def test_differing_size_is_changed(self, store):
        store.record_remote(RemoteItem("a.txt", size=20))
        store.record_local(LocalItem("a.txt", size=10))
        assert statuses(store.diff())["a.txt"] == DIFF_CHANGED

    def test_matching_size_is_omitted_by_default(self, store):
        """A plan should list work, not everything that needs no work."""
        store.record_remote(RemoteItem("a.txt", size=10))
        store.record_local(LocalItem("a.txt", size=10))
        assert store.diff() == []

    def test_matching_size_is_reported_when_asked(self, store):
        store.record_remote(RemoteItem("a.txt", size=10))
        store.record_local(LocalItem("a.txt", size=10))
        assert statuses(store.diff(include_unchanged=True))["a.txt"] == DIFF_UNCHANGED

    def test_local_only_is_reported(self, store):
        """Deleted in iCloud, or never from iCloud - either way, say so."""
        store.record_local(LocalItem("orphan.txt", size=5))
        assert statuses(store.diff())["orphan.txt"] == DIFF_LOCAL_ONLY

    def test_missing_remote_size_is_unknown_not_guessed(self, store):
        store.record_remote(RemoteItem("a.txt", size=None))
        store.record_local(LocalItem("a.txt", size=10))
        entry = store.diff()[0]
        assert entry.status == DIFF_UNKNOWN
        assert "cannot judge" in entry.detail

    def test_packages_are_always_reported_as_unjudgeable(self, store):
        """Their sizes never match, so neither "changed" nor silence is right."""
        store.record_remote(RemoteItem("Deck.key", kind=KIND_PACKAGE, size=900))
        store.record_local(LocalItem("Deck.key", kind=KIND_PACKAGE, size=1200))

        for entry in (store.diff()[0], store.diff(include_unchanged=True)[0]):
            assert entry.status == DIFF_UNKNOWN
            assert "manifest digest" in entry.detail

    def test_directories_are_not_diffed(self, store):
        store.record_remote(RemoteItem("dir", kind=KIND_DIR))
        assert store.diff() == []

    def test_a_mixed_tree_classifies_every_case(self, store):
        store.record_remote_many([
            RemoteItem("new.txt", size=1),
            RemoteItem("changed.txt", size=2),
            RemoteItem("same.txt", size=3),
        ])
        store.record_local_many([
            LocalItem("changed.txt", size=99),
            LocalItem("same.txt", size=3),
            LocalItem("gone.txt", size=4),
        ])
        assert statuses(store.diff()) == {
            "new.txt": DIFF_NEW,
            "changed.txt": DIFF_CHANGED,
            "gone.txt": DIFF_LOCAL_ONLY,
        }

    def test_diff_entries_are_json_serialisable(self, store):
        store.record_remote(RemoteItem("a.txt", size=1))
        json.dumps([e.to_dict() for e in store.diff()])


class TestDigestQueries:
    def test_find_by_digest(self, store):
        store.record_local_many([
            LocalItem("a.txt", size=5, sha256="dead"),
            LocalItem("copy.txt", size=5, sha256="dead"),
            LocalItem("other.txt", size=5, sha256="beef"),
        ])
        assert {r["path"] for r in store.find_by_digest("dead")} == {"a.txt", "copy.txt"}

    def test_find_by_digest_can_exclude_the_query_path(self, store):
        """The move-detection shape: what *else* has these bytes?"""
        store.record_local_many([
            LocalItem("old/a.txt", size=5, sha256="dead"),
            LocalItem("new/a.txt", size=5, sha256="dead"),
        ])
        found = store.find_by_digest("dead", exclude_path="new/a.txt")
        assert [r["path"] for r in found] == ["old/a.txt"]

    def test_empty_digest_finds_nothing(self, store):
        store.record_local(LocalItem("a.txt", sha256=None))
        assert store.find_by_digest("") == []

    def test_duplicates_are_grouped_with_recoverable_space(self, store):
        store.record_local_many([
            LocalItem("a.txt", size=100, sha256="x"),
            LocalItem("b.txt", size=100, sha256="x"),
            LocalItem("c.txt", size=100, sha256="x"),
            LocalItem("unique.txt", size=50, sha256="y"),
        ])
        dupes = store.duplicate_digests()
        assert len(dupes) == 1
        assert dupes[0]["count"] == 3
        assert sorted(dupes[0]["paths"]) == ["a.txt", "b.txt", "c.txt"]
        # Two of three copies are recoverable.
        assert dupes[0]["wasted_bytes"] == 200

    def test_files_without_a_digest_are_not_treated_as_duplicates(self, store):
        store.record_local_many([
            LocalItem("a.txt", size=1, sha256=None),
            LocalItem("b.txt", size=1, sha256=None),
        ])
        assert store.duplicate_digests() == []


class TestTransfers:
    def test_state_survives_reopening_the_database(self, tmp_path):
        """The property that makes crash-safe resume possible at all."""
        root = tmp_path / "dest"
        with IndexStore(root) as s:
            s.set_transfer("big.iso", TRANSFER_ACTIVE, bytes_done=5_000, total_bytes=9_000)

        with IndexStore(root) as s:
            row = s.get_transfer("big.iso")
            assert row["state"] == TRANSFER_ACTIVE
            assert row["bytes_done"] == 5_000

    def test_incomplete_transfers_include_active_and_failed_not_done(self, store):
        store.set_transfer("a", TRANSFER_DONE)
        store.set_transfer("b", TRANSFER_ACTIVE)
        store.set_transfer("c", TRANSFER_FAILED)
        assert {r["path"] for r in store.incomplete_transfers()} == {"b", "c"}

    def test_attempts_increment_only_when_asked(self, store):
        store.set_transfer("a", TRANSFER_FAILED, bump_attempts=True)
        store.set_transfer("a", TRANSFER_FAILED, bump_attempts=True)
        store.set_transfer("a", TRANSFER_ACTIVE)
        assert store.get_transfer("a")["attempts"] == 2

    def test_total_bytes_is_not_erased_by_a_later_update(self, store):
        store.set_transfer("a", TRANSFER_ACTIVE, bytes_done=1, total_bytes=100)
        store.set_transfer("a", TRANSFER_ACTIVE, bytes_done=2)
        assert store.get_transfer("a")["total_bytes"] == 100

    def test_last_error_is_recorded(self, store):
        store.set_transfer("a", TRANSFER_FAILED, error="connection reset")
        assert store.get_transfer("a")["last_error"] == "connection reset"

    def test_clearing_done_transfers_leaves_the_rest(self, store):
        store.set_transfer("a", TRANSFER_DONE)
        store.set_transfer("b", TRANSFER_FAILED)
        store.clear_transfers(only_done=True)
        assert {r["path"] for r in store.incomplete_transfers()} == {"b"}
        assert store.get_transfer("a") is None


class TestSnapshots:
    def test_snapshot_freezes_the_current_index(self, store):
        store.record_local(LocalItem("a.txt", size=1, sha256="v1"))
        store.create_snapshot("march")

        store.record_local(LocalItem("a.txt", size=2, sha256="v2"))

        entries = store.diff_snapshots("march", "march")
        assert entries == []  # a snapshot is not a live view of local_items

    def test_added_file_between_snapshots(self, store):
        store.record_local(LocalItem("a.txt", sha256="x"))
        store.create_snapshot("before")
        store.record_local(LocalItem("b.txt", sha256="y"))
        store.create_snapshot("after")

        assert statuses(store.diff_snapshots("before", "after")) == {"b.txt": DIFF_NEW}

    def test_removed_file_between_snapshots(self, store):
        store.record_local_many([LocalItem("a.txt", sha256="x"), LocalItem("b.txt", sha256="y")])
        store.create_snapshot("before")
        store.forget_local("b.txt")
        store.create_snapshot("after")

        assert statuses(store.diff_snapshots("before", "after")) == {"b.txt": DIFF_LOCAL_ONLY}

    def test_changed_file_is_detected_by_digest_not_size(self, store):
        """Same size, different bytes - the case size comparison cannot see."""
        store.record_local(LocalItem("a.txt", size=100, sha256="old"))
        store.create_snapshot("before")
        store.record_local(LocalItem("a.txt", size=100, sha256="new"))
        store.create_snapshot("after")

        assert statuses(store.diff_snapshots("before", "after")) == {"a.txt": DIFF_CHANGED}

    def test_identical_snapshots_report_nothing(self, store):
        store.record_local(LocalItem("a.txt", sha256="x"))
        store.create_snapshot("one")
        store.create_snapshot("two")
        assert store.diff_snapshots("one", "two") == []

    def test_listing_includes_entry_counts(self, store):
        store.record_local_many([LocalItem("a"), LocalItem("b")])
        store.create_snapshot("s1")
        listed = store.list_snapshots()
        assert listed[0]["label"] == "s1" and listed[0]["entry_count"] == 2

    def test_duplicate_labels_are_rejected(self, store):
        store.create_snapshot("dup")
        with pytest.raises(sqlite3.IntegrityError):
            store.create_snapshot("dup")

    def test_deleting_a_snapshot_removes_its_entries(self, store):
        store.record_local(LocalItem("a.txt", sha256="x"))
        store.create_snapshot("gone")
        assert store.delete_snapshot("gone") is True
        assert store.get_snapshot("gone") is None
        remaining = store._conn.execute(
            "SELECT COUNT(*) AS n FROM snapshot_entries"
        ).fetchone()["n"]
        assert remaining == 0

    def test_deleting_a_missing_snapshot_is_false_not_an_error(self, store):
        assert store.delete_snapshot("never-existed") is False

    def test_diffing_an_unknown_snapshot_raises_with_its_name(self, store):
        store.create_snapshot("real")
        with pytest.raises(KeyError) as excinfo:
            store.diff_snapshots("real", "imaginary")
        assert "imaginary" in str(excinfo.value)


class TestMigration:
    def write_state(self, root, files):
        (root / ".ifetch_state.json").write_text(
            json.dumps({"version": 1, "files": files}), encoding="utf-8"
        )

    def write_manifest(self, root, files):
        (root / ".ifetch_manifest.json").write_text(
            json.dumps({"manifest": {"version": 1, "files": files}}), encoding="utf-8"
        )

    def test_state_file_is_imported(self, tmp_path):
        root = tmp_path / "dest"
        root.mkdir()
        self.write_state(root, {
            "a.txt": {
                "remote_size": 10, "remote_modified": "t1",
                "local_size": 10, "local_mtime": 1.0, "status": "completed",
            }
        })

        store = open_index(root)
        assert store.get_local("a.txt")["size"] == 10
        assert store.get_remote("a.txt")["modified_token"] == "t1"
        store.close()

    def test_manifest_digests_are_imported(self, tmp_path):
        root = tmp_path / "dest"
        root.mkdir()
        self.write_manifest(root, {
            "a.txt": {"kind": "file", "sha256": "abc123", "size": 10}
        })

        store = open_index(root)
        assert store.get_local("a.txt")["sha256"] == "abc123"
        store.close()

    def test_manifest_digest_wins_over_state_which_has_none(self, tmp_path):
        root = tmp_path / "dest"
        root.mkdir()
        self.write_state(root, {
            "a.txt": {"remote_size": 10, "local_size": 10, "status": "completed"}
        })
        self.write_manifest(root, {"a.txt": {"kind": "file", "sha256": "d", "size": 10}})

        store = open_index(root)
        row = store.get_local("a.txt")
        assert row["sha256"] == "d" and row["size"] == 10
        store.close()

    def test_expanded_packages_keep_their_kind(self, tmp_path):
        root = tmp_path / "dest"
        root.mkdir()
        self.write_state(root, {
            "Deck.key": {
                "remote_size": 900, "remote_modified": "t",
                "local_file_count": 3, "local_total_bytes": 1200,
                "status": "completed_package",
            }
        })

        store = open_index(root)
        assert store.get_local("Deck.key")["kind"] == KIND_PACKAGE
        assert store.get_remote("Deck.key")["kind"] == KIND_PACKAGE
        store.close()

    def test_migration_is_non_destructive(self, tmp_path):
        """Downgrading to an older iFetch must keep working."""
        root = tmp_path / "dest"
        root.mkdir()
        self.write_state(root, {"a.txt": {"remote_size": 1, "local_size": 1}})
        before = (root / ".ifetch_state.json").read_text()

        open_index(root).close()

        assert (root / ".ifetch_state.json").read_text() == before

    def test_migration_runs_only_once(self, tmp_path):
        """A second import would resurrect entries the user has since deleted."""
        root = tmp_path / "dest"
        root.mkdir()
        self.write_state(root, {"a.txt": {"remote_size": 1, "local_size": 1}})

        store = open_index(root)
        store.forget_local("a.txt")
        store.close()

        store = open_index(root)
        assert store.get_local("a.txt") is None
        store.close()

    def test_missing_json_files_are_fine(self, tmp_path):
        store = open_index(tmp_path / "fresh")
        assert store.local_count() == 0
        store.close()

    def test_corrupt_json_does_not_break_startup(self, tmp_path):
        root = tmp_path / "dest"
        root.mkdir()
        (root / ".ifetch_state.json").write_text("{not json", encoding="utf-8")
        (root / ".ifetch_manifest.json").write_text("[]", encoding="utf-8")

        store = open_index(root)
        assert store.local_count() == 0
        store.close()

    def test_migration_can_be_disabled(self, tmp_path):
        root = tmp_path / "dest"
        root.mkdir()
        self.write_state(root, {"a.txt": {"remote_size": 1, "local_size": 1}})

        store = open_index(root, migrate=False)
        assert store.local_count() == 0
        store.close()


class TestConcurrency:
    def test_parallel_writers_do_not_lose_rows(self, store):
        """The downloader is threaded; a lost row means a lost file."""
        errors = []

        def worker(n):
            try:
                store.record_local_many(
                    [LocalItem(f"w{n}/f{i}.txt", size=i) for i in range(50)]
                )
            except Exception as exc:  # pragma: no cover
                errors.append(exc)

        threads = [threading.Thread(target=worker, args=(n,)) for n in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert errors == []
        assert store.local_count() == 400

    def test_reads_during_writes_do_not_raise(self, store):
        store.record_local_many([LocalItem(f"f{i}", size=i) for i in range(200)])
        errors = []
        stop = threading.Event()

        def reader():
            try:
                while not stop.is_set():
                    store.local_count()
                    list(store.iter_local())
            except Exception as exc:  # pragma: no cover
                errors.append(exc)

        thread = threading.Thread(target=reader)
        thread.start()
        try:
            for i in range(200, 400):
                store.record_local(LocalItem(f"f{i}", size=i))
        finally:
            stop.set()
            thread.join()

        assert errors == []
