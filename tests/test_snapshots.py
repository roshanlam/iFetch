"""Tests for dated snapshots and restoring from them.

The contract:

* a snapshot is metadata, so creating one is cheap and does not copy files -
  and therefore cannot, on its own, restore contents;
* a restore plan is honest about where bytes would come from, and names the
  files whose bytes exist nowhere instead of quietly omitting them;
* ``.versions`` is preferred over iCloud, because it holds the *exact* recorded
  bytes while iCloud holds whatever is there now;
* restoring is dry-run by default, and applying it archives what it replaces so
  the restore itself is reversible.
"""

import json
import sys
import time
from pathlib import Path

import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))

from ifetch.index import IndexStore, LocalItem, RemoteItem  # noqa: E402
from ifetch.manifest import sha256_file  # noqa: E402
from ifetch.snapshots import (  # noqa: E402
    RESTORE_MISSING,
    RESTORE_MODIFIED,
    SOURCE_ICLOUD,
    SOURCE_NONE,
    SOURCE_VERSIONS,
    SnapshotError,
    apply_restore,
    auto_label,
    create_snapshot,
    plan_restore,
    render_restore_plan,
    render_snapshot_diff,
    render_snapshot_list,
    validate_label,
)
from ifetch.versioning import VersionManager  # noqa: E402


@pytest.fixture
def mirror(tmp_path):
    root = tmp_path / "dest"
    root.mkdir()
    return root


@pytest.fixture
def store(mirror):
    with IndexStore(mirror) as s:
        yield s


def add_file(root, rel, content=b"content"):
    path = root / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    return path


def index_file(store, root, rel):
    path = root / rel
    store.record_local(LocalItem(
        path=rel, size=path.stat().st_size, sha256=sha256_file(path)
    ))


# ---------------------------------------------------------------------------
# Labels
# ---------------------------------------------------------------------------

class TestLabels:
    @pytest.mark.parametrize(
        "label", ["march", "2026-03-01", "before_upgrade", "v1.2.3", "a"]
    )
    def test_sensible_labels_are_accepted(self, label):
        assert validate_label(label) == label

    @pytest.mark.parametrize(
        "label",
        ["", "  ", "has space", "-leading", ".hidden", "with/slash",
         "semi;colon", "quote'", "x" * 65],
    )
    def test_labels_that_would_need_quoting_are_rejected(self, label):
        """They are typed on a command line and printed in reports."""
        with pytest.raises(SnapshotError):
            validate_label(label)

    def test_the_error_says_what_is_allowed(self):
        with pytest.raises(SnapshotError) as excinfo:
            validate_label("bad name")
        assert "letters, digits" in str(excinfo.value)

    def test_auto_labels_are_timestamped_and_valid(self):
        label = auto_label(now=1_800_000_000)
        assert label.startswith("auto-")
        assert validate_label(label) == label


# ---------------------------------------------------------------------------
# Creating
# ---------------------------------------------------------------------------

class TestCreate:
    def test_records_every_indexed_file(self, store, mirror):
        add_file(mirror, "a.txt")
        index_file(store, mirror, "a.txt")

        result = create_snapshot(store, "march")
        assert result["entries"] == 1
        assert store.get_snapshot("march") is not None

    def test_an_empty_index_is_refused_rather_than_recorded_as_empty(self, store):
        """"The mirror was empty" is a claim, and usually a false one."""
        with pytest.raises(SnapshotError) as excinfo:
            create_snapshot(store, "march")
        assert "empty" in str(excinfo.value)

    def test_duplicate_labels_are_refused_by_default(self, store, mirror):
        add_file(mirror, "a.txt")
        index_file(store, mirror, "a.txt")
        create_snapshot(store, "march")

        with pytest.raises(SnapshotError) as excinfo:
            create_snapshot(store, "march")
        assert "--replace" in str(excinfo.value)

    def test_replace_overwrites(self, store, mirror):
        add_file(mirror, "a.txt")
        index_file(store, mirror, "a.txt")
        create_snapshot(store, "march")

        add_file(mirror, "b.txt")
        index_file(store, mirror, "b.txt")
        result = create_snapshot(store, "march", replace=True)
        assert result["entries"] == 2

    def test_notes_are_stored(self, store, mirror):
        add_file(mirror, "a.txt")
        index_file(store, mirror, "a.txt")
        create_snapshot(store, "march", note="before the migration")
        assert store.get_snapshot("march")["note"] == "before the migration"

    def test_an_index_with_no_digests_at_all_is_refused(self, store, mirror):
        """Such a snapshot could not detect corruption or drive a restore."""
        add_file(mirror, "a.txt")
        store.record_local(LocalItem(path="a.txt", size=7, sha256=None))

        with pytest.raises(SnapshotError) as excinfo:
            create_snapshot(store, "march")
        assert "--fast" in str(excinfo.value)

    def test_partial_digest_coverage_is_allowed_but_counted(self, store, mirror):
        """A caller can warn about it; refusing outright would be too blunt."""
        add_file(mirror, "hashed.txt")
        index_file(store, mirror, "hashed.txt")
        store.record_local(LocalItem(path="unhashed.txt", size=1, sha256=None))

        result = create_snapshot(store, "march")
        assert result["entries"] == 2
        assert result["entries_without_digest"] == 1

    def test_a_fully_hashed_snapshot_reports_no_gaps(self, store, mirror):
        add_file(mirror, "a.txt")
        index_file(store, mirror, "a.txt")
        assert create_snapshot(store, "march")["entries_without_digest"] == 0

    def test_creating_does_not_copy_any_file_bytes(self, store, mirror):
        """A snapshot must stay cheap, or nobody takes one before a risky sync."""
        add_file(mirror, "big.bin", b"x" * 100_000)
        index_file(store, mirror, "big.bin")

        before = sum(p.stat().st_size for p in mirror.rglob("*") if p.is_file())
        create_snapshot(store, "march")
        after = sum(p.stat().st_size for p in mirror.rglob("*") if p.is_file())

        # Only the index grew, and by far less than the file itself.
        assert after - before < 100_000


# ---------------------------------------------------------------------------
# Restore planning
# ---------------------------------------------------------------------------

class TestRestorePlan:
    def test_an_unchanged_mirror_needs_no_restore(self, store, mirror):
        add_file(mirror, "a.txt")
        index_file(store, mirror, "a.txt")
        create_snapshot(store, "march")

        plan = plan_restore(store, mirror, "march")
        assert plan.candidates == []

    def test_a_deleted_file_is_reported_missing(self, store, mirror):
        path = add_file(mirror, "a.txt")
        index_file(store, mirror, "a.txt")
        create_snapshot(store, "march")
        path.unlink()

        plan = plan_restore(store, mirror, "march")
        assert plan.candidates[0].state == RESTORE_MISSING

    def test_a_changed_file_is_reported_modified(self, store, mirror):
        path = add_file(mirror, "a.txt", b"original")
        index_file(store, mirror, "a.txt")
        create_snapshot(store, "march")
        path.write_bytes(b"tampered")

        candidate = plan_restore(store, mirror, "march").candidates[0]
        assert candidate.state == RESTORE_MODIFIED
        assert candidate.current_sha256 != candidate.expected_sha256

    def test_same_size_different_bytes_is_still_caught(self, store, mirror):
        """Digest comparison, not size - the whole reason snapshots hash."""
        path = add_file(mirror, "a.txt", b"aaaa")
        index_file(store, mirror, "a.txt")
        create_snapshot(store, "march")
        path.write_bytes(b"bbbb")

        assert plan_restore(store, mirror, "march").candidates[0].state == RESTORE_MODIFIED

    def test_an_unknown_snapshot_raises_with_its_name(self, store, mirror):
        with pytest.raises(SnapshotError) as excinfo:
            plan_restore(store, mirror, "nonexistent")
        assert "nonexistent" in str(excinfo.value)

    def test_planning_changes_nothing_on_disk(self, store, mirror):
        path = add_file(mirror, "a.txt", b"original")
        index_file(store, mirror, "a.txt")
        create_snapshot(store, "march")
        path.write_bytes(b"tampered")

        plan_restore(store, mirror, "march")
        assert path.read_bytes() == b"tampered"

    def test_plan_is_json_serialisable(self, store, mirror):
        path = add_file(mirror, "a.txt")
        index_file(store, mirror, "a.txt")
        create_snapshot(store, "march")
        path.unlink()
        json.dumps(plan_restore(store, mirror, "march").to_dict())


class TestRestoreSources:
    def snapshot_then_damage(self, store, mirror, versions=True, remote=False):
        """Reproduce the real sequence: archive the old copy, then write a new one.

        ``record_version`` *moves* the file into ``.versions`` - it exists to be
        called immediately before an overwrite - so the size is captured first
        and the damaged copy written afterwards.
        """
        path = add_file(mirror, "a.txt", b"the original bytes")
        digest = sha256_file(path)
        original_size = path.stat().st_size
        index_file(store, mirror, "a.txt")
        create_snapshot(store, "march")

        manager = VersionManager(mirror)
        if versions:
            manager.record_version(Path("a.txt"), digest, path)
        if remote:
            store.record_remote(RemoteItem("a.txt", size=original_size))

        path.write_bytes(b"damaged")
        return manager, digest

    def test_versions_is_the_preferred_source(self, store, mirror):
        """It holds the exact recorded bytes; iCloud holds whatever is there now."""
        manager, _ = self.snapshot_then_damage(store, mirror, versions=True, remote=True)

        candidate = plan_restore(store, mirror, "march", manager).candidates[0]
        assert candidate.source == SOURCE_VERSIONS
        assert "exact bytes" in candidate.detail

    def test_icloud_is_used_when_no_archive_exists(self, store, mirror):
        manager, _ = self.snapshot_then_damage(store, mirror, versions=False, remote=True)

        candidate = plan_restore(store, mirror, "march", manager).candidates[0]
        assert candidate.source == SOURCE_ICLOUD
        assert "may differ from the snapshot" in candidate.detail

    def test_unrecoverable_files_are_named_not_omitted(self, store, mirror):
        """Silently dropping them would imply a complete restore is possible."""
        manager, _ = self.snapshot_then_damage(store, mirror, versions=False, remote=False)

        plan = plan_restore(store, mirror, "march", manager)
        assert plan.unrecoverable
        assert plan.unrecoverable[0].source == SOURCE_NONE
        assert plan.unrecoverable[0].recoverable is False

    def test_an_archive_whose_file_vanished_is_not_offered(self, store, mirror):
        path = add_file(mirror, "a.txt", b"original")
        digest = sha256_file(path)
        index_file(store, mirror, "a.txt")
        create_snapshot(store, "march")

        manager = VersionManager(mirror)
        manager.record_version(Path("a.txt"), digest, path)
        # The archived copy is deleted out from under us.
        for version in manager.versions_for(Path("a.txt")):
            archived = Path(version["archived_path"])
            if archived.exists() and archived != path:
                archived.unlink()
        path.write_bytes(b"damaged")

        candidate = plan_restore(store, mirror, "march", manager).candidates[0]
        assert candidate.source != SOURCE_VERSIONS


class TestApplyRestore:
    def prepare(self, store, mirror):
        path = add_file(mirror, "a.txt", b"the original bytes")
        digest = sha256_file(path)
        index_file(store, mirror, "a.txt")
        create_snapshot(store, "march")

        manager = VersionManager(mirror)
        manager.record_version(Path("a.txt"), digest, path)
        path.write_bytes(b"damaged")
        return manager, path

    def test_dry_run_changes_nothing(self, store, mirror):
        manager, path = self.prepare(store, mirror)
        plan = plan_restore(store, mirror, "march", manager)

        outcomes = apply_restore(plan, mirror, manager, dry_run=True)

        assert path.read_bytes() == b"damaged"
        assert outcomes[0].status == "would_restore"

    def test_applying_restores_the_original_bytes(self, store, mirror):
        manager, path = self.prepare(store, mirror)
        plan = plan_restore(store, mirror, "march", manager)

        outcomes = apply_restore(plan, mirror, manager, dry_run=False)

        assert outcomes[0].status == "restored"
        assert path.read_bytes() == b"the original bytes"

    def test_a_restore_archives_what_it_replaces(self, store, mirror):
        """The restore itself must be undoable."""
        manager, path = self.prepare(store, mirror)
        plan = plan_restore(store, mirror, "march", manager)
        before = len(manager.versions_for(Path("a.txt")))

        apply_restore(plan, mirror, manager, dry_run=False)

        assert len(manager.versions_for(Path("a.txt"))) > before

    def test_only_versions_backed_files_are_touched(self, store, mirror):
        """Fetching from iCloud is a network operation with its own flags."""
        add_file(mirror, "remote-only.txt", b"x")
        index_file(store, mirror, "remote-only.txt")
        manager, _ = self.prepare(store, mirror)
        store.record_remote(RemoteItem("remote-only.txt", size=1))
        (mirror / "remote-only.txt").unlink()

        plan = plan_restore(store, mirror, "march", manager)
        outcomes = apply_restore(plan, mirror, manager, dry_run=False)

        assert {o.path for o in outcomes} == {"a.txt"}
        assert not (mirror / "remote-only.txt").exists()

    def test_no_temporary_files_are_left_behind(self, store, mirror):
        manager, _ = self.prepare(store, mirror)
        plan = plan_restore(store, mirror, "march", manager)
        apply_restore(plan, mirror, manager, dry_run=False)

        assert list(mirror.rglob("*.ifetch-restore")) == []

    def test_restoring_a_deleted_file_recreates_it(self, store, mirror):
        manager, path = self.prepare(store, mirror)
        path.unlink()

        plan = plan_restore(store, mirror, "march", manager)
        apply_restore(plan, mirror, manager, dry_run=False)

        assert path.exists()
        assert path.read_bytes() == b"the original bytes"


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------

class TestRendering:
    def test_empty_snapshot_list_explains_how_to_make_one(self):
        assert "snapshot create" in render_snapshot_list([])

    def test_snapshot_list_shows_labels_and_counts(self):
        text = render_snapshot_list([
            {"label": "march", "created_at": time.time(), "entry_count": 42, "note": "x"}
        ])
        assert "march" in text and "42" in text

    def test_identical_snapshots_render_as_no_differences(self):
        assert "identical" in render_snapshot_diff([], "a", "b")

    def test_restore_plan_names_unrecoverable_files(self, store, mirror):
        path = add_file(mirror, "gone.txt", b"x")
        index_file(store, mirror, "gone.txt")
        create_snapshot(store, "march")
        path.unlink()

        text = render_restore_plan(plan_restore(store, mirror, "march"))
        assert "cannot be recovered" in text and "gone.txt" in text

    def test_matching_mirror_says_so(self, store, mirror):
        add_file(mirror, "a.txt")
        index_file(store, mirror, "a.txt")
        create_snapshot(store, "march")

        text = render_restore_plan(plan_restore(store, mirror, "march"))
        assert "already matches" in text
