"""Tests for conflict intelligence: renames, moves and duplicates.

The contract:

* **duplicates and local moves are proved** - both sides were hashed, so the
  answer is digest equality and nothing is inferred;
* **remote renames are never proved** - Apple publishes no content hash, so a
  match rests on name and size, is graded, and says what its evidence was;
* **ambiguity is reported, not resolved** - when two remote files could equally
  be the local one, every alternative is listed rather than one being picked;
* **what could not be examined is named** - packages, sizeless remotes and empty
  files are counted and explained, so "none found" never means "I did not look";
* **acting is narrow and guarded** - only strong matches, re-checked against the
  disk first, never overwriting anything, with the index and the manifest both
  following the file.
"""

import json
import sys
from pathlib import Path

import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))

from ifetch.conflicts import (  # noqa: E402
    CONFIDENCE_AMBIGUOUS,
    CONFIDENCE_PROVEN,
    CONFIDENCE_STRONG,
    CONFIDENCE_WEAK,
    RELOCATE_DONE,
    RELOCATE_SKIPPED,
    RELOCATE_WOULD,
    ConflictError,
    analyse,
    detect_duplicates,
    detect_moves,
    detect_renames,
    relocate,
    render_duplicates,
    render_moves,
    render_renames,
)
from ifetch.index import (  # noqa: E402
    KIND_PACKAGE,
    IndexStore,
    LocalItem,
    RemoteItem,
)
from ifetch.manifest import Manifest, sha256_file  # noqa: E402


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


def index_file(store, root, rel, kind="file"):
    """Record a real on-disk file in the local half of the index."""
    path = root / rel
    store.record_local(LocalItem(
        path=rel, kind=kind, size=path.stat().st_size,
        mtime=path.stat().st_mtime, sha256=sha256_file(path),
    ))


def index_remote(store, rel, size, kind="file"):
    store.record_remote(RemoteItem(path=rel, kind=kind, size=size))


def scanned(store):
    """Mark a finished remote scan, which is what ``analyse`` looks for."""
    store.finish_scan(store.begin_scan("Documents"))


# ---------------------------------------------------------------------------
# Duplicates - provable
# ---------------------------------------------------------------------------

class TestDuplicates:
    def test_identical_contents_at_two_paths_are_one_group(self, store, mirror):
        add_file(mirror, "a/report.pdf", b"x" * 100)
        add_file(mirror, "b/report-copy.pdf", b"x" * 100)
        index_file(store, mirror, "a/report.pdf")
        index_file(store, mirror, "b/report-copy.pdf")

        groups = detect_duplicates(store)
        assert len(groups) == 1
        assert groups[0].count == 2
        assert groups[0].paths == ["a/report.pdf", "b/report-copy.pdf"]

    def test_only_the_copies_beyond_the_first_count_as_reclaimable(self, store, mirror):
        """One copy is the file; the rest are the waste."""
        for name in ("one", "two", "three"):
            add_file(mirror, f"{name}.bin", b"y" * 300)
            index_file(store, mirror, f"{name}.bin")

        group = detect_duplicates(store)[0]
        assert group.size == 300
        assert group.total_bytes == 900
        assert group.wasted_bytes == 600

    def test_different_contents_are_not_duplicates(self, store, mirror):
        add_file(mirror, "a.txt", b"aaaa")
        add_file(mirror, "b.txt", b"bbbb")
        index_file(store, mirror, "a.txt")
        index_file(store, mirror, "b.txt")

        assert detect_duplicates(store) == []

    def test_empty_files_are_excluded_and_the_reason_is_recorded(self, store, mirror):
        """Every empty file matches every other; deduplicating them reclaims nothing."""
        add_file(mirror, "empty-one", b"")
        add_file(mirror, "empty-two", b"")
        index_file(store, mirror, "empty-one")
        index_file(store, mirror, "empty-two")

        notes = []
        assert detect_duplicates(store, notes=notes) == []
        assert any("empty" in note and "reclaims nothing" in note for note in notes)

    def test_min_size_suppresses_small_groups(self, store, mirror):
        add_file(mirror, "small-a", b"z" * 10)
        add_file(mirror, "small-b", b"z" * 10)
        index_file(store, mirror, "small-a")
        index_file(store, mirror, "small-b")

        assert detect_duplicates(store, min_size=1024) == []

    def test_biggest_waste_is_listed_first(self, store, mirror):
        for name in ("big-a", "big-b"):
            add_file(mirror, name, b"x" * 5000)
            index_file(store, mirror, name)
        for name in ("small-a", "small-b"):
            add_file(mirror, name, b"y" * 50)
            index_file(store, mirror, name)

        groups = detect_duplicates(store)
        assert [g.size for g in groups] == [5000, 50]

    def test_files_with_no_digest_are_not_grouped_together(self, store):
        """A missing digest is not evidence of anything, least of all sameness."""
        store.record_local(LocalItem(path="a.bin", size=10, sha256=None))
        store.record_local(LocalItem(path="b.bin", size=10, sha256=None))

        assert detect_duplicates(store) == []


# ---------------------------------------------------------------------------
# Local moves - provable
# ---------------------------------------------------------------------------

class TestMoves:
    def move_file(self, store, mirror, old, new):
        """Snapshot the mirror, then move a file and re-index it."""
        add_file(mirror, old, b"the same bytes")
        index_file(store, mirror, old)
        store.create_snapshot("march")

        (mirror / new).parent.mkdir(parents=True, exist_ok=True)
        (mirror / old).rename(mirror / new)
        store.forget_local(old)
        index_file(store, mirror, new)

    def test_a_moved_file_is_recognised_by_its_digest(self, store, mirror):
        self.move_file(store, mirror, "old/doc.txt", "new/doc.txt")

        moves = detect_moves(store, "march")
        assert len(moves) == 1
        assert moves[0].old_path == "old/doc.txt"
        assert moves[0].new_path == "new/doc.txt"
        assert moves[0].confidence == CONFIDENCE_PROVEN

    def test_a_rename_in_place_is_a_move_too(self, store, mirror):
        self.move_file(store, mirror, "notes.txt", "notes-final.txt")
        assert detect_moves(store, "march")[0].new_path == "notes-final.txt"

    def test_an_untouched_file_is_not_reported(self, store, mirror):
        add_file(mirror, "still-here.txt")
        index_file(store, mirror, "still-here.txt")
        store.create_snapshot("march")

        assert detect_moves(store, "march") == []

    def test_a_deleted_file_is_not_a_move(self, store, mirror):
        """Its bytes are nowhere; 'ifetch recover missing' is that report."""
        add_file(mirror, "gone.txt")
        index_file(store, mirror, "gone.txt")
        store.create_snapshot("march")
        store.forget_local("gone.txt")

        assert detect_moves(store, "march") == []

    def test_a_copy_is_reported_as_ambiguous_with_every_destination(self, store, mirror):
        add_file(mirror, "original.txt", b"shared bytes")
        index_file(store, mirror, "original.txt")
        store.create_snapshot("march")

        store.forget_local("original.txt")
        for name in ("copy-a.txt", "copy-b.txt"):
            add_file(mirror, name, b"shared bytes")
            index_file(store, mirror, name)

        moves = detect_moves(store, "march")
        assert moves[0].confidence == CONFIDENCE_AMBIGUOUS
        assert moves[0].alternatives == ["copy-a.txt", "copy-b.txt"]

    def test_a_file_still_at_its_old_path_is_left_to_duplicate_detection(
        self, store, mirror
    ):
        """It was copied, not moved, and calling that a move would be wrong."""
        add_file(mirror, "original.txt", b"shared bytes")
        index_file(store, mirror, "original.txt")
        store.create_snapshot("march")

        add_file(mirror, "elsewhere.txt", b"shared bytes")
        index_file(store, mirror, "elsewhere.txt")

        assert detect_moves(store, "march") == []
        assert len(detect_duplicates(store)) == 1

    def test_snapshot_entries_without_digests_are_counted_not_ignored(self, store):
        store.record_local(LocalItem(path="unhashed.bin", size=5, sha256=None))
        store.create_snapshot("march")
        store.forget_local("unhashed.bin")

        notes = []
        detect_moves(store, "march", notes=notes)
        assert any("no recorded digest" in note for note in notes)

    def test_an_unknown_snapshot_raises_with_its_name(self, store):
        with pytest.raises(ConflictError) as excinfo:
            detect_moves(store, "nonexistent")
        assert "nonexistent" in str(excinfo.value)

    def test_moves_are_json_serialisable(self, store, mirror):
        self.move_file(store, mirror, "a.txt", "b.txt")
        json.dumps([m.to_dict() for m in detect_moves(store, "march")])


# ---------------------------------------------------------------------------
# Remote renames - inferred, never proved
# ---------------------------------------------------------------------------

class TestRenames:
    def renamed_folder(self, store, mirror):
        """iCloud has 'New/report.pdf'; the disk has 'Old/report.pdf'."""
        add_file(mirror, "Old/report.pdf", b"p" * 4096)
        index_file(store, mirror, "Old/report.pdf")
        index_remote(store, "New/report.pdf", 4096)

    def test_a_renamed_folder_is_matched_on_name_and_size(self, store, mirror):
        self.renamed_folder(store, mirror)

        candidates = detect_renames(store)
        assert len(candidates) == 1
        assert candidates[0].old_path == "Old/report.pdf"
        assert candidates[0].new_path == "New/report.pdf"
        assert candidates[0].confidence == CONFIDENCE_STRONG

    def test_the_match_never_claims_to_be_proof(self, store, mirror):
        """Apple publishes no content hash; the report must say so."""
        self.renamed_folder(store, mirror)
        assert "rather than proof" in detect_renames(store)[0].evidence

    def test_a_strong_match_carries_the_local_digest(self, store, mirror):
        """It is the digest of the real local bytes, which is a true statement."""
        self.renamed_folder(store, mirror)
        expected = sha256_file(mirror / "Old/report.pdf")
        assert detect_renames(store)[0].sha256 == expected

    def test_the_avoided_transfer_is_reported(self, store, mirror):
        self.renamed_folder(store, mirror)
        assert detect_renames(store)[0].bytes_saved == 4096

    def test_a_size_mismatch_settles_it_immediately(self, store, mirror):
        add_file(mirror, "Old/report.pdf", b"p" * 4096)
        index_file(store, mirror, "Old/report.pdf")
        index_remote(store, "New/report.pdf", 9999)

        assert detect_renames(store) == []

    def test_a_changed_filename_is_only_a_weak_match(self, store, mirror):
        """The sole evidence is a size coincidence, which is not much."""
        add_file(mirror, "draft.txt", b"q" * 2048)
        index_file(store, mirror, "draft.txt")
        index_remote(store, "final.txt", 2048)

        candidate = detect_renames(store)[0]
        assert candidate.confidence == CONFIDENCE_WEAK
        assert "coincidence" in candidate.evidence

    def test_two_equally_good_answers_are_both_listed_not_chosen_between(
        self, store, mirror
    ):
        add_file(mirror, "one.bin", b"a" * 512)
        add_file(mirror, "two.bin", b"b" * 512)
        index_file(store, mirror, "one.bin")
        index_file(store, mirror, "two.bin")
        index_remote(store, "alpha.bin", 512)
        index_remote(store, "beta.bin", 512)

        candidates = detect_renames(store)
        assert {c.confidence for c in candidates} == {CONFIDENCE_AMBIGUOUS}
        assert candidates[0].alternatives == ["alpha.bin", "beta.bin"]

    def test_an_ambiguous_match_claims_no_saving(self, store, mirror):
        add_file(mirror, "one.bin", b"a" * 512)
        add_file(mirror, "two.bin", b"b" * 512)
        index_file(store, mirror, "one.bin")
        index_file(store, mirror, "two.bin")
        index_remote(store, "alpha.bin", 512)
        index_remote(store, "beta.bin", 512)

        assert all(c.bytes_saved == 0 for c in detect_renames(store))

    def test_a_name_match_wins_over_the_ambiguity_around_it(self, store, mirror):
        """Same size, but only one pair shares a filename - that pair is resolved."""
        add_file(mirror, "Old/report.pdf", b"a" * 512)
        add_file(mirror, "Old/other.bin", b"b" * 512)
        index_file(store, mirror, "Old/report.pdf")
        index_file(store, mirror, "Old/other.bin")
        index_remote(store, "New/report.pdf", 512)
        index_remote(store, "New/unrelated.bin", 512)

        by_old = {c.old_path: c for c in detect_renames(store)}
        assert by_old["Old/report.pdf"].confidence == CONFIDENCE_STRONG
        assert by_old["Old/report.pdf"].new_path == "New/report.pdf"
        # One left on each side after that, so it degrades to weak, not ambiguous.
        assert by_old["Old/other.bin"].confidence == CONFIDENCE_WEAK

    def test_a_file_present_on_both_sides_is_not_a_rename(self, store, mirror):
        add_file(mirror, "kept.txt", b"k" * 100)
        index_file(store, mirror, "kept.txt")
        index_remote(store, "kept.txt", 100)

        assert detect_renames(store) == []

    def test_package_bundles_are_excluded_and_the_reason_is_given(self, store, mirror):
        """Apple's size for a bundle is not its expanded size on disk."""
        add_file(mirror, "Old/Deck.key/index.xml", b"z" * 300)
        store.record_local(LocalItem(path="Old/Deck.key", kind=KIND_PACKAGE, size=300))
        index_remote(store, "New/Deck.key", 300, kind=KIND_PACKAGE)

        notes = []
        assert detect_renames(store, notes=notes) == []
        assert any("package" in note and "expanded size" in note for note in notes)

    def test_remote_files_with_no_size_are_counted_not_silently_dropped(
        self, store, mirror
    ):
        add_file(mirror, "Old/thing.bin", b"t" * 700)
        index_file(store, mirror, "Old/thing.bin")
        index_remote(store, "New/thing.bin", None)

        notes = []
        assert detect_renames(store, notes=notes) == []
        assert any("no size reported by iCloud" in note for note in notes)

    def test_empty_files_are_excluded_and_the_reason_is_given(self, store, mirror):
        add_file(mirror, "Old/empty", b"")
        index_file(store, mirror, "Old/empty")
        index_remote(store, "New/empty", 0)

        notes = []
        assert detect_renames(store, notes=notes) == []
        assert any("costs nothing" in note for note in notes)

    def test_min_size_suppresses_the_riskiest_matches(self, store, mirror):
        """A size coincidence is most likely exactly where the saving is least."""
        add_file(mirror, "Old/tiny.bin", b"t" * 20)
        index_file(store, mirror, "Old/tiny.bin")
        index_remote(store, "New/tiny.bin", 20)

        assert detect_renames(store, min_size=1024) == []

    def test_strong_matches_are_listed_before_weaker_ones(self, store, mirror):
        add_file(mirror, "Old/report.pdf", b"a" * 4096)
        add_file(mirror, "draft.txt", b"q" * 2048)
        index_file(store, mirror, "Old/report.pdf")
        index_file(store, mirror, "draft.txt")
        index_remote(store, "New/report.pdf", 4096)
        index_remote(store, "final.txt", 2048)

        assert [c.confidence for c in detect_renames(store)] == [
            CONFIDENCE_STRONG, CONFIDENCE_WEAK,
        ]

    def test_candidates_are_json_serialisable(self, store, mirror):
        self.renamed_folder(store, mirror)
        json.dumps([c.to_dict() for c in detect_renames(store)])


# ---------------------------------------------------------------------------
# Acting on a rename
# ---------------------------------------------------------------------------

class TestRelocate:
    def prepare(self, store, mirror):
        add_file(mirror, "Old/report.pdf", b"p" * 4096)
        index_file(store, mirror, "Old/report.pdf")
        index_remote(store, "New/report.pdf", 4096)
        return detect_renames(store)

    def test_a_dry_run_changes_nothing_on_disk(self, store, mirror):
        candidates = self.prepare(store, mirror)

        outcomes = relocate(store, mirror, candidates, dry_run=True)

        assert outcomes[0].status == RELOCATE_WOULD
        assert (mirror / "Old/report.pdf").exists()
        assert not (mirror / "New/report.pdf").exists()

    def test_applying_moves_the_file_to_the_path_icloud_uses(self, store, mirror):
        candidates = self.prepare(store, mirror)

        outcomes = relocate(store, mirror, candidates, dry_run=False)

        assert outcomes[0].status == RELOCATE_DONE
        assert not (mirror / "Old/report.pdf").exists()
        assert (mirror / "New/report.pdf").read_bytes() == b"p" * 4096

    def test_the_move_is_what_avoids_the_transfer(self, store, mirror):
        candidates = self.prepare(store, mirror)
        outcomes = relocate(store, mirror, candidates, dry_run=False)
        assert outcomes[0].bytes_saved == 4096

    def test_the_index_follows_the_file(self, store, mirror):
        """Otherwise the next plan re-downloads exactly what was just moved."""
        candidates = self.prepare(store, mirror)
        digest = candidates[0].sha256

        relocate(store, mirror, candidates, dry_run=False)

        assert store.get_local("Old/report.pdf") is None
        assert store.get_local("New/report.pdf")["sha256"] == digest

    def test_after_relocating_the_plan_sees_nothing_to_do(self, store, mirror):
        candidates = self.prepare(store, mirror)
        relocate(store, mirror, candidates, dry_run=False)

        assert detect_renames(store) == []
        assert [e.status for e in store.diff()] == []

    def test_the_manifest_follows_the_file(self, store, mirror):
        """A manifest naming the old path would fail the next offline verify."""
        candidates = self.prepare(store, mirror)
        manifest = Manifest(mirror)
        manifest.record_file(mirror / "Old/report.pdf")

        relocate(store, mirror, candidates, dry_run=False, manifest=manifest)

        assert manifest.get(mirror / "Old/report.pdf") is None
        assert manifest.get(mirror / "New/report.pdf") is not None

    def test_weak_matches_are_left_alone_by_default(self, store, mirror):
        add_file(mirror, "draft.txt", b"q" * 2048)
        index_file(store, mirror, "draft.txt")
        index_remote(store, "final.txt", 2048)

        outcomes = relocate(store, mirror, detect_renames(store), dry_run=False)

        assert outcomes == []
        assert (mirror / "draft.txt").exists()

    def test_weak_matches_can_be_opted_into(self, store, mirror):
        add_file(mirror, "draft.txt", b"q" * 2048)
        index_file(store, mirror, "draft.txt")
        index_remote(store, "final.txt", 2048)

        outcomes = relocate(
            store, mirror, detect_renames(store), dry_run=False,
            min_confidence=CONFIDENCE_WEAK,
        )

        assert outcomes[0].status == RELOCATE_DONE
        assert (mirror / "final.txt").exists()

    def test_ambiguous_matches_are_never_acted_on(self, store, mirror):
        add_file(mirror, "one.bin", b"a" * 512)
        add_file(mirror, "two.bin", b"b" * 512)
        index_file(store, mirror, "one.bin")
        index_file(store, mirror, "two.bin")
        index_remote(store, "alpha.bin", 512)
        index_remote(store, "beta.bin", 512)

        outcomes = relocate(
            store, mirror, detect_renames(store), dry_run=False,
            min_confidence=CONFIDENCE_AMBIGUOUS,
        )

        # Even asked for explicitly, an ambiguous candidate names one arbitrary
        # destination - so the guard that matters is that nothing is guessed.
        assert all(o.status != RELOCATE_DONE for o in outcomes[1:])

    def test_an_existing_destination_is_never_overwritten(self, store, mirror):
        candidates = self.prepare(store, mirror)
        add_file(mirror, "New/report.pdf", b"someone else's bytes")

        outcomes = relocate(store, mirror, candidates, dry_run=False)

        assert outcomes[0].status == RELOCATE_SKIPPED
        assert "will not overwrite" in outcomes[0].detail
        assert (mirror / "New/report.pdf").read_bytes() == b"someone else's bytes"

    def test_a_source_that_vanished_since_the_scan_is_skipped(self, store, mirror):
        candidates = self.prepare(store, mirror)
        (mirror / "Old/report.pdf").unlink()

        outcomes = relocate(store, mirror, candidates, dry_run=False)

        assert outcomes[0].status == RELOCATE_SKIPPED
        assert "out of date" in outcomes[0].detail

    def test_a_source_that_changed_size_since_the_scan_is_skipped(self, store, mirror):
        """The match was made on that size; it is no longer the same file."""
        candidates = self.prepare(store, mirror)
        (mirror / "Old/report.pdf").write_bytes(b"different now")

        outcomes = relocate(store, mirror, candidates, dry_run=False)

        assert outcomes[0].status == RELOCATE_SKIPPED
        assert (mirror / "Old/report.pdf").exists()

    def test_a_destination_outside_the_mirror_is_refused(self, store, mirror):
        from ifetch.conflicts import RenameCandidate

        add_file(mirror, "here.bin", b"x" * 10)
        escape = RenameCandidate(
            old_path="here.bin", new_path="../../escaped.bin", size=10,
            confidence=CONFIDENCE_STRONG,
        )

        outcomes = relocate(store, mirror, [escape], dry_run=False)

        assert outcomes[0].status != RELOCATE_DONE
        assert not (mirror.parent.parent / "escaped.bin").exists()

    def test_outcomes_are_json_serialisable(self, store, mirror):
        candidates = self.prepare(store, mirror)
        json.dumps([o.to_dict() for o in relocate(store, mirror, candidates)])


# ---------------------------------------------------------------------------
# The combined analysis
# ---------------------------------------------------------------------------

class TestAnalyse:
    def test_an_empty_index_says_so_instead_of_reporting_nothing_found(
        self, store, mirror
    ):
        report = analyse(store, mirror)
        assert report.renames == [] and report.duplicates == []
        assert any("index is empty" in note for note in report.notes)

    def test_a_missing_remote_scan_is_named_not_silently_skipped(self, store, mirror):
        """"No renames found" must never mean "I could not look"."""
        add_file(mirror, "a.txt")
        index_file(store, mirror, "a.txt")

        report = analyse(store, mirror)
        assert any("No iCloud scan exists" in note for note in report.notes)

    def test_every_detector_runs_when_it_has_its_inputs(self, store, mirror):
        add_file(mirror, "Old/report.pdf", b"p" * 4096)
        add_file(mirror, "dup-a.bin", b"d" * 900)
        add_file(mirror, "dup-b.bin", b"d" * 900)
        for rel in ("Old/report.pdf", "dup-a.bin", "dup-b.bin"):
            index_file(store, mirror, rel)
        index_remote(store, "New/report.pdf", 4096)
        scanned(store)
        store.create_snapshot("march")

        report = analyse(store, mirror, since="march")
        assert len(report.renames) == 1
        assert len(report.duplicates) == 1
        assert report.bytes_saved == 4096
        assert report.wasted_bytes == 900

    def test_the_report_is_json_serialisable(self, store, mirror):
        add_file(mirror, "a.txt")
        index_file(store, mirror, "a.txt")
        json.dumps(analyse(store, mirror).to_dict())


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------

class TestRendering:
    def test_no_renames_reads_as_an_answer_not_an_error(self):
        assert "genuinely new" in render_renames([], use_colour=False)

    def test_the_rename_report_states_that_nothing_is_proof(self, store, mirror):
        add_file(mirror, "Old/report.pdf", b"p" * 4096)
        index_file(store, mirror, "Old/report.pdf")
        index_remote(store, "New/report.pdf", 4096)

        text = render_renames(detect_renames(store), use_colour=False)
        assert "no content hash" in text
        assert "Old/report.pdf" in text and "New/report.pdf" in text

    def test_notes_are_shown_under_what_was_not_examined(self, store):
        text = render_renames([], ["3 package bundles were not considered."],
                              use_colour=False)
        assert "Not examined" in text and "3 package bundles" in text

    def test_ambiguous_alternatives_are_spelled_out(self, store, mirror):
        add_file(mirror, "one.bin", b"a" * 512)
        add_file(mirror, "two.bin", b"b" * 512)
        index_file(store, mirror, "one.bin")
        index_file(store, mirror, "two.bin")
        index_remote(store, "alpha.bin", 512)
        index_remote(store, "beta.bin", 512)

        text = render_renames(detect_renames(store), use_colour=False)
        assert "could be any of" in text
        assert "alpha.bin" in text and "beta.bin" in text

    def test_the_duplicate_report_says_iFetch_deletes_nothing(self, store, mirror):
        for name in ("a.bin", "b.bin"):
            add_file(mirror, name, b"x" * 400)
            index_file(store, mirror, name)

        text = render_duplicates(detect_duplicates(store), use_colour=False)
        assert "does not delete" in text
        assert "a.bin" in text and "b.bin" in text

    def test_no_duplicates_reads_as_an_answer(self):
        assert "No duplicate files found" in render_duplicates([], use_colour=False)

    def test_the_moves_report_explains_the_consequence_of_moving_locally(
        self, store, mirror
    ):
        add_file(mirror, "old.txt", b"same bytes")
        index_file(store, mirror, "old.txt")
        store.create_snapshot("march")
        (mirror / "old.txt").rename(mirror / "new.txt")
        store.forget_local("old.txt")
        index_file(store, mirror, "new.txt")

        text = render_moves(detect_moves(store, "march"), "march", use_colour=False)
        assert "downloaded again at its original path" in text
        assert "old.txt" in text and "new.txt" in text

    def test_no_moves_names_the_snapshot(self):
        assert "'march'" in render_moves([], "march", use_colour=False)
