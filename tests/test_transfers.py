"""Tests for the transfer journal, resume and repair.

The contract:

* **the journal is optional** - a downloader with no index behaves exactly as
  it did before, because bookkeeping must never be the reason a download fails;
* **an attempt is recorded before the first byte**, since a process that is
  killed never reaches a failure handler and that is the attempt worth counting;
* **failures survive the process** - the in-memory summary report does not, so
  a killed run used to take every record of what failed with it;
* **a skip settles the journal too**, or a file marked failed by an earlier run
  and since verified fine would be re-fetched forever;
* **repair queues work, it does not invent bytes** - iFetch cannot reconstruct
  a missing tail locally, so repairing means making the next run fetch it;
* **a wrong prefix is never built on** - a partial whose digest disagrees is
  discarded rather than resumed from.
"""

import json
import sys
import types
from pathlib import Path

import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))

from ifetch.downloader import DownloadManager  # noqa: E402
from ifetch.index import (  # noqa: E402
    TRANSFER_ACTIVE,
    TRANSFER_DONE,
    TRANSFER_FAILED,
    TRANSFER_PENDING,
    IndexStore,
)
from ifetch.manifest import Manifest, sha256_file  # noqa: E402
from ifetch.transfers import (  # noqa: E402
    FINDING_CORRUPT,
    FINDING_FAILED,
    FINDING_INTERRUPTED,
    FINDING_ORPHAN,
    FINDING_PENDING,
    REPAIR_QUEUED,
    REPAIR_WOULD,
    TransferJournal,
    apply_repair,
    build_repair_report,
    find_orphan_artifacts,
    render_repair,
    render_resume_plan,
)


@pytest.fixture
def mirror(tmp_path):
    root = tmp_path / "dest"
    root.mkdir()
    return root


@pytest.fixture
def store(mirror):
    with IndexStore(mirror) as s:
        yield s


@pytest.fixture
def journal(store, mirror):
    return TransferJournal(store, mirror)


def add_file(root, rel, content=b"content"):
    path = root / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    return path


class DummyItem:
    """An iCloud file object with the minimum download_drive_item needs."""

    def __init__(self, name="test.txt", size=10, content=b"0123456789"):
        self.name = name
        self.size = size
        self.type = "file"
        self._content = content
        self.url = "https://dummy.download/url"

    def open(self, stream=True):
        outer = self

        class _Ctx:
            headers = {"content-length": str(outer.size)}
            url = outer.url

            def __enter__(self):
                return self

            def __exit__(self, *exc):
                return False

        return _Ctx()


def make_downloader(store, root, monkeypatch, chunk=b"0123456789"):
    dm = DownloadManager(email="user@example.com", max_retries=1)
    dm.root_path = root
    dm.journal = TransferJournal(store, root)
    monkeypatch.setattr(dm, "download_chunk", lambda url, s, e, item=None: chunk)
    monkeypatch.setattr(dm, "calculate_checksum", lambda p: "dummy")
    return dm


# ---------------------------------------------------------------------------
# The journal
# ---------------------------------------------------------------------------

class TestJournal:
    def test_paths_are_recorded_relative_to_the_mirror(self, journal, mirror):
        """Matching every other table, so a moved mirror is still readable."""
        journal.begin(mirror / "a/b.txt", total_bytes=10)
        assert journal.store.get_transfer("a/b.txt") is not None

    def test_beginning_marks_the_transfer_active(self, journal, mirror):
        journal.begin(mirror / "a.txt", total_bytes=100, remote_path="Docs/a.txt")

        row = journal.get(mirror / "a.txt")
        assert row["state"] == TRANSFER_ACTIVE
        assert row["total_bytes"] == 100
        assert row["remote_path"] == "Docs/a.txt"

    def test_the_attempt_is_counted_at_the_start_not_the_end(self, journal, mirror):
        """A killed process never reaches a failure handler."""
        journal.begin(mirror / "a.txt")
        assert journal.get(mirror / "a.txt")["attempts"] == 1

        journal.begin(mirror / "a.txt")
        assert journal.get(mirror / "a.txt")["attempts"] == 2

    def test_progress_is_recorded_without_losing_the_remote_path(self, journal, mirror):
        """It is the one thing a resume needs; a progress write must not erase it."""
        journal.begin(mirror / "a.txt", total_bytes=100, remote_path="Docs/a.txt")
        journal.progress(mirror / "a.txt", 40)

        row = journal.get(mirror / "a.txt")
        assert row["bytes_done"] == 40
        assert row["remote_path"] == "Docs/a.txt"

    def test_progress_does_not_count_as_a_new_attempt(self, journal, mirror):
        journal.begin(mirror / "a.txt")
        for position in (10, 20, 30):
            journal.progress(mirror / "a.txt", position)
        assert journal.get(mirror / "a.txt")["attempts"] == 1

    def test_completing_takes_it_off_the_work_list(self, journal, mirror):
        journal.begin(mirror / "a.txt", total_bytes=10)
        journal.complete(mirror / "a.txt", 10)

        assert journal.get(mirror / "a.txt")["state"] == TRANSFER_DONE
        assert journal.incomplete() == []

    def test_a_failure_records_why(self, journal, mirror):
        journal.begin(mirror / "a.txt")
        journal.fail(mirror / "a.txt", "connection reset by peer", bytes_done=64)

        row = journal.get(mirror / "a.txt")
        assert row["state"] == TRANSFER_FAILED
        assert "connection reset" in row["last_error"]
        assert row["bytes_done"] == 64

    def test_a_very_long_error_is_truncated_not_stored_whole(self, journal, mirror):
        """A traceback-sized error would bloat every row that hit it."""
        journal.fail(mirror / "a.txt", "x" * 10_000)
        assert len(journal.get(mirror / "a.txt")["last_error"]) <= 2000

    def test_requeueing_resets_progress_to_zero(self, journal, mirror):
        journal.begin(mirror / "a.txt", total_bytes=100)
        journal.progress(mirror / "a.txt", 50)
        journal.requeue(mirror / "a.txt")

        row = journal.get(mirror / "a.txt")
        assert row["state"] == TRANSFER_PENDING
        assert row["bytes_done"] == 0

    def test_pruning_keeps_the_unfinished_work(self, journal, mirror):
        journal.complete(mirror / "done.txt", 10)
        journal.fail(mirror / "broken.txt", "boom")

        journal.prune_completed()

        assert {r["path"] for r in journal.incomplete()} == {"broken.txt"}

    def test_a_disabled_journal_is_a_silent_no_op(self, mirror):
        """A mirror with no index must download exactly as it always did."""
        disabled = TransferJournal(None)

        disabled.begin(mirror / "a.txt", total_bytes=10)
        disabled.progress(mirror / "a.txt", 5)
        disabled.complete(mirror / "a.txt")
        disabled.fail(mirror / "a.txt", "boom")

        assert disabled.enabled is False
        assert disabled.incomplete() == []
        assert disabled.get(mirror / "a.txt") is None

    def test_a_broken_store_never_raises_into_the_download_path(self, mirror):
        class Exploding:
            def set_transfer(self, *a, **k):
                raise RuntimeError("disk full")

            def incomplete_transfers(self):
                raise RuntimeError("disk full")

        broken = TransferJournal(Exploding(), mirror)
        broken.begin(mirror / "a.txt")          # must not raise
        broken.fail(mirror / "a.txt", "boom")   # must not raise
        assert broken.incomplete() == []


# ---------------------------------------------------------------------------
# The downloader writes to it
# ---------------------------------------------------------------------------

class TestDownloaderWiring:
    def test_a_completed_download_leaves_nothing_owed(self, store, mirror, monkeypatch):
        dm = make_downloader(store, mirror, monkeypatch)
        monkeypatch.setattr(
            dm.chunker, "compute_download_ranges",
            lambda resp, local_path=None, force=False: [(0, 9)],
        )

        assert dm.download_drive_item(DummyItem(), mirror / "test.txt") is True
        assert store.get_transfer("test.txt")["state"] == TRANSFER_DONE
        assert store.incomplete_transfers() == []

    def test_a_failed_download_is_recorded_durably(self, store, mirror, monkeypatch):
        """download_results is in-memory; a killed run would lose it."""
        dm = make_downloader(store, mirror, monkeypatch)
        monkeypatch.setattr(
            dm.chunker, "compute_download_ranges",
            lambda resp, local_path=None, force=False: [(0, 9)],
        )
        monkeypatch.setattr(
            dm, "download_chunk",
            lambda *a, **k: (_ for _ in ()).throw(Exception("connection reset")),
        )

        assert dm.download_drive_item(DummyItem(), mirror / "test.txt") is False

        row = store.get_transfer("test.txt")
        assert row["state"] == TRANSFER_FAILED
        assert "connection reset" in row["last_error"]

    def test_the_remote_path_is_recorded_so_a_resume_can_reopen_it(
        self, store, mirror, monkeypatch
    ):
        dm = make_downloader(store, mirror, monkeypatch)
        monkeypatch.setattr(
            dm.chunker, "compute_download_ranges",
            lambda resp, local_path=None, force=False: [(0, 9)],
        )
        monkeypatch.setattr(
            dm, "download_chunk",
            lambda *a, **k: (_ for _ in ()).throw(Exception("boom")),
        )

        dm.download_drive_item(
            DummyItem(), mirror / "test.txt", remote_path="Documents/test.txt"
        )
        assert store.get_transfer("test.txt")["remote_path"] == "Documents/test.txt"

    def test_progress_is_recorded_as_chunks_land(self, store, mirror, monkeypatch):
        dm = make_downloader(store, mirror, monkeypatch, chunk=b"01234")
        monkeypatch.setattr(
            dm.chunker, "compute_download_ranges",
            lambda resp, local_path=None, force=False: [(0, 4), (5, 9)],
        )
        seen = []
        real = dm.journal.progress
        monkeypatch.setattr(
            dm.journal, "progress",
            lambda p, n: (seen.append(n), real(p, n))[1],
        )
        # Non-contiguous so the ranges are not merged into one request.
        monkeypatch.setattr(dm, "_merge_ranges", staticmethod(lambda r: r))

        dm.download_drive_item(DummyItem(), mirror / "test.txt")
        assert seen == [5, 10]

    def test_a_skip_settles_a_row_left_behind_by_an_earlier_run(
        self, store, mirror, monkeypatch
    ):
        """Otherwise every resume re-fetches a file that turned out to be fine."""
        add_file(mirror, "test.txt", b"0123456789")
        store.set_transfer("test.txt", TRANSFER_FAILED, error="an earlier run failed")

        dm = make_downloader(store, mirror, monkeypatch)
        monkeypatch.setattr(
            dm.chunker, "compute_download_ranges",
            lambda resp, local_path=None, force=False: [],
        )

        assert dm.download_drive_item(DummyItem(), mirror / "test.txt") is True
        assert store.get_transfer("test.txt")["state"] == TRANSFER_DONE

    def test_a_download_with_no_journal_behaves_exactly_as_before(
        self, mirror, monkeypatch
    ):
        dm = DownloadManager(email="user@example.com", max_retries=1)
        monkeypatch.setattr(
            dm.chunker, "compute_download_ranges",
            lambda resp, local_path=None, force=False: [(0, 9)],
        )
        monkeypatch.setattr(dm, "download_chunk", lambda *a, **k: b"0123456789")
        monkeypatch.setattr(dm, "calculate_checksum", lambda p: "dummy")

        assert dm.journal.enabled is False
        assert dm.download_drive_item(DummyItem(), mirror / "test.txt") is True
        assert (mirror / "test.txt").stat().st_size == 10

    def test_an_unopenable_index_downgrades_instead_of_failing(
        self, mirror, monkeypatch
    ):
        """Resumability is worth having, not worth refusing to download over."""
        dm = DownloadManager(email="user@example.com")
        monkeypatch.setattr(
            "ifetch.index.open_index",
            lambda *a, **k: (_ for _ in ()).throw(OSError("read-only filesystem")),
        )
        warnings = []
        monkeypatch.setattr(
            dm.logger, "warning", lambda p: warnings.append(json.loads(p))
        )

        dm._open_journal(mirror)

        assert dm.journal.enabled is False
        assert any(w["event"] == "transfer_journal_unavailable" for w in warnings)


# ---------------------------------------------------------------------------
# Orphaned artifacts
# ---------------------------------------------------------------------------

class TestOrphanArtifacts:
    def test_partial_files_are_grouped_under_the_file_they_belong_to(self, mirror):
        add_file(mirror, "big.iso.temp", b"partial")
        add_file(mirror, "big.iso.download", b'{"position": 7}')

        found = find_orphan_artifacts(mirror)
        assert set(found) == {"big.iso"}
        assert found["big.iso"] == ["big.iso.download", "big.iso.temp"]

    def test_artifacts_the_journal_knows_about_are_not_orphans(self, mirror):
        add_file(mirror, "big.iso.temp", b"partial")
        assert find_orphan_artifacts(mirror, known={"big.iso"}) == {}

    def test_ordinary_files_are_not_mistaken_for_artifacts(self, mirror):
        add_file(mirror, "notes.txt")
        add_file(mirror, "archive.zip")
        assert find_orphan_artifacts(mirror) == {}

    def test_the_version_archive_is_not_searched(self, mirror):
        """It holds deliberately retained copies, not abandoned transfers."""
        add_file(mirror, ".versions/old.bin.temp", b"archived")
        assert find_orphan_artifacts(mirror) == {}

    def test_nested_artifacts_are_found(self, mirror):
        add_file(mirror, "a/b/c/deep.bin.temp", b"partial")
        assert "a/b/c/deep.bin" in find_orphan_artifacts(mirror)


# ---------------------------------------------------------------------------
# The repair report
# ---------------------------------------------------------------------------

class TestRepairReport:
    def test_a_clean_mirror_has_nothing_to_repair(self, store, mirror):
        report = build_repair_report(store, mirror)
        assert report.findings == []

    def test_an_active_row_means_the_process_was_killed(self, store, mirror):
        store.set_transfer("big.iso", TRANSFER_ACTIVE, bytes_done=500, total_bytes=1000)

        finding = build_repair_report(store, mirror).findings[0]
        assert finding.kind == FINDING_INTERRUPTED
        assert "killed mid-download" in finding.detail
        assert finding.bytes_done == 500

    def test_a_failed_row_carries_its_reason_forward(self, store, mirror):
        store.set_transfer("a.bin", TRANSFER_FAILED, error="connection reset")

        finding = build_repair_report(store, mirror).findings[0]
        assert finding.kind == FINDING_FAILED
        assert finding.detail == "connection reset"

    def test_a_failure_with_no_recorded_reason_still_says_something(
        self, store, mirror
    ):
        store.set_transfer("a.bin", TRANSFER_FAILED)
        finding = build_repair_report(store, mirror).findings[0]
        assert "without recording a reason" in finding.detail

    def test_a_pending_row_is_reported_separately_from_a_failure(self, store, mirror):
        store.set_transfer("a.bin", TRANSFER_PENDING)
        assert build_repair_report(store, mirror).findings[0].kind == FINDING_PENDING

    def test_completed_transfers_are_not_findings(self, store, mirror):
        store.set_transfer("done.bin", TRANSFER_DONE)
        assert build_repair_report(store, mirror).findings == []

    def test_existing_partials_are_attached_to_their_transfer(self, store, mirror):
        add_file(mirror, "big.iso.temp", b"partial")
        store.set_transfer("big.iso", TRANSFER_ACTIVE, bytes_done=7, total_bytes=100)

        assert build_repair_report(store, mirror).findings[0].artifacts == [
            "big.iso.temp"
        ]

    def test_stray_partials_with_no_journal_row_are_reported_too(self, store, mirror):
        """From a run whose index was deleted, or an iFetch older than the journal."""
        add_file(mirror, "forgotten.bin.temp", b"partial")

        finding = build_repair_report(store, mirror).findings[0]
        assert finding.kind == FINDING_ORPHAN
        assert "no run recorded them" in finding.detail

    def test_a_missing_index_is_named_rather_than_reported_as_clean(self, mirror):
        report = build_repair_report(None, mirror)
        assert any("No index exists" in note for note in report.notes)

    def test_partial_bytes_already_on_disk_are_totalled(self, store, mirror):
        store.set_transfer("a.bin", TRANSFER_ACTIVE, bytes_done=300, total_bytes=1000)
        store.set_transfer("b.bin", TRANSFER_ACTIVE, bytes_done=200, total_bytes=1000)

        assert build_repair_report(store, mirror).bytes_already_fetched == 500

    def test_attempts_are_carried_into_the_report(self, store, mirror):
        for _ in range(4):
            store.set_transfer("flaky.bin", TRANSFER_FAILED, bump_attempts=True)

        assert build_repair_report(store, mirror).findings[0].attempts == 4

    def test_the_report_is_json_serialisable(self, store, mirror):
        store.set_transfer("a.bin", TRANSFER_FAILED, error="boom")
        json.dumps(build_repair_report(store, mirror).to_dict())


class TestDigestChecking:
    def build(self, mirror, content=b"the original bytes"):
        path = add_file(mirror, "doc.txt", content)
        manifest = Manifest(mirror)
        manifest.record_file(path)
        return manifest, path

    def test_an_intact_file_is_not_a_finding(self, store, mirror):
        manifest, _ = self.build(mirror)
        report = build_repair_report(store, mirror, manifest, check_digests=True)
        assert report.findings == []
        assert report.checked_digests == 1

    def test_damaged_bytes_are_caught_even_at_the_same_size(self, store, mirror):
        """Size comparison cannot see this; re-hashing is the only way."""
        manifest, path = self.build(mirror, b"aaaa")
        path.write_bytes(b"bbbb")

        finding = build_repair_report(
            store, mirror, manifest, check_digests=True
        ).findings[0]
        assert finding.kind == FINDING_CORRUPT
        assert "do not match the digest" in finding.detail

    def test_a_corrupt_file_is_never_treated_as_resumable(self, store, mirror):
        """Continuing from a wrong prefix would preserve the damage."""
        manifest, path = self.build(mirror, b"aaaa")
        path.write_bytes(b"bbbb")

        finding = build_repair_report(
            store, mirror, manifest, check_digests=True
        ).findings[0]
        assert finding.resumable is False

    def test_an_absent_file_is_left_to_the_recovery_report(self, store, mirror):
        manifest, path = self.build(mirror)
        path.unlink()

        report = build_repair_report(store, mirror, manifest, check_digests=True)
        assert report.findings == []

    def test_asking_for_digests_with_no_manifest_says_so(self, store, mirror):
        report = build_repair_report(store, mirror, None, check_digests=True)
        assert any("no manifest was found" in note for note in report.notes)

    def test_digests_are_not_checked_unless_asked(self, store, mirror):
        """It reads the whole mirror, so it must never be the silent default."""
        manifest, path = self.build(mirror, b"aaaa")
        path.write_bytes(b"bbbb")

        report = build_repair_report(store, mirror, manifest, check_digests=False)
        assert report.findings == []
        assert report.checked_digests == 0

    def test_corruption_is_not_masked_by_a_stray_partial_for_the_same_file(
        self, store, mirror
    ):
        """"There are leftovers" must never hide "the bytes are wrong"."""
        manifest, path = self.build(mirror, b"aaaa")
        path.write_bytes(b"bbbb")
        add_file(mirror, "doc.txt.temp", b"partial")

        findings = build_repair_report(
            store, mirror, manifest, check_digests=True
        ).findings

        assert [f.kind for f in findings] == [FINDING_CORRUPT]
        # The partial is carried onto the corruption so it is discarded with it.
        assert findings[0].artifacts == ["doc.txt.temp"]

    def test_a_file_already_reported_unfinished_is_not_reported_twice(
        self, store, mirror
    ):
        manifest, path = self.build(mirror, b"aaaa")
        path.write_bytes(b"bbbb")
        store.set_transfer("doc.txt", TRANSFER_FAILED, error="boom")

        report = build_repair_report(store, mirror, manifest, check_digests=True)
        assert len(report.findings) == 1
        assert report.findings[0].kind == FINDING_FAILED


# ---------------------------------------------------------------------------
# Applying a repair
# ---------------------------------------------------------------------------

class TestApplyRepair:
    def test_a_dry_run_changes_nothing(self, store, mirror):
        add_file(mirror, "big.iso.temp", b"partial")
        store.set_transfer("big.iso", TRANSFER_ACTIVE, bytes_done=7)
        report = build_repair_report(store, mirror)

        outcomes = apply_repair(store, mirror, report, dry_run=True)

        assert outcomes[0].status == REPAIR_WOULD
        assert (mirror / "big.iso.temp").exists()
        assert store.get_transfer("big.iso")["state"] == TRANSFER_ACTIVE

    def test_applying_queues_the_file_for_a_fresh_fetch(self, store, mirror):
        store.set_transfer("a.bin", TRANSFER_FAILED, error="boom")
        report = build_repair_report(store, mirror)

        outcomes = apply_repair(store, mirror, report, dry_run=False)

        assert outcomes[0].status == REPAIR_QUEUED
        assert store.get_transfer("a.bin")["state"] == TRANSFER_PENDING

    def test_partials_are_kept_by_default(self, store, mirror):
        """They are exactly what lets a resume avoid re-fetching gigabytes."""
        add_file(mirror, "big.iso.temp", b"partial")
        store.set_transfer("big.iso", TRANSFER_ACTIVE, bytes_done=7)
        report = build_repair_report(store, mirror)

        apply_repair(store, mirror, report, dry_run=False)

        assert (mirror / "big.iso.temp").exists()

    def test_partials_can_be_discarded_on_request(self, store, mirror):
        add_file(mirror, "big.iso.temp", b"partial")
        add_file(mirror, "big.iso.download", b"{}")
        store.set_transfer("big.iso", TRANSFER_ACTIVE, bytes_done=7)
        report = build_repair_report(store, mirror)

        apply_repair(store, mirror, report, dry_run=False, discard_partials=True)

        assert not (mirror / "big.iso.temp").exists()
        assert not (mirror / "big.iso.download").exists()

    def test_a_corrupt_partial_is_discarded_even_without_the_flag(self, store, mirror):
        """A proven-wrong prefix must never be built on."""
        path = add_file(mirror, "doc.txt", b"aaaa")
        manifest = Manifest(mirror)
        manifest.record_file(path)
        path.write_bytes(b"bbbb")
        add_file(mirror, "doc.txt.temp", b"partial")

        report = build_repair_report(store, mirror, manifest, check_digests=True)
        apply_repair(store, mirror, report, dry_run=False, discard_partials=False)

        assert not (mirror / "doc.txt.temp").exists()

    def test_a_stray_partial_left_in_place_is_not_called_queued(self, store, mirror):
        """Nothing was recorded and nothing cleaned, so there is no work owed."""
        add_file(mirror, "forgotten.bin.temp", b"partial")
        report = build_repair_report(store, mirror)

        outcomes = apply_repair(store, mirror, report, dry_run=False)

        assert outcomes[0].status != REPAIR_QUEUED
        assert "--discard-partials" in outcomes[0].detail

    def test_the_recorded_remote_path_survives_requeueing(self, store, mirror):
        store.set_transfer(
            "a.bin", TRANSFER_FAILED, error="boom", remote_path="Docs/a.bin"
        )
        report = build_repair_report(store, mirror)

        apply_repair(store, mirror, report, dry_run=False)

        assert store.get_transfer("a.bin")["remote_path"] == "Docs/a.bin"

    def test_outcomes_are_json_serialisable(self, store, mirror):
        store.set_transfer("a.bin", TRANSFER_FAILED, error="boom")
        report = build_repair_report(store, mirror)
        json.dumps([o.to_dict() for o in apply_repair(store, mirror, report)])


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------

class TestRendering:
    def test_a_clean_mirror_reads_as_an_answer(self, store, mirror):
        text = render_repair(build_repair_report(store, mirror), use_colour=False)
        assert "Nothing to repair" in text

    def test_the_report_names_the_files_and_the_next_command(self, store, mirror):
        store.set_transfer("big.iso", TRANSFER_ACTIVE, bytes_done=500, total_bytes=1000)

        text = render_repair(build_repair_report(store, mirror), use_colour=False)
        assert "big.iso" in text
        assert "ifetch repair --apply" in text and "ifetch resume" in text

    def test_repeatedly_failing_files_are_called_out(self, store, mirror):
        """Retrying a file that has failed five times is not a plan."""
        for _ in range(5):
            store.set_transfer(
                "flaky.bin", TRANSFER_FAILED, error="timeout", bump_attempts=True
            )

        text = render_repair(build_repair_report(store, mirror), use_colour=False)
        assert "Failing repeatedly" in text and "flaky.bin" in text

    def test_a_digest_mismatch_is_not_silently_overwritten(self, store, mirror):
        path = add_file(mirror, "doc.txt", b"aaaa")
        manifest = Manifest(mirror)
        manifest.record_file(path)
        path.write_bytes(b"bbbb")

        text = render_repair(
            build_repair_report(store, mirror, manifest, check_digests=True),
            use_colour=False,
        )
        assert "will not overwrite them without being asked" in text

    def test_a_stray_partial_only_report_suggests_the_right_flag(self, store, mirror):
        add_file(mirror, "forgotten.bin.temp", b"partial")

        text = render_repair(build_repair_report(store, mirror), use_colour=False)
        assert "--discard-partials" in text

    def test_an_empty_resume_plan_explains_where_else_to_look(self):
        text = render_resume_plan([])
        assert "Nothing to resume" in text and "ifetch repair" in text

    def test_transfers_with_no_remote_path_are_flagged_in_the_plan(self):
        """An older iFetch journalled them without one; say so, do not drop them."""
        text = render_resume_plan(
            [{"path": "a.bin", "bytes_done": 5, "total_bytes": 10,
              "attempts": 1, "remote_path": None}],
            use_colour=False,
        )
        assert "no recorded remote path" in text
        assert "a.bin" in text


# ---------------------------------------------------------------------------
# The CLI
# ---------------------------------------------------------------------------

class TestCli:
    def run(self, argv, capsys):
        from ifetch import repair_cli

        code = repair_cli.main(argv)
        return code, capsys.readouterr().out

    def test_a_clean_mirror_exits_zero(self, mirror, capsys):
        code, out = self.run([str(mirror)], capsys)
        assert code == 0
        assert "Nothing to repair" in out

    def test_findings_exit_non_zero_so_a_cron_job_notices(self, store, mirror, capsys):
        store.set_transfer("a.bin", TRANSFER_FAILED, error="boom")
        code, out = self.run([str(mirror)], capsys)
        assert code == 1
        assert "a.bin" in out

    def test_without_apply_the_report_says_nothing_changed(
        self, store, mirror, capsys
    ):
        store.set_transfer("a.bin", TRANSFER_FAILED, error="boom")
        _, out = self.run([str(mirror)], capsys)
        assert "Nothing has been changed" in out

    def test_apply_queues_the_work(self, store, mirror, capsys):
        store.set_transfer("a.bin", TRANSFER_FAILED, error="boom")

        code, out = self.run([str(mirror), "--apply"], capsys)

        assert code == 0
        assert "ifetch resume" in out
        assert store.get_transfer("a.bin")["state"] == TRANSFER_PENDING

    def test_json_output_is_machine_readable(self, store, mirror, capsys):
        store.set_transfer("a.bin", TRANSFER_FAILED, error="boom")
        _, out = self.run([str(mirror), "--json"], capsys)
        assert json.loads(out)["counts"] == {FINDING_FAILED: 1}

    def test_resume_with_nothing_owed_says_so(self, mirror, capsys):
        from ifetch import repair_cli

        code = repair_cli.resume_main([str(mirror)])
        assert code == 0
        assert "Nothing to resume" in capsys.readouterr().out

    def test_resume_dry_run_contacts_nobody(self, store, mirror, capsys, monkeypatch):
        store.set_transfer(
            "a.bin", TRANSFER_ACTIVE, bytes_done=5, total_bytes=10,
            remote_path="Docs/a.bin",
        )
        monkeypatch.setattr(
            "ifetch.downloader.DownloadManager.authenticate",
            lambda *a, **k: pytest.fail("resume --dry-run must not authenticate"),
        )
        from ifetch import repair_cli

        code = repair_cli.resume_main([str(mirror), "--dry-run"])
        assert code == 1
        assert "a.bin" in capsys.readouterr().out

    def test_resume_refuses_when_no_remote_path_was_recorded(
        self, store, mirror, capsys
    ):
        store.set_transfer("a.bin", TRANSFER_ACTIVE, bytes_done=5, total_bytes=10)
        from ifetch import repair_cli

        code = repair_cli.resume_main([str(mirror)])

        assert code == 2
        assert "cannot reopen them individually" in capsys.readouterr().err

    def test_resume_fetches_only_the_unfinished_transfer(
        self, store, mirror, capsys, monkeypatch
    ):
        """The whole point: no re-walk of the drive to rediscover one file."""
        store.set_transfer(
            "test.txt", TRANSFER_ACTIVE, bytes_done=0, total_bytes=10,
            remote_path="Documents/test.txt",
        )
        opened = []

        monkeypatch.setattr(
            "ifetch.downloader.DownloadManager.authenticate", lambda *a, **k: None
        )
        monkeypatch.setattr(
            "ifetch.downloader.DownloadManager.get_drive_item",
            lambda self, path: opened.append(path) or DummyItem(),
        )
        monkeypatch.setattr(
            "ifetch.downloader.DownloadManager.download_chunk",
            lambda self, url, s, e, item=None: b"0123456789",
        )
        monkeypatch.setattr(
            "ifetch.downloader.DownloadManager.calculate_checksum",
            lambda self, p: "dummy",
        )
        from ifetch import repair_cli

        code = repair_cli.resume_main([str(mirror), "--email", "user@example.com"])

        assert code == 0
        assert opened == ["Documents/test.txt"]
        assert (mirror / "test.txt").read_bytes() == b"0123456789"
        assert store.get_transfer("test.txt")["state"] == TRANSFER_DONE
