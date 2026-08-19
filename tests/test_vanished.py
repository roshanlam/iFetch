"""Tests for detecting files that have disappeared from iCloud.

The contract, in the order it matters:

* **"none found" must never be able to mean "I could not look".** A scan that
  errored, died partway or came back empty is refused as evidence, at any
  volume, including zero - and the refusal names what it cannot rule out;
* a mass disappearance is not reported as a mass deletion, because a listing
  failure, an expired token or an absent mount looks exactly the same;
* the Trash purge date is an **upper bound**, never a date, because iFetch knows
  when it first noticed an absence and not when the delete happened - and that
  bound tightens across runs rather than resetting;
* the four classes are kept apart because they call for different actions, and
  the one that looks safest - an evicted placeholder - is the dangerous one;
* a rename that :mod:`ifetch.conflicts` can prove is a move is not a deletion,
  and one it cannot prove is ambiguous with every alternative named.
"""

import json
import sys
import unicodedata
from pathlib import Path

import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))

from ifetch.index import (  # noqa: E402
    KIND_PACKAGE,
    IndexStore,
    LocalItem,
    RemoteItem,
)
from ifetch.recovery import (  # noqa: E402
    CONFIDENCE_LIKELY,
    EVIDENCE_BRICK,
    EVIDENCE_DATALESS,
    Placeholder,
    PlaceholderDetector,
    PlaceholderReport,
    write_csv,
)
from ifetch.scanner import RemoteScanner  # noqa: E402
from ifetch.vanish_cli import (  # noqa: E402
    EXIT_ERROR,
    EXIT_FINDINGS,
    EXIT_OK,
    EXIT_REFUSED,
    build_parser,
    main,
)
from ifetch.vanished import (  # noqa: E402
    BREAKER_COUNT,
    BREAKER_FRACTION,
    BREAKER_SCAN,
    CLASS_AMBIGUOUS,
    CLASS_LOST,
    CLASS_PLACEHOLDER,
    CLASS_SAFE,
    TRASH_RETENTION_DAYS,
    BreakerVerdict,
    ScanEvidence,
    VanishedError,
    analyse,
    assess_scan,
    check_breaker,
    collect_baseline,
    csv_rows,
    render_observations,
    render_vanished,
)

DAY = 86400.0


# ---------------------------------------------------------------------------
# Fixtures and helpers
# ---------------------------------------------------------------------------

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
    """Put a real file on disk."""
    path = root / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    return path


def write_brick(root, relative, size=4096):
    """Write the ``.icloud`` eviction stub macOS leaves behind."""
    import plistlib

    target = root / relative
    target.parent.mkdir(parents=True, exist_ok=True)
    stub = target.parent / f".{target.name}.icloud"
    with stub.open("wb") as handle:
        plistlib.dump({"NSURLFileSizeKey": size}, handle)
    return stub


def remote_scan(store, items, errors=None, icloud_path="Documents"):
    """Record a complete remote scan, the way ``RemoteScanner`` would."""
    scan_id = store.begin_scan(icloud_path)
    store.clear_remote()
    store.record_remote_many(items, scan_id=scan_id)
    store.finish_scan(scan_id, errors=errors)
    return scan_id


def placeholders_for(mirror, **kwargs):
    return PlaceholderDetector(mirror, check_dataless=False, **kwargs).scan()


def fake_placeholder(path, size=900):
    """A placeholder report for a file that *exists* at full size on disk.

    This is the dataless case: only meaningful on APFS, impossible to fabricate
    portably, and the one that matters most - the file is there in Finder and
    holds nothing. The detector itself is covered in ``test_recovery.py``; what
    is under test here is what this module does with its verdict.
    """
    return PlaceholderReport(
        root="", files_checked=1,
        signals_available=[EVIDENCE_BRICK, EVIDENCE_DATALESS],
        placeholders=[Placeholder(
            path=path, evidence=EVIDENCE_DATALESS, confidence=CONFIDENCE_LIKELY,
            reported_size=size,
            detail="reports a full size but occupies no blocks on disk.",
        )],
    )


# ---------------------------------------------------------------------------
# Classification
# ---------------------------------------------------------------------------

class TestClassification:
    def test_gone_remotely_but_intact_locally_is_safe(self, store, mirror):
        add_file(mirror, "keep.txt", b"real bytes")
        store.record_local(LocalItem("keep.txt", size=10, sha256="deadbeef"))
        remote_scan(store, [])

        report = analyse(store, mirror, placeholders=placeholders_for(mirror))

        assert [i.classification for i in report.items] == [CLASS_SAFE]

    def test_the_safe_verdict_quotes_the_digest_as_proof(self, store, mirror):
        add_file(mirror, "keep.txt", b"real bytes")
        store.record_local(LocalItem("keep.txt", size=10, sha256="deadbeef"))
        remote_scan(store, [])

        item = analyse(store, mirror, placeholders=placeholders_for(mirror)).items[0]
        assert item.evidence == "sha256:deadbeef"

    def test_a_local_copy_without_a_digest_is_not_called_intact(self, store, mirror):
        """"A file is at that path" is a weaker claim than "the bytes survive"."""
        add_file(mirror, "keep.txt", b"real bytes")
        store.record_local(LocalItem("keep.txt", size=10))
        remote_scan(store, [])

        report = analyse(store, mirror, placeholders=placeholders_for(mirror))
        item = report.items[0]

        assert item.classification == CLASS_SAFE
        assert "no digest was recorded" in item.detail
        assert any("without a recorded digest" in g["what"] for g in report.unexamined)

    def test_gone_remotely_and_absent_locally_is_lost(self, store, mirror):
        store.record_local(LocalItem("gone.txt", size=10, sha256="abc"))
        remote_scan(store, [])

        item = analyse(store, mirror, placeholders=placeholders_for(mirror)).items[0]
        assert item.classification == CLASS_LOST
        assert "not on this disk" in item.detail

    def test_the_lost_case_points_at_trash_and_at_versions(self, store, mirror):
        store.record_local(LocalItem("gone.txt", size=10))
        remote_scan(store, [])

        item = analyse(store, mirror, placeholders=placeholders_for(mirror)).items[0]
        assert "iCloud Trash" in item.detail and ".versions" in item.detail

    def test_an_evicted_stub_is_a_placeholder_not_a_loss(self, store, mirror):
        write_brick(mirror, "evicted.pdf", size=900)
        store.record_local(LocalItem("evicted.pdf", size=900))
        remote_scan(store, [])

        item = analyse(store, mirror, placeholders=placeholders_for(mirror)).items[0]
        assert item.classification == CLASS_PLACEHOLDER

    def test_a_placeholder_that_looks_safe_in_finder_is_not_called_safe(
        self, store, mirror
    ):
        """The whole point: full size on disk, no bytes underneath."""
        add_file(mirror, "ghost.pdf", b"\0" * 900)
        store.record_local(LocalItem("ghost.pdf", size=900, sha256="abc"))
        remote_scan(store, [])

        report = analyse(store, mirror, placeholders=fake_placeholder("ghost.pdf"))
        item = report.items[0]

        assert item.classification == CLASS_PLACEHOLDER
        assert "no bytes" in item.detail

    def test_without_placeholder_detection_the_gap_is_named_not_hidden(
        self, store, mirror
    ):
        """Skipping the check turns the case above into a false 'safe'."""
        add_file(mirror, "ghost.pdf", b"\0" * 900)
        store.record_local(LocalItem("ghost.pdf", size=900, sha256="abc"))
        remote_scan(store, [])

        report = analyse(store, mirror, placeholders=None)

        assert report.items[0].classification == CLASS_SAFE
        assert any(g["what"] == "placeholder detection" for g in report.unexamined)
        assert "evicted shell" in report.unexamined[0]["why"]

    def test_an_unavailable_placeholder_signal_is_reported_as_a_gap(
        self, store, mirror
    ):
        store.record_local(LocalItem("gone.txt", size=1))
        remote_scan(store, [])

        report = analyse(store, mirror, placeholders=placeholders_for(mirror))
        assert any("dataless" in g["what"] for g in report.unexamined)

    def test_a_file_still_in_icloud_is_not_reported(self, store, mirror):
        add_file(mirror, "here.txt")
        store.record_local(LocalItem("here.txt", size=7))
        remote_scan(store, [RemoteItem("here.txt", size=7)])

        assert analyse(store, mirror, placeholders=placeholders_for(mirror)).items == []

    def test_the_loud_class_sorts_first(self, store, mirror):
        add_file(mirror, "safe.txt", b"x")
        store.record_local_many([
            LocalItem("safe.txt", size=1, sha256="a"),
            LocalItem("zzz_lost.txt", size=2, sha256="b"),
        ])
        remote_scan(store, [])

        report = analyse(store, mirror, placeholders=placeholders_for(mirror))
        assert [i.classification for i in report.items] == [CLASS_LOST, CLASS_SAFE]

    def test_counts_and_byte_floor(self, store, mirror):
        add_file(mirror, "safe.txt", b"x")
        store.record_local_many([
            LocalItem("safe.txt", size=1), LocalItem("lost.txt", size=99),
        ])
        remote_scan(store, [])

        report = analyse(store, mirror, placeholders=placeholders_for(mirror))
        assert report.counts() == {CLASS_LOST: 1, CLASS_SAFE: 1}
        assert report.total_bytes == 100


# ---------------------------------------------------------------------------
# The Trash deadline: a bound, never a date
# ---------------------------------------------------------------------------

class TestPurgeDeadline:
    def setup_index(self, store, mirror):
        store.record_local(LocalItem("gone.txt", size=10, sha256="abc"))
        remote_scan(store, [RemoteItem("other.txt", size=1)])

    def test_the_bound_is_the_first_observation_plus_the_retention_window(
        self, store, mirror
    ):
        self.setup_index(store, mirror)
        item = analyse(store, mirror, now=1_000_000).items[0]

        assert item.first_missing_at == 1_000_000
        assert item.purge_deadline_before == 1_000_000 + TRASH_RETENTION_DAYS * DAY

    def test_the_bound_never_moves_later_on_a_second_run(self, store, mirror):
        self.setup_index(store, mirror)
        first = analyse(store, mirror, now=1_000_000).items[0].purge_deadline_before
        later = analyse(store, mirror, now=9_000_000).items[0].purge_deadline_before

        assert later == first

    def test_the_bound_tightens_when_an_earlier_observation_arrives(
        self, store, mirror
    ):
        self.setup_index(store, mirror)
        loose = analyse(store, mirror, now=5_000_000).items[0].purge_deadline_before
        tight = analyse(store, mirror, now=1_000_000).items[0].purge_deadline_before

        assert tight < loose

    def test_the_bound_is_monotone_across_many_runs(self, store, mirror):
        self.setup_index(store, mirror)
        bounds = [
            analyse(store, mirror, now=when).items[0].purge_deadline_before
            for when in (4_000_000, 2_000_000, 8_000_000, 3_000_000)
        ]
        assert bounds == sorted(bounds, reverse=True) or all(
            b <= bounds[0] for b in bounds
        )
        assert bounds[-1] == min(bounds)

    def test_not_recording_leaves_no_trace_in_the_index(self, store, mirror):
        self.setup_index(store, mirror)
        analyse(store, mirror, now=1_000_000, record=False)
        assert store.get_missing_observation("gone.txt") is None

    def test_recording_persists_the_first_observation(self, store, mirror):
        self.setup_index(store, mirror)
        analyse(store, mirror, now=1_000_000)
        recorded = store.get_missing_observation("gone.txt")
        assert recorded["first_missing_at"] == 1_000_000

    def test_the_payload_labels_the_deadline_as_a_bound(self, store, mirror):
        self.setup_index(store, mirror)
        payload = analyse(store, mirror, now=1_000_000).items[0].to_dict()

        assert payload["deadline_is_upper_bound"] is True
        assert "at or before" in payload["deadline_basis"]

    def test_the_report_says_upper_bound_and_not_a_scheduled_date(
        self, store, mirror
    ):
        self.setup_index(store, mirror)
        text = render_vanished(analyse(store, mirror, now=1_000_000))

        assert "UPPER BOUND" in text
        assert "no later than" in text
        assert "not an appointment" in text
        assert "will be purged on" not in text

    def test_the_report_explains_why_it_is_only_a_bound(self, store, mirror):
        self.setup_index(store, mirror)
        text = render_vanished(analyse(store, mirror, now=1_000_000))
        assert "does not know when these files were deleted" in text

    def test_the_soonest_bound_is_reported(self, store, mirror):
        store.record_local_many([
            LocalItem("a.txt", size=1), LocalItem("b.txt", size=1),
        ])
        remote_scan(store, [RemoteItem("keep.txt", size=1)])

        report = analyse(store, mirror, now=1_000_000)
        assert report.soonest_deadline == 1_000_000 + TRASH_RETENTION_DAYS * DAY


class TestReappearance:
    def test_a_file_that_comes_back_clears_its_clock(self, store, mirror):
        store.record_local(LocalItem("flaky.txt", size=5))
        remote_scan(store, [RemoteItem("anchor.txt", size=1)])
        analyse(store, mirror, now=1_000_000)
        assert store.get_missing_observation("flaky.txt") is not None

        remote_scan(store, [RemoteItem("flaky.txt", size=5)])
        report = analyse(store, mirror, now=2_000_000)

        assert report.items == []
        assert store.get_missing_observation("flaky.txt") is None

    def test_a_second_disappearance_starts_a_fresh_clock(self, store, mirror):
        store.record_local(LocalItem("flaky.txt", size=5))
        remote_scan(store, [RemoteItem("anchor.txt", size=1)])
        analyse(store, mirror, now=1_000_000)

        remote_scan(store, [RemoteItem("flaky.txt", size=5)])
        analyse(store, mirror, now=2_000_000)

        remote_scan(store, [RemoteItem("anchor.txt", size=1)])
        item = analyse(store, mirror, now=3_000_000).items[0]

        assert item.first_missing_at == 3_000_000

    def test_reappearance_is_not_recorded_when_recording_is_off(self, store, mirror):
        store.record_local(LocalItem("flaky.txt", size=5))
        remote_scan(store, [RemoteItem("anchor.txt", size=1)])
        analyse(store, mirror, now=1_000_000)

        remote_scan(store, [RemoteItem("flaky.txt", size=5)])
        analyse(store, mirror, now=2_000_000, record=False)

        assert store.get_missing_observation("flaky.txt") is not None


# ---------------------------------------------------------------------------
# Scan health: the evidence behind everything above
# ---------------------------------------------------------------------------

class TestScanEvidence:
    def test_no_scan_at_all_is_not_usable(self, store):
        evidence = assess_scan(store)
        assert evidence.usable is False
        assert "no completed iCloud scan" in evidence.problems[0]

    def test_a_clean_scan_is_usable(self, store):
        remote_scan(store, [RemoteItem("a.txt", size=1)])
        assert assess_scan(store).usable is True

    def test_a_scan_with_listing_errors_is_not_usable(self, store):
        remote_scan(
            store, [RemoteItem("a.txt", size=1)],
            errors=[{"path": "Photos", "error": "HTTP 500"}],
        )
        evidence = assess_scan(store)

        assert evidence.usable is False
        assert evidence.error_count == 1
        assert "listing failure" in evidence.problems[0]

    def test_the_listing_error_explanation_names_the_confusion(self, store):
        remote_scan(store, [RemoteItem("a.txt", size=1)],
                    errors=[{"path": "Photos", "error": "HTTP 500"}])
        assert "an absent file and a deleted file are the same thing" in \
            assess_scan(store).problems[0]

    def test_an_empty_scan_is_not_usable(self, store):
        remote_scan(store, [])
        evidence = assess_scan(store)
        assert evidence.usable is False
        assert "no items at all" in evidence.problems[0]

    def test_a_scan_that_started_and_died_is_not_usable(self, store):
        remote_scan(store, [RemoteItem("a.txt", size=1)])
        store.begin_scan("Documents")

        evidence = assess_scan(store)
        assert evidence.usable is False
        assert "never finished" in evidence.problems[0]

    def test_the_error_sample_is_readable_back(self, store):
        remote_scan(store, [RemoteItem("a.txt", size=1)],
                    errors=[{"path": "Photos", "error": "HTTP 500"}])
        assert assess_scan(store).errors[0]["path"] == "Photos"

    def test_the_scanner_persists_its_listing_failures(self, store):
        """The wiring that makes the whole guarantee real."""
        class FakeFolder:
            type = "folder"

            def __init__(self, children=None, fail=False):
                self._children = children or {}
                self._fail = fail

            def dir(self):
                if self._fail:
                    raise RuntimeError("Apple returned HTTP 400 for this folder")
                return list(self._children.keys())

            def __getitem__(self, name):
                return self._children[name]

        tree = FakeFolder({"broken": FakeFolder(fail=True)})
        RemoteScanner(store).scan("Documents", node=tree)

        assert store.latest_scan()["error_count"] == 1
        assert assess_scan(store).usable is False

    def test_evidence_is_json_serialisable(self, store):
        remote_scan(store, [RemoteItem("a.txt", size=1)])
        json.dumps(assess_scan(store).to_dict())


# ---------------------------------------------------------------------------
# The circuit breaker
# ---------------------------------------------------------------------------

def healthy_scan():
    return ScanEvidence(usable=True, scan_id=1, item_count=100)


class TestBreakerThresholds:
    def test_below_the_absolute_threshold_it_does_not_trip(self):
        verdict = check_breaker(9, 1000, healthy_scan(), max_count=10,
                                max_fraction=1.0)
        assert verdict.tripped is False

    def test_at_the_absolute_threshold_it_trips(self):
        """The threshold is where a result stops being ordinary, not after it."""
        verdict = check_breaker(10, 1000, healthy_scan(), max_count=10,
                                max_fraction=1.0)
        assert verdict.tripped is True
        assert verdict.reason == BREAKER_COUNT

    def test_above_the_absolute_threshold_it_trips(self):
        verdict = check_breaker(50, 1000, healthy_scan(), max_count=10,
                                max_fraction=1.0)
        assert verdict.tripped is True
        assert verdict.reason == BREAKER_COUNT

    def test_below_the_proportional_threshold_it_does_not_trip(self):
        verdict = check_breaker(24, 100, healthy_scan(), max_count=10_000,
                                max_fraction=0.25)
        assert verdict.tripped is False

    def test_at_the_proportional_threshold_it_trips(self):
        verdict = check_breaker(25, 100, healthy_scan(), max_count=10_000,
                                max_fraction=0.25)
        assert verdict.tripped is True
        assert verdict.reason == BREAKER_FRACTION

    def test_the_proportional_rule_is_not_applied_to_a_tiny_baseline(self):
        """One delete out of three is not a mass deletion."""
        verdict = check_breaker(1, 3, healthy_scan(), max_count=10_000,
                                max_fraction=0.25, min_baseline=20)
        assert verdict.tripped is False

    def test_the_absolute_rule_still_applies_to_a_tiny_baseline(self):
        verdict = check_breaker(3, 3, healthy_scan(), max_count=3,
                                max_fraction=1.0, min_baseline=20)
        assert verdict.tripped is True
        assert verdict.reason == BREAKER_COUNT

    def test_nothing_vanished_never_trips_on_a_healthy_scan(self):
        assert check_breaker(0, 100, healthy_scan(), max_count=1,
                             max_fraction=0.0).tripped is False

    def test_the_detail_quotes_the_numbers_and_the_limit(self):
        verdict = check_breaker(25, 100, healthy_scan(), max_count=10_000,
                                max_fraction=0.25)
        assert "25 of 100" in verdict.detail and "25.0%" in verdict.detail

    def test_a_tripped_breaker_lists_what_it_cannot_rule_out(self):
        verdict = check_breaker(50, 100, healthy_scan(), max_count=10)
        joined = " ".join(verdict.cannot_rule_out)
        assert "listing" in joined
        assert "sign-in" in joined
        assert "mount" in joined

    def test_fraction_of_an_empty_baseline_is_zero_not_an_error(self):
        assert BreakerVerdict(baseline_count=0).fraction == 0.0

    def test_verdict_is_json_serialisable(self):
        json.dumps(check_breaker(50, 100, healthy_scan(), max_count=10).to_dict())


class TestBreakerAndBrokenScans:
    """The case that matters most: broken evidence must not look like deletion."""

    def test_an_errored_scan_trips_the_breaker_at_any_volume(self):
        broken = ScanEvidence(usable=False, problems=["a folder failed to list"])
        verdict = check_breaker(1, 1000, broken, max_count=10_000,
                                max_fraction=1.0)
        assert verdict.tripped is True
        assert verdict.reason == BREAKER_SCAN

    def test_an_errored_scan_trips_even_when_nothing_vanished(self):
        """A clean-looking result from a broken scan is the worst outcome."""
        broken = ScanEvidence(usable=False, problems=["a folder failed to list"])
        assert check_breaker(0, 1000, broken).tripped is True

    def test_a_broken_scan_is_never_described_as_a_mass_deletion(self):
        """Ordering: the reason must be the scan, not the volume."""
        broken = ScanEvidence(usable=False, problems=["a folder failed to list"])
        verdict = check_breaker(9_999, 10_000, broken, max_count=10)

        assert verdict.reason == BREAKER_SCAN
        assert verdict.reason != BREAKER_COUNT
        assert "not usable evidence" in verdict.detail

    def test_the_scan_refusal_repeats_the_scan_problems(self):
        broken = ScanEvidence(usable=False, problems=["Photos failed to list"])
        verdict = check_breaker(5, 100, broken)
        assert "Photos failed to list" in " ".join(verdict.cannot_rule_out)

    def test_an_errored_scan_end_to_end_is_refused_not_reported(self, store, mirror):
        for i in range(5):
            store.record_local(LocalItem(f"f{i}.txt", size=1))
        remote_scan(store, [RemoteItem("f0.txt", size=1)],
                    errors=[{"path": "Docs", "error": "HTTP 500"}])

        report = analyse(store, mirror, placeholders=placeholders_for(mirror))

        assert report.refused is True
        assert report.breaker.reason == BREAKER_SCAN

    def test_a_refused_report_does_not_table_the_findings(self, store, mirror):
        for i in range(5):
            store.record_local(LocalItem(f"f{i}.txt", size=1))
        remote_scan(store, [RemoteItem("f0.txt", size=1)],
                    errors=[{"path": "Docs", "error": "HTTP 500"}])

        text = render_vanished(analyse(store, mirror,
                                       placeholders=placeholders_for(mirror)))

        assert "REFUSED" in text
        assert "f1.txt" not in text
        assert "cannot support" in text

    def test_a_refused_report_still_carries_the_paths_in_json(self, store, mirror):
        store.record_local(LocalItem("f1.txt", size=1))
        remote_scan(store, [RemoteItem("f0.txt", size=1)],
                    errors=[{"path": "Docs", "error": "HTTP 500"}])

        payload = analyse(store, mirror,
                          placeholders=placeholders_for(mirror)).to_dict()

        assert payload["refused"] is True
        assert [i["path"] for i in payload["items"]] == ["f1.txt"]

    def test_a_clean_scan_with_nothing_gone_is_not_refused(self, store, mirror):
        add_file(mirror, "a.txt")
        store.record_local(LocalItem("a.txt", size=7))
        remote_scan(store, [RemoteItem("a.txt", size=7)])

        report = analyse(store, mirror, placeholders=placeholders_for(mirror))
        assert report.refused is False and report.items == []


class TestBreakerEndToEnd:
    def populate(self, store, total=40, gone=None):
        gone = total if gone is None else gone
        rows = [LocalItem(f"f{i:03}.txt", size=1) for i in range(total)]
        store.record_local_many(rows)
        remote_scan(store, [
            RemoteItem(r.path, size=1) for r in rows[gone:]
        ] or [RemoteItem("anchor.txt", size=1)])

    def test_a_modest_number_of_deletions_is_an_ordinary_finding(self, store, mirror):
        self.populate(store, total=40, gone=2)
        report = analyse(store, mirror, placeholders=placeholders_for(mirror),
                         max_count=100, max_fraction=0.25)

        assert report.refused is False
        assert len(report.items) == 2

    def test_a_wholesale_disappearance_is_refused(self, store, mirror):
        self.populate(store, total=40, gone=40)
        report = analyse(store, mirror, placeholders=placeholders_for(mirror),
                         max_count=1000, max_fraction=0.25)

        assert report.refused is True
        assert report.breaker.reason == BREAKER_FRACTION

    def test_the_absolute_limit_is_configurable(self, store, mirror):
        self.populate(store, total=40, gone=3)
        report = analyse(store, mirror, placeholders=placeholders_for(mirror),
                         max_count=3, max_fraction=1.0)

        assert report.refused is True
        assert report.breaker.reason == BREAKER_COUNT

    def test_the_proportional_limit_is_configurable(self, store, mirror):
        self.populate(store, total=40, gone=5)
        loose = analyse(store, mirror, placeholders=placeholders_for(mirror),
                        max_count=1000, max_fraction=0.5)
        strict = analyse(store, mirror, placeholders=placeholders_for(mirror),
                         max_count=1000, max_fraction=0.1)

        assert loose.refused is False
        assert strict.refused is True


# ---------------------------------------------------------------------------
# Renames and moves
# ---------------------------------------------------------------------------

class TestRenamesAndMoves:
    def snapshot_move(self, store, mirror):
        """Record A, snapshot it, then move the same digest to B."""
        add_file(mirror, "Old/report.pdf", b"payload")
        store.record_local(LocalItem("Old/report.pdf", size=7, sha256="digest-1"))
        store.create_snapshot("march")

        store.forget_local("Old/report.pdf")
        add_file(mirror, "New/report.pdf", b"payload")
        store.record_local(LocalItem("New/report.pdf", size=7, sha256="digest-1"))

    def test_a_move_proved_by_digest_is_not_reported_as_a_deletion(
        self, store, mirror
    ):
        self.snapshot_move(store, mirror)
        remote_scan(store, [RemoteItem("New/report.pdf", size=7)])

        report = analyse(store, mirror, since="march",
                         placeholders=placeholders_for(mirror))

        assert [i.path for i in report.items] == []
        assert report.moved[0]["old_path"] == "Old/report.pdf"
        assert report.moved[0]["new_path"] == "New/report.pdf"

    def test_the_move_is_named_in_the_report(self, store, mirror):
        self.snapshot_move(store, mirror)
        remote_scan(store, [RemoteItem("New/report.pdf", size=7)])

        text = render_vanished(analyse(store, mirror, since="march",
                                       placeholders=placeholders_for(mirror)))
        assert "Old/report.pdf -> New/report.pdf" in text

    def test_a_copy_to_two_paths_is_ambiguous_with_both_named(self, store, mirror):
        add_file(mirror, "Old/report.pdf", b"payload")
        store.record_local(LocalItem("Old/report.pdf", size=7, sha256="digest-1"))
        store.create_snapshot("march")

        store.forget_local("Old/report.pdf")
        for rel in ("A/report.pdf", "B/report.pdf"):
            add_file(mirror, rel, b"payload")
            store.record_local(LocalItem(rel, size=7, sha256="digest-1"))
        remote_scan(store, [RemoteItem("A/report.pdf", size=7)])

        report = analyse(store, mirror, since="march",
                         placeholders=placeholders_for(mirror))
        item = [i for i in report.items if i.path == "Old/report.pdf"][0]

        assert item.classification == CLASS_AMBIGUOUS
        assert item.alternatives == ["A/report.pdf", "B/report.pdf"]

    def test_a_remote_rename_is_ambiguous_not_a_deletion(self, store, mirror):
        """Apple publishes no content hash, so this can never be proved."""
        store.record_local(LocalItem("Old/report.pdf", size=1234))
        remote_scan(store, [RemoteItem("New/report.pdf", size=1234)])

        report = analyse(store, mirror, placeholders=placeholders_for(mirror))
        item = [i for i in report.items if i.path == "Old/report.pdf"][0]

        assert item.classification == CLASS_AMBIGUOUS
        assert item.alternatives == ["New/report.pdf"]
        assert "renamed" in item.detail

    def test_the_ambiguous_case_says_why_it_cannot_be_settled(self, store, mirror):
        store.record_local(LocalItem("Old/report.pdf", size=1234))
        remote_scan(store, [RemoteItem("New/report.pdf", size=1234)])

        item = analyse(store, mirror,
                       placeholders=placeholders_for(mirror)).items[0]
        assert "no content hash" in item.detail

    def test_several_candidates_are_all_listed(self, store, mirror):
        store.record_local_many([
            LocalItem("Old/a.pdf", size=1234), LocalItem("Old/b.pdf", size=1234),
        ])
        remote_scan(store, [
            RemoteItem("New/x.pdf", size=1234), RemoteItem("New/y.pdf", size=1234),
        ])

        report = analyse(store, mirror, placeholders=placeholders_for(mirror))
        assert all(i.classification == CLASS_AMBIGUOUS for i in report.items)
        assert report.items[0].alternatives == ["New/x.pdf", "New/y.pdf"]

    def test_the_rename_check_can_be_turned_off(self, store, mirror):
        store.record_local(LocalItem("Old/report.pdf", size=1234))
        remote_scan(store, [RemoteItem("New/report.pdf", size=1234)])

        report = analyse(store, mirror, check_renames=False,
                         placeholders=placeholders_for(mirror))
        assert report.items[0].classification == CLASS_LOST

    def test_ambiguity_is_rendered_with_its_alternatives(self, store, mirror):
        store.record_local(LocalItem("Old/report.pdf", size=1234))
        remote_scan(store, [RemoteItem("New/report.pdf", size=1234)])

        text = render_vanished(analyse(store, mirror,
                                       placeholders=placeholders_for(mirror)))
        assert "may not be deletions at all" in text
        assert "could be: New/report.pdf" in text


# ---------------------------------------------------------------------------
# Baselines
# ---------------------------------------------------------------------------

class TestBaseline:
    def test_the_default_baseline_is_the_local_index(self, store):
        store.record_local(LocalItem("a.txt", size=1))
        kind, label, rows = collect_baseline(store)

        assert kind == "index" and label == ""
        assert [r["path"] for r in rows] == ["a.txt"]

    def test_a_snapshot_can_be_the_baseline(self, store):
        store.record_local(LocalItem("a.txt", size=1, sha256="x"))
        store.create_snapshot("march")
        kind, label, rows = collect_baseline(store, since="march")

        assert kind == "snapshot" and label == "march"
        assert rows[0]["sha256"] == "x"

    def test_an_unknown_snapshot_is_refused_by_name(self, store):
        with pytest.raises(VanishedError) as excinfo:
            collect_baseline(store, since="nope")
        assert "nope" in str(excinfo.value)

    def test_the_index_baseline_admits_it_cannot_tell_the_difference(
        self, store, mirror
    ):
        """A file never in iCloud looks like one iCloud deleted."""
        store.record_local(LocalItem("gone.txt", size=1))
        remote_scan(store, [RemoteItem("keep.txt", size=1)])

        report = analyse(store, mirror, placeholders=placeholders_for(mirror))
        assert any("never in iCloud" in note for note in report.notes)

    def test_no_baseline_says_no_baseline_not_nothing_vanished(self, store, mirror):
        remote_scan(store, [RemoteItem("a.txt", size=1)])

        report = analyse(store, mirror, placeholders=placeholders_for(mirror))

        assert report.has_baseline is False
        assert report.items == []
        text = render_vanished(report)
        assert "No baseline" in text
        assert "Nothing has vanished" not in text

    def test_no_baseline_is_not_presented_as_a_clean_bill_of_health(
        self, store, mirror
    ):
        remote_scan(store, [RemoteItem("a.txt", size=1)])
        text = render_vanished(analyse(store, mirror))
        assert "absence of evidence" in text

    def test_no_baseline_is_not_a_refusal(self, store, mirror):
        remote_scan(store, [RemoteItem("a.txt", size=1)])
        assert analyse(store, mirror).refused is False

    def test_an_entirely_empty_index_reports_no_baseline(self, store, mirror):
        report = analyse(store, mirror)
        assert report.has_baseline is False
        assert report.baseline_count == 0

    def test_an_empty_index_still_names_the_missing_scan(self, store, mirror):
        report = analyse(store, mirror)
        assert any("no completed iCloud scan" in g["why"]
                   for g in report.unexamined)

    def test_an_empty_mirror_with_a_baseline_loses_everything(self, store, mirror):
        store.record_local_many([LocalItem(f"f{i}.txt", size=1) for i in range(3)])
        # A different size on the surviving remote file, so nothing can be
        # mistaken for a rename of the three that went.
        remote_scan(store, [RemoteItem("anchor.txt", size=999)])

        report = analyse(store, mirror, placeholders=placeholders_for(mirror),
                         max_count=1000, max_fraction=1.0)
        assert {i.classification for i in report.items} == {CLASS_LOST}

    def test_the_baseline_is_described_in_the_report(self, store, mirror):
        store.record_local(LocalItem("a.txt", size=1, sha256="x"))
        store.create_snapshot("march")
        remote_scan(store, [RemoteItem("b.txt", size=1)])

        report = analyse(store, mirror, since="march",
                         placeholders=placeholders_for(mirror))
        assert report.baseline_description == "snapshot 'march'"
        assert "snapshot 'march'" in render_vanished(report)

    def test_a_snapshot_without_digests_is_reported_as_a_blind_spot(
        self, store, mirror
    ):
        store.record_local(LocalItem("a.txt", size=1))
        store.create_snapshot("march")
        remote_scan(store, [RemoteItem("b.txt", size=1)])

        report = analyse(store, mirror, since="march",
                         placeholders=placeholders_for(mirror))
        assert any("no recorded digest" in g["what"] for g in report.unexamined)


# ---------------------------------------------------------------------------
# Awkward data
# ---------------------------------------------------------------------------

class TestAwkwardData:
    def test_an_nfd_path_matching_an_nfc_listing_is_not_a_deletion(
        self, store, mirror
    ):
        """Apple returns NFD; a mirror may hold NFC. Same file, either way."""
        decomposed = unicodedata.normalize("NFD", "Café/Résumé.pdf")
        composed = unicodedata.normalize("NFC", "Café/Résumé.pdf")
        assert decomposed != composed

        store.record_local(LocalItem(decomposed, size=10))
        remote_scan(store, [RemoteItem(composed, size=10)])

        report = analyse(store, mirror, placeholders=placeholders_for(mirror))
        assert report.items == []

    def test_the_normalisation_match_is_stated_rather_than_hidden(
        self, store, mirror
    ):
        decomposed = unicodedata.normalize("NFD", "Café.pdf")
        store.record_local(LocalItem(decomposed, size=10))
        remote_scan(store, [RemoteItem(unicodedata.normalize("NFC", "Café.pdf"),
                                       size=10)])

        report = analyse(store, mirror, placeholders=placeholders_for(mirror))
        assert any("Unicode normalisation" in note for note in report.notes)

    def test_a_genuinely_vanished_unicode_path_is_still_reported(
        self, store, mirror
    ):
        store.record_local(LocalItem(unicodedata.normalize("NFD", "Café.pdf"),
                                     size=10))
        remote_scan(store, [RemoteItem("other.txt", size=1)])

        report = analyse(store, mirror, placeholders=placeholders_for(mirror))
        assert len(report.items) == 1

    def test_case_is_not_folded_away(self, store, mirror):
        """On a case-sensitive filesystem these are two files, not one."""
        store.record_local(LocalItem("README", size=10))
        remote_scan(store, [RemoteItem("readme", size=10)])

        report = analyse(store, mirror, check_renames=False,
                         placeholders=placeholders_for(mirror))
        assert [i.path for i in report.items] == ["README"]

    def test_a_vanished_package_keeps_its_kind(self, store, mirror):
        store.record_local(LocalItem("Deck.key", kind=KIND_PACKAGE, size=900))
        remote_scan(store, [RemoteItem("other.txt", size=1)])

        item = analyse(store, mirror,
                       placeholders=placeholders_for(mirror)).items[0]
        assert item.kind == KIND_PACKAGE

    def test_package_sizes_are_flagged_as_incomparable(self, store, mirror):
        store.record_local(LocalItem("Deck.key", kind=KIND_PACKAGE, size=900))
        remote_scan(store, [RemoteItem("other.txt", size=1)])

        report = analyse(store, mirror, placeholders=placeholders_for(mirror))
        assert any("package bundle" in note for note in report.notes)

    def test_a_present_package_directory_counts_as_a_safe_copy(self, store, mirror):
        (mirror / "Deck.key").mkdir()
        store.record_local(LocalItem("Deck.key", kind=KIND_PACKAGE, size=900,
                                     sha256="abc"))
        remote_scan(store, [RemoteItem("other.txt", size=1)])

        item = analyse(store, mirror,
                       placeholders=placeholders_for(mirror)).items[0]
        assert item.classification == CLASS_SAFE

    def test_a_file_with_no_recorded_size_is_reported_not_skipped(
        self, store, mirror
    ):
        store.record_local(LocalItem("mystery.bin", size=None))
        remote_scan(store, [RemoteItem("other.txt", size=1)])

        report = analyse(store, mirror, placeholders=placeholders_for(mirror))
        assert [i.path for i in report.items] == ["mystery.bin"]
        assert report.sizeless == 1

    def test_the_byte_total_is_labelled_a_floor_when_sizes_are_missing(
        self, store, mirror
    ):
        store.record_local(LocalItem("mystery.bin", size=None))
        remote_scan(store, [RemoteItem("other.txt", size=1)])

        report = analyse(store, mirror, placeholders=placeholders_for(mirror))
        assert any("floor, not a total" in note for note in report.notes)

    def test_an_unknown_size_renders_as_unknown_not_zero(self, store, mirror):
        store.record_local(LocalItem("mystery.bin", size=None))
        remote_scan(store, [RemoteItem("other.txt", size=1)])

        text = render_vanished(analyse(store, mirror,
                                       placeholders=placeholders_for(mirror)))
        assert "unknown" in text

    def test_remote_directories_do_not_count_as_deletions(self, store, mirror):
        add_file(mirror, "Docs/a.txt")
        store.record_local(LocalItem("Docs/a.txt", size=7, sha256="x"))
        remote_scan(store, [
            RemoteItem("Docs", kind="dir"), RemoteItem("Docs/a.txt", size=7),
        ])

        assert analyse(store, mirror,
                       placeholders=placeholders_for(mirror)).items == []


# ---------------------------------------------------------------------------
# Output shapes
# ---------------------------------------------------------------------------

class TestOutput:
    def build(self, store, mirror):
        add_file(mirror, "safe.txt", b"x")
        store.record_local_many([
            LocalItem("safe.txt", size=1, sha256="aaa"),
            LocalItem("lost.txt", size=99, sha256="bbb"),
        ])
        remote_scan(store, [RemoteItem("anchor.txt", size=1)])
        return analyse(store, mirror, placeholders=placeholders_for(mirror),
                       now=1_000_000, max_count=100, max_fraction=1.0)

    def test_report_is_json_serialisable(self, store, mirror):
        json.dumps(self.build(store, mirror).to_dict())

    def test_payload_carries_the_top_level_contract(self, store, mirror):
        payload = self.build(store, mirror).to_dict()
        for key in ("baseline", "scan", "breaker", "refused", "counts",
                    "items", "unexamined", "notes", "moved_not_deleted",
                    "deadlines_are_upper_bounds"):
            assert key in payload

    def test_payload_marks_deadlines_as_bounds_at_the_top_level(
        self, store, mirror
    ):
        assert self.build(store, mirror).to_dict()["deadlines_are_upper_bounds"] \
            is True

    def test_payload_items_carry_classification_and_iso_dates(self, store, mirror):
        item = self.build(store, mirror).to_dict()["items"][0]
        assert item["classification"] == CLASS_LOST
        assert item["purge_deadline_before_iso"].endswith("Z")

    def test_csv_headers_and_rows(self, store, mirror):
        headers, rows = csv_rows(self.build(store, mirror))
        assert headers[0] == "path"
        assert "deadline_is_upper_bound" in headers
        assert len(rows) == 2

    def test_csv_marks_every_deadline_as_a_bound(self, store, mirror):
        headers, rows = csv_rows(self.build(store, mirror))
        column = headers.index("deadline_is_upper_bound")
        assert {row[column] for row in rows} == {"yes"}

    def test_csv_can_be_written(self, store, mirror, tmp_path):
        headers, rows = csv_rows(self.build(store, mirror))
        target = tmp_path / "out" / "vanished.csv"
        write_csv(target, headers, rows)

        lines = target.read_text().strip().splitlines()
        assert lines[0].startswith("path,classification")
        assert len(lines) == 3

    def test_show_limits_the_table(self, store, mirror):
        store.record_local_many([LocalItem(f"f{i}.txt", size=1) for i in range(10)])
        remote_scan(store, [RemoteItem("anchor.txt", size=1)])
        report = analyse(store, mirror, placeholders=placeholders_for(mirror),
                         max_count=100, max_fraction=1.0)

        text = render_vanished(report, show=3)
        assert "... and 7 more" in text

    def test_a_clean_result_says_nothing_vanished(self, store, mirror):
        add_file(mirror, "a.txt")
        store.record_local(LocalItem("a.txt", size=7))
        remote_scan(store, [RemoteItem("a.txt", size=7)])

        text = render_vanished(analyse(store, mirror,
                                       placeholders=placeholders_for(mirror)))
        assert "Nothing has vanished" in text

    def test_the_loud_line_counts_only_the_unrecoverable(self, store, mirror):
        report = self.build(store, mirror)
        text = render_vanished(report)
        assert "1 file is gone from iCloud with no usable copy" in text

    def test_observations_render(self, store, mirror):
        self.build(store, mirror)
        text = render_observations(list(store.iter_missing_observations()))
        assert "lost.txt" in text and "no later than" in text

    def test_empty_observations_do_not_claim_all_is_well(self, store):
        text = render_observations([])
        assert "different things" in text


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def run(argv):
    import io

    out = io.StringIO()
    code = main(argv, stdout=out)
    return code, out.getvalue()


def seed(mirror, local=(), remote=(), errors=None, on_disk=()):
    with IndexStore(mirror) as store:
        for rel in on_disk:
            add_file(mirror, rel)
        if local:
            store.record_local_many(local)
        if remote or errors is not None:
            remote_scan(store, list(remote), errors=errors)


class TestCli:
    def test_parser_exposes_the_subcommands(self):
        assert build_parser().parse_args(["check"]).command == "check"
        assert build_parser().parse_args(["forget"]).command == "forget"

    def test_no_scan_is_an_error_not_a_clean_result(self, mirror):
        code, _ = run(["check", str(mirror)])
        assert code == EXIT_ERROR

    def test_a_clean_mirror_exits_ok(self, mirror):
        seed(mirror, local=[LocalItem("a.txt", size=7)],
             remote=[RemoteItem("a.txt", size=7)], on_disk=["a.txt"])
        code, text = run(["check", str(mirror)])

        assert code == EXIT_OK
        assert "Nothing has vanished" in text

    def test_no_baseline_exits_ok_and_says_so(self, mirror):
        seed(mirror, remote=[RemoteItem("a.txt", size=7)])
        code, text = run(["check", str(mirror)])

        assert code == EXIT_OK
        assert "No baseline" in text

    def test_findings_exit_one(self, mirror):
        seed(mirror, local=[LocalItem("gone.txt", size=7)],
             remote=[RemoteItem("anchor.txt", size=1)])
        code, text = run(["check", str(mirror), "--max-fraction", "1.0"])

        assert code == EXIT_FINDINGS
        assert "gone.txt" in text

    def test_a_tripped_breaker_exits_with_its_own_code(self, mirror):
        seed(mirror, local=[LocalItem("gone.txt", size=7)],
             remote=[RemoteItem("anchor.txt", size=1)])
        code, text = run(["check", str(mirror), "--max-vanished", "1"])

        assert code == EXIT_REFUSED
        assert code not in (EXIT_OK, EXIT_FINDINGS, EXIT_ERROR)
        assert "REFUSED" in text

    def test_an_errored_scan_exits_refused_not_findings(self, mirror):
        seed(mirror, local=[LocalItem("gone.txt", size=7)],
             remote=[RemoteItem("anchor.txt", size=1)],
             errors=[{"path": "Docs", "error": "HTTP 500"}])
        code, _ = run(["check", str(mirror)])
        assert code == EXIT_REFUSED

    def test_an_unknown_snapshot_is_an_error(self, mirror):
        seed(mirror, local=[LocalItem("a.txt", size=7)],
             remote=[RemoteItem("a.txt", size=7)], on_disk=["a.txt"])
        code, _ = run(["check", str(mirror), "--since", "nope"])
        assert code == EXIT_ERROR

    def test_json_output_is_valid_json(self, mirror):
        seed(mirror, local=[LocalItem("gone.txt", size=7)],
             remote=[RemoteItem("anchor.txt", size=1)])
        code, text = run(["check", str(mirror), "--json", "--max-fraction", "1.0"])

        payload = json.loads(text)
        assert code == EXIT_FINDINGS
        assert payload["items"][0]["path"] == "gone.txt"

    def test_report_is_written_to_disk(self, mirror, tmp_path):
        seed(mirror, local=[LocalItem("gone.txt", size=7)],
             remote=[RemoteItem("anchor.txt", size=1)])
        target = tmp_path / "reports" / "vanished.json"
        run(["check", str(mirror), "--report", str(target), "--max-fraction", "1.0"])

        assert json.loads(target.read_text())["items"][0]["path"] == "gone.txt"

    def test_csv_is_written(self, mirror, tmp_path):
        seed(mirror, local=[LocalItem("gone.txt", size=7)],
             remote=[RemoteItem("anchor.txt", size=1)])
        target = tmp_path / "vanished.csv"
        run(["check", str(mirror), "--csv", str(target), "--max-fraction", "1.0"])

        assert "gone.txt" in target.read_text()

    def test_show_limits_rows(self, mirror):
        seed(mirror, local=[LocalItem(f"f{i}.txt", size=1) for i in range(6)],
             remote=[RemoteItem("anchor.txt", size=1)])
        _, text = run(["check", str(mirror), "--show", "2",
                       "--max-fraction", "1.0"])
        assert "... and 4 more" in text

    def test_no_placeholders_flag_names_the_gap(self, mirror):
        seed(mirror, local=[LocalItem("gone.txt", size=7)],
             remote=[RemoteItem("anchor.txt", size=1)])
        _, text = run(["check", str(mirror), "--no-placeholders",
                       "--max-fraction", "1.0"])
        assert "placeholder detection" in text

    def test_no_record_leaves_the_index_untouched(self, mirror):
        seed(mirror, local=[LocalItem("gone.txt", size=7)],
             remote=[RemoteItem("anchor.txt", size=1)])
        run(["check", str(mirror), "--no-record", "--max-fraction", "1.0"])

        with IndexStore(mirror) as store:
            assert store.get_missing_observation("gone.txt") is None


class TestForget:
    def prepare(self, mirror):
        seed(mirror, local=[LocalItem("gone.txt", size=7)],
             remote=[RemoteItem("anchor.txt", size=1)])
        run(["check", str(mirror), "--max-fraction", "1.0"])

    def test_forget_is_dry_run_by_default(self, mirror):
        self.prepare(mirror)
        code, text = run(["forget", str(mirror)])

        assert code == EXIT_OK
        assert "Nothing was changed" in text
        with IndexStore(mirror) as store:
            assert store.get_missing_observation("gone.txt") is not None

    def test_apply_forgets_the_absence(self, mirror):
        self.prepare(mirror)
        run(["forget", str(mirror), "--apply"])

        with IndexStore(mirror) as store:
            assert store.get_missing_observation("gone.txt") is None

    def test_named_paths_only(self, mirror):
        seed(mirror, local=[LocalItem("a.txt", size=1), LocalItem("b.txt", size=1)],
             remote=[RemoteItem("anchor.txt", size=1)])
        run(["check", str(mirror), "--max-fraction", "1.0"])
        run(["forget", str(mirror), "a.txt", "--apply"])

        with IndexStore(mirror) as store:
            assert store.get_missing_observation("a.txt") is None
            assert store.get_missing_observation("b.txt") is not None

    def test_a_path_with_no_recorded_absence_is_named_not_ignored(self, mirror):
        self.prepare(mirror)
        _, text = run(["forget", str(mirror), "never-seen.txt"])
        assert "never-seen.txt" in text

    def test_forgetting_nothing_is_honest_about_it(self, mirror):
        seed(mirror, remote=[RemoteItem("a.txt", size=1)])
        code, text = run(["forget", str(mirror)])

        assert code == EXIT_OK
        assert "No absences recorded" in text

    def test_json_shape(self, mirror):
        self.prepare(mirror)
        _, text = run(["forget", str(mirror), "--json"])
        payload = json.loads(text)

        assert payload["applied"] is False
        assert payload["paths"] == ["gone.txt"]
