"""Tests for the recovery workflows.

The contract that matters most here is **honesty about coverage**. A
placeholder detector that quietly reports "none found" on a platform where it
cannot look is worse than not shipping one, because it invites someone to trust
a backup full of empty shells. So:

* every signal that cannot be evaluated is named, with the reason;
* ``complete`` is False whenever any signal was unavailable;
* the three kinds of "missing" are kept distinct, because they call for
  different actions - and a file gone from *both* sides is called out loudest.
"""

import json
import os
import platform
import plistlib
import sys
from pathlib import Path

import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))

from ifetch.index import IndexStore, LocalItem, RemoteItem  # noqa: E402
from ifetch.recovery import (  # noqa: E402
    CONFIDENCE_CERTAIN,
    EVIDENCE_BRICK,
    EVIDENCE_DATALESS,
    MISSING_DISAPPEARED,
    MISSING_NEVER_DOWNLOADED,
    MISSING_PLACEHOLDER,
    PlaceholderDetector,
    brick_target,
    build_inventory,
    build_missing_report,
    fetch_account_storage,
    render_inventory,
    render_missing,
    render_placeholders,
    write_csv,
)


@pytest.fixture
def store(tmp_path):
    with IndexStore(tmp_path / "dest") as s:
        yield s


@pytest.fixture
def mirror(tmp_path):
    root = tmp_path / "dest"
    root.mkdir(exist_ok=True)
    return root


def write_brick(root: Path, relative: str, size: int = 4096):
    """Write a macOS ``.icloud`` eviction stub for ``relative``."""
    target = root / relative
    target.parent.mkdir(parents=True, exist_ok=True)
    stub = target.parent / f".{target.name}{'.icloud'}"
    with stub.open("wb") as handle:
        plistlib.dump({"NSURLFileSizeKey": size, "NSURLNameKey": target.name}, handle)
    return stub


# ---------------------------------------------------------------------------
# Brick stubs
# ---------------------------------------------------------------------------

class TestBrickTarget:
    @pytest.mark.parametrize(
        "stub,expected",
        [
            (".report.pdf.icloud", "report.pdf"),
            (".Deck.key.icloud", "Deck.key"),
            (".a.b.c.txt.icloud", "a.b.c.txt"),
        ],
    )
    def test_recovers_the_original_name(self, stub, expected):
        assert brick_target(stub) == expected

    @pytest.mark.parametrize(
        "name", ["report.pdf", "report.pdf.icloud", ".icloud", "notes.txt"]
    )
    def test_rejects_things_that_are_not_stubs(self, name):
        assert brick_target(name) is None


class TestPlaceholderDetection:
    def test_finds_an_evicted_file_from_its_stub(self, mirror):
        write_brick(mirror, "Documents/report.pdf", size=8192)

        report = PlaceholderDetector(mirror, check_dataless=False).scan()

        assert len(report.placeholders) == 1
        found = report.placeholders[0]
        assert found.path == "Documents/report.pdf"
        assert found.evidence == EVIDENCE_BRICK
        assert found.confidence == CONFIDENCE_CERTAIN

    def test_reads_the_real_size_out_of_the_stub(self, mirror):
        write_brick(mirror, "big.iso", size=5_000_000_000)
        report = PlaceholderDetector(mirror, check_dataless=False).scan()
        assert report.placeholders[0].reported_size == 5_000_000_000
        assert report.total_reported_bytes == 5_000_000_000

    def test_a_stub_at_the_root_is_found(self, mirror):
        write_brick(mirror, "top.txt")
        report = PlaceholderDetector(mirror, check_dataless=False).scan()
        assert report.placeholders[0].path == "top.txt"

    def test_real_files_are_not_reported(self, mirror):
        (mirror / "real.txt").write_bytes(b"actually here")
        report = PlaceholderDetector(mirror, check_dataless=False).scan()
        assert report.placeholders == []
        assert report.files_checked == 1

    def test_detection_explains_what_the_user_should_do(self, mirror):
        write_brick(mirror, "report.pdf")
        report = PlaceholderDetector(mirror, check_dataless=False).scan()
        assert "not on this disk" in report.placeholders[0].detail

    def test_a_corrupt_stub_is_still_reported(self, mirror):
        """The stub's existence is the finding; its contents are a bonus."""
        (mirror / ".broken.txt.icloud").write_bytes(b"not a plist at all")
        report = PlaceholderDetector(mirror, check_dataless=False).scan()
        assert len(report.placeholders) == 1
        assert report.placeholders[0].reported_size is None

    def test_ifetch_artifacts_are_not_counted_as_checked_files(self, mirror):
        (mirror / ".ifetch_state.json").write_text("{}")
        (mirror / "real.txt").write_text("x")
        report = PlaceholderDetector(mirror, check_dataless=False).scan()
        assert report.files_checked == 1

    def test_versions_directory_is_skipped(self, mirror):
        (mirror / ".versions").mkdir()
        write_brick(mirror / ".versions", "archived.txt")
        report = PlaceholderDetector(mirror, check_dataless=False).scan()
        assert report.placeholders == []

    def test_results_are_sorted_for_stable_output(self, mirror):
        for name in ("c.txt", "a.txt", "b.txt"):
            write_brick(mirror, name)
        report = PlaceholderDetector(mirror, check_dataless=False).scan()
        assert [p.path for p in report.placeholders] == ["a.txt", "b.txt", "c.txt"]


class TestCoverageHonesty:
    """The property that makes this feature safe to trust."""

    def test_disabling_dataless_is_reported_as_a_gap(self, mirror):
        report = PlaceholderDetector(mirror, check_dataless=False).scan()
        assert report.complete is False
        assert [g["signal"] for g in report.signals_unavailable] == [EVIDENCE_DATALESS]

    def test_the_gap_explains_itself(self, mirror):
        report = PlaceholderDetector(mirror, check_dataless=False).scan()
        assert "macOS" in report.signals_unavailable[0]["reason"]

    def test_a_clean_but_incomplete_scan_does_not_claim_certainty(self, mirror):
        (mirror / "real.txt").write_text("x")
        report = PlaceholderDetector(mirror, check_dataless=False).scan()
        text = render_placeholders(report)
        assert "signals available on this platform" in text

    def test_brick_detection_is_always_available(self, mirror):
        """It works off a real file, so it is platform-independent."""
        report = PlaceholderDetector(mirror, check_dataless=False).scan()
        assert EVIDENCE_BRICK in report.signals_available

    def test_a_complete_scan_says_so(self, mirror):
        report = PlaceholderDetector(mirror, check_dataless=True).scan()
        assert report.complete is True

    def test_report_is_json_serialisable(self, mirror):
        write_brick(mirror, "a.txt")
        json.dumps(PlaceholderDetector(mirror, check_dataless=False).scan().to_dict())


@pytest.mark.skipif(platform.system() != "Darwin", reason="APFS-specific")
class TestDatalessDetection:
    def test_an_ordinary_file_is_not_dataless(self, mirror):
        (mirror / "real.txt").write_bytes(b"x" * 10_000)
        report = PlaceholderDetector(mirror, check_dataless=True).scan()
        assert [p for p in report.placeholders if p.evidence == EVIDENCE_DATALESS] == []

    def test_an_empty_file_is_not_flagged(self, mirror):
        """Zero bytes legitimately occupies zero blocks."""
        (mirror / "empty.txt").write_bytes(b"")
        report = PlaceholderDetector(mirror, check_dataless=True).scan()
        assert report.placeholders == []


# ---------------------------------------------------------------------------
# Missing files
# ---------------------------------------------------------------------------

class TestMissingReport:
    def test_remote_file_with_no_local_copy_is_never_downloaded(self, store, mirror):
        store.record_remote(RemoteItem("a.txt", size=100))
        report = build_missing_report(store, mirror)
        assert report.items[0].reason == MISSING_NEVER_DOWNLOADED

    def test_a_recorded_file_that_vanished_is_disappeared_not_new(self, store, mirror):
        """Local data loss, which deserves different language than 'not fetched yet'."""
        store.record_remote(RemoteItem("a.txt", size=100))
        store.record_local(LocalItem("a.txt", size=100))

        report = build_missing_report(store, mirror)
        assert report.items[0].reason == MISSING_DISAPPEARED

    def test_a_file_gone_from_both_sides_is_called_out(self, store, mirror):
        """The most alarming case: no local copy, and iCloud no longer has it."""
        store.record_local(LocalItem("lost.txt", size=100, sha256="abc"))

        report = build_missing_report(store, mirror)
        item = report.items[0]
        assert item.reason == MISSING_DISAPPEARED
        assert item.sha256 == "abc"
        assert ".versions" in item.detail

    def test_a_present_file_is_not_reported(self, store, mirror):
        (mirror / "a.txt").write_bytes(b"here")
        store.record_remote(RemoteItem("a.txt", size=4))
        store.record_local(LocalItem("a.txt", size=4))

        assert build_missing_report(store, mirror).items == []

    def test_a_placeholder_counts_as_missing(self, store, mirror):
        """It exists in Finder and contains nothing; a mirror is not complete."""
        write_brick(mirror, "evicted.pdf", size=900)
        store.record_remote(RemoteItem("evicted.pdf", size=900))

        placeholders = PlaceholderDetector(mirror, check_dataless=False).scan()
        report = build_missing_report(store, mirror, placeholders=placeholders)

        assert report.items[0].reason == MISSING_PLACEHOLDER

    def test_totals_and_counts(self, store, mirror):
        store.record_remote_many([
            RemoteItem("a.txt", size=100), RemoteItem("b.txt", size=250)
        ])
        report = build_missing_report(store, mirror)
        assert report.total_bytes == 350
        assert report.counts()[MISSING_NEVER_DOWNLOADED] == 2

    def test_results_are_sorted(self, store, mirror):
        store.record_remote_many([RemoteItem(p) for p in ("c.txt", "a.txt", "b.txt")])
        report = build_missing_report(store, mirror)
        assert [i.path for i in report.items] == ["a.txt", "b.txt", "c.txt"]

    def test_report_is_json_serialisable(self, store, mirror):
        store.record_remote(RemoteItem("a.txt", size=1))
        json.dumps(build_missing_report(store, mirror).to_dict())

    def test_rendering_highlights_local_data_loss(self, store, mirror):
        store.record_remote(RemoteItem("a.txt", size=1))
        store.record_local(LocalItem("a.txt", size=1))
        text = render_missing(build_missing_report(store, mirror))
        assert "gone from this disk" in text

    def test_rendering_says_so_when_nothing_is_missing(self, store, mirror):
        assert "Nothing missing" in render_missing(build_missing_report(store, mirror))

    def test_pluralisation_is_correct_for_one_file(self, store, mirror):
        store.record_remote(RemoteItem("a.txt", size=1))
        store.record_local(LocalItem("a.txt", size=1))
        text = render_missing(build_missing_report(store, mirror))
        assert "1 file recorded by an earlier run is gone" in text


# ---------------------------------------------------------------------------
# Inventory
# ---------------------------------------------------------------------------

class TestInventory:
    def populate(self, store):
        store.record_remote_many([
            RemoteItem("Photos/a.heic", size=3_000_000),
            RemoteItem("Photos/b.heic", size=2_000_000),
            RemoteItem("Docs/report.pdf", size=500_000),
            RemoteItem("Docs/notes.txt", size=1_000),
            RemoteItem("Archive/backup.dmg", size=9_000_000_000),
        ])

    def test_totals(self, store):
        self.populate(store)
        report = build_inventory(store)
        assert report.file_count == 5
        assert report.total_bytes == 9_005_501_000

    def test_folders_are_ranked_by_size(self, store):
        self.populate(store)
        report = build_inventory(store)
        assert [f["folder"] for f in report.by_folder] == ["Archive", "Photos", "Docs"]

    def test_folder_grouping_depth_can_be_widened(self, store):
        store.record_remote_many([
            RemoteItem("A/B/one.txt", size=10), RemoteItem("A/C/two.txt", size=20),
        ])
        report = build_inventory(store, depth=2)
        assert {f["folder"] for f in report.by_folder} == {"A/B", "A/C"}

    def test_root_level_files_are_grouped_as_root(self, store):
        store.record_remote(RemoteItem("loose.txt", size=10))
        assert build_inventory(store).by_folder[0]["folder"] == "(root)"

    def test_extensions_are_ranked_by_size(self, store):
        self.populate(store)
        report = build_inventory(store)
        assert report.by_extension[0]["extension"] == "dmg"
        assert report.by_extension[0]["files"] == 1

    def test_files_without_an_extension_are_grouped(self, store):
        store.record_remote(RemoteItem("Makefile", size=100))
        assert build_inventory(store).by_extension[0]["extension"] == "(no extension)"

    def test_largest_files_are_listed_biggest_first(self, store):
        self.populate(store)
        report = build_inventory(store, top=3)
        assert [f["path"] for f in report.largest] == [
            "Archive/backup.dmg", "Photos/a.heic", "Photos/b.heic",
        ]

    def test_top_limits_the_largest_list(self, store):
        self.populate(store)
        assert len(build_inventory(store, top=2).largest) == 2

    def test_missing_sizes_are_treated_as_zero_not_an_error(self, store):
        store.record_remote(RemoteItem("unknown.bin", size=None))
        assert build_inventory(store).total_bytes == 0

    def test_empty_inventory(self, store):
        report = build_inventory(store)
        assert report.file_count == 0 and report.by_folder == []

    def test_report_is_json_serialisable(self, store):
        self.populate(store)
        json.dumps(build_inventory(store).to_dict())

    def test_rendering_covers_every_section(self, store):
        self.populate(store)
        text = render_inventory(build_inventory(store))
        for heading in ("Largest folders", "By file type", "Largest files"):
            assert heading in text


class TestAccountStorage:
    class Usage:
        total_storage_in_bytes = 2_000_000_000_000
        used_storage_in_bytes = 1_500_000_000_000
        available_storage_in_bytes = 500_000_000_000

    class Api:
        def __init__(self, usage):
            self.account = type("A", (), {"storage": type("S", (), {"usage": usage})()})()

    def test_reads_quota_figures(self):
        payload = fetch_account_storage(self.Api(self.Usage()))
        assert payload["total_bytes"] == 2_000_000_000_000
        assert payload["used_percent"] == 75.0

    def test_an_unavailable_endpoint_degrades_rather_than_raises(self):
        """Apple's storage endpoint is separate and may simply not answer."""
        class Broken:
            @property
            def account(self):
                raise RuntimeError("503 from Apple")

        assert fetch_account_storage(Broken()) is None

    def test_missing_fields_yield_none_rather_than_a_wrong_number(self):
        class Empty:
            pass

        assert fetch_account_storage(self.Api(Empty())) is None

    def test_inventory_rendering_includes_quota_when_present(self, store):
        report = build_inventory(store)
        report.account = fetch_account_storage(self.Api(self.Usage()))
        text = render_inventory(report)
        assert "Apple account storage" in text and "75.0%" in text


class TestCsvExport:
    def test_writes_a_header_and_rows(self, tmp_path):
        target = tmp_path / "out" / "report.csv"
        write_csv(target, ["a", "b"], [[1, 2], [3, 4]])

        lines = target.read_text().strip().splitlines()
        assert lines[0] == "a,b"
        assert lines[1] == "1,2"

    def test_creates_missing_parent_directories(self, tmp_path):
        target = tmp_path / "deep" / "deeper" / "x.csv"
        write_csv(target, ["a"], [[1]])
        assert target.exists()

    def test_values_containing_commas_are_quoted(self, tmp_path):
        target = tmp_path / "x.csv"
        write_csv(target, ["path"], [["Documents/a,b.txt"]])
        assert '"Documents/a,b.txt"' in target.read_text()
