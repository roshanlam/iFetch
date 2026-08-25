"""Tests for ``ifetch guard``.

Two contracts carry the whole feature, and almost every test below is about
one of them.

**The arithmetic must be right.** The headline number - bytes that exist only on
Apple's servers - is what a user decides whether to trust their backups on. It
is checked here against trees mixing resident files, ``.icloud`` stubs, evicted
files with no recorded size, empty files, and iFetch's own artifacts.

**A number must never look more complete than it is.** On a non-macOS platform
the ``dataless`` signal cannot be evaluated at all, so a tree full of evicted
files can look pristine. The report must say so in the rendered text, must set
``complete`` False, and must not exit 0. Directories that could not be read,
symlinks that were not followed, and evicted files whose size is unknown are
each counted and named rather than folded into a total.

Nothing here touches a network or a credential: materialisation is driven
through the injectable fetcher, and the platform-specific signals are faked so
the suite behaves identically on macOS and on Linux CI.
"""

import json
import os
import platform
import plistlib
import sys
import unicodedata
from pathlib import Path

import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))

from ifetch import guard_cli  # noqa: E402
from ifetch.guard import (  # noqa: E402
    MATERIALIZE_DONE,
    MATERIALIZE_FAILED,
    MATERIALIZE_REFUSED,
    MATERIALIZE_UNVERIFIED,
    MATERIALIZE_WOULD,
    STRATEGY_BRCTL,
    STRATEGY_FETCH,
    STRATEGY_NONE,
    ByteAccount,
    EvictedFile,
    GuardError,
    GuardReport,
    GuardScanner,
    choose_strategy,
    default_icloud_folder,
    evicted_csv_rows,
    materialize,
    render_guard,
    verify_resident,
)
from ifetch.recovery import (  # noqa: E402
    CONFIDENCE_LIKELY,
    EVIDENCE_BRICK,
    EVIDENCE_DATALESS,
    Placeholder,
    PlaceholderDetector,
)

EXIT_OK = guard_cli.EXIT_OK
EXIT_FINDINGS = guard_cli.EXIT_FINDINGS
EXIT_ERROR = guard_cli.EXIT_ERROR


# ---------------------------------------------------------------------------
# Fixtures and helpers
# ---------------------------------------------------------------------------

@pytest.fixture
def mirror(tmp_path):
    root = tmp_path / "CloudDocs"
    root.mkdir()
    return root


@pytest.fixture
def mac(monkeypatch):
    """Pretend this is a Mac, so the ``dataless`` signal counts as evaluated."""
    monkeypatch.setattr(platform, "system", lambda: "Darwin")


@pytest.fixture
def not_mac(monkeypatch):
    """Pretend this is not a Mac, where ``dataless`` cannot be evaluated."""
    monkeypatch.setattr(platform, "system", lambda: "Linux")


@pytest.fixture
def dataless(monkeypatch):
    """Fake APFS zero-block files so the suite runs anywhere.

    A real dataless file cannot be created portably - it needs an APFS volume
    and an eviction. What it *looks like* is entirely defined by
    :meth:`PlaceholderDetector._check_dataless`, so that is what is faked here:
    registered paths report a full logical size while holding nothing.
    """
    registry = {}

    def fake_check(self, path, rel):
        if rel not in registry:
            return None
        return Placeholder(
            path=rel,
            evidence=EVIDENCE_DATALESS,
            confidence=CONFIDENCE_LIKELY,
            reported_size=registry[rel],
            detail="reports a full size but occupies no blocks on disk.",
        )

    monkeypatch.setattr(PlaceholderDetector, "_check_dataless", fake_check)

    def mark(root, relative, size):
        target = Path(root) / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(b"")
        registry[relative] = size
        return target

    mark.registry = registry
    return mark


def write_brick(root: Path, relative: str, size=4096):
    """Write the ``.icloud`` stub macOS leaves when it evicts a file."""
    target = Path(root) / relative
    target.parent.mkdir(parents=True, exist_ok=True)
    stub = target.parent / f".{target.name}.icloud"
    payload = {"NSURLNameKey": target.name}
    if size is not None:
        payload["NSURLFileSizeKey"] = size
    with stub.open("wb") as handle:
        plistlib.dump(payload, handle)
    return stub


def write_file(root: Path, relative: str, size: int):
    target = Path(root) / relative
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(b"x" * size)
    return target


def snapshot(root: Path):
    """Every file under ``root`` with its exact bytes, for mutation checks."""
    out = {}
    for current, _dirs, files in os.walk(root):
        for name in files:
            full = Path(current) / name
            out[str(full.relative_to(root))] = full.read_bytes()
    return out


def scan(root, check_dataless=False):
    return GuardScanner(root, check_dataless=check_dataless).scan()


# ---------------------------------------------------------------------------
# Locating the folder
# ---------------------------------------------------------------------------

class TestDefaultFolder:
    def test_points_at_apples_container(self):
        assert default_icloud_folder().name == "com~apple~CloudDocs"
        assert "Mobile Documents" in str(default_icloud_folder())

    def test_an_explicit_path_wins(self, mirror):
        args = guard_cli.build_parser().parse_args([str(mirror)])
        assert guard_cli.resolve_root(args) == mirror.resolve()

    def test_a_missing_default_is_an_error_not_a_home_directory_scan(
        self, tmp_path, monkeypatch
    ):
        """Scanning ``~`` instead would produce a confident, irrelevant number."""
        monkeypatch.setattr(Path, "home", classmethod(lambda cls: tmp_path))
        args = guard_cli.build_parser().parse_args([])

        with pytest.raises(GuardError) as excinfo:
            guard_cli.resolve_root(args)
        assert "does not exist" in str(excinfo.value)
        assert "home directory" in str(excinfo.value)

    def test_a_missing_default_exits_with_the_error_code(self, tmp_path, monkeypatch, capsys):
        monkeypatch.setattr(Path, "home", classmethod(lambda cls: tmp_path))
        assert guard_cli.main([]) == EXIT_ERROR
        assert "iCloud Drive folder does not exist" in capsys.readouterr().err

    def test_a_file_is_not_a_folder(self, tmp_path):
        target = tmp_path / "notadir"
        target.write_text("x")
        with pytest.raises(GuardError):
            GuardScanner(target).scan()


# ---------------------------------------------------------------------------
# Byte accounting - the headline
# ---------------------------------------------------------------------------

class TestByteAccounting:
    def test_a_fully_resident_tree_is_all_resident(self, mirror):
        write_file(mirror, "Docs/a.txt", 1000)
        write_file(mirror, "Docs/b.txt", 500)

        report = scan(mirror)
        assert report.total.files == 2
        assert report.total.logical_bytes == 1500
        assert report.total.resident_bytes == 1500
        assert report.total.evicted_bytes == 0
        assert report.total.evicted_files == 0

    def test_an_evicted_file_counts_towards_logical_but_not_resident(self, mirror):
        write_brick(mirror, "Movies/holiday.mov", size=4_000_000_000)

        report = scan(mirror)
        assert report.total.files == 1
        assert report.total.logical_bytes == 4_000_000_000
        assert report.total.resident_bytes == 0
        assert report.total.evicted_bytes == 4_000_000_000
        assert report.total.evicted_files == 1

    def test_a_mixed_tree_splits_the_bytes_correctly(self, mirror, dataless):
        write_file(mirror, "Docs/here.txt", 1_000)
        write_brick(mirror, "Docs/evicted.pdf", size=9_000)
        dataless(mirror, "Media/zero-blocks.mov", 90_000)

        report = scan(mirror, check_dataless=True)
        assert report.total.files == 3
        assert report.total.resident_files == 1
        assert report.total.evicted_files == 2
        assert report.total.resident_bytes == 1_000
        assert report.total.evicted_bytes == 99_000
        assert report.total.logical_bytes == 100_000

    def test_logical_is_always_resident_plus_evicted(self, mirror, dataless):
        write_file(mirror, "a.bin", 128)
        write_brick(mirror, "b.bin", size=256)
        dataless(mirror, "c.bin", 512)

        total = scan(mirror, check_dataless=True).total
        assert total.logical_bytes == total.resident_bytes + total.evicted_bytes

    def test_exposure_percent(self, mirror):
        write_file(mirror, "here.bin", 250)
        write_brick(mirror, "gone.bin", size=750)
        assert scan(mirror).total.exposure_percent == 75.0

    def test_exposure_percent_of_an_empty_tree_is_zero_not_a_crash(self, mirror):
        assert scan(mirror).total.exposure_percent == 0.0

    def test_an_evicted_file_with_no_recorded_size_is_counted_separately(self, mirror):
        """Folding a guess into the total is how a total becomes fiction."""
        write_brick(mirror, "unknown.bin", size=None)
        write_brick(mirror, "known.bin", size=100)

        total = scan(mirror).total
        assert total.evicted_files == 2
        assert total.evicted_unknown_size == 1
        assert total.evicted_bytes == 100
        assert total.logical_bytes == 100

    def test_the_report_says_the_figure_is_a_floor_when_a_size_is_unknown(self, mirror):
        write_brick(mirror, "unknown.bin", size=None)
        assert "a floor, not a total" in render_guard(scan(mirror))

    def test_empty_files_are_resident_not_evicted(self, mirror):
        write_file(mirror, "empty.txt", 0)
        total = scan(mirror).total
        assert total.resident_files == 1
        assert total.evicted_files == 0
        assert total.logical_bytes == 0

    def test_stub_bytes_are_kept_out_of_resident(self, mirror):
        """The stub is real, tiny and not user content."""
        write_brick(mirror, "gone.bin", size=10_000)

        report = scan(mirror)
        assert report.stub_files == 1
        assert report.stub_bytes > 0
        assert report.total.resident_bytes == 0

    def test_ifetch_artifacts_are_not_counted(self, mirror):
        (mirror / ".ifetch_state.json").write_text("{}")
        write_file(mirror, "real.txt", 10)
        assert scan(mirror).total.files == 1

    def test_the_versions_store_is_skipped(self, mirror):
        write_brick(mirror / ".versions", "archived.txt", size=999)
        write_file(mirror, "real.txt", 10)

        report = scan(mirror)
        assert report.total.evicted_files == 0
        assert report.total.files == 1


class TestPerFolderAccounting:
    def test_top_level_folders_are_split(self, mirror):
        write_file(mirror, "Docs/a.txt", 100)
        write_brick(mirror, "Movies/big.mov", size=5_000)

        report = scan(mirror)
        by_label = {a.label: a for a in report.by_folder}
        assert by_label["Docs"].resident_bytes == 100
        assert by_label["Docs"].evicted_bytes == 0
        assert by_label["Movies"].evicted_bytes == 5_000

    def test_loose_files_are_grouped_as_root(self, mirror):
        write_file(mirror, "loose.txt", 5)
        assert [a.label for a in scan(mirror).by_folder] == ["(root)"]

    def test_folders_are_ranked_by_what_is_missing(self, mirror):
        write_file(mirror, "Big/a.bin", 10_000_000)
        write_brick(mirror, "Small/b.bin", size=10)
        write_brick(mirror, "Worst/c.bin", size=1_000)

        assert [a.label for a in scan(mirror).by_folder][:2] == ["Worst", "Small"]

    def test_nested_paths_roll_up_to_their_top_level_folder(self, mirror):
        write_brick(mirror, "Docs/2024/Q1/report.pdf", size=42)
        assert [a.label for a in scan(mirror).by_folder] == ["Docs"]


class TestOffenders:
    def test_largest_offenders_come_first(self, mirror):
        write_brick(mirror, "small.bin", size=1)
        write_brick(mirror, "huge.bin", size=1_000_000)
        write_brick(mirror, "medium.bin", size=500)

        biggest = scan(mirror).largest(3)
        assert [e.path for e in biggest] == ["huge.bin", "medium.bin", "small.bin"]

    def test_unknown_sizes_sort_last_rather_than_masquerading_as_largest(self, mirror):
        write_brick(mirror, "unknown.bin", size=None)
        write_brick(mirror, "known.bin", size=5)
        assert [e.path for e in scan(mirror).largest(2)] == ["known.bin", "unknown.bin"]

    def test_the_evidence_and_confidence_travel_with_each_file(self, mirror):
        write_brick(mirror, "gone.bin", size=5)
        found = scan(mirror).evicted[0]
        assert found.evidence == EVIDENCE_BRICK
        assert found.confidence == "certain"

    def test_show_limits_the_table_and_says_how_many_were_hidden(self, mirror):
        for index in range(5):
            write_brick(mirror, f"file{index}.bin", size=100)
        assert "and 3 more" in render_guard(scan(mirror), show=2)


# ---------------------------------------------------------------------------
# Platform honesty
# ---------------------------------------------------------------------------

class TestPlatformHonesty:
    """The rule that makes the byte totals safe to act on."""

    def test_dataless_is_named_as_unevaluated_off_macos(self, mirror, not_mac):
        report = GuardScanner(mirror).scan()
        assert report.unevaluated_signals == [EVIDENCE_DATALESS]
        assert report.complete is False

    def test_an_empty_tree_off_macos_does_not_read_as_clean(self, mirror, not_mac):
        write_file(mirror, "looks-fine.txt", 100)
        text = render_guard(GuardScanner(mirror).scan())

        assert "not a clean result" in text
        assert EVIDENCE_DATALESS in text
        assert "would be a real one" not in text

    def test_the_gap_explains_what_would_be_invisible(self, mirror, not_mac):
        text = render_guard(GuardScanner(mirror).scan())
        assert "without leaving a '.icloud' stub would be invisible" in text
        assert "Coverage gaps" in text

    def test_the_platform_is_named_in_the_report(self, mirror, not_mac):
        report = GuardScanner(mirror).scan()
        assert report.platform_name == "Linux"
        assert "Linux" in render_guard(report)

    def test_bricks_are_still_found_off_macos(self, mirror, not_mac):
        """The stub is a real file, which is what makes it portable evidence."""
        write_brick(mirror, "gone.bin", size=64)
        report = GuardScanner(mirror).scan()
        assert report.total.evicted_bytes == 64

    def test_a_clean_tree_on_macos_may_say_so(self, mirror, mac, dataless):
        write_file(mirror, "here.txt", 10)
        report = GuardScanner(mirror).scan()

        assert report.complete is True
        assert "would be a real one" in render_guard(report)

    def test_disabling_dataless_is_reported_as_a_gap(self, mirror, mac):
        report = GuardScanner(mirror, check_dataless=False).scan()
        assert report.unevaluated_signals == [EVIDENCE_DATALESS]

    def test_incomplete_coverage_alone_exits_nonzero(self, mirror, not_mac, capsys):
        """A monitoring job must not read 'could not look' as 'nothing wrong'."""
        write_file(mirror, "here.txt", 10)
        assert guard_cli.main([str(mirror)]) == EXIT_FINDINGS
        capsys.readouterr()

    def test_signal_confidence_is_stated_in_the_report(self, mirror, mac, dataless):
        text = render_guard(GuardScanner(mirror).scan())
        assert "brick" in text and "certain" in text
        assert "dataless" in text and "likely" in text

    def test_signal_confidence_is_in_the_json_too(self, mirror, mac, dataless):
        payload = GuardScanner(mirror).scan().to_dict()
        grades = {s["signal"]: s["confidence"] for s in payload["signals_available"]}
        assert grades == {EVIDENCE_BRICK: "certain", EVIDENCE_DATALESS: "likely"}


class TestBackupFraming:
    def test_the_backup_consequence_is_stated_plainly(self, mirror):
        write_brick(mirror, "tax-return.pdf", size=2_000_000)
        text = render_guard(scan(mirror))

        assert "Time Machine" in text
        assert "not on this Mac" in text
        assert "restore" in text

    def test_a_resident_tree_says_a_backup_would_be_real(self, mirror, mac, dataless):
        write_file(mirror, "here.txt", 10)
        assert "would copy real bytes" in render_guard(GuardScanner(mirror).scan())


# ---------------------------------------------------------------------------
# Adversarial inputs
# ---------------------------------------------------------------------------

class TestAdversarialInputs:
    def test_decomposed_unicode_names_are_accounted_for(self, mirror):
        name = unicodedata.normalize("NFD", "café-résumé.pdf")
        write_brick(mirror, name, size=1234)

        report = scan(mirror)
        assert report.total.evicted_bytes == 1234
        found = unicodedata.normalize("NFC", report.evicted[0].path)
        assert found == unicodedata.normalize("NFC", name)

    def test_a_composed_and_a_decomposed_name_are_both_counted(self, mirror):
        write_file(mirror, unicodedata.normalize("NFC", "über.txt"), 10)
        write_brick(mirror, unicodedata.normalize("NFD", "ångström.txt"), size=20)

        report = scan(mirror)
        assert report.total.files == 2
        assert report.total.evicted_bytes == 20

    def test_a_zero_byte_stub_is_still_a_finding(self, mirror):
        """The stub's existence is the evidence; its contents are a bonus."""
        (mirror / ".broken.pdf.icloud").write_bytes(b"")

        report = scan(mirror)
        assert [e.path for e in report.evicted] == ["broken.pdf"]
        assert report.total.evicted_unknown_size == 1

    def test_a_symlink_out_of_the_tree_is_recorded_and_not_followed(
        self, mirror, tmp_path
    ):
        outside = tmp_path / "outside.bin"
        outside.write_bytes(b"y" * 5_000)
        (mirror / "link.bin").symlink_to(outside)

        report = scan(mirror)
        assert report.symlinks == ["link.bin"]
        assert report.total.files == 0
        assert report.total.logical_bytes == 0

    def test_a_symlinked_directory_is_not_descended(self, mirror, tmp_path):
        elsewhere = tmp_path / "elsewhere"
        elsewhere.mkdir()
        (elsewhere / "huge.bin").write_bytes(b"z" * 9_000)
        (mirror / "shortcut").symlink_to(elsewhere, target_is_directory=True)

        report = scan(mirror)
        assert report.symlinks == ["shortcut"]
        assert report.total.logical_bytes == 0

    def test_symlinks_are_named_in_the_report(self, mirror, tmp_path):
        target = tmp_path / "x.bin"
        target.write_bytes(b"1")
        (mirror / "link.bin").symlink_to(target)
        assert "link.bin" in render_guard(scan(mirror))

    @pytest.mark.skipif(os.geteuid() == 0, reason="root can read anything")
    def test_an_unreadable_directory_is_reported_not_swallowed(self, mirror):
        locked = mirror / "Locked"
        locked.mkdir()
        write_file(locked, "secret.bin", 100)
        os.chmod(locked, 0o000)
        try:
            report = scan(mirror)
        finally:
            os.chmod(locked, 0o700)

        assert [entry["path"] for entry in report.unreadable] == ["Locked"]
        assert report.complete is False

    @pytest.mark.skipif(os.geteuid() == 0, reason="root can read anything")
    def test_an_unreadable_directory_appears_in_the_rendered_gaps(self, mirror):
        locked = mirror / "Locked"
        locked.mkdir()
        os.chmod(locked, 0o000)
        try:
            text = render_guard(scan(mirror))
        finally:
            os.chmod(locked, 0o700)

        assert "could not be read" in text
        assert "Locked" in text

    @pytest.mark.skipif(os.geteuid() == 0, reason="root can read anything")
    def test_an_unreadable_directory_alone_exits_nonzero(self, mirror, mac, dataless, capsys):
        locked = mirror / "Locked"
        locked.mkdir()
        os.chmod(locked, 0o000)
        try:
            code = guard_cli.main([str(mirror)])
        finally:
            os.chmod(locked, 0o700)
        capsys.readouterr()
        assert code == EXIT_FINDINGS


# ---------------------------------------------------------------------------
# Strategy selection
# ---------------------------------------------------------------------------

class TestStrategySelection:
    def test_a_fetcher_wins_over_brctl(self):
        strategy, reason = choose_strategy(lambda p, d: True, brctl_present=True)
        assert strategy == STRATEGY_FETCH
        assert "FileProvider out of the loop" in reason

    def test_brctl_is_the_fallback_and_says_it_is_unreliable(self):
        strategy, reason = choose_strategy(None, brctl_present=True)
        assert strategy == STRATEGY_BRCTL
        assert "silently do nothing" in reason

    def test_nothing_available_is_stated_rather_than_implied(self):
        strategy, reason = choose_strategy(None, brctl_present=False)
        assert strategy == STRATEGY_NONE
        assert "nothing can be materialised" in reason

    def test_asking_for_fetch_without_credentials_is_refused(self):
        strategy, reason = choose_strategy(None, prefer=STRATEGY_FETCH, brctl_present=True)
        assert strategy == STRATEGY_NONE
        assert "no iCloud connection" in reason

    def test_asking_for_brctl_where_it_does_not_exist_is_refused(self):
        strategy, _ = choose_strategy(
            lambda p, d: True, prefer=STRATEGY_BRCTL, brctl_present=False
        )
        assert strategy == STRATEGY_NONE

    def test_brctl_can_be_chosen_explicitly_over_a_fetcher(self):
        strategy, reason = choose_strategy(
            lambda p, d: True, prefer=STRATEGY_BRCTL, brctl_present=True
        )
        assert strategy == STRATEGY_BRCTL
        assert "explicitly requested" in reason


# ---------------------------------------------------------------------------
# Materialisation
# ---------------------------------------------------------------------------

def recovering_fetcher(root, contents=b"real bytes", fail=(), silent=()):
    """A fake fetcher: writes the file and removes the stub, like a real one.

    ``fail`` returns False for those paths. ``silent`` returns True having
    written nothing at all - the failure mode verification exists to catch.
    """
    calls = []

    def fetch(relative_path, destination):
        calls.append(relative_path)
        if relative_path in fail:
            return False
        if relative_path in silent:
            return True
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(contents)
        stub = destination.parent / f".{destination.name}.icloud"
        if stub.exists():
            stub.unlink()
        return True

    fetch.calls = calls
    return fetch


class TestDryRunGating:
    def test_a_dry_run_changes_nothing_on_disk(self, mirror):
        write_brick(mirror, "Docs/a.pdf", size=100)
        write_brick(mirror, "Docs/b.pdf", size=200)
        before = snapshot(mirror)

        report = scan(mirror)
        materialize(report, mirror, fetcher=recovering_fetcher(mirror), dry_run=True)

        assert snapshot(mirror) == before

    def test_the_fetcher_is_never_called_in_a_dry_run(self, mirror):
        write_brick(mirror, "a.pdf", size=100)
        fetcher = recovering_fetcher(mirror)

        materialize(scan(mirror), mirror, fetcher=fetcher, dry_run=True)
        assert fetcher.calls == []

    def test_a_dry_run_lists_exactly_what_it_would_do(self, mirror):
        write_brick(mirror, "a.pdf", size=100)
        write_brick(mirror, "b.pdf", size=200)

        result = materialize(scan(mirror), mirror,
                             fetcher=recovering_fetcher(mirror), dry_run=True)
        assert [o.status for o in result.outcomes] == [MATERIALIZE_WOULD] * 2
        assert [o.path for o in result.outcomes] == ["a.pdf", "b.pdf"]
        assert [o.size for o in result.outcomes] == [100, 200]

    def test_a_dry_run_says_nothing_has_been_written(self, mirror):
        write_brick(mirror, "a.pdf", size=100)
        report = scan(mirror)
        report.materialization = materialize(
            report, mirror, fetcher=recovering_fetcher(mirror), dry_run=True
        )
        text = render_guard(report)
        assert "Nothing has been written" in text
        assert "--apply" in text

    def test_a_dry_run_does_not_verify(self, mirror):
        write_brick(mirror, "a.pdf", size=100)
        result = materialize(scan(mirror), mirror,
                             fetcher=recovering_fetcher(mirror), dry_run=True)
        assert result.verified is False
        assert result.dry_run is True

    def test_the_cli_does_not_write_without_apply(self, mirror, capsys):
        write_brick(mirror, "a.pdf", size=100)
        before = snapshot(mirror)

        code = guard_cli.main([str(mirror), "--materialize", "--no-dataless"])
        capsys.readouterr()

        assert code == EXIT_FINDINGS
        assert snapshot(mirror) == before


class TestMaterializeWithAFetcher:
    def test_every_file_recovers(self, mirror):
        write_brick(mirror, "Docs/a.pdf", size=10)
        write_brick(mirror, "Docs/b.pdf", size=20)

        result = materialize(scan(mirror), mirror,
                             fetcher=recovering_fetcher(mirror), dry_run=False)

        assert [o.status for o in result.outcomes] == [MATERIALIZE_DONE] * 2
        assert result.still_evicted == []
        assert result.recovered_bytes == 30
        assert (mirror / "Docs/a.pdf").read_bytes() == b"real bytes"

    def test_the_strategy_used_is_named(self, mirror):
        write_brick(mirror, "a.pdf", size=10)
        result = materialize(scan(mirror), mirror,
                             fetcher=recovering_fetcher(mirror), dry_run=False)
        assert result.strategy == STRATEGY_FETCH
        assert result.strategy_reason

    def test_a_partial_failure_names_the_file_that_did_not_arrive(self, mirror):
        write_brick(mirror, "good.pdf", size=10)
        write_brick(mirror, "bad.pdf", size=20)

        result = materialize(
            scan(mirror), mirror,
            fetcher=recovering_fetcher(mirror, fail={"bad.pdf"}), dry_run=False,
        )

        assert [o.path for o in result.still_evicted] == ["bad.pdf"]
        assert result.by_status(MATERIALIZE_FAILED)[0].detail == (
            "the download reported failure"
        )
        assert [o.path for o in result.by_status(MATERIALIZE_DONE)] == ["good.pdf"]

    def test_a_total_failure_recovers_nothing_and_says_so(self, mirror):
        write_brick(mirror, "a.pdf", size=10)
        write_brick(mirror, "b.pdf", size=20)

        result = materialize(
            scan(mirror), mirror,
            fetcher=recovering_fetcher(mirror, fail={"a.pdf", "b.pdf"}), dry_run=False,
        )

        assert len(result.still_evicted) == 2
        assert result.recovered_bytes == 0

    def test_files_that_did_not_arrive_are_listed_individually(self, mirror):
        for name in ("one.pdf", "two.pdf", "three.pdf"):
            write_brick(mirror, name, size=10)

        report = scan(mirror)
        report.materialization = materialize(
            report, mirror,
            fetcher=recovering_fetcher(mirror, fail={"one.pdf", "two.pdf"}),
            dry_run=False,
        )
        text = render_guard(report)

        assert "did NOT become resident" in text
        assert "- one.pdf" in text
        assert "- two.pdf" in text

    def test_a_fetcher_that_raises_is_a_finding_not_a_crash(self, mirror):
        write_brick(mirror, "a.pdf", size=10)

        def explode(relative_path, destination):
            raise RuntimeError("connection reset")

        result = materialize(scan(mirror), mirror, fetcher=explode, dry_run=False)

        outcome = result.by_status(MATERIALIZE_FAILED)[0]
        assert "RuntimeError" in outcome.detail
        assert "connection reset" in outcome.detail

    def test_limit_takes_the_largest_offenders_only(self, mirror):
        write_brick(mirror, "small.bin", size=1)
        write_brick(mirror, "huge.bin", size=1_000)

        fetcher = recovering_fetcher(mirror)
        materialize(scan(mirror), mirror, fetcher=fetcher, dry_run=False, limit=1)
        assert fetcher.calls == ["huge.bin"]

    def test_nothing_evicted_means_nothing_attempted(self, mirror):
        write_file(mirror, "here.txt", 10)
        fetcher = recovering_fetcher(mirror)

        result = materialize(scan(mirror), mirror, fetcher=fetcher, dry_run=False)
        assert result.outcomes == []
        assert fetcher.calls == []


class TestVerificationAfterMaterialize:
    """A claim of success is not evidence of success."""

    def test_a_fetcher_that_writes_nothing_is_caught(self, mirror):
        write_brick(mirror, "a.pdf", size=10)

        result = materialize(
            scan(mirror), mirror,
            fetcher=recovering_fetcher(mirror, silent={"a.pdf"}), dry_run=False,
        )

        outcome = result.outcomes[0]
        assert outcome.status == MATERIALIZE_UNVERIFIED
        assert "reported success" in outcome.detail
        assert result.recovered_bytes == 0

    def test_the_unverified_file_is_named(self, mirror):
        write_brick(mirror, "Docs/ghost.pdf", size=10)

        report = scan(mirror)
        report.materialization = materialize(
            report, mirror,
            fetcher=recovering_fetcher(mirror, silent={"Docs/ghost.pdf"}), dry_run=False,
        )
        assert "Docs/ghost.pdf" in render_guard(report)

    def test_a_file_written_but_left_with_its_stub_is_not_accepted(self, mirror):
        """The stub still there means the FileProvider still owns the eviction."""
        write_brick(mirror, "a.pdf", size=10)

        def writes_but_leaves_the_stub(relative_path, destination):
            destination.write_bytes(b"content")
            return True

        result = materialize(scan(mirror), mirror,
                             fetcher=writes_but_leaves_the_stub, dry_run=False)
        assert result.outcomes[0].status == MATERIALIZE_UNVERIFIED

    def test_a_zero_byte_result_is_not_a_recovery(self, mirror):
        write_brick(mirror, "a.pdf", size=10)

        def writes_an_empty_file(relative_path, destination):
            destination.write_bytes(b"")
            (destination.parent / f".{destination.name}.icloud").unlink()
            return True

        result = materialize(scan(mirror), mirror,
                             fetcher=writes_an_empty_file, dry_run=False)
        assert result.outcomes[0].status == MATERIALIZE_UNVERIFIED
        assert "zero bytes" in result.outcomes[0].detail

    def test_verification_ran_at_all(self, mirror):
        write_brick(mirror, "a.pdf", size=10)
        result = materialize(scan(mirror), mirror,
                             fetcher=recovering_fetcher(mirror), dry_run=False)
        assert result.verified is True

    def test_verify_resident_reports_a_missing_file(self, mirror):
        problems = verify_resident(mirror, ["nope.txt"], check_dataless=False)
        assert "no file exists" in problems["nope.txt"]

    def test_verify_resident_is_silent_about_healthy_files(self, mirror):
        write_file(mirror, "fine.txt", 10)
        assert verify_resident(mirror, ["fine.txt"], check_dataless=False) == {}

    def test_verify_resident_refuses_a_path_outside_the_root(self, mirror):
        problems = verify_resident(mirror, ["../escape.txt"], check_dataless=False)
        assert "outside" in problems["../escape.txt"]


class TestPathSafety:
    def test_a_path_escaping_the_root_is_refused_and_nothing_is_written(
        self, mirror, tmp_path
    ):
        report = GuardReport(root=str(mirror))
        report.evicted.append(
            EvictedFile("../escaped.txt", 10, EVIDENCE_BRICK, "certain")
        )

        fetcher = recovering_fetcher(mirror)
        result = materialize(report, mirror, fetcher=fetcher, dry_run=False)

        assert result.outcomes[0].status == MATERIALIZE_REFUSED
        assert "outside" in result.outcomes[0].detail
        assert fetcher.calls == []
        assert not (tmp_path / "escaped.txt").exists()

    def test_an_absolute_looking_escape_is_refused(self, mirror):
        report = GuardReport(root=str(mirror))
        report.evicted.append(
            EvictedFile("a/../../b.txt", 10, EVIDENCE_BRICK, "certain")
        )
        result = materialize(report, mirror,
                             fetcher=recovering_fetcher(mirror), dry_run=False)
        assert result.outcomes[0].status == MATERIALIZE_REFUSED

    def test_escapes_are_refused_in_a_dry_run_too(self, mirror):
        report = GuardReport(root=str(mirror))
        report.evicted.append(EvictedFile("../x.txt", 1, EVIDENCE_BRICK, "certain"))
        result = materialize(report, mirror,
                             fetcher=recovering_fetcher(mirror), dry_run=True)
        assert result.outcomes[0].status == MATERIALIZE_REFUSED


class TestNoStrategyAvailable:
    def test_every_file_is_refused_with_a_reason(self, mirror):
        write_brick(mirror, "a.pdf", size=10)

        result = materialize(scan(mirror), mirror, fetcher=None, dry_run=False,
                             brctl_present=False)

        assert result.strategy == STRATEGY_NONE
        assert result.outcomes[0].status == MATERIALIZE_REFUSED
        assert result.still_evicted

    def test_nothing_is_written(self, mirror):
        write_brick(mirror, "a.pdf", size=10)
        before = snapshot(mirror)
        materialize(scan(mirror), mirror, fetcher=None, dry_run=False,
                    brctl_present=False)
        assert snapshot(mirror) == before


class TestBrctlStrategy:
    def test_a_zero_exit_is_not_taken_as_proof(self, mirror):
        """``brctl download`` returns 0 for work it has only queued."""
        write_brick(mirror, "a.pdf", size=10)
        commands = []

        def runner(command):
            commands.append(list(command))
            return 0

        result = materialize(scan(mirror), mirror, fetcher=None, dry_run=False,
                             brctl_present=True, brctl_runner=runner)

        assert result.strategy == STRATEGY_BRCTL
        assert commands[0][:2] == ["brctl", "download"]
        assert result.outcomes[0].status == MATERIALIZE_UNVERIFIED

    def test_a_nonzero_exit_is_a_failure(self, mirror):
        write_brick(mirror, "a.pdf", size=10)
        result = materialize(scan(mirror), mirror, fetcher=None, dry_run=False,
                             brctl_present=True, brctl_runner=lambda cmd: 3)
        assert result.outcomes[0].status == MATERIALIZE_FAILED
        assert "exited 3" in result.outcomes[0].detail

    def test_brctl_success_is_accepted_only_when_the_disk_agrees(self, mirror):
        write_brick(mirror, "a.pdf", size=10)

        def runner(command):
            target = Path(command[-1])
            target.write_bytes(b"materialised")
            (target.parent / f".{target.name}.icloud").unlink()
            return 0

        result = materialize(scan(mirror), mirror, fetcher=None, dry_run=False,
                             brctl_present=True, brctl_runner=runner)
        assert result.outcomes[0].status == MATERIALIZE_DONE

    def test_a_runner_that_raises_is_a_finding(self, mirror):
        write_brick(mirror, "a.pdf", size=10)

        def runner(command):
            raise FileNotFoundError("brctl")

        result = materialize(scan(mirror), mirror, fetcher=None, dry_run=False,
                             brctl_present=True, brctl_runner=runner)
        assert result.outcomes[0].status == MATERIALIZE_FAILED
        assert "FileNotFoundError" in result.outcomes[0].detail


# ---------------------------------------------------------------------------
# Output shapes and exit codes
# ---------------------------------------------------------------------------

class TestJsonOutput:
    def test_the_report_is_json_serialisable(self, mirror, dataless):
        write_file(mirror, "here.txt", 5)
        write_brick(mirror, "gone.bin", size=10)
        dataless(mirror, "zero.bin", 20)
        json.dumps(scan(mirror, check_dataless=True).to_dict())

    def test_the_json_carries_the_headline_numbers(self, mirror, capsys):
        write_file(mirror, "here.txt", 5)
        write_brick(mirror, "gone.bin", size=95)

        guard_cli.main([str(mirror), "--json", "--no-dataless"])
        payload = json.loads(capsys.readouterr().out)

        assert payload["totals"]["logical_bytes"] == 100
        assert payload["totals"]["resident_bytes"] == 5
        assert payload["totals"]["evicted_bytes"] == 95
        assert payload["totals"]["exposure_percent"] == 95.0

    def test_the_json_names_what_was_not_examined(self, mirror, capsys):
        guard_cli.main([str(mirror), "--json", "--no-dataless"])
        payload = json.loads(capsys.readouterr().out)

        assert payload["complete"] is False
        assert payload["signals_unavailable"][0]["signal"] == EVIDENCE_DATALESS
        assert "reason" in payload["signals_unavailable"][0]

    def test_the_json_lists_evicted_files_with_their_evidence(self, mirror, capsys):
        write_brick(mirror, "gone.bin", size=95)
        guard_cli.main([str(mirror), "--json", "--no-dataless"])
        payload = json.loads(capsys.readouterr().out)

        assert payload["evicted"][0]["path"] == "gone.bin"
        assert payload["evicted"][0]["evidence"] == EVIDENCE_BRICK
        assert payload["evicted"][0]["confidence"] == "certain"

    def test_a_report_file_is_written(self, mirror, tmp_path, capsys):
        write_brick(mirror, "gone.bin", size=1)
        target = tmp_path / "out" / "guard.json"

        guard_cli.main([str(mirror), "--report", str(target), "--no-dataless"])
        capsys.readouterr()

        assert json.loads(target.read_text())["totals"]["evicted_files"] == 1

    def test_the_materialisation_plan_is_in_the_json(self, mirror, capsys, monkeypatch):
        # Materialisation via brctl is macOS-only, so on Linux (CI) the CLI has
        # no strategy and correctly refuses. Force brctl available so the JSON
        # plan itself is exercised on every platform, matching how the unit-level
        # dry-run tests inject a strategy.
        monkeypatch.setattr("ifetch.guard.brctl_available", lambda: True)
        write_brick(mirror, "gone.bin", size=1)
        guard_cli.main([str(mirror), "--json", "--no-dataless", "--materialize"])
        payload = json.loads(capsys.readouterr().out)

        assert payload["materialization"]["dry_run"] is True
        assert payload["materialization"]["outcomes"][0]["status"] == MATERIALIZE_WOULD


class TestCsvOutput:
    def test_rows_carry_path_size_and_evidence(self, mirror):
        write_brick(mirror, "gone.bin", size=95)
        rows = evicted_csv_rows(scan(mirror))
        assert rows[0][:4] == ["gone.bin", 95, EVIDENCE_BRICK, "certain"]

    def test_the_cli_writes_a_csv_with_a_header(self, mirror, tmp_path, capsys):
        write_brick(mirror, "gone.bin", size=95)
        target = tmp_path / "guard.csv"

        guard_cli.main([str(mirror), "--csv", str(target), "--no-dataless"])
        capsys.readouterr()

        lines = target.read_text().strip().splitlines()
        assert lines[0] == "path,bytes,evidence,confidence,detail"
        assert lines[1].startswith("gone.bin,95,brick,certain,")

    def test_a_clean_tree_still_writes_a_header_only_csv(self, mirror, tmp_path, capsys):
        target = tmp_path / "guard.csv"
        guard_cli.main([str(mirror), "--csv", str(target), "--no-dataless"])
        capsys.readouterr()
        assert target.read_text().strip() == "path,bytes,evidence,confidence,detail"


class TestExitCodes:
    def test_evicted_files_exit_one(self, mirror, mac, dataless, capsys):
        write_brick(mirror, "gone.bin", size=1)
        assert guard_cli.main([str(mirror)]) == EXIT_FINDINGS
        capsys.readouterr()

    def test_a_fully_examined_clean_tree_exits_zero(self, mirror, mac, dataless, capsys):
        write_file(mirror, "here.txt", 10)
        assert guard_cli.main([str(mirror)]) == EXIT_OK
        capsys.readouterr()

    def test_a_missing_folder_exits_two(self, tmp_path, capsys):
        assert guard_cli.main([str(tmp_path / "nope")]) == EXIT_ERROR
        assert "Error" in capsys.readouterr().err

    def test_a_successful_apply_clears_the_finding(self, mirror, mac, dataless,
                                                  monkeypatch, capsys):
        write_brick(mirror, "gone.bin", size=10)
        monkeypatch.setattr(
            guard_cli, "_build_fetcher",
            lambda args, stdout: recovering_fetcher(mirror),
        )

        code = guard_cli.main([str(mirror), "--materialize", "--apply"])
        capsys.readouterr()

        assert code == EXIT_OK
        assert (mirror / "gone.bin").read_bytes() == b"real bytes"

    def test_a_failed_apply_keeps_the_finding(self, mirror, mac, dataless,
                                              monkeypatch, capsys):
        write_brick(mirror, "gone.bin", size=10)
        monkeypatch.setattr(
            guard_cli, "_build_fetcher",
            lambda args, stdout: recovering_fetcher(mirror, fail={"gone.bin"}),
        )

        assert guard_cli.main([str(mirror), "--materialize", "--apply"]) == EXIT_FINDINGS
        capsys.readouterr()

    def test_apply_does_not_sign_in_when_there_is_nothing_to_fetch(
        self, mirror, mac, dataless, monkeypatch, capsys
    ):
        write_file(mirror, "here.txt", 10)

        def explode(args, stdout):
            raise AssertionError("should not authenticate")

        monkeypatch.setattr(guard_cli, "_build_fetcher", explode)
        assert guard_cli.main([str(mirror), "--materialize", "--apply"]) == EXIT_OK
        capsys.readouterr()


class TestByteAccountUnit:
    def test_resident_and_evicted_accumulate_independently(self):
        account = ByteAccount(label="x")
        account.add_resident(100)
        account.add_evicted(400)

        assert account.files == 2
        assert account.logical_bytes == 500
        assert account.exposure_percent == 80.0

    def test_an_unknown_size_does_not_move_any_total(self):
        account = ByteAccount()
        account.add_evicted(None)

        assert account.files == 1
        assert account.evicted_files == 1
        assert account.evicted_unknown_size == 1
        assert account.evicted_bytes == 0
        assert account.logical_bytes == 0

    def test_it_serialises(self):
        assert ByteAccount(label="Docs").to_dict()["label"] == "Docs"
