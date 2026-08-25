"""Tests for the scanner and the dry-run planner.

The contract:

* a scan reads **metadata only** - it must never open a download stream, and a
  test asserts that by making ``open()`` explode;
* a scan that hits an unreadable folder records the error and keeps going,
  rather than losing the inventory of everything else;
* the plan classifies every file as download / overwrite / skip / verify /
  local-only, and never claims certainty it does not have;
* an ETA is produced only from measured or supplied throughput - never invented;
* disk sufficiency accounts for headroom, and a shortfall is stated plainly.
"""

import io
import json
import sys
from pathlib import Path

import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))

from ifetch.index import KIND_DIR, KIND_PACKAGE, IndexStore, LocalItem, RemoteItem  # noqa: E402
from ifetch.planner import (  # noqa: E402
    ACTION_DOWNLOAD,
    ACTION_ORPHAN,
    ACTION_OVERWRITE,
    ACTION_SKIP,
    ACTION_VERIFY,
    THROUGHPUT_KEY,
    Planner,
    record_throughput,
    render_audit,
    render_plan,
)
from ifetch.render import human_bytes, human_duration, table  # noqa: E402
from ifetch.scanner import LocalScanner, RemoteScanner, is_local_artifact  # noqa: E402


# ---------------------------------------------------------------------------
# Fake iCloud tree
# ---------------------------------------------------------------------------

class FakeFile:
    type = "file"

    def __init__(self, name, size=100, token="t1"):
        self.name = name
        self.size = size
        self.date_modified = token
        self.date_changed = None
        self.data = {"docwsid": f"doc-{name}", "zone": "com.apple.CloudDocs", "etag": "1::1"}

    def open(self, stream=True):  # pragma: no cover - must never be called
        raise AssertionError("a scan opened a download stream")


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


def sample_tree():
    return FakeFolder({
        "a.txt": FakeFile("a.txt", size=100),
        "b.pdf": FakeFile("b.pdf", size=250),
        "Deck.key": FakeFile("Deck.key", size=900),
        "sub": FakeFolder({
            "c.txt": FakeFile("c.txt", size=50),
            "deeper": FakeFolder({"d.txt": FakeFile("d.txt", size=25)}),
        }),
    })


@pytest.fixture
def store(tmp_path):
    with IndexStore(tmp_path / "dest") as s:
        yield s


# ---------------------------------------------------------------------------
# Remote scanning
# ---------------------------------------------------------------------------

class TestRemoteScanner:
    def test_walks_the_whole_tree(self, store):
        stats = RemoteScanner(store).scan("Documents", node=sample_tree())
        assert stats.files == 5
        assert {r["path"] for r in store.iter_remote()} == {
            "a.txt", "b.pdf", "Deck.key", "sub/c.txt", "sub/deeper/d.txt",
        }

    def test_never_opens_a_download_stream(self, store):
        """FakeFile.open() raises; reaching it would fail the scan."""
        RemoteScanner(store).scan("Documents", node=sample_tree())  # must not raise

    def test_totals_only_files_not_directories(self, store):
        stats = RemoteScanner(store).scan("Documents", node=sample_tree())
        assert stats.total_bytes == 100 + 250 + 900 + 50 + 25
        assert stats.directories == 2

    def test_package_extensions_are_recorded_as_packages(self, store):
        RemoteScanner(store).scan("Documents", node=sample_tree())
        assert store.get_remote("Deck.key")["kind"] == KIND_PACKAGE
        assert store.get_remote("a.txt")["kind"] != KIND_PACKAGE

    def test_apple_identifiers_are_captured_for_later_download(self, store):
        RemoteScanner(store).scan("Documents", node=sample_tree())
        row = store.get_remote("a.txt")
        assert row["docwsid"] == "doc-a.txt"
        assert row["zone"] == "com.apple.CloudDocs"

    def test_an_unreadable_folder_does_not_lose_the_rest(self, store):
        tree = FakeFolder({
            "good.txt": FakeFile("good.txt", size=10),
            "broken": FakeFolder(fail=True),
            "also-good.txt": FakeFile("also-good.txt", size=20),
        })
        stats = RemoteScanner(store).scan("Documents", node=tree)

        assert stats.files == 2
        assert len(stats.errors) == 1
        assert "broken" in stats.errors[0]["path"]

    def test_exclude_patterns_prune_files(self, store):
        scanner = RemoteScanner(store, exclude_patterns=["*.pdf"])
        scanner.scan("Documents", node=sample_tree())
        assert "b.pdf" not in {r["path"] for r in store.iter_remote()}

    def test_include_patterns_still_traverse_directories(self, store):
        """An include glob must not prune the folders holding the matches."""
        scanner = RemoteScanner(store, include_patterns=["*.txt"])
        scanner.scan("Documents", node=sample_tree())
        paths = {r["path"] for r in store.iter_remote()}
        assert "sub/deeper/d.txt" in paths
        assert "b.pdf" not in paths

    def test_rescanning_replaces_the_previous_inventory(self, store):
        """A stale entry would show as a phantom download forever."""
        RemoteScanner(store).scan("Documents", node=sample_tree())
        RemoteScanner(store).scan("Documents", node=FakeFolder({"only.txt": FakeFile("only.txt")}))
        assert {r["path"] for r in store.iter_remote()} == {"only.txt"}

    def test_scan_totals_are_recorded_for_later_reuse(self, store):
        RemoteScanner(store).scan("Documents", node=sample_tree())
        assert store.latest_scan()["item_count"] == 5

    def test_empty_tree_is_not_an_error(self, store):
        stats = RemoteScanner(store).scan("Documents", node=FakeFolder({}))
        assert stats.files == 0

    def test_a_large_tree_is_batched_not_lost(self, store):
        wide = FakeFolder({f"f{i}.txt": FakeFile(f"f{i}.txt", size=1) for i in range(1200)})
        stats = RemoteScanner(store).scan("Documents", node=wide)
        assert stats.files == 1200
        assert store.remote_count() == 1200

    def test_stats_are_json_serialisable(self, store):
        json.dumps(RemoteScanner(store).scan("Documents", node=sample_tree()).to_dict())


# ---------------------------------------------------------------------------
# Local scanning
# ---------------------------------------------------------------------------

class TestLocalScanner:
    def build_mirror(self, root):
        (root / "sub").mkdir(parents=True)
        (root / "a.txt").write_bytes(b"a" * 100)
        (root / "sub" / "c.txt").write_bytes(b"c" * 50)
        return root

    def test_indexes_every_file(self, store, tmp_path):
        root = self.build_mirror(tmp_path / "dest")
        stats = LocalScanner(store, root).scan()
        assert stats.files == 2
        assert {r["path"] for r in store.iter_local()} == {"a.txt", "sub/c.txt"}

    def test_records_sizes(self, store, tmp_path):
        root = self.build_mirror(tmp_path / "dest")
        LocalScanner(store, root).scan()
        assert store.get_local("a.txt")["size"] == 100

    @pytest.mark.parametrize(
        "name",
        [".ifetch_state.json", ".ifetch_manifest.json", ".ifetch_index.db",
         "download_report.json", "x.temp", "y.download", ".DS_Store"],
    )
    def test_ifetch_artifacts_are_not_indexed_as_content(self, store, tmp_path, name):
        root = tmp_path / "dest"
        root.mkdir(exist_ok=True)   # the store fixture already created it
        (root / name).write_text("x")
        assert LocalScanner(store, root).scan().files == 0

    def test_versions_directory_is_skipped(self, store, tmp_path):
        root = tmp_path / "dest"
        (root / ".versions" / "old").mkdir(parents=True)
        (root / ".versions" / "old" / "f.txt").write_text("archived")
        (root / "real.txt").write_text("real")

        LocalScanner(store, root).scan()
        assert {r["path"] for r in store.iter_local()} == {"real.txt"}

    def test_rehash_computes_digests(self, store, tmp_path):
        root = self.build_mirror(tmp_path / "dest")
        LocalScanner(store, root, rehash=True).scan()
        assert len(store.get_local("a.txt")["sha256"]) == 64

    def test_without_rehash_no_digest_is_invented(self, store, tmp_path):
        """A made-up digest would be worse than none at all."""
        root = self.build_mirror(tmp_path / "dest")
        LocalScanner(store, root).scan()
        assert store.get_local("a.txt")["sha256"] is None

    def test_digests_are_reused_from_the_manifest(self, store, tmp_path):
        from ifetch.manifest import Manifest

        root = self.build_mirror(tmp_path / "dest")
        manifest = Manifest(root)
        manifest.record_file(root / "a.txt")
        recorded = manifest.get(root / "a.txt")["sha256"]

        LocalScanner(store, root, manifest=manifest).scan()
        assert store.get_local("a.txt")["sha256"] == recorded

    def test_a_stale_manifest_digest_is_not_reused(self, store, tmp_path):
        """If the file changed since it was recorded, the digest is a lie."""
        from ifetch.manifest import Manifest

        root = self.build_mirror(tmp_path / "dest")
        manifest = Manifest(root)
        manifest.record_file(root / "a.txt")
        (root / "a.txt").write_bytes(b"different length entirely")

        LocalScanner(store, root, manifest=manifest).scan()
        assert store.get_local("a.txt")["sha256"] is None

    def test_expanded_packages_are_one_item_not_a_directory_of_items(self, store, tmp_path):
        root = tmp_path / "dest"
        bundle = root / "Deck.key"
        (bundle / "Data").mkdir(parents=True)
        (bundle / "Index.zip").write_bytes(b"index")
        (bundle / "Data" / "img.jpg").write_bytes(b"jpeg")

        store.record_local(LocalItem("Deck.key", kind=KIND_PACKAGE))
        LocalScanner(store, root).scan()

        paths = {r["path"] for r in store.iter_local()}
        assert "Deck.key" in paths
        assert not any(p.startswith("Deck.key/") for p in paths)

    def test_scanning_does_not_modify_the_tree(self, store, tmp_path):
        """The read-only guarantee, over the user's files.

        iFetch's own index lives in the destination and is expected to change;
        it is excluded here because it is not the user's data.
        """
        root = self.build_mirror(tmp_path / "dest")

        def user_files():
            return {
                p: (p.stat().st_mtime, p.read_bytes())
                for p in root.rglob("*")
                if p.is_file() and not is_local_artifact(p.relative_to(root).as_posix())
            }

        before = user_files()
        LocalScanner(store, root, rehash=True).scan()
        assert user_files() == before

    def test_artifact_detection(self):
        assert is_local_artifact(".ifetch_index.db")
        assert is_local_artifact(".versions/old/f.txt")
        assert not is_local_artifact("Documents/report.pdf")


# ---------------------------------------------------------------------------
# Planning
# ---------------------------------------------------------------------------

class TestPlanner:
    def plan_for(self, store, tmp_path, remote, local, **kwargs):
        store.record_remote_many(remote)
        store.record_local_many(local)
        return Planner(store, tmp_path, icloud_path="Documents", **kwargs).build()

    def test_new_files_are_downloads(self, store, tmp_path):
        plan = self.plan_for(store, tmp_path, [RemoteItem("a.txt", size=10)], [])
        assert [i.action for i in plan.items] == [ACTION_DOWNLOAD]
        assert plan.bytes_to_transfer == 10

    def test_changed_files_are_overwrites_and_say_what_changes(self, store, tmp_path):
        plan = self.plan_for(
            store, tmp_path, [RemoteItem("a.txt", size=200)], [LocalItem("a.txt", size=100)]
        )
        item = plan.items[0]
        assert item.action == ACTION_OVERWRITE
        assert ".versions" in item.reason

    def test_matching_files_are_skips_and_cost_nothing(self, store, tmp_path):
        plan = self.plan_for(
            store, tmp_path, [RemoteItem("a.txt", size=100)], [LocalItem("a.txt", size=100)]
        )
        assert plan.items[0].action == ACTION_SKIP
        assert plan.bytes_to_transfer == 0

    def test_local_only_files_are_flagged_but_never_deleted(self, store, tmp_path):
        plan = self.plan_for(store, tmp_path, [], [LocalItem("mine.txt", size=5)])
        item = plan.items[0]
        assert item.action == ACTION_ORPHAN
        assert "never deletes" in item.reason

    def test_packages_need_verification_rather_than_a_size_comparison(self, store, tmp_path):
        plan = self.plan_for(
            store, tmp_path,
            [RemoteItem("Deck.key", kind=KIND_PACKAGE, size=900)],
            [LocalItem("Deck.key", kind=KIND_PACKAGE, size=1500)],
        )
        assert plan.items[0].action == ACTION_VERIFY

    def test_files_with_no_reported_size_are_counted_separately(self, store, tmp_path):
        """They make the byte total a lower bound, and the user must know."""
        plan = self.plan_for(
            store, tmp_path,
            [RemoteItem("a.txt", size=None), RemoteItem("b.txt", size=100)], [],
        )
        assert plan.bytes_to_transfer == 100
        assert plan.unknown_size_count == 1

    def test_disk_requirement_includes_headroom(self, store, tmp_path):
        plan = self.plan_for(store, tmp_path, [RemoteItem("a.txt", size=1000)], [])
        assert plan.disk_required_bytes > plan.bytes_to_transfer

    def test_nothing_to_do_requires_no_disk(self, store, tmp_path):
        plan = self.plan_for(
            store, tmp_path, [RemoteItem("a.txt", size=10)], [LocalItem("a.txt", size=10)]
        )
        assert plan.disk_required_bytes == 0

    def test_insufficient_disk_is_detected(self, store, tmp_path):
        plan = self.plan_for(store, tmp_path, [RemoteItem("huge", size=10**15)], [])
        assert plan.disk_sufficient is False
        assert plan.has_risks

    def test_sufficient_disk_is_detected(self, store, tmp_path):
        plan = self.plan_for(store, tmp_path, [RemoteItem("small", size=10)], [])
        assert plan.disk_sufficient is True

    def test_eta_is_unknown_without_evidence_rather_than_guessed(self, store, tmp_path):
        """Inventing a throughput would make the whole plan untrustworthy."""
        plan = self.plan_for(store, tmp_path, [RemoteItem("a", size=10**9)], [])
        assert plan.estimated_seconds is None
        assert plan.throughput_source == "unknown"

    def test_eta_uses_a_supplied_throughput(self, store, tmp_path):
        plan = self.plan_for(
            store, tmp_path, [RemoteItem("a", size=100_000_000)], [],
            throughput_override=10_000_000,
        )
        assert plan.estimated_seconds == pytest.approx(10.0)
        assert "supplied" in plan.throughput_source

    def test_eta_uses_measured_throughput_from_previous_runs(self, store, tmp_path):
        store.set_meta(THROUGHPUT_KEY, "5000000")
        plan = self.plan_for(store, tmp_path, [RemoteItem("a", size=50_000_000)], [])
        assert plan.estimated_seconds == pytest.approx(10.0)
        assert "measured" in plan.throughput_source

    def test_supplied_throughput_beats_the_measured_one(self, store, tmp_path):
        store.set_meta(THROUGHPUT_KEY, "1")
        plan = self.plan_for(
            store, tmp_path, [RemoteItem("a", size=100)], [], throughput_override=100
        )
        assert plan.estimated_seconds == pytest.approx(1.0)

    def test_scan_errors_surface_as_a_risk(self, store, tmp_path):
        store.record_remote(RemoteItem("a.txt", size=1))
        plan = Planner(store, tmp_path).build(
            scan_errors=[{"path": "locked", "error": "HTTP 400"}]
        )
        assert plan.has_risks

    def test_a_clean_plan_has_no_risks(self, store, tmp_path):
        plan = self.plan_for(store, tmp_path, [RemoteItem("a.txt", size=10)], [])
        assert plan.has_risks is False

    def test_plan_is_json_serialisable(self, store, tmp_path):
        plan = self.plan_for(store, tmp_path, [RemoteItem("a.txt", size=10)], [])
        payload = json.loads(json.dumps(plan.to_dict()))
        assert payload["counts"]["download"] == 1


class TestThroughputRecording:
    def test_first_measurement_is_stored(self, store):
        record_throughput(store, 1000, 2.0)
        assert float(store.get_meta(THROUGHPUT_KEY)) == pytest.approx(500.0)

    def test_later_runs_blend_rather_than_replace(self, store):
        """One stalled run must not destroy a good estimate."""
        record_throughput(store, 1000, 1.0)   # 1000 B/s
        record_throughput(store, 100, 1.0)    # 100 B/s
        blended = float(store.get_meta(THROUGHPUT_KEY))
        assert 100 < blended < 1000

    @pytest.mark.parametrize("size,seconds", [(0, 1.0), (100, 0.0), (-5, 1.0)])
    def test_nonsense_measurements_are_ignored(self, store, size, seconds):
        record_throughput(store, size, seconds)
        assert store.get_meta(THROUGHPUT_KEY) is None


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------

class TestRendering:
    def make_plan(self, store, tmp_path):
        store.record_remote_many([
            RemoteItem("new.txt", size=1000),
            RemoteItem("changed.txt", size=2000),
            RemoteItem("same.txt", size=300),
        ])
        store.record_local_many([
            LocalItem("changed.txt", size=100),
            LocalItem("same.txt", size=300),
            LocalItem("mine.txt", size=50),
        ])
        return Planner(store, tmp_path, icloud_path="Documents").build()

    def test_plan_report_names_every_category(self, store, tmp_path):
        text = render_plan(self.make_plan(store, tmp_path), use_colour=False)
        for label in ("New downloads", "Overwrites", "Skipped", "Local only", "Disk required"):
            assert label in text

    def test_plan_report_states_it_transfers_nothing(self, store, tmp_path):
        text = render_plan(self.make_plan(store, tmp_path), use_colour=False)
        assert "dry run" in text.lower()

    def test_plan_report_lists_risks(self, store, tmp_path):
        text = render_plan(self.make_plan(store, tmp_path), use_colour=False)
        assert "Risks" in text and ".versions" in text

    def test_up_to_date_plan_says_so(self, store, tmp_path):
        store.record_remote(RemoteItem("a.txt", size=10))
        store.record_local(LocalItem("a.txt", size=10))
        text = render_plan(Planner(store, tmp_path).build(), use_colour=False)
        assert "already up to date" in text

    def test_audit_report_frames_it_as_reconciliation(self, store, tmp_path):
        text = render_audit(self.make_plan(store, tmp_path), use_colour=False)
        assert "Missing locally" in text and "Missing in iCloud" in text

    def test_matching_mirror_audit_says_it_matches(self, store, tmp_path):
        store.record_remote(RemoteItem("a.txt", size=10))
        store.record_local(LocalItem("a.txt", size=10))
        text = render_audit(Planner(store, tmp_path).build(), use_colour=False)
        assert "matches iCloud" in text

    def test_colour_can_be_disabled(self, store, tmp_path):
        text = render_plan(self.make_plan(store, tmp_path), use_colour=False)
        assert "\033[" not in text

    def test_show_files_limit_is_respected(self, store, tmp_path):
        store.record_remote_many([RemoteItem(f"f{i}.txt", size=1) for i in range(50)])
        text = render_plan(Planner(store, tmp_path).build(), show_files=5, use_colour=False)
        assert "and 45 more" in text


class TestRenderHelpers:
    @pytest.mark.parametrize(
        "value,expected",
        [(None, "unknown"), (0, "0 B"), (512, "512 B"), (1024, "1.0 KB"),
         (1536, "1.5 KB"), (1024**3, "1.0 GB")],
    )
    def test_human_bytes(self, value, expected):
        assert human_bytes(value) == expected

    @pytest.mark.parametrize(
        "value,expected",
        [(None, "unknown"), (0.4, "under a second"), (45, "45s"),
         (90, "1m 30s"), (3700, "1h 1m"), (200000, "2d 7h")],
    )
    def test_human_duration(self, value, expected):
        assert human_duration(value) == expected

    def test_table_aligns_and_includes_every_row(self):
        text = table(["a", "b"], [["1", "22"], ["333", "4"]])
        assert len(text.splitlines()) == 4  # header + rule + 2 rows

    def test_table_truncates_the_widest_column_keeping_the_tail(self):
        long_path = "a/" * 100 + "important.txt"
        text = table(["status", "path"], [["ok", long_path]], max_width=50)
        assert "important.txt" in text
        assert all(len(line) <= 60 for line in text.splitlines())

    def test_empty_table_still_renders_headers(self):
        assert "status" in table(["status", "path"], [])
