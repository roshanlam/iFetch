"""Tests for the shared-folder validation harness (:mod:`ifetch.sharecheck`).

The harness itself needs two Apple IDs to be useful. What can be tested without
them is the part that decides what a run is allowed to *claim*: that a skipped
step is never counted as a pass, that a failure at step 4 is called out as the
known bug, and that a pass at step 5 which did not actually exercise inheritance
says so.

Those are the properties that make the report trustworthy, and they are exactly
the ones that would rot silently if untested.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))

from ifetch import sharecheck as sc  # noqa: E402
from ifetch import sharecheck_cli as sc_cli  # noqa: E402
from ifetch.manifest import Manifest  # noqa: E402
from ifetch.sharing import SHARE_ID_KEY, SOURCE_INHERITED, SOURCE_OWN  # noqa: E402


class FakeNode:
    def __init__(self, data=None):
        self.data = dict(data or {})


class FakeDownloader:
    """Stands in for an authenticated DownloadManager.

    ``layout`` maps a remote path to the files it should produce. A path mapped
    to ``None`` fails to resolve; a path mapped to an exception is raised, which
    is how Apple's HTTP 400 on a share subfolder is reproduced.
    """

    def __init__(self, layout, nodes=None, transferred=0):
        self.layout = layout
        self.nodes = nodes or {}
        self.transferred = transferred
        self.downloads = []
        self.resolved = []

    def get_drive_item(self, path):
        self.resolved.append(path)
        node = self.nodes.get(path, "__missing__")
        if node == "__missing__":
            return FakeNode({"name": path}) if path in self.layout else None
        if isinstance(node, Exception):
            raise node
        return node

    def download(self, remote, local, **kwargs):
        self.downloads.append(remote)
        entry = self.layout.get(remote)
        if isinstance(entry, Exception):
            raise entry
        target = Path(local)
        target.mkdir(parents=True, exist_ok=True)
        for name in entry or []:
            path = target / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(f"contents of {name}")

    def generate_summary_report(self):
        return {"summary": {"total_bytes_transferred": self.transferred}}


def working_layout():
    return {
        "SharedTest": ["root-file.txt"],
        "SharedTest/nested": ["nested-file.txt"],
        "SharedTest/nested/deeper": ["deepest.txt"],
    }


def inherited_nodes():
    """Share root owns the ID; everything below inherits it, as after the fix."""
    return {
        "SharedTest": FakeNode({SHARE_ID_KEY: "SHARE-ABC"}),
        "SharedTest/nested/deeper": FakeNode({
            SHARE_ID_KEY: "SHARE-ABC",
            "_ifetch_share_source": SOURCE_INHERITED,
            "_ifetch_share_depth": 2,
        }),
    }


def run(layout=None, nodes=None, transferred=0, tmp_path=None, **kwargs):
    downloader = FakeDownloader(layout or working_layout(), nodes, transferred)
    checker = sc.ShareChecker(
        downloader=downloader,
        share_path="SharedTest",
        workdir=tmp_path,
        keep=True,
        **kwargs,
    )
    return checker.run(), downloader


# ---------------------------------------------------------------------------
# The verdict
# ---------------------------------------------------------------------------


class TestVerdict:
    def test_a_fully_working_share_validates(self, tmp_path):
        report, _ = run(nodes=inherited_nodes(), tmp_path=tmp_path)
        _write_manifest_for(tmp_path / "root")
        report, _ = run(nodes=inherited_nodes(), tmp_path=tmp_path)
        assert report.verdict == sc.VALIDATED
        assert report.counts()[sc.PASS] == len(report.steps)

    def test_a_failed_step_is_broken(self, tmp_path):
        layout = working_layout()
        layout["SharedTest/nested"] = []
        report, _ = run(layout, nodes=inherited_nodes(), tmp_path=tmp_path)
        assert report.verdict == sc.BROKEN

    def test_a_skipped_step_is_never_a_pass(self, tmp_path):
        """The whole point: 'we never got that far' must not read as 'fine'."""
        report, _ = run({"SharedTest": None}, nodes={"SharedTest": None},
                        tmp_path=tmp_path)
        assert report.verdict != sc.VALIDATED
        assert any(s.status == sc.SKIPPED for s in report.steps)
        assert not any(s.status == sc.PASS for s in report.steps)

    def test_a_run_with_no_steps_is_incomplete_not_validated(self):
        assert sc.ShareCheckReport(share_path="x").verdict == sc.INCOMPLETE

    def test_skipped_steps_name_the_precondition_that_failed(self, tmp_path):
        report, _ = run({"SharedTest": None}, nodes={"SharedTest": None},
                        tmp_path=tmp_path)
        skipped = [s for s in report.steps if s.status == sc.SKIPPED]
        assert skipped
        assert all(s.detail for s in skipped)


# ---------------------------------------------------------------------------
# Step 4 — the case that fails in other clients
# ---------------------------------------------------------------------------


class TestTheCriticalStep:
    def test_an_http_400_below_the_share_root_fails_step_four(self, tmp_path):
        layout = working_layout()
        layout["SharedTest/nested"] = RuntimeError("HTTP 400 Bad Request")
        report, _ = run(layout, nodes=inherited_nodes(), tmp_path=tmp_path)
        step = report.critical_step
        assert step.number == 4
        assert step.status == sc.ERROR

    def test_an_empty_subfolder_download_fails_rather_than_passing_quietly(self, tmp_path):
        layout = working_layout()
        layout["SharedTest/nested"] = []
        report, _ = run(layout, nodes=inherited_nodes(), tmp_path=tmp_path)
        assert report.critical_step.status == sc.FAIL

    def test_a_step_four_failure_is_called_out_in_the_report(self, tmp_path):
        layout = working_layout()
        layout["SharedTest/nested"] = []
        report, _ = run(layout, nodes=inherited_nodes(), tmp_path=tmp_path)
        text = sc.render_report(report)
        assert "#15" in text and "9477" in text

    def test_the_callout_also_fires_when_step_four_raises(self, tmp_path):
        """Apple's HTTP 400 arrives as an exception, not a tidy False.

        The callout originally checked only for `fail`, so the one failure mode
        it exists to explain — the real one — printed nothing.
        """
        layout = working_layout()
        layout["SharedTest/nested"] = RuntimeError("HTTP 400 Bad Request")
        report, _ = run(layout, nodes=inherited_nodes(), tmp_path=tmp_path)
        assert report.critical_step.status == sc.ERROR
        assert "#15" in sc.render_report(report)

    def test_step_four_walks_both_levels(self, tmp_path):
        _, downloader = run(nodes=inherited_nodes(), tmp_path=tmp_path)
        assert "SharedTest/nested" in downloader.downloads
        assert "SharedTest/nested/deeper" in downloader.downloads

    def test_step_five_is_skipped_when_step_four_failed(self, tmp_path):
        """There is nothing to inspect if the folder never came down."""
        layout = working_layout()
        layout["SharedTest/nested"] = []
        report, _ = run(layout, nodes=inherited_nodes(), tmp_path=tmp_path)
        five = next(s for s in report.steps if s.number == 5)
        assert five.status == sc.SKIPPED


# ---------------------------------------------------------------------------
# Step 5 — did it work for the reason we think?
# ---------------------------------------------------------------------------


class TestShareIdInheritance:
    def test_an_inherited_id_is_reported_as_the_fix_being_exercised(self, tmp_path):
        report, _ = run(nodes=inherited_nodes(), tmp_path=tmp_path)
        five = next(s for s in report.steps if s.number == 5)
        assert five.status == sc.PASS
        assert any("#15" in e for e in five.evidence)

    def test_a_missing_id_fails_even_though_the_download_worked(self, tmp_path):
        """Downloads can succeed while the request is still unscoped.

        That is a passing run today and a broken one the moment Apple starts
        requiring the ID, so it must not be recorded as a validation.
        """
        nodes = {"SharedTest": FakeNode({SHARE_ID_KEY: "SHARE-ABC"}),
                 "SharedTest/nested/deeper": FakeNode({"name": "deeper"})}
        report, _ = run(nodes=nodes, tmp_path=tmp_path)
        five = next(s for s in report.steps if s.number == 5)
        assert five.status == sc.FAIL
        assert report.verdict == sc.BROKEN

    def test_an_owned_id_passes_but_says_inheritance_was_not_tested(self, tmp_path):
        """If Apple happened to send the ID, the fix was never exercised."""
        nodes = {
            "SharedTest": FakeNode({SHARE_ID_KEY: "SHARE-ABC"}),
            "SharedTest/nested/deeper": FakeNode({
                SHARE_ID_KEY: "SHARE-ABC", "_ifetch_share_source": SOURCE_OWN,
            }),
        }
        report, _ = run(nodes=nodes, tmp_path=tmp_path)
        five = next(s for s in report.steps if s.number == 5)
        assert five.status == sc.PASS
        assert any("untested" in e for e in five.evidence)


# ---------------------------------------------------------------------------
# Step 2 — is this even a share?
# ---------------------------------------------------------------------------


class TestShareVisibility:
    def test_a_folder_you_own_stops_the_run_rather_than_validating(self, tmp_path):
        """Pointed at your own folder, every step would otherwise pass.

        The files download, they verify, the re-run skips — and the report would
        say "validated" having tested nothing about shares at all. A validation
        that passes on the wrong input is worse than one that fails.
        """
        nodes = {"SharedTest": FakeNode({"name": "SharedTest"})}
        report, _ = run(nodes=nodes, tmp_path=tmp_path)
        two = next(s for s in report.steps if s.number == 2)
        assert two.status == sc.SKIPPED
        assert "not a folder shared with you" in two.detail
        assert report.verdict == sc.INCOMPLETE
        assert not any(s.status == sc.PASS for s in report.steps)

    def test_assume_shared_overrides_that_stop(self, tmp_path):
        """An escape hatch for a share whose root genuinely carries no ID."""
        nodes = {"SharedTest": FakeNode({"name": "SharedTest"})}
        report, _ = run(nodes=nodes, tmp_path=tmp_path, assume_shared=True)
        two = next(s for s in report.steps if s.number == 2)
        assert two.status == sc.PASS

    def test_the_skip_reason_distinguishes_not_found_from_not_shared(self, tmp_path):
        """Two different problems, two different fixes."""
        not_found, _ = run({"SharedTest": None}, nodes={"SharedTest": None},
                           tmp_path=tmp_path)
        not_shared, _ = run(nodes={"SharedTest": FakeNode({"name": "mine"})},
                            tmp_path=tmp_path)
        three_a = next(s for s in not_found.steps if s.number == 3)
        three_b = next(s for s in not_shared.steps if s.number == 3)
        assert three_a.detail != three_b.detail
        assert "did not resolve" in three_a.detail
        assert "not shared with you" in three_b.detail

    def test_an_unresolvable_share_fails(self, tmp_path):
        report, _ = run({"SharedTest": None}, nodes={"SharedTest": None},
                        tmp_path=tmp_path)
        two = next(s for s in report.steps if s.number == 2)
        assert two.status == sc.FAIL


# ---------------------------------------------------------------------------
# Read-only, and other safety properties
# ---------------------------------------------------------------------------


class TestSafety:
    def test_nothing_is_written_to_icloud(self, tmp_path):
        """A validation that could cost you data is not worth running."""
        _, downloader = run(nodes=inherited_nodes(), tmp_path=tmp_path)
        for forbidden in ("upload", "mkdir", "rename", "delete", "move"):
            assert not hasattr(downloader, forbidden) or not getattr(
                downloader, forbidden, None
            )

    def test_an_exception_in_one_step_does_not_abort_the_rest(self, tmp_path):
        layout = working_layout()
        layout["SharedTest"] = RuntimeError("boom")
        report, _ = run(layout, nodes=inherited_nodes(), tmp_path=tmp_path)
        assert len(report.steps) >= 4
        assert any(s.status == sc.ERROR for s in report.steps)

    def test_an_error_detail_stays_on_one_line_and_bounded(self, tmp_path):
        layout = working_layout()
        layout["SharedTest"] = RuntimeError("line one\nline two\n" + "x" * 500)
        report, _ = run(layout, nodes=inherited_nodes(), tmp_path=tmp_path)
        step = next(s for s in report.steps if s.status == sc.ERROR)
        assert "\n" not in step.detail
        assert len(step.detail) <= 300

    def test_the_temporary_directory_is_removed_when_not_kept(self):
        downloader = FakeDownloader(working_layout(), inherited_nodes())
        checker = sc.ShareChecker(downloader, "SharedTest")
        workdir = checker.workdir
        checker.run()
        assert not workdir.exists()

    def test_keep_leaves_the_downloads_in_place(self):
        downloader = FakeDownloader(working_layout(), inherited_nodes())
        checker = sc.ShareChecker(downloader, "SharedTest", keep=True)
        checker.run()
        assert checker.workdir.exists()
        import shutil
        shutil.rmtree(checker.workdir, ignore_errors=True)


# ---------------------------------------------------------------------------
# Offline verification step
# ---------------------------------------------------------------------------


def _write_manifest_for(root: Path) -> None:
    root.mkdir(parents=True, exist_ok=True)
    manifest = Manifest(root)
    for path in sorted(p for p in root.rglob("*") if p.is_file()):
        if path.name.startswith("."):
            continue
        manifest.record_file(path)
    manifest.save()


class TestOfflineVerification:
    def test_a_missing_manifest_fails_rather_than_being_skipped(self, tmp_path):
        """No manifest means the integrity promise was not kept for these files."""
        report, _ = run(nodes=inherited_nodes(), tmp_path=tmp_path)
        six = next(s for s in report.steps if s.number == 6)
        assert six.status == sc.FAIL
        assert "manifest" in six.detail

    def test_a_matching_manifest_passes(self, tmp_path):
        run(nodes=inherited_nodes(), tmp_path=tmp_path)
        _write_manifest_for(tmp_path / "root")
        report, _ = run(nodes=inherited_nodes(), tmp_path=tmp_path)
        six = next(s for s in report.steps if s.number == 6)
        assert six.status == sc.PASS

    def test_corrupted_contents_fail(self, tmp_path):
        """Tamper after the download, then verify.

        The step is exercised on its own here because a full run re-downloads
        and would overwrite the damage before the check ever saw it.
        """
        run(nodes=inherited_nodes(), tmp_path=tmp_path)
        root = tmp_path / "root"
        _write_manifest_for(root)
        (root / "root-file.txt").write_text("tampered")

        checker = sc.ShareChecker(
            FakeDownloader(working_layout(), inherited_nodes()),
            "SharedTest", workdir=tmp_path, keep=True,
        )
        step = sc.StepResult(number=6, name="offline verify")
        checker.step_offline_verify(step)

        assert step.status == sc.FAIL
        assert any("root-file.txt" in e for e in step.evidence)


class TestRerunStep:
    def test_a_rerun_that_transfers_bytes_fails(self, tmp_path):
        report, _ = run(nodes=inherited_nodes(), transferred=4096, tmp_path=tmp_path)
        seven = next(s for s in report.steps if s.number == 7)
        assert seven.status == sc.FAIL
        assert "4,096" in seven.detail

    def test_a_rerun_that_transfers_nothing_passes(self, tmp_path):
        report, _ = run(nodes=inherited_nodes(), tmp_path=tmp_path)
        seven = next(s for s in report.steps if s.number == 7)
        assert seven.status == sc.PASS


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------


class TestOutput:
    def test_the_markdown_row_has_a_cell_per_column(self, tmp_path):
        report, _ = run(nodes=inherited_nodes(), tmp_path=tmp_path)
        row = report.markdown_row()
        assert row.startswith("| ") and row.endswith(" |")
        assert row.count("|") == 10

    def test_the_markdown_row_records_the_verdict(self, tmp_path):
        report, _ = run(nodes={"SharedTest": FakeNode({"name": "mine"})},
                        tmp_path=tmp_path)
        assert sc.INCOMPLETE in report.markdown_row()

    def test_an_incomplete_run_says_it_proves_less_than_a_pass(self, tmp_path):
        report, _ = run(nodes={"SharedTest": FakeNode({"name": "mine"})},
                        tmp_path=tmp_path)
        assert "unanswered question" in sc.render_report(report)

    def test_to_dict_is_json_serialisable(self, tmp_path):
        import json

        report, _ = run(nodes=inherited_nodes(), tmp_path=tmp_path)
        assert json.loads(json.dumps(report.to_dict()))["verdict"] == report.verdict


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


class TestCli:
    def _run(self, tmp_path, layout=None, nodes=None, extra=()):
        downloader = FakeDownloader(layout or working_layout(),
                                    nodes or inherited_nodes())
        argv = ["SharedTest", "--workdir", str(tmp_path), "--keep", *extra]
        import io

        out = io.StringIO()
        code = sc_cli.main(argv, stdout=out, downloader=downloader)
        return code, out.getvalue()

    def test_a_broken_share_exits_one(self, tmp_path):
        layout = working_layout()
        layout["SharedTest/nested"] = []
        code, _ = self._run(tmp_path, layout)
        assert code == sc_cli.EXIT_BROKEN

    def test_an_incomplete_run_has_its_own_exit_code(self, tmp_path):
        """Not 0. A CI job treating 'incomplete' as success defeats the point."""
        code, _ = self._run(tmp_path, nodes={"SharedTest": FakeNode({"name": "mine"})})
        assert code == sc_cli.EXIT_INCOMPLETE
        assert code != sc_cli.EXIT_OK

    def test_a_full_pass_exits_zero(self, tmp_path):
        self._run(tmp_path)
        _write_manifest_for(tmp_path / "root")
        code, _ = self._run(tmp_path)
        assert code == sc_cli.EXIT_OK

    def test_json_output_parses(self, tmp_path):
        import json

        _, text = self._run(tmp_path, extra=["--json"])
        payload = json.loads(text[text.index("{"):])
        assert "verdict" in payload and "steps" in payload

    def test_the_report_file_is_written(self, tmp_path):
        import json

        path = tmp_path / "out.json"
        self._run(tmp_path, extra=["--report", str(path)])
        assert json.loads(path.read_text())["share_path"] == "SharedTest"

    def test_a_failed_authentication_exits_two_with_a_pointer(self, tmp_path, capsys):
        def boom(args, stdout):
            raise RuntimeError("invalid session token")

        import io

        original = sc_cli._authenticate
        sc_cli._authenticate = boom
        try:
            code = sc_cli.main(["SharedTest"], stdout=io.StringIO())
        finally:
            sc_cli._authenticate = original
        assert code == sc_cli.EXIT_ERROR
        assert "auth doctor" in capsys.readouterr().err
