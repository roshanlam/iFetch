"""Tests for the live ADP validation harness (:mod:`ifetch.adpcheck`, #31)."""

import io
import json
import sys
from pathlib import Path

import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))

from ifetch import adpcheck as ac  # noqa: E402
from ifetch import adpcheck_cli as ac_cli  # noqa: E402
from ifetch.auth import Check, Diagnosis, PCSCookieState  # noqa: E402
from ifetch.manifest import Manifest  # noqa: E402
from ifetch.models import DownloadStatus  # noqa: E402


# -- fakes ----------------------------------------------------------------------

def diagnosis(adp_state="on", web_access="enabled", adp_status="info",
              drive_status="ok", extra=()):
    checks = [
        Check(name="live_authentication", status="ok", detail="Apple accepted the stored session."),
        Check(name="trusted_session", status="ok", detail="Session is trusted."),
        Check(name="advanced_data_protection", status=adp_status,
              detail="ADP detail", remedy="ADP remedy" if adp_status == "fail" else "",
              data={"state": adp_state, "web_access": web_access}),
        Check(name="drive_access", status=drive_status,
              detail="iCloud Drive responded with 3 top-level items."),
        *extra,
    ]
    return Diagnosis(account="alice@example.com", region="global", checks=checks)


def live_cookie():
    return PCSCookieState(service="drive", present=("X-APPLE-WEBAUTH-PCS-Documents",))


def missing_cookie():
    return PCSCookieState(service="drive", missing=("X-APPLE-WEBAUTH-PCS-Documents",))


class FakeDownloader:
    """Behaves like DownloadManager where it matters: results accumulate.

    The first download transfers every file; a second one into the same
    directory finds them unchanged and transfers nothing - but, exactly as in
    the real class, ``download_results`` keeps the first call's entries.
    """

    def __init__(self, files=("a.txt", "b.txt"), fail=0, resolves=True, rerun_bytes=0):
        self.files = list(files)
        self.fail = fail
        self.resolves = resolves
        self.rerun_bytes = rerun_bytes
        self.download_results = []
        self.calls = 0

    def get_drive_item(self, path):
        return object() if self.resolves else None

    def _list_child_names(self, item):
        return list(self.files)

    def download(self, remote, local, **kwargs):
        self.calls += 1
        root = Path(local)
        root.mkdir(parents=True, exist_ok=True)
        first = self.calls == 1
        for name in self.files:
            path = root / name
            if first:
                path.write_text(f"contents of {name}")
            size = path.stat().st_size
            downloaded = size if first else self.rerun_bytes
            self.download_results.append(DownloadStatus(
                path=str(path), size=size, downloaded=downloaded,
                status="completed" if first else "skipped",
            ))
        for i in range(self.fail):
            self.download_results.append(DownloadStatus(
                path=str(root / f"bad{i}"), size=1, downloaded=0, status="failed",
                error="Missing PCS cookies from the request (423)",
            ))
        if first:
            manifest = Manifest(root)
            for path in sorted(root.glob("*.txt")):
                manifest.record_file(path)
            manifest.save()

    def generate_summary_report(self):  # cumulative, like the real thing
        return {"summary": {
            "total_bytes_transferred": sum(r.downloaded for r in self.download_results),
            "failed": sum(r.status == "failed" for r in self.download_results),
        }}


def renewed(status="already_present", waited=0.0):
    return {"account": "alice@example.com", "renewed": True,
            "adp": {"service": "drive", "status": status, "waited_seconds": waited}}


def run(tmp_path, *, diag=None, downloader=None, cookie=None, renew=None, **kwargs):
    downloader = downloader or FakeDownloader()
    calls = {"doctor": 0, "renew": 0}

    def doctor():
        calls["doctor"] += 1
        return diag or diagnosis()

    def do_renew():
        calls["renew"] += 1
        return renew if renew is not None else renewed()

    checker = ac.ADPChecker(
        folder="ADPTest", doctor=doctor, downloader=downloader,
        cookies=lambda: cookie or live_cookie(), renew=do_renew,
        account="alice@example.com", workdir=tmp_path / "work", keep=True, **kwargs,
    )
    return checker.run(), downloader, calls


def step(report, number):
    return next(s for s in report.steps if s.number == number)


# -- verdicts -----------------------------------------------------------------

def test_a_healthy_adp_account_validates(tmp_path):
    report, downloader, calls = run(tmp_path)
    assert report.verdict == ac.VALIDATED, [s.to_dict() for s in report.steps]
    assert [s.number for s in report.steps] == list(ac.STEP_NUMBERS)
    assert calls == {"doctor": 1, "renew": 1}  # one doctor run feeds steps 1 and 2
    assert downloader.calls == 2


def test_account_without_adp_stops_rather_than_passing(tmp_path):
    report, downloader, calls = run(tmp_path, diag=diagnosis(adp_state="off", adp_status="ok"))
    assert report.verdict == ac.INCOMPLETE
    assert step(report, 1).status == ac.SKIPPED
    assert all(s.status == ac.SKIPPED for s in report.steps[1:])
    assert downloader.calls == 0 and calls["renew"] == 0


def test_undetermined_adp_can_be_assumed(tmp_path):
    report, _, _ = run(tmp_path, diag=diagnosis(adp_state="undetermined"), assume_adp=True)
    assert report.verdict == ac.VALIDATED
    assert "--assume-adp" in step(report, 1).detail
    assert step(report, 1).evidence  # the caveat is recorded, not hidden


def test_web_access_off_is_a_named_failure(tmp_path):
    diag = diagnosis(web_access="disabled", adp_status="fail")
    report, downloader, _ = run(tmp_path, diag=diag)
    assert report.verdict == ac.BROKEN
    assert step(report, 1).status == ac.FAIL
    assert "ADP remedy" in step(report, 1).evidence
    assert downloader.calls == 0


def test_drive_423_fails_the_doctor_step_and_skips_downloads(tmp_path):
    diag = diagnosis(drive_status="fail")
    report, downloader, _ = run(tmp_path, diag=diag)
    assert step(report, 2).status == ac.FAIL
    assert step(report, 4).status == ac.SKIPPED
    assert downloader.calls == 0


def test_missing_pcs_cookie_fails_with_the_renew_remedy(tmp_path):
    report, _, _ = run(tmp_path, cookie=missing_cookie())
    three = step(report, 3)
    assert three.status == ac.FAIL
    assert any("ifetch auth renew --adp" in e for e in three.evidence)


def test_failed_downloads_fail_step_5(tmp_path):
    report, _, _ = run(tmp_path, downloader=FakeDownloader(fail=2))
    assert step(report, 5).status == ac.FAIL
    assert "2 file(s) failed" in step(report, 5).detail


def test_unresolvable_folder_fails_and_skips_the_rest_of_the_transfer(tmp_path):
    report, _, _ = run(tmp_path, downloader=FakeDownloader(resolves=False))
    assert step(report, 4).status == ac.FAIL
    assert {step(report, n).status for n in (5, 6, 7)} == {ac.SKIPPED}
    assert step(report, 8).status == ac.PASS  # renew is independent of the folder


def test_rerun_counts_only_its_own_transfers(tmp_path):
    """The real downloader's results accumulate across calls; step 7 must not
    blame the second run for the first run's bytes."""
    report, downloader, _ = run(tmp_path)
    assert downloader.generate_summary_report()["summary"]["total_bytes_transferred"] > 0
    assert step(report, 7).status == ac.PASS


def test_rerun_that_transfers_fails(tmp_path):
    report, _, _ = run(tmp_path, downloader=FakeDownloader(rerun_bytes=5))
    assert step(report, 7).status == ac.FAIL


@pytest.mark.parametrize("payload, status", [
    (renewed("already_present"), ac.PASS),
    (renewed("not_required"), ac.PASS),
    (renewed("acquired", waited=0.4), ac.PASS),
    (renewed("acquired", waited=42.0), ac.FAIL),
    ({"renewed": False, "summary": "Apple wants a 2FA code", "remedy": "run renew"}, ac.FAIL),
])
def test_unattended_renew(tmp_path, payload, status):
    report, _, _ = run(tmp_path, renew=payload)
    assert step(report, 8).status == status


def test_a_crashing_step_is_an_error_not_an_abort(tmp_path):
    def boom():
        raise RuntimeError("keyring locked")

    checker = ac.ADPChecker(
        folder="ADPTest", doctor=diagnosis, downloader=FakeDownloader(),
        cookies=boom, renew=renewed, workdir=tmp_path, keep=True,
    )
    report = checker.run()
    assert step(report, 3).status == ac.ERROR
    assert len(report.steps) == len(ac.STEP_NUMBERS)
    assert report.verdict == ac.BROKEN


# -- what gets published ----------------------------------------------------------

def test_account_is_masked_everywhere_it_is_published(tmp_path):
    report, _, _ = run(tmp_path)
    text = json.dumps(report.to_dict()) + ac.render_report(report)
    assert "alice@example.com" not in text
    assert report.account == "a***@example.com"


@pytest.mark.parametrize("raw, masked", [
    ("bob@icloud.com", "b***@icloud.com"), ("", ""), ("nodomain", "***"),
])
def test_mask_account(raw, masked):
    assert ac.mask_account(raw) == masked


def test_markdown_row_has_one_cell_per_step(tmp_path):
    report, _, _ = run(tmp_path)
    row = report.markdown_row()
    cells = [c.strip() for c in row.strip("|").split("|")]
    assert len(cells) == 3 + len(ac.STEP_NUMBERS) + 1
    assert cells[-1] == "verdict: validated"
    assert cells[3:-1] == ["PASS"] * len(ac.STEP_NUMBERS)


def test_temp_workdir_is_removed_unless_kept():
    checker = ac.ADPChecker(
        folder="ADPTest", doctor=diagnosis, downloader=FakeDownloader(),
        cookies=live_cookie, renew=renewed,
    )
    workdir = checker.workdir
    checker.run()
    assert not workdir.exists()


# -- the CLI ----------------------------------------------------------------------

def test_cli_exit_codes_and_report_file(tmp_path, monkeypatch):
    monkeypatch.setenv("ICLOUD_EMAIL", "alice@example.com")
    out = io.StringIO()
    report_path = tmp_path / "adp.json"
    code = ac_cli.main(
        ["ADPTest", "--report", str(report_path), "--workdir", str(tmp_path / "w")],
        stdout=out, downloader=FakeDownloader(), doctor=diagnosis,
        cookies=live_cookie, renew=renewed,
    )
    assert code == ac_cli.EXIT_OK
    saved = json.loads(report_path.read_text())
    assert saved["verdict"] == "validated"
    assert saved["account"] == "a***@example.com"
    assert "docs/adp-validation.md" in out.getvalue()


def test_cli_incomplete_has_its_own_exit_code(monkeypatch):
    monkeypatch.setenv("ICLOUD_EMAIL", "alice@example.com")
    code = ac_cli.main(
        ["ADPTest"], stdout=io.StringIO(), downloader=FakeDownloader(),
        doctor=lambda: diagnosis(adp_state="off", adp_status="ok"),
        cookies=live_cookie, renew=renewed,
    )
    assert code == ac_cli.EXIT_INCOMPLETE


def test_cli_without_an_account_is_a_start_error(monkeypatch):
    monkeypatch.delenv("ICLOUD_EMAIL", raising=False)
    assert ac_cli.main(["ADPTest"], stdout=io.StringIO()) == ac_cli.EXIT_ERROR


def test_default_renew_runs_auth_renew_unattended(monkeypatch):
    seen = {}

    def fake_main(argv, stdout=None):
        seen["argv"] = argv
        print(json.dumps(renewed()), file=stdout)
        return 0

    monkeypatch.setattr("ifetch.auth_cli.main", fake_main)
    args = ac_cli.build_parser().parse_args(["ADPTest", "--region", "china"])
    payload = ac_cli._default_renew(args, "alice@example.com")()
    assert payload["renewed"] is True
    argv = seen["argv"]
    assert argv[0] == "renew"
    for flag in ("--adp", "--non-interactive", "--no-stdin", "--json"):
        assert flag in argv
    assert argv[argv.index("--region") + 1] == "china"


def test_subcommand_is_registered():
    from ifetch.cli import SUBCOMMANDS

    assert SUBCOMMANDS["adpcheck"] == ("adpcheck_cli", "main")
