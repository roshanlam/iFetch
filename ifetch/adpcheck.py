"""Running the live Advanced Data Protection validation as one command.

Why this exists
---------------
iFetch's ADP support (the PCS-cookie flow: ``requestWebAccessState``, device
consent, bounded ``requestPCS`` polling) is pinned by ``tests/test_adp.py``,
which replays Apple's recorded responses. That proves iFetch reacts correctly
to the replies we believe Apple sends. It cannot prove Apple sends them, and
until someone runs it against a real ADP Apple ID the README has to call the
support "replay-proven, not field-proven" (issue #31).

Turning ADP on needs a physical device and a recovery key, so that part stays
manual. This module is everything after it: the acceptance procedure from #31 -
``auth doctor`` -> list -> download -> renew - as one command with a verdict and
a row to paste into ``docs/adp-validation.md``.

Read-only
---------
Every step lists, downloads or re-validates the stored session. Nothing is
written to iCloud, and downloads go to a temporary directory that is removed
afterwards unless ``--keep`` is given.

What a result means
-------------------
Steps are ``pass``, ``fail``, ``skipped`` or ``error``, exactly as in
:mod:`ifetch.sharecheck`, and the verdict is only ``validated`` when every step
passed. The first step is the guard that keeps the run honest: on an account
*without* ADP every later step passes trivially - no PCS cookie is needed, so
none is tested - and a report that said ``validated`` there would be the exact
false claim this command exists to prevent. So an account iFetch cannot show
to be under ADP stops the run unless ``--assume-adp`` is given.
"""

from __future__ import annotations

import shutil
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from .render import plural, rule, table
from .sharecheck import (
    BROKEN,
    ERROR,
    FAIL,
    INCOMPLETE,
    PASS,
    SKIPPED,
    VALIDATED,
    StepResult,
    _STATUS_MARK,
    _versions,
)

#: PCS outcomes that prove an unattended re-run needed no human.
_UNATTENDED_PCS = ("already_present", "not_required")

#: An "acquired" PCS result this fast cannot have waited for a tap on a device.
_NO_HUMAN_SECONDS = 5.0

#: The step numbers, which the docs table columns are keyed on.
STEP_NUMBERS = (1, 2, 3, 4, 5, 6, 7, 8)


def mask_account(account: str) -> str:
    """``alice@example.com`` -> ``a***@example.com``: safe to publish."""
    account = (account or "").strip()
    if "@" not in account:
        return "***" if account else ""
    user, _, domain = account.partition("@")
    return f"{user[:1]}***@{domain}"


@dataclass
class ADPCheckReport:
    """Every step, the verdict, and what the verdict is allowed to claim."""

    folder: str
    account: str = ""
    adp_service: str = "drive"
    steps: List[StepResult] = field(default_factory=list)
    ifetch_version: str = ""
    pyicloud_version: str = ""
    started_at: float = 0.0

    @property
    def verdict(self) -> str:
        if any(s.status in (FAIL, ERROR) for s in self.steps):
            return BROKEN
        if any(s.status == SKIPPED for s in self.steps):
            return INCOMPLETE
        return VALIDATED if self.steps else INCOMPLETE

    def counts(self) -> Dict[str, int]:
        out = {PASS: 0, FAIL: 0, SKIPPED: 0, ERROR: 0}
        for step in self.steps:
            out[step.status] = out.get(step.status, 0) + 1
        return out

    def to_dict(self) -> Dict[str, Any]:
        return {
            "folder": self.folder,
            "account": self.account,
            "adp_service": self.adp_service,
            "verdict": self.verdict,
            "counts": self.counts(),
            "ifetch_version": self.ifetch_version,
            "pyicloud_version": self.pyicloud_version,
            "started_at": self.started_at,
            "steps": [s.to_dict() for s in self.steps],
        }

    def markdown_row(self) -> str:
        """The row for the results table in docs/adp-validation.md."""
        date = time.strftime("%Y-%m-%d", time.gmtime(self.started_at or time.time()))
        cells = [date, self.ifetch_version or "?", self.pyicloud_version or "?"]
        for number in STEP_NUMBERS:
            step = next((s for s in self.steps if s.number == number), None)
            cells.append(_STATUS_MARK.get(step.status, "--") if step else "--")
        cells.append(f"verdict: {self.verdict}")
        return "| " + " | ".join(cells) + " |"


def _payload_files(root: Path) -> List[Path]:
    """Files that came from iCloud, not iFetch's own bookkeeping."""
    from .index import INDEX_FILENAME
    from .manifest import ARTIFACT_DIRS, ARTIFACT_NAMES

    if not root.exists():
        return []
    out = []
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        rel = path.relative_to(root)
        if rel.parts[0] in ARTIFACT_DIRS:
            continue
        if path.name in ARTIFACT_NAMES or path.name.startswith(INDEX_FILENAME):
            continue
        out.append(path)
    return out


def _check(diagnosis: Any, name: str) -> Optional[Any]:
    for check in getattr(diagnosis, "checks", []) or []:
        if getattr(check, "name", None) == name:
            return check
    return None


class ADPChecker:
    """Runs the procedure. Every iCloud interaction is injected.

    * ``doctor()`` returns an online :class:`~ifetch.auth.Diagnosis` run with
      ADP checking forced on;
    * ``downloader`` is a signed-in :class:`~ifetch.downloader.DownloadManager`;
    * ``cookies()`` returns the stored :class:`~ifetch.auth.PCSCookieState`;
    * ``renew()`` performs an unattended ``ifetch auth renew --adp`` and returns
      its JSON payload.

    Injection is what lets the whole procedure run against fakes in tests; the
    CLI wires up the real ones.
    """

    def __init__(
        self,
        *,
        folder: str,
        doctor: Callable[[], Any],
        downloader: Any,
        cookies: Callable[[], Any],
        renew: Callable[[], Dict[str, Any]],
        account: str = "",
        adp_service: str = "drive",
        workdir: Optional[Path] = None,
        keep: bool = False,
        assume_adp: bool = False,
        log: Optional[Callable[[str], None]] = None,
    ):
        self.folder = folder.strip("/")
        self.doctor = doctor
        self.downloader = downloader
        self.cookies = cookies
        self.renew = renew
        self.keep = keep
        self.assume_adp = assume_adp
        self.log = log or (lambda message: None)
        self._temp_owner = workdir is None
        self.workdir = Path(workdir) if workdir else Path(
            tempfile.mkdtemp(prefix="ifetch-adpcheck-")
        )
        self._diagnosis: Any = None
        versions = _versions()
        self.report = ADPCheckReport(
            folder=self.folder,
            account=mask_account(account),
            adp_service=adp_service,
            ifetch_version=versions["ifetch"],
            pyicloud_version=versions["pyicloud"],
            started_at=time.time(),
        )

    # -- plumbing ---------------------------------------------------------

    def _run_step(self, number: int, name: str, fn: Callable[[StepResult], None],
                  precondition: bool = True, skip_reason: str = "") -> StepResult:
        step = StepResult(number=number, name=name)
        self.report.steps.append(step)
        if not precondition:
            step.status = SKIPPED
            step.detail = skip_reason or "an earlier step did not pass"
            self.log(f"  {number}. {name}: skipped - {step.detail}")
            return step
        started = time.time()
        try:
            fn(step)
        except Exception as exc:  # a step must not abort the rest of the run
            step.status = ERROR
            step.detail = f"{type(exc).__name__}: {exc}".replace("\n", " ")[:300]
        step.duration = time.time() - started
        self.log(f"  {number}. {name}: {step.status}"
                 + (f" - {step.detail}" if step.detail else ""))
        return step

    def _diagnose(self) -> Any:
        if self._diagnosis is None:
            self._diagnosis = self.doctor()
        return self._diagnosis

    def _download(self, target: Path) -> Dict[str, int]:
        """Download the folder; return what *this* call did, not the run total.

        ``DownloadManager.download_results`` accumulates across calls, so the
        summary report after a second call still counts the first call's bytes.
        Only the results appended by this call are counted here.
        """
        results = getattr(self.downloader, "download_results", None)
        before = len(results) if isinstance(results, list) else 0
        self.downloader.download(self.folder, str(target))
        results = getattr(self.downloader, "download_results", None)
        if not isinstance(results, list):
            summary = self.downloader.generate_summary_report().get("summary", {})
            return {
                "failed": int(summary.get("failed", 0)),
                "bytes": int(summary.get("total_bytes_transferred", 0)),
            }
        new = results[before:]
        return {
            "failed": sum(1 for r in new if getattr(r, "status", "") == "failed"),
            "bytes": sum(int(getattr(r, "downloaded", 0) or 0) for r in new),
        }

    # -- the steps --------------------------------------------------------

    def step_account_is_adp(self, step: StepResult) -> None:
        """Confirm this account is actually under ADP, or stop the run."""
        check = _check(self._diagnose(), "advanced_data_protection")
        data = getattr(check, "data", None) or {}
        state = data.get("state")
        web_access = data.get("web_access")
        if check is not None and getattr(check, "status", "") == "fail":
            step.status = FAIL
            step.detail = getattr(check, "detail", "") or "ADP check failed"
            if getattr(check, "remedy", ""):
                step.evidence.append(check.remedy)
            return
        if state == "on":
            step.status = PASS
            step.detail = "Apple reports Advanced Data Protection is in force"
            if web_access:
                step.evidence.append(f"'Access iCloud Data on the Web': {web_access}")
            return
        if self.assume_adp:
            step.status = PASS
            step.detail = (
                f"ADP state reported as '{state or 'unknown'}'; continuing "
                "because --assume-adp was given"
            )
            step.evidence.append(
                "the report is only as good as that assumption: if this account "
                "is not under ADP, no step below exercised the PCS flow"
            )
            return
        step.status = SKIPPED
        step.detail = (
            "Apple does not report ADP as on for this account"
            if state == "off" else
            "whether ADP is on could not be determined"
        )
        step.evidence.append(
            "on an account without ADP every step below passes without testing "
            "the PCS flow, so the run stops here rather than reporting a pass it "
            "did not earn. If you are certain ADP is on, re-run with --assume-adp."
        )

    def step_doctor_healthy(self, step: StepResult) -> None:
        """``auth doctor --online --adp`` reports nothing broken."""
        diagnosis = self._diagnose()
        failing = [c for c in getattr(diagnosis, "checks", []) if c.status == "fail"]
        warnings = [c for c in getattr(diagnosis, "checks", []) if c.status == "warn"]
        if failing:
            step.status = FAIL
            step.detail = "; ".join(f"{c.name}: {c.detail}" for c in failing)[:300]
            step.evidence.extend(c.remedy for c in failing if c.remedy)
            return
        drive = _check(diagnosis, "drive_access")
        if drive is None or drive.status != "ok":
            step.status = FAIL
            step.detail = "the doctor did not reach iCloud Drive"
            return
        step.status = PASS
        step.detail = drive.detail or "iCloud Drive responded"
        step.evidence.extend(f"warning - {c.name}: {c.detail}" for c in warnings)

    def step_pcs_cookie_stored(self, step: StepResult) -> None:
        """A live PCS cookie is on disk, so later runs need no approval."""
        state = self.cookies()
        if getattr(state, "usable", False):
            step.status = PASS
            step.detail = getattr(state, "detail", "a live PCS cookie is stored")
            expires = state.to_dict().get("expires_at") if hasattr(state, "to_dict") else None
            if expires:
                step.evidence.append(f"PCS cookie expires {expires}")
            return
        step.status = FAIL
        step.detail = getattr(state, "detail", "no usable PCS cookie is stored")
        step.evidence.append(
            "run 'ifetch auth renew --adp' and tap Allow on the 'Access iCloud "
            "Data on the Web' prompt on a trusted device, then re-run this check"
        )

    def step_folder_lists(self, step: StepResult) -> None:
        """The folder resolves and its contents can be listed."""
        item = self.downloader.get_drive_item(self.folder)
        if item is None:
            step.status = FAIL
            step.detail = f"'{self.folder}' did not resolve"
            return
        names = self.downloader._list_child_names(item)
        if not names:
            step.status = FAIL
            step.detail = f"'{self.folder}' listed no entries"
            step.evidence.append("put a few files in the folder so there is something to read")
            return
        step.status = PASS
        step.detail = f"'{self.folder}' listed {len(names)} {plural(len(names), 'entry', 'entries')}"

    def step_folder_downloads(self, step: StepResult) -> None:
        """Files come down - the request that fails with 423 without PCS."""
        target = self.workdir / "download"
        outcome = self._download(target)
        files = _payload_files(target)
        if outcome["failed"]:
            step.status = FAIL
            step.detail = f"{outcome['failed']} file(s) failed to download"
            step.evidence.append("re-run with --log-file and look for 423 / PCS errors")
            return
        if not files:
            step.status = FAIL
            step.detail = "the folder downloaded no files"
            return
        step.status = PASS
        step.detail = f"{len(files)} file(s), {outcome['bytes']:,} bytes downloaded"

    def step_offline_verify(self, step: StepResult) -> None:
        """What came back verifies against its own manifest, with no network."""
        from .manifest import MANIFEST_FILENAME, Manifest

        target = self.workdir / "download"
        if not (target / MANIFEST_FILENAME).exists():
            step.status = FAIL
            step.detail = f"no {MANIFEST_FILENAME} was written during the download"
            return
        audit = Manifest.load(target).verify()
        if audit.ok:
            n = len(audit.entries)
            step.status = PASS
            step.detail = f"{n} {plural(n, 'entry', 'entries')} verified offline"
            return
        failures = audit.failures
        step.status = FAIL
        step.detail = (f"{len(failures)} {plural(len(failures), 'entry', 'entries')} "
                       "did not match the manifest")
        step.evidence.extend(f"{e.path}: {e.status}" for e in failures[:5])

    def step_rerun_skips(self, step: StepResult) -> None:
        """A second run transfers nothing."""
        outcome = self._download(self.workdir / "download")
        if outcome["failed"] or outcome["bytes"]:
            step.status = FAIL
            step.detail = (f"re-run transferred {outcome['bytes']:,} bytes and "
                           f"failed {outcome['failed']} file(s); expected nothing")
            return
        step.status = PASS
        step.detail = "re-run transferred nothing"

    def step_unattended_renew(self, step: StepResult) -> None:
        """A non-interactive renew succeeds without a fresh device approval.

        This is the claim that matters for scheduled backups: once approved,
        the stored PCS cookie is reused and nobody has to tap anything.
        """
        payload = self.renew() or {}
        if not payload.get("renewed"):
            step.status = FAIL
            step.detail = (payload.get("summary") or payload.get("error")
                           or "the unattended renew did not succeed")
            if payload.get("remedy"):
                step.evidence.append(payload["remedy"])
            return
        adp = payload.get("adp") or {}
        status = adp.get("status")
        waited = float(adp.get("waited_seconds") or 0.0)
        if status in _UNATTENDED_PCS:
            step.status = PASS
            step.detail = f"renewed with no approval needed (PCS: {status})"
            return
        if status == "acquired" and waited < _NO_HUMAN_SECONDS:
            step.status = PASS
            step.detail = "renewed; Apple re-issued the PCS cookie without waiting for approval"
            return
        step.status = FAIL
        step.detail = (
            f"the renew needed a fresh PCS approval (status: {status or 'unknown'}, "
            f"waited {waited:.0f}s): the stored cookie was not reused, so a "
            "scheduled run would stall"
        )

    # -- the whole procedure ---------------------------------------------

    def run(self) -> ADPCheckReport:
        try:
            adp = self._run_step(1, "account is under ADP", self.step_account_is_adp)
            gate = adp.ok
            why = "the account is not shown to be under ADP" if adp.status == SKIPPED \
                else "the ADP check failed"
            doctor = self._run_step(2, "auth doctor is healthy", self.step_doctor_healthy,
                                    precondition=gate, skip_reason=why)
            self._run_step(3, "a PCS cookie is stored", self.step_pcs_cookie_stored,
                           precondition=gate, skip_reason=why)
            listed = self._run_step(4, "the folder lists", self.step_folder_lists,
                                    precondition=gate and doctor.ok,
                                    skip_reason=why if not gate else "auth doctor did not pass")
            downloaded = self._run_step(
                5, "the folder downloads", self.step_folder_downloads,
                precondition=listed.ok, skip_reason="the folder did not list")
            self._run_step(6, "downloads verify offline", self.step_offline_verify,
                           precondition=downloaded.ok,
                           skip_reason="the folder was not downloaded")
            self._run_step(7, "a re-run transfers nothing", self.step_rerun_skips,
                           precondition=downloaded.ok,
                           skip_reason="the folder was not downloaded")
            self._run_step(8, "unattended renew needs no approval", self.step_unattended_renew,
                           precondition=gate, skip_reason=why)
        finally:
            self.cleanup()
        return self.report

    def cleanup(self) -> None:
        if self.keep or not self._temp_owner:
            return
        shutil.rmtree(self.workdir, ignore_errors=True)


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------

_VERDICT_LINE = {
    VALIDATED: "VALIDATED - every step passed against a live ADP account.",
    BROKEN: "BROKEN - at least one step failed. Details below.",
    INCOMPLETE: "INCOMPLETE - some steps never ran, so this proves less than a "
                "full pass. A skipped step is an unanswered question, not a pass.",
}


def render_report(report: ADPCheckReport) -> str:
    lines = [
        rule("="),
        "iFetch Advanced Data Protection validation",
        rule("="),
        f"  Account    {report.account}",
        f"  Folder     {report.folder}",
        f"  Service    {report.adp_service}",
        f"  iFetch     {report.ifetch_version}",
        f"  pyicloud   {report.pyicloud_version}",
        "",
        _VERDICT_LINE.get(report.verdict, report.verdict),
        "",
    ]
    rows = [
        [str(s.number), _STATUS_MARK.get(s.status, s.status), s.name, s.detail]
        for s in report.steps
    ]
    lines.append(table(["#", "result", "step", "detail"], rows))

    evidence = [(s, e) for s in report.steps for e in s.evidence]
    if evidence:
        lines.extend(["", "Notes"])
        for step, note in evidence:
            lines.append(f"  {step.number}. {note}")

    if report.verdict == BROKEN:
        lines.extend([
            "",
            "Please attach the --report JSON and the --log-file log to",
            "https://github.com/roshanlam/iFetch/issues/31 - a live ADP failure",
            "is exactly what this run exists to find.",
        ])
    lines.extend([
        "",
        "Paste this into the results table in docs/adp-validation.md:",
        "",
        "  " + report.markdown_row(),
        rule("="),
    ])
    return "\n".join(lines)
