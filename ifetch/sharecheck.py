"""Running the cross-account shared-folder validation as one command.

Why this exists
---------------
iFetch's handling of folders shared by *another* Apple ID is covered by
``tests/test_shared_folder_contract.py``, which replays saved copies of Apple's
replies. Those prove iFetch branches correctly on the payloads we believe Apple
sends. They cannot prove Apple sends them.

Closing that gap needs a second Apple ID, which no test suite can provision, so
``docs/shared-folder-validation.md`` has always been a manual checklist. A
checklist that takes fifteen minutes and careful reading is a checklist that
does not get run. This module is the same procedure as one command, with a
verdict at the end and a table row to paste into the doc.

It does not replace the human part. Somebody still has to make two accounts and
share a folder. It replaces the tedious part.

Read-only
---------
Every step here lists or downloads. Nothing writes to iCloud, renames anything,
or deletes anything, and downloads go to a temporary directory that is removed
afterwards unless ``--keep`` is given. Validating a backup tool should never be
able to cost you data.

What a result means
-------------------
Each step is ``pass``, ``fail``, ``skipped`` (a precondition was not met, so the
step never ran) or ``error`` (the step ran and something unexpected broke).

A step that did not run is **never** reported as a pass, and the overall verdict
is only ``validated`` when every step passed. If anything was skipped the
verdict is ``incomplete``, because a run that could not reach the important step
proves nothing about it - and the important step is step 4.

Why step 4 is the one that matters
----------------------------------
Apple puts ``shareID`` on the shared folder and leaves it off the items it
returns for that folder's contents, so a client that reads the ID off each file
loses it one level down and every request below that goes out unscoped. The
symptom is that the share root works and everything inside it fails. That is the
bug this validation exists to catch, and it is also why rclone's iCloud backend
can only operate at a share root.

Step 4 walks two levels into the share. Step 5 checks the same thing one level
lower and directly: it asks whether the file iFetch resolved is actually
carrying the share ID, and whether that ID was inherited rather than its own.
Step 4 proves the downloads work; step 5 proves they work *for the reason we
think*, which is what stops a passing run from being a coincidence.
"""

from __future__ import annotations

import shutil
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from . import sharing
from .render import plural, rule, table

#: Step outcomes.
PASS = "pass"
FAIL = "fail"
SKIPPED = "skipped"
ERROR = "error"

#: Overall verdicts.
VALIDATED = "validated"
BROKEN = "broken"
INCOMPLETE = "incomplete"

_STATUS_MARK = {PASS: "PASS", FAIL: "FAIL", SKIPPED: "--", ERROR: "ERR"}


class ShareCheckError(Exception):
    """The validation could not be started at all."""


@dataclass
class StepResult:
    """One step of the procedure, and the evidence behind its verdict."""

    number: int
    name: str
    status: str = SKIPPED
    detail: str = ""
    evidence: List[str] = field(default_factory=list)
    duration: float = 0.0

    @property
    def ok(self) -> bool:
        return self.status == PASS

    def to_dict(self) -> Dict[str, Any]:
        return {
            "number": self.number,
            "name": self.name,
            "status": self.status,
            "detail": self.detail,
            "evidence": list(self.evidence),
            "duration_seconds": round(self.duration, 2),
        }


@dataclass
class ShareCheckReport:
    """Every step, the verdict, and what the verdict is allowed to claim."""

    share_path: str
    steps: List[StepResult] = field(default_factory=list)
    ifetch_version: str = ""
    pyicloud_version: str = ""
    started_at: float = 0.0

    @property
    def verdict(self) -> str:
        """``validated`` only when nothing failed *and* nothing was skipped.

        A skipped step is an unanswered question. Folding it into a pass would
        turn "we never got that far" into "we checked and it was fine", which is
        the one thing this report must not do.
        """
        if any(s.status in (FAIL, ERROR) for s in self.steps):
            return BROKEN
        if any(s.status == SKIPPED for s in self.steps):
            return INCOMPLETE
        return VALIDATED if self.steps else INCOMPLETE

    @property
    def critical_step(self) -> Optional[StepResult]:
        """Step 4 - reading below the share root - decides the headline."""
        for step in self.steps:
            if step.number == 4:
                return step
        return None

    def counts(self) -> Dict[str, int]:
        out = {PASS: 0, FAIL: 0, SKIPPED: 0, ERROR: 0}
        for step in self.steps:
            out[step.status] = out.get(step.status, 0) + 1
        return out

    def to_dict(self) -> Dict[str, Any]:
        return {
            "share_path": self.share_path,
            "verdict": self.verdict,
            "counts": self.counts(),
            "ifetch_version": self.ifetch_version,
            "pyicloud_version": self.pyicloud_version,
            "started_at": self.started_at,
            "steps": [s.to_dict() for s in self.steps],
        }

    def markdown_row(self) -> str:
        """The row for the results table in docs/shared-folder-validation.md."""
        date = time.strftime("%Y-%m-%d", time.gmtime(self.started_at or time.time()))
        cells = [date, self.ifetch_version or "?", self.pyicloud_version or "?"]
        for number in (2, 3, 4, 5, 6):
            step = next((s for s in self.steps if s.number == number), None)
            cells.append(_STATUS_MARK.get(step.status, "--") if step else "--")
        cells.append(f"verdict: {self.verdict}")
        return "| " + " | ".join(cells) + " |"


def _versions() -> Dict[str, str]:
    out = {"ifetch": "", "pyicloud": ""}
    for name in out:
        try:
            from importlib.metadata import version

            out[name] = version(name)
        except Exception:
            out[name] = "unknown"
    return out


def _step2_skip_reason(step: StepResult) -> str:
    """Why steps 3 and 4 did not run - and they are two different reasons."""
    if step.status == SKIPPED:
        return "the folder is not shared with you by another Apple ID"
    return "the share did not resolve"


class ShareChecker:
    """Runs the procedure. Every iCloud interaction goes through ``downloader``.

    The downloader is injected rather than constructed here so the whole
    procedure can be driven against a fake in tests. Nothing in this class
    authenticates; the CLI does that and hands over a signed-in manager.
    """

    def __init__(
        self,
        downloader: Any,
        share_path: str,
        nested: str = "nested",
        deeper: str = "nested/deeper",
        workdir: Optional[Path] = None,
        keep: bool = False,
        assume_shared: bool = False,
        log: Optional[Callable[[str], None]] = None,
    ):
        self.downloader = downloader
        self.share_path = share_path.strip("/")
        self.nested = nested.strip("/")
        self.deeper = deeper.strip("/")
        self.keep = keep
        self.assume_shared = assume_shared
        self.log = log or (lambda message: None)
        self._temp_owner = workdir is None
        self.workdir = Path(workdir) if workdir else Path(
            tempfile.mkdtemp(prefix="ifetch-sharecheck-")
        )
        versions = _versions()
        self.report = ShareCheckReport(
            share_path=self.share_path,
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

    def _remote(self, *parts: str) -> str:
        return "/".join([self.share_path, *[p for p in parts if p]])

    # -- the steps --------------------------------------------------------

    def step_share_visible(self, step: StepResult) -> None:
        """Resolve the folder, and confirm it is actually somebody else's share.

        The second half matters more than it looks. Pointed at a folder you own,
        every remaining step would pass - the files download, they verify, the
        re-run skips - and the report would say ``validated`` having tested
        nothing about share handling at all. A validation that passes on the
        wrong input is worse than one that fails, so a folder with no shareID
        stops the run rather than sailing through it.
        """
        item = self.downloader.get_drive_item(self.share_path)
        if item is None:
            step.status = FAIL
            step.detail = f"'{self.share_path}' did not resolve"
            step.evidence.append(
                "check the name against 'ifetch --list-shared', and that the "
                "share invitation was accepted on this account"
            )
            return

        if sharing.read_share_id(item) is None and not self.assume_shared:
            step.status = SKIPPED
            step.detail = (
                f"'{self.share_path}' resolved but carries no shareID, so it is "
                "not a folder shared with you by another Apple ID"
            )
            step.evidence.append(
                "nothing below would test share handling, so the run stops here "
                "rather than reporting a pass it did not earn. If you are certain "
                "this is a share, re-run with --assume-shared."
            )
            return

        step.status = PASS
        step.detail = f"'{self.share_path}' resolved and carries a shareID"

    def step_root_readable(self, step: StepResult) -> None:
        """Files directly inside the share come down."""
        target = self.workdir / "root"
        self.downloader.download(self.share_path, str(target))
        files = [p for p in target.rglob("*") if p.is_file()] if target.exists() else []
        if not files:
            step.status = FAIL
            step.detail = "the share root downloaded no files"
            return
        step.status = PASS
        step.detail = f"{len(files)} file(s) downloaded from the share root"
        packages = [p for p in target.rglob("*.key") if p.is_dir()]
        if packages:
            step.evidence.append(
                f"{len(packages)} Keynote package(s) arrived as directories, not ZIPs"
            )

    def step_nested_readable(self, step: StepResult) -> None:
        """The critical case: reading below the share root.

        This is where other clients get HTTP 400, and where iFetch did too
        before share context was inherited down the tree.
        """
        levels = 0
        for label, relative in (("one level down", self.nested),
                                ("two levels down", self.deeper)):
            if not relative:
                continue
            levels += 1
            remote = self._remote(relative)
            target = self.workdir / relative.replace("/", "_")
            self.downloader.download(remote, str(target))
            files = [p for p in target.rglob("*") if p.is_file()] if target.exists() else []
            if not files:
                step.status = FAIL
                step.detail = f"'{remote}' downloaded no files ({label})"
                step.evidence.append(
                    "this is the failure other iCloud clients have: the share "
                    "root works and everything inside it does not"
                )
                return
            step.evidence.append(f"{label}: {len(files)} file(s) from '{remote}'")
        step.status = PASS
        step.detail = f"read {levels} level(s) below the share root"

    def step_share_id_inherited(self, step: StepResult) -> None:
        """Prove the downloads worked *for the reason we think they did*.

        Step 4 passing is necessary and not sufficient: it could pass because
        Apple happened to include a shareID on those particular items. This
        checks the mechanism directly - that a file two levels down carries the
        share ID, and that it was inherited rather than its own.
        """
        remote = self._remote(self.deeper or self.nested)
        node = self.downloader.get_drive_item(remote)
        if node is None:
            step.status = FAIL
            step.detail = f"'{remote}' did not resolve"
            return

        context = sharing.carried_context(node)
        if context is None:
            step.status = FAIL
            step.detail = "no shareID reached this folder"
            step.evidence.append(
                "downloads may still work today, but the request is going out "
                "unscoped and will fail as soon as Apple requires the ID"
            )
            return

        step.status = PASS
        step.detail = context.describe()
        if context.source == sharing.SOURCE_OWN:
            step.evidence.append(
                "Apple named the share on this item itself, so inheritance was "
                "not exercised here - the fix is untested by this step even "
                "though the step passed"
            )
        else:
            step.evidence.append(
                f"the ID was carried down {context.depth} level(s), which is "
                "the code path that fixes issue #15"
            )

    def step_offline_verify(self, step: StepResult) -> None:
        """What came back verifies against its own manifest, with no network.

        This is the check a user can still run years later with no credentials,
        so it is worth proving it works on shared files too - the manifest is
        written during download, and a download path with its own fallback is
        exactly where a missing manifest entry would go unnoticed.
        """
        from .manifest import MANIFEST_FILENAME, Manifest

        target = self.workdir / "root"
        if not (target / MANIFEST_FILENAME).exists():
            step.status = FAIL
            step.detail = f"no {MANIFEST_FILENAME} was written during the download"
            return

        audit = Manifest.load(target).verify()
        if audit.ok:
            step.status = PASS
            n = len(audit.entries)
            step.detail = f"{n} {plural(n, 'entry', 'entries')} verified offline"
            return
        step.status = FAIL
        failures = audit.failures
        step.detail = (f"{len(failures)} {plural(len(failures), 'entry', 'entries')} "
                       "did not match the manifest")
        step.evidence.extend(f"{e.path}: {e.status}" for e in failures[:5])

    def step_rerun_skips(self, step: StepResult) -> None:
        """A second run transfers nothing, which is the incremental promise."""
        target = self.workdir / "root"
        self.downloader.download(self.share_path, str(target))
        summary = self.downloader.generate_summary_report().get("summary", {})
        transferred = summary.get("total_bytes_transferred", 0)
        if transferred:
            step.status = FAIL
            step.detail = f"re-run transferred {transferred:,} bytes; expected 0"
            return
        step.status = PASS
        step.detail = "re-run transferred nothing"

    # -- the whole procedure ---------------------------------------------

    def run(self) -> ShareCheckReport:
        try:
            visible = self._run_step(2, "share is visible", self.step_share_visible)
            root = self._run_step(
                3, "share root is readable", self.step_root_readable,
                precondition=visible.ok,
                skip_reason=_step2_skip_reason(visible),
            )
            nested = self._run_step(
                4, "subfolders of the share are readable", self.step_nested_readable,
                precondition=visible.ok,
                skip_reason=_step2_skip_reason(visible),
            )
            self._run_step(
                5, "share ID reaches the files inside", self.step_share_id_inherited,
                precondition=nested.ok,
                skip_reason="subfolders were not readable, so there is nothing to inspect",
            )
            self._run_step(
                6, "downloads verify offline", self.step_offline_verify,
                precondition=root.ok,
                skip_reason="the share root was not downloaded",
            )
            self._run_step(
                7, "a re-run transfers nothing", self.step_rerun_skips,
                precondition=root.ok,
                skip_reason="the share root was not downloaded",
            )
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
    VALIDATED: "VALIDATED - every step passed against a live share.",
    BROKEN: "BROKEN - at least one step failed. Details below.",
    INCOMPLETE: "INCOMPLETE - some steps never ran, so this proves less than a "
                "full pass. A skipped step is an unanswered question, not a pass.",
}


def render_report(report: ShareCheckReport) -> str:
    lines = [
        rule("="),
        "iFetch shared-folder validation",
        rule("="),
        f"  Share      {report.share_path}",
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

    critical = report.critical_step
    if critical is not None and critical.status in (FAIL, ERROR):
        lines.extend([
            "",
            "Step 4 is the one that matters. Its failure is the exact behaviour",
            "reported in iFetch issue #15 and rclone #9477: the share root works",
            "and everything inside it does not. Please attach the log from",
            "--log-file to https://github.com/roshanlam/iFetch/issues.",
        ])

    lines.extend([
        "",
        "Paste this into the results table in docs/shared-folder-validation.md:",
        "",
        "  " + report.markdown_row(),
        rule("="),
    ])
    return "\n".join(lines)
