"""Tests for restoring missing files back into iCloud.

This is the only code in iFetch that writes to a user's account, so the
contract is mostly a list of refusals:

* **an unusable scan uploads nothing, at any count.** If the remote listing
  failed, was truncated or came back empty, every local file looks missing and
  this feature would push a whole mirror back into the account;
* **dry run is the default and contacts nobody** - the fake drive must record
  zero calls;
* **nothing is ever overwritten.** A file that reappeared in iCloud between the
  plan and the upload is skipped, and an absence that could not be confirmed is
  treated as unknown rather than as "not there";
* a placeholder, a file that no longer matches its recorded digest, a path that
  escapes the mirror and a package bundle are each refused **and named** - the
  report never omits a file it declined to handle;
* a failure on one file is recorded against that file and the run continues;
* an upload is written to the index as it happens, so an interrupted run
  resumes and a repeated one sends nothing.
"""

import csv
import io
import json
import os
import sys
import unicodedata
from pathlib import Path

import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))

from ifetch.index import (  # noqa: E402
    KIND_DIR,
    KIND_PACKAGE,
    SCHEMA_VERSION,
    UPLOAD_DONE,
    UPLOAD_FAILED,
    IndexStore,
    LocalItem,
    RemoteItem,
)
from ifetch.manifest import Manifest  # noqa: E402
from ifetch.recovery import (  # noqa: E402
    CONFIDENCE_LIKELY,
    EVIDENCE_BRICK,
    EVIDENCE_DATALESS,
    Placeholder,
    PlaceholderReport,
)
from ifetch.uplink import (  # noqa: E402
    REFUSE_DIGEST_MISMATCH,
    REFUSE_OUTSIDE_ROOT,
    REFUSE_PACKAGE,
    REFUSE_PLACEHOLDER,
    STATUS_FAILED,
    STATUS_SKIPPED,
    STATUS_UPLOADED,
    STATUS_WOULD_UPLOAD,
    DriveUplink,
    UplinkError,
    apply_uploads,
    csv_rows,
    plan_uploads,
    render_plan,
    render_run,
    render_uploads,
)
from ifetch.uplink_cli import (  # noqa: E402
    EXIT_ERROR,
    EXIT_FINDINGS,
    EXIT_OK,
    EXIT_REFUSED,
    build_parser,
    main,
)
from ifetch.vanished import BREAKER_COUNT, BREAKER_FRACTION, BREAKER_SCAN  # noqa: E402

BASE = "Documents"


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


def add_file(root, rel, content=b"contents"):
    path = root / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    return path


def write_brick(root, relative, size=4096):
    """The ``.icloud`` stub macOS leaves when it evicts a file's contents."""
    import plistlib

    target = root / relative
    target.parent.mkdir(parents=True, exist_ok=True)
    stub = target.parent / f".{target.name}.icloud"
    with stub.open("wb") as handle:
        plistlib.dump({"NSURLFileSizeKey": size}, handle)
    return stub


def record_local(store, *specs):
    store.record_local_many([
        LocalItem(**spec) if isinstance(spec, dict) else LocalItem(path=spec)
        for spec in specs
    ])


def remote_scan(store, items=(), errors=None, icloud_path=BASE, finish=True):
    """Record a remote scan the way ``RemoteScanner`` would."""
    scan_id = store.begin_scan(icloud_path)
    store.clear_remote()
    store.record_remote_many(list(items), scan_id=scan_id)
    if finish:
        store.finish_scan(scan_id, errors=errors)
    return scan_id


def no_placeholders(root=""):
    """A completed placeholder scan that found nothing, with both signals up."""
    return PlaceholderReport(
        root=str(root), files_checked=1,
        signals_available=[EVIDENCE_BRICK, EVIDENCE_DATALESS],
    )


def dataless_placeholder(path, size=900):
    """A placeholder report for a file that *exists* at full size on disk.

    The dataless signal is APFS-only and cannot be fabricated portably, so the
    detector's verdict is injected. What is under test is what uplink does with
    it - refuse, and say which file.
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
# A fake iCloud Drive
# ---------------------------------------------------------------------------

class FakeNode:
    """A folder or file in the fake drive, with the pyicloud node interface."""

    def __init__(self, name, drive, is_dir=True, content=b""):
        self.name = name
        self.drive = drive
        self.is_dir = is_dir
        self.content = content
        self.children = {}

    def dir(self):
        self.drive.calls.append(("dir", self.name))
        if not self.is_dir:
            raise NotADirectoryError(self.name)
        if self.drive.listing_fails_for == self.name:
            raise RuntimeError("Apple said 503")
        return list(self.children)

    def get_children(self, force=False):
        self.drive.calls.append(("get_children", self.name))
        return list(self.children.values())

    def __getitem__(self, key):
        try:
            return self.children[key]
        except KeyError as exc:
            raise KeyError(f"No child named '{key}' exists") from exc

    def mkdir(self, folder):
        self.drive.calls.append(("mkdir", f"{self.name}/{folder}"))
        self.children[folder] = FakeNode(folder, self.drive)
        return {"created": folder}

    def upload(self, file_object):
        name = os.path.basename(file_object.name)
        self.drive.calls.append(("upload", f"{self.name}/{name}"))
        if name in self.drive.upload_fails_for:
            raise RuntimeError("Apple rejected this file")
        payload = file_object.read()
        self.drive.uploads.append((self.name, name, payload))
        self.children[name] = FakeNode(name, self.drive, is_dir=False, content=payload)
        return {"uploaded": name}


class FakeDrive:
    """A stand-in for ``DownloadManager``: it can only resolve a path."""

    def __init__(self, base=BASE):
        self.base = base
        self.calls = []
        self.uploads = []
        self.upload_fails_for = set()
        self.listing_fails_for = None
        self.root = FakeNode(base, self)
        self.api = object()

    # -- construction helpers -------------------------------------------
    def add_folder(self, relative):
        node = self.root
        for part in [p for p in relative.split("/") if p]:
            node = node.children.setdefault(part, FakeNode(part, self))
        return node

    def add_file(self, relative, content=b"remote"):
        parts = [p for p in relative.split("/") if p]
        parent = self.add_folder("/".join(parts[:-1]))
        parent.children[parts[-1]] = FakeNode(
            parts[-1], self, is_dir=False, content=content
        )

    # -- the one method DriveUplink uses --------------------------------
    def get_drive_item(self, path):
        self.calls.append(("get_drive_item", path))
        parts = [p for p in str(path).strip("/").split("/") if p]
        if not parts or parts[0] != self.base:
            raise Exception(f"Path not found: {path}")
        node = self.root
        for part in parts[1:]:
            node = node.children[part]
        return node

    @property
    def upload_names(self):
        return [name for _, name, _ in self.uploads]


def uplink_for(drive, base=BASE):
    return DriveUplink(drive, base=base)


def simple_mirror(store, mirror, extra=()):
    """One file iCloud has and one it does not, plus anything extra."""
    add_file(mirror, "kept.txt", b"kept")
    add_file(mirror, "Sub/lost.txt", b"lost")
    record_local(store, "kept.txt", "Sub/lost.txt", *extra)
    remote_scan(store, [RemoteItem(path="kept.txt", size=4)])


# ---------------------------------------------------------------------------
# The happy path
# ---------------------------------------------------------------------------

class TestPlanning:
    def test_a_file_missing_remotely_and_present_locally_is_planned(self, store, mirror):
        simple_mirror(store, mirror)
        plan = plan_uploads(store, mirror, placeholders=no_placeholders())

        assert [c.path for c in plan.candidates] == ["Sub/lost.txt"]
        assert plan.candidates[0].remote_path == "Documents/Sub/lost.txt"
        assert plan.candidates[0].parent == "Sub"
        assert plan.total_bytes == 4

    def test_a_file_icloud_already_has_is_not_planned(self, store, mirror):
        simple_mirror(store, mirror)
        plan = plan_uploads(store, mirror, placeholders=no_placeholders())
        assert "kept.txt" not in [c.path for c in plan.candidates]

    def test_planning_changes_nothing_anywhere(self, store, mirror):
        simple_mirror(store, mirror)
        plan_uploads(store, mirror, placeholders=no_placeholders())
        assert list(store.iter_uploads()) == []
        assert (mirror / "Sub/lost.txt").read_bytes() == b"lost"

    def test_the_remote_base_comes_from_the_last_scan(self, store, mirror):
        simple_mirror(store, mirror)
        plan = plan_uploads(store, mirror, placeholders=no_placeholders())
        assert plan.icloud_path == BASE

    def test_an_explicit_remote_base_overrides_the_scan(self, store, mirror):
        simple_mirror(store, mirror)
        plan = plan_uploads(
            store, mirror, icloud_path="Backups", placeholders=no_placeholders()
        )
        assert plan.candidates[0].remote_path == "Backups/Sub/lost.txt"

    def test_an_empty_local_index_is_reported_as_unexamined(self, store, mirror):
        remote_scan(store, [RemoteItem(path="kept.txt", size=4)])
        plan = plan_uploads(store, mirror, placeholders=no_placeholders())
        assert plan.candidates == []
        assert any("local mirror" in gap["what"] for gap in plan.unexamined)


class TestUploading:
    def test_the_file_is_uploaded_to_the_right_parent_folder(self, store, mirror):
        simple_mirror(store, mirror)
        drive = FakeDrive()
        plan = plan_uploads(store, mirror, placeholders=no_placeholders())

        run = apply_uploads(
            plan, store, mirror, drive=uplink_for(drive), dry_run=False,
            placeholders=no_placeholders(),
        )

        assert drive.uploads == [("Sub", "lost.txt", b"lost")]
        assert [o.status for o in run.outcomes] == [STATUS_UPLOADED]

    def test_a_successful_upload_is_recorded_in_the_index(self, store, mirror):
        simple_mirror(store, mirror)
        apply_uploads(
            plan_uploads(store, mirror, placeholders=no_placeholders()),
            store, mirror, drive=uplink_for(FakeDrive()), dry_run=False,
            placeholders=no_placeholders(),
        )
        recorded = store.get_upload("Sub/lost.txt")
        assert recorded["state"] == UPLOAD_DONE
        assert recorded["remote_path"] == "Documents/Sub/lost.txt"

    def test_applying_without_a_connection_is_an_error_not_a_silent_no_op(
        self, store, mirror
    ):
        simple_mirror(store, mirror)
        plan = plan_uploads(store, mirror, placeholders=no_placeholders())
        with pytest.raises(UplinkError):
            apply_uploads(plan, store, mirror, drive=None, dry_run=False)


# ---------------------------------------------------------------------------
# Dry run
# ---------------------------------------------------------------------------

class TestDryRun:
    def test_a_dry_run_makes_no_calls_at_all(self, store, mirror):
        simple_mirror(store, mirror)
        drive = FakeDrive()
        plan = plan_uploads(store, mirror, placeholders=no_placeholders())

        run = apply_uploads(plan, store, mirror, drive=uplink_for(drive), dry_run=True)

        assert drive.calls == []
        assert drive.uploads == []
        assert [o.status for o in run.outcomes] == [STATUS_WOULD_UPLOAD]

    def test_a_dry_run_records_no_uploads(self, store, mirror):
        simple_mirror(store, mirror)
        apply_uploads(
            plan_uploads(store, mirror, placeholders=no_placeholders()),
            store, mirror, dry_run=True,
        )
        assert list(store.iter_uploads()) == []

    def test_a_dry_run_needs_no_connection(self, store, mirror):
        simple_mirror(store, mirror)
        run = apply_uploads(
            plan_uploads(store, mirror, placeholders=no_placeholders()),
            store, mirror, drive=None, dry_run=True,
        )
        assert run.dry_run is True

    def test_the_dry_run_report_names_the_destination_and_the_bytes(self, store, mirror):
        simple_mirror(store, mirror)
        plan = plan_uploads(store, mirror, placeholders=no_placeholders())
        text = render_plan(plan)
        assert "Documents/Sub/lost.txt" in text
        assert "Bytes to send" in text
        assert "--apply" in text


# ---------------------------------------------------------------------------
# The circuit breaker - the single most dangerous failure mode
# ---------------------------------------------------------------------------

class TestCircuitBreaker:
    def _one_missing(self, store, mirror):
        add_file(mirror, "kept.txt", b"kept")
        add_file(mirror, "lost.txt", b"lost")
        record_local(store, "kept.txt", "lost.txt")

    def test_a_scan_that_recorded_listing_errors_refuses(self, store, mirror):
        self._one_missing(store, mirror)
        remote_scan(
            store, [RemoteItem(path="kept.txt", size=4)],
            errors=[{"path": "Sub", "error": "listing failed"}],
        )
        plan = plan_uploads(store, mirror, placeholders=no_placeholders())

        assert plan.refused is True
        assert plan.breaker.reason == BREAKER_SCAN
        assert plan.candidates == []

    def test_a_scan_that_never_finished_refuses(self, store, mirror):
        self._one_missing(store, mirror)
        remote_scan(store, [RemoteItem(path="kept.txt", size=4)])
        store.begin_scan(BASE)  # started, died, never closed

        plan = plan_uploads(store, mirror, placeholders=no_placeholders())
        assert plan.refused is True
        assert plan.breaker.reason == BREAKER_SCAN

    def test_a_scan_that_came_back_empty_refuses(self, store, mirror):
        self._one_missing(store, mirror)
        remote_scan(store, [])

        plan = plan_uploads(store, mirror, placeholders=no_placeholders())
        assert plan.refused is True
        assert plan.breaker.reason == BREAKER_SCAN

    def test_no_scan_at_all_refuses(self, store, mirror):
        self._one_missing(store, mirror)
        plan = plan_uploads(store, mirror, placeholders=no_placeholders())
        assert plan.refused is True
        assert plan.breaker.reason == BREAKER_SCAN

    @pytest.mark.parametrize(
        "errors",
        [
            [{"path": "Sub", "error": "listing failed"}],
            None,
        ],
    )
    def test_a_broken_scan_refuses_even_for_a_single_file(self, store, mirror, errors):
        """One file is not a mass deletion, and that is not the point.

        The breaker is not counting: it is refusing to read a listing that
        cannot be trusted as a statement about what iCloud holds.
        """
        self._one_missing(store, mirror)
        if errors is None:
            remote_scan(store, [])  # empty scan
        else:
            remote_scan(store, [RemoteItem(path="kept.txt", size=4)], errors=errors)

        plan = plan_uploads(store, mirror, placeholders=no_placeholders())
        assert plan.missing_count <= 2
        assert plan.refused is True

    def test_a_refused_plan_uploads_nothing_and_calls_nobody(self, store, mirror):
        self._one_missing(store, mirror)
        remote_scan(store, [], errors=[{"path": "/", "error": "boom"}])
        drive = FakeDrive()

        plan = plan_uploads(store, mirror, placeholders=no_placeholders())
        run = apply_uploads(
            plan, store, mirror, drive=uplink_for(drive), dry_run=False,
            placeholders=no_placeholders(),
        )

        assert run.refused is True
        assert run.outcomes == []
        assert drive.calls == []
        assert list(store.iter_uploads()) == []

    def test_the_missing_paths_are_still_in_the_payload(self, store, mirror):
        self._one_missing(store, mirror)
        remote_scan(store, [], errors=[{"path": "/", "error": "boom"}])
        plan = plan_uploads(store, mirror, placeholders=no_placeholders())
        assert "lost.txt" in plan.to_dict()["missing_paths"]

    def test_the_refusal_names_what_it_cannot_rule_out(self, store, mirror):
        self._one_missing(store, mirror)
        remote_scan(store, [], errors=[{"path": "/", "error": "boom"}])
        plan = plan_uploads(store, mirror, placeholders=no_placeholders())
        assert plan.breaker.cannot_rule_out
        text = render_plan(plan)
        assert "REFUSED" in text
        assert "still in iCloud" in text

    def test_a_large_missing_count_trips_the_count_rule(self, store, mirror):
        record_local(store, "kept.txt", *[f"f{i:04d}.txt" for i in range(500)])
        remote_scan(store, [RemoteItem(path="kept.txt", size=4)])

        plan = plan_uploads(store, mirror, placeholders=no_placeholders())
        assert plan.refused is True
        assert plan.breaker.reason == BREAKER_COUNT

    def test_a_large_missing_fraction_trips_the_fraction_rule(self, store, mirror):
        names = [f"f{i:04d}.txt" for i in range(100)]
        record_local(store, *names)
        remote_scan(
            store, [RemoteItem(path=n, size=4) for n in names[:75]]
        )

        plan = plan_uploads(store, mirror, placeholders=no_placeholders())
        assert plan.refused is True
        assert plan.breaker.reason == BREAKER_FRACTION

    def test_an_ordinary_result_does_not_trip(self, store, mirror):
        simple_mirror(store, mirror)
        plan = plan_uploads(store, mirror, placeholders=no_placeholders())
        assert plan.refused is False
        assert plan.breaker.tripped is False


# ---------------------------------------------------------------------------
# Re-checking immediately before each upload
# ---------------------------------------------------------------------------

class TestRecheckBeforeUpload:
    def test_a_file_that_appeared_between_plan_and_apply_is_skipped(self, store, mirror):
        simple_mirror(store, mirror)
        drive = FakeDrive()
        plan = plan_uploads(store, mirror, placeholders=no_placeholders())

        # Somebody else put it back while we were thinking about it.
        drive.add_file("Sub/lost.txt", b"someone else's copy")

        run = apply_uploads(
            plan, store, mirror, drive=uplink_for(drive), dry_run=False,
            placeholders=no_placeholders(),
        )

        assert [o.status for o in run.outcomes] == [STATUS_SKIPPED]
        assert drive.uploads == []
        assert "lost.txt" not in drive.upload_names

    def test_the_skip_is_reported_rather_than_hidden(self, store, mirror):
        simple_mirror(store, mirror)
        drive = FakeDrive()
        plan = plan_uploads(store, mirror, placeholders=no_placeholders())
        drive.add_file("Sub/lost.txt")

        run = apply_uploads(
            plan, store, mirror, drive=uplink_for(drive), dry_run=False,
            placeholders=no_placeholders(),
        )
        assert "appeared between the plan and this upload" in run.outcomes[0].detail
        assert "Skipped" in render_run(run)

    def test_the_remote_copy_is_left_exactly_as_it_was(self, store, mirror):
        simple_mirror(store, mirror)
        drive = FakeDrive()
        plan = plan_uploads(store, mirror, placeholders=no_placeholders())
        drive.add_file("Sub/lost.txt", b"theirs")

        apply_uploads(
            plan, store, mirror, drive=uplink_for(drive), dry_run=False,
            placeholders=no_placeholders(),
        )
        assert drive.root.children["Sub"].children["lost.txt"].content == b"theirs"

    def test_an_unreadable_listing_is_not_read_as_absence(self, store, mirror):
        """"I could not look" must never become "it is not there".

        Treating a failed listing as an absence is exactly how this feature
        would overwrite a file.
        """
        simple_mirror(store, mirror)
        drive = FakeDrive()
        drive.add_folder("Sub")
        drive.listing_fails_for = "Sub"
        plan = plan_uploads(store, mirror, placeholders=no_placeholders())

        run = apply_uploads(
            plan, store, mirror, drive=uplink_for(drive), dry_run=False,
            placeholders=no_placeholders(),
        )

        assert [o.status for o in run.outcomes] == [STATUS_FAILED]
        assert drive.uploads == []
        assert "could not confirm" in run.outcomes[0].detail

    def test_a_missing_parent_folder_is_a_definite_absence(self, store, mirror):
        """A folder that is not there proves the file inside it is not either."""
        simple_mirror(store, mirror)
        drive = FakeDrive()
        assert uplink_for(drive).exists("Sub", "lost.txt") is False


# ---------------------------------------------------------------------------
# Placeholders
# ---------------------------------------------------------------------------

class TestPlaceholders:
    def test_an_evicted_file_with_a_stub_is_refused_and_named(self, store, mirror):
        add_file(mirror, "kept.txt", b"kept")
        write_brick(mirror, "evicted.txt", size=4096)
        record_local(store, "kept.txt", {"path": "evicted.txt", "size": 4096})
        remote_scan(store, [RemoteItem(path="kept.txt", size=4)])

        plan = plan_uploads(store, mirror)

        assert plan.candidates == []
        refusals = plan.refusals_by_reason(REFUSE_PLACEHOLDER)
        assert [r.path for r in refusals] == ["evicted.txt"]

    def test_a_dataless_placeholder_is_refused_even_though_the_file_exists(
        self, store, mirror
    ):
        add_file(mirror, "kept.txt", b"kept")
        add_file(mirror, "shell.txt", b"x" * 900)
        record_local(store, "kept.txt", {"path": "shell.txt", "size": 900})
        remote_scan(store, [RemoteItem(path="kept.txt", size=4)])

        plan = plan_uploads(
            store, mirror, placeholders=dataless_placeholder("shell.txt")
        )

        assert [r.path for r in plan.refusals] == ["shell.txt"]
        assert plan.refusals[0].reason == REFUSE_PLACEHOLDER

    def test_the_refusal_appears_in_the_text_report(self, store, mirror):
        add_file(mirror, "kept.txt", b"kept")
        add_file(mirror, "shell.txt", b"x" * 900)
        record_local(store, "kept.txt", {"path": "shell.txt", "size": 900})
        remote_scan(store, [RemoteItem(path="kept.txt", size=4)])

        plan = plan_uploads(
            store, mirror, placeholders=dataless_placeholder("shell.txt")
        )
        text = render_plan(plan)
        assert "shell.txt" in text
        assert "placeholder" in text

    def test_a_signal_that_could_not_run_is_named_not_assumed_clean(
        self, store, mirror, monkeypatch
    ):
        import platform

        monkeypatch.setattr(platform, "system", lambda: "Linux")
        simple_mirror(store, mirror)

        plan = plan_uploads(store, mirror)
        assert any("placeholder signal" in gap["what"] for gap in plan.unexamined)

    def test_a_placeholder_is_still_refused_at_upload_time(self, store, mirror):
        """The plan is not the last word: the file may be evicted after it."""
        simple_mirror(store, mirror)
        plan = plan_uploads(store, mirror, placeholders=no_placeholders())
        drive = FakeDrive()

        run = apply_uploads(
            plan, store, mirror, drive=uplink_for(drive), dry_run=False,
            placeholders=dataless_placeholder("Sub/lost.txt"),
        )

        assert [o.status for o in run.outcomes] == [STATUS_FAILED]
        assert drive.uploads == []
        assert "placeholder" in run.outcomes[0].detail


# ---------------------------------------------------------------------------
# Local integrity
# ---------------------------------------------------------------------------

class TestDigestVerification:
    def _with_manifest(self, store, mirror):
        add_file(mirror, "kept.txt", b"kept")
        target = add_file(mirror, "Sub/lost.txt", b"original")
        record_local(store, "kept.txt", "Sub/lost.txt")
        remote_scan(store, [RemoteItem(path="kept.txt", size=4)])
        manifest = Manifest(mirror)
        manifest.record_file(target)
        return manifest, target

    def test_a_file_matching_its_manifest_digest_is_planned(self, store, mirror):
        manifest, _ = self._with_manifest(store, mirror)
        plan = plan_uploads(
            store, mirror, manifest=manifest, placeholders=no_placeholders()
        )
        assert [c.path for c in plan.candidates] == ["Sub/lost.txt"]
        assert plan.candidates[0].digest_source == "the manifest"

    def test_a_file_disagreeing_with_the_manifest_is_refused_and_named(
        self, store, mirror
    ):
        manifest, target = self._with_manifest(store, mirror)
        target.write_bytes(b"CORRUPTX")  # same length, different bytes

        plan = plan_uploads(
            store, mirror, manifest=manifest, placeholders=no_placeholders()
        )

        assert plan.candidates == []
        assert [r.path for r in plan.refusals] == ["Sub/lost.txt"]
        assert plan.refusals[0].reason == REFUSE_DIGEST_MISMATCH

    def test_the_mismatch_is_refused_again_at_upload_time(self, store, mirror):
        manifest, target = self._with_manifest(store, mirror)
        plan = plan_uploads(
            store, mirror, manifest=manifest, placeholders=no_placeholders()
        )
        target.write_bytes(b"CORRUPTX")
        drive = FakeDrive()

        run = apply_uploads(
            plan, store, mirror, drive=uplink_for(drive), dry_run=False,
            manifest=manifest, placeholders=no_placeholders(),
        )

        assert [o.status for o in run.outcomes] == [STATUS_FAILED]
        assert drive.uploads == []
        assert "digest mismatch" in run.outcomes[0].detail

    def test_the_index_digest_is_used_when_the_manifest_has_none(self, store, mirror):
        add_file(mirror, "kept.txt", b"kept")
        add_file(mirror, "Sub/lost.txt", b"original")
        record_local(
            store, "kept.txt",
            {"path": "Sub/lost.txt", "size": 8, "sha256": "0" * 64},
        )
        remote_scan(store, [RemoteItem(path="kept.txt", size=4)])

        plan = plan_uploads(store, mirror, placeholders=no_placeholders())
        assert [r.reason for r in plan.refusals] == [REFUSE_DIGEST_MISMATCH]
        assert "the index" in plan.refusals[0].detail

    def test_a_file_with_no_recorded_digest_is_uploaded_but_counted(
        self, store, mirror
    ):
        simple_mirror(store, mirror)
        plan = plan_uploads(store, mirror, placeholders=no_placeholders())
        assert [c.path for c in plan.candidates] == ["Sub/lost.txt"]
        assert any("no recorded digest" in gap["what"] for gap in plan.unexamined)


# ---------------------------------------------------------------------------
# Path containment
# ---------------------------------------------------------------------------

class TestPathContainment:
    def test_a_traversal_path_is_refused(self, store, mirror, tmp_path):
        (tmp_path / "outside.txt").write_bytes(b"not yours")
        add_file(mirror, "kept.txt", b"kept")
        record_local(store, "kept.txt", "../outside.txt")
        remote_scan(store, [RemoteItem(path="kept.txt", size=4)])

        plan = plan_uploads(store, mirror, placeholders=no_placeholders())

        assert plan.candidates == []
        assert [r.reason for r in plan.refusals] == [REFUSE_OUTSIDE_ROOT]
        assert plan.refusals[0].path == "../outside.txt"

    def test_a_symlink_escaping_the_mirror_is_refused(self, store, mirror, tmp_path):
        secret = tmp_path / "secret.txt"
        secret.write_bytes(b"private")
        os.symlink(secret, mirror / "link.txt")

        add_file(mirror, "kept.txt", b"kept")
        record_local(store, "kept.txt", "link.txt")
        remote_scan(store, [RemoteItem(path="kept.txt", size=4)])

        plan = plan_uploads(store, mirror, placeholders=no_placeholders())

        assert plan.candidates == []
        assert [r.reason for r in plan.refusals] == [REFUSE_OUTSIDE_ROOT]

    def test_the_escape_is_refused_at_upload_time_too(self, store, mirror, tmp_path):
        secret = tmp_path / "secret.txt"
        secret.write_bytes(b"private")
        add_file(mirror, "kept.txt", b"kept")
        add_file(mirror, "link.txt", b"decoy")
        record_local(store, "kept.txt", "link.txt")
        remote_scan(store, [RemoteItem(path="kept.txt", size=4)])

        plan = plan_uploads(store, mirror, placeholders=no_placeholders())
        assert [c.path for c in plan.candidates] == ["link.txt"]

        # Between plan and apply, the file becomes a symlink out of the mirror.
        (mirror / "link.txt").unlink()
        os.symlink(secret, mirror / "link.txt")

        drive = FakeDrive()
        run = apply_uploads(
            plan, store, mirror, drive=uplink_for(drive), dry_run=False,
            placeholders=no_placeholders(),
        )
        assert [o.status for o in run.outcomes] == [STATUS_FAILED]
        assert drive.uploads == []


# ---------------------------------------------------------------------------
# Package bundles
# ---------------------------------------------------------------------------

class TestPackageBundles:
    def _with_package(self, store, mirror, name="Deck.key"):
        add_file(mirror, "kept.txt", b"kept")
        add_file(mirror, f"{name}/index.apxl", b"slides")
        record_local(store, "kept.txt", {"path": name, "kind": KIND_PACKAGE, "size": 6})
        remote_scan(store, [RemoteItem(path="kept.txt", size=4)])

    @pytest.mark.parametrize(
        "name", ["Deck.key", "Notes.pages", "Budget.numbers", "App.xcodeproj"]
    )
    def test_a_bundle_is_never_uploaded(self, store, mirror, name):
        self._with_package(store, mirror, name)
        plan = plan_uploads(store, mirror, placeholders=no_placeholders())
        assert plan.candidates == []
        assert [r.reason for r in plan.refusals] == [REFUSE_PACKAGE]

    def test_the_bundle_appears_in_the_report_with_its_reason(self, store, mirror):
        self._with_package(store, mirror)
        plan = plan_uploads(store, mirror, placeholders=no_placeholders())
        text = render_plan(plan)
        assert "Deck.key" in text
        assert "package bundle" in text
        assert "Restore it by hand" in text

    def test_the_bundle_count_is_stated_in_the_notes(self, store, mirror):
        self._with_package(store, mirror)
        plan = plan_uploads(store, mirror, placeholders=no_placeholders())
        assert any("package bundle" in note for note in plan.notes)

    def test_a_bundle_detected_only_by_its_extension_is_refused(self, store, mirror):
        """The index may have it recorded as a plain file; the name still says."""
        add_file(mirror, "kept.txt", b"kept")
        add_file(mirror, "Deck.key", b"not really a file")
        record_local(store, "kept.txt", "Deck.key")
        remote_scan(store, [RemoteItem(path="kept.txt", size=4)])

        plan = plan_uploads(store, mirror, placeholders=no_placeholders())
        assert [r.reason for r in plan.refusals] == [REFUSE_PACKAGE]


# ---------------------------------------------------------------------------
# Folders
# ---------------------------------------------------------------------------

class TestFolders:
    def test_missing_intermediate_folders_are_planned(self, store, mirror):
        add_file(mirror, "kept.txt", b"kept")
        add_file(mirror, "A/B/c.txt", b"deep")
        record_local(store, "kept.txt", "A/B/c.txt")
        remote_scan(store, [RemoteItem(path="kept.txt", size=4)])

        plan = plan_uploads(store, mirror, placeholders=no_placeholders())
        assert plan.folders_to_create == ["A", "A/B"]

    def test_missing_intermediate_folders_are_created(self, store, mirror):
        add_file(mirror, "kept.txt", b"kept")
        add_file(mirror, "A/B/c.txt", b"deep")
        record_local(store, "kept.txt", "A/B/c.txt")
        remote_scan(store, [RemoteItem(path="kept.txt", size=4)])
        drive = FakeDrive()

        run = apply_uploads(
            plan_uploads(store, mirror, placeholders=no_placeholders()),
            store, mirror, drive=uplink_for(drive), dry_run=False,
            placeholders=no_placeholders(),
        )

        assert run.folders_created == ["A", "A/B"]
        assert drive.uploads == [("B", "c.txt", b"deep")]

    def test_a_folder_that_already_exists_is_not_recreated(self, store, mirror):
        add_file(mirror, "kept.txt", b"kept")
        add_file(mirror, "A/c.txt", b"deep")
        record_local(store, "kept.txt", "A/c.txt")
        remote_scan(store, [
            RemoteItem(path="kept.txt", size=4),
            RemoteItem(path="A", kind=KIND_DIR),
        ])
        drive = FakeDrive()
        drive.add_folder("A")

        plan = plan_uploads(store, mirror, placeholders=no_placeholders())
        assert plan.folders_to_create == []

        run = apply_uploads(
            plan, store, mirror, drive=uplink_for(drive), dry_run=False,
            placeholders=no_placeholders(),
        )
        assert run.folders_created == []
        assert [c for c in drive.calls if c[0] == "mkdir"] == []


# ---------------------------------------------------------------------------
# Partial failure
# ---------------------------------------------------------------------------

class TestPartialFailure:
    def _three_missing(self, store, mirror):
        add_file(mirror, "kept.txt", b"kept")
        for name in ("one.txt", "two.txt", "three.txt"):
            add_file(mirror, name, name.encode())
        record_local(store, "kept.txt", "one.txt", "two.txt", "three.txt")
        remote_scan(store, [RemoteItem(path="kept.txt", size=4)])

    def test_one_failure_does_not_stop_the_others(self, store, mirror):
        self._three_missing(store, mirror)
        drive = FakeDrive()
        drive.upload_fails_for = {"two.txt"}

        run = apply_uploads(
            plan_uploads(store, mirror, placeholders=no_placeholders()),
            store, mirror, drive=uplink_for(drive), dry_run=False,
            placeholders=no_placeholders(),
        )

        assert sorted(drive.upload_names) == ["one.txt", "three.txt"]
        assert run.counts() == {STATUS_UPLOADED: 2, STATUS_FAILED: 1}

    def test_the_failure_is_reported_per_file(self, store, mirror):
        self._three_missing(store, mirror)
        drive = FakeDrive()
        drive.upload_fails_for = {"two.txt"}

        run = apply_uploads(
            plan_uploads(store, mirror, placeholders=no_placeholders()),
            store, mirror, drive=uplink_for(drive), dry_run=False,
            placeholders=no_placeholders(),
        )

        failed = run.by_status(STATUS_FAILED)
        assert [o.path for o in failed] == ["two.txt"]
        assert "Apple rejected this file" in failed[0].detail
        assert "two.txt" in render_run(run)

    def test_a_failure_is_journalled_so_a_retry_knows(self, store, mirror):
        self._three_missing(store, mirror)
        drive = FakeDrive()
        drive.upload_fails_for = {"two.txt"}

        apply_uploads(
            plan_uploads(store, mirror, placeholders=no_placeholders()),
            store, mirror, drive=uplink_for(drive), dry_run=False,
            placeholders=no_placeholders(),
        )

        assert store.get_upload("two.txt")["state"] == UPLOAD_FAILED
        assert store.get_upload("one.txt")["state"] == UPLOAD_DONE


# ---------------------------------------------------------------------------
# Resume and idempotency
# ---------------------------------------------------------------------------

class TestResume:
    def _three_missing(self, store, mirror):
        add_file(mirror, "kept.txt", b"kept")
        for name in ("one.txt", "two.txt", "three.txt"):
            add_file(mirror, name, name.encode())
        record_local(store, "kept.txt", "one.txt", "two.txt", "three.txt")
        remote_scan(store, [RemoteItem(path="kept.txt", size=4)])

    def test_a_second_run_sends_only_what_is_left(self, store, mirror):
        self._three_missing(store, mirror)
        first = FakeDrive()
        first.upload_fails_for = {"two.txt", "three.txt"}
        apply_uploads(
            plan_uploads(store, mirror, placeholders=no_placeholders()),
            store, mirror, drive=uplink_for(first), dry_run=False,
            placeholders=no_placeholders(),
        )
        assert first.upload_names == ["one.txt"]

        second = FakeDrive()
        run = apply_uploads(
            plan_uploads(store, mirror, placeholders=no_placeholders()),
            store, mirror, drive=uplink_for(second), dry_run=False,
            placeholders=no_placeholders(),
        )

        assert sorted(second.upload_names) == ["three.txt", "two.txt"]
        assert [o.path for o in run.outcomes] == ["three.txt", "two.txt"]

    def test_the_already_sent_file_is_named_in_the_plan(self, store, mirror):
        self._three_missing(store, mirror)
        apply_uploads(
            plan_uploads(store, mirror, placeholders=no_placeholders()),
            store, mirror, drive=uplink_for(FakeDrive()), dry_run=False,
            placeholders=no_placeholders(),
        )
        plan = plan_uploads(store, mirror, placeholders=no_placeholders())
        assert plan.already_uploaded == ["one.txt", "three.txt", "two.txt"]
        assert plan.candidates == []

    def test_re_running_the_same_plan_uploads_nothing_twice(self, store, mirror):
        self._three_missing(store, mirror)
        plan = plan_uploads(store, mirror, placeholders=no_placeholders())
        drive = FakeDrive()

        apply_uploads(
            plan, store, mirror, drive=uplink_for(drive), dry_run=False,
            placeholders=no_placeholders(),
        )
        run = apply_uploads(
            plan, store, mirror, drive=uplink_for(drive), dry_run=False,
            placeholders=no_placeholders(),
        )

        assert sorted(drive.upload_names) == ["one.txt", "three.txt", "two.txt"]
        assert {o.status for o in run.outcomes} == {STATUS_SKIPPED}
        assert "already uploaded by an earlier run" in run.outcomes[0].detail


# ---------------------------------------------------------------------------
# Quota
# ---------------------------------------------------------------------------

class TestQuota:
    def test_insufficient_space_refuses_the_whole_run(self, store, mirror):
        simple_mirror(store, mirror)
        plan = plan_uploads(
            store, mirror, placeholders=no_placeholders(),
            account_storage={"available_bytes": 1},
        )

        assert plan.quota.checked is True
        assert plan.quota.sufficient is False
        assert plan.refused is True

    def test_a_refused_quota_uploads_nothing(self, store, mirror):
        simple_mirror(store, mirror)
        drive = FakeDrive()
        plan = plan_uploads(
            store, mirror, placeholders=no_placeholders(),
            account_storage={"available_bytes": 0},
        )
        run = apply_uploads(
            plan, store, mirror, drive=uplink_for(drive), dry_run=False,
            placeholders=no_placeholders(),
        )
        assert run.refused is True
        assert drive.calls == []

    def test_enough_space_does_not_refuse(self, store, mirror):
        simple_mirror(store, mirror)
        plan = plan_uploads(
            store, mirror, placeholders=no_placeholders(),
            account_storage={"available_bytes": 10 ** 9},
        )
        assert plan.quota.sufficient is True
        assert plan.refused is False

    def test_available_space_is_derived_when_apple_reports_only_totals(
        self, store, mirror
    ):
        simple_mirror(store, mirror)
        plan = plan_uploads(
            store, mirror, placeholders=no_placeholders(),
            account_storage={"total_bytes": 100, "used_bytes": 99},
        )
        assert plan.quota.available_bytes == 1
        assert plan.quota.sufficient is False

    def test_unavailable_quota_is_reported_as_unchecked_not_as_passed(
        self, store, mirror
    ):
        simple_mirror(store, mirror)
        plan = plan_uploads(store, mirror, placeholders=no_placeholders())

        assert plan.quota.checked is False
        assert plan.quota.sufficient is None
        assert plan.refused is False
        assert "NOT checked" in plan.quota.detail
        assert "NOT CHECKED" in render_plan(plan)


# ---------------------------------------------------------------------------
# Unicode
# ---------------------------------------------------------------------------

class TestUnicode:
    def test_an_nfd_remote_name_matches_an_nfc_local_one(self, store, mirror):
        """Apple returns NFD; the mirror may hold NFC. They are the same file.

        Getting this wrong would upload a duplicate of every accented filename.
        """
        nfc = unicodedata.normalize("NFC", "café.txt")
        nfd = unicodedata.normalize("NFD", "café.txt")
        add_file(mirror, nfc, b"beans")
        record_local(store, nfc)
        remote_scan(store, [RemoteItem(path=nfd, size=5)])

        plan = plan_uploads(store, mirror, placeholders=no_placeholders())
        assert plan.candidates == []
        assert plan.missing_count == 0

    def test_an_nfd_name_in_the_folder_listing_counts_as_present(self, store, mirror):
        nfc = unicodedata.normalize("NFC", "résumé.txt")
        nfd = unicodedata.normalize("NFD", "résumé.txt")
        add_file(mirror, "kept.txt", b"kept")
        add_file(mirror, nfc, b"cv")
        record_local(store, "kept.txt", nfc)
        remote_scan(store, [RemoteItem(path="kept.txt", size=4)])

        drive = FakeDrive()
        drive.root.children[nfd] = FakeNode(nfd, drive, is_dir=False)
        plan = plan_uploads(store, mirror, placeholders=no_placeholders())

        run = apply_uploads(
            plan, store, mirror, drive=uplink_for(drive), dry_run=False,
            placeholders=no_placeholders(),
        )

        assert [o.status for o in run.outcomes] == [STATUS_SKIPPED]
        assert drive.uploads == []

    def test_an_accented_file_that_really_is_missing_is_uploaded(self, store, mirror):
        nfc = unicodedata.normalize("NFC", "año.txt")
        add_file(mirror, "kept.txt", b"kept")
        add_file(mirror, nfc, b"year")
        record_local(store, "kept.txt", nfc)
        remote_scan(store, [RemoteItem(path="kept.txt", size=4)])

        drive = FakeDrive()
        run = apply_uploads(
            plan_uploads(store, mirror, placeholders=no_placeholders()),
            store, mirror, drive=uplink_for(drive), dry_run=False,
            placeholders=no_placeholders(),
        )

        assert [o.status for o in run.outcomes] == [STATUS_UPLOADED]
        assert drive.uploads[0][2] == b"year"


# ---------------------------------------------------------------------------
# Exports
# ---------------------------------------------------------------------------

class TestExports:
    def test_the_json_payload_carries_the_evidence_and_the_refusals(
        self, store, mirror
    ):
        add_file(mirror, "kept.txt", b"kept")
        add_file(mirror, "Sub/lost.txt", b"lost")
        add_file(mirror, "Deck.key/index.apxl", b"slides")
        record_local(
            store, "kept.txt", "Sub/lost.txt",
            {"path": "Deck.key", "kind": KIND_PACKAGE},
        )
        remote_scan(store, [RemoteItem(path="kept.txt", size=4)])

        payload = plan_uploads(
            store, mirror, placeholders=no_placeholders()
        ).to_dict()

        assert payload["refused"] is False
        assert payload["upload_count"] == 1
        assert payload["candidates"][0]["remote_path"] == "Documents/Sub/lost.txt"
        assert payload["refusal_counts"] == {REFUSE_PACKAGE: 1}
        for key in ("scan", "breaker", "quota", "notes", "unexamined"):
            assert key in payload
        json.dumps(payload)  # must be serialisable

    def test_the_run_payload_is_serialisable(self, store, mirror):
        simple_mirror(store, mirror)
        plan = plan_uploads(store, mirror, placeholders=no_placeholders())
        run = apply_uploads(plan, store, mirror, dry_run=True)
        json.dumps(run.to_dict())
        assert run.to_dict()["dry_run"] is True

    def test_the_csv_holds_a_row_for_every_candidate_and_refusal(self, store, mirror):
        add_file(mirror, "kept.txt", b"kept")
        add_file(mirror, "Sub/lost.txt", b"lost")
        add_file(mirror, "Deck.key/index.apxl", b"slides")
        record_local(
            store, "kept.txt", "Sub/lost.txt",
            {"path": "Deck.key", "kind": KIND_PACKAGE},
        )
        remote_scan(store, [RemoteItem(path="kept.txt", size=4)])

        plan = plan_uploads(store, mirror, placeholders=no_placeholders())
        headers, rows = csv_rows(plan)

        assert headers[:3] == ["path", "remote_path", "disposition"]
        dispositions = {row[0]: row[2] for row in rows}
        assert dispositions["Sub/lost.txt"] == STATUS_WOULD_UPLOAD
        assert dispositions["Deck.key"] == f"refused:{REFUSE_PACKAGE}"

    def test_the_csv_reflects_what_actually_happened_after_a_run(self, store, mirror):
        simple_mirror(store, mirror)
        plan = plan_uploads(store, mirror, placeholders=no_placeholders())
        run = apply_uploads(
            plan, store, mirror, drive=uplink_for(FakeDrive()), dry_run=False,
            placeholders=no_placeholders(),
        )
        _, rows = csv_rows(plan, run)
        assert rows[0][2] == STATUS_UPLOADED

    def test_the_history_report_says_so_when_nothing_was_ever_uploaded(self):
        assert "never uploaded anything" in render_uploads([])


# ---------------------------------------------------------------------------
# The CLI
# ---------------------------------------------------------------------------

def run_cli(argv):
    out = io.StringIO()
    code = main(argv, stdout=out)
    return code, out.getvalue()


class TestCommandLine:
    def test_the_parser_exposes_the_three_subcommands(self):
        parser = build_parser()
        for command in ("plan", "push", "history"):
            assert parser.parse_args([command]).command == command

    def test_plan_without_a_scan_is_an_error(self, mirror):
        code, _ = run_cli(["plan", str(mirror)])
        assert code == EXIT_ERROR

    def test_plan_with_nothing_to_upload_exits_zero(self, store, mirror):
        add_file(mirror, "kept.txt", b"kept")
        record_local(store, "kept.txt")
        remote_scan(store, [RemoteItem(path="kept.txt", size=4)])
        store.close()

        code, text = run_cli(["plan", str(mirror)])
        assert code == EXIT_OK
        assert "Nothing to upload" in text

    def test_plan_with_work_to_do_exits_one(self, store, mirror):
        simple_mirror(store, mirror)
        store.close()
        code, text = run_cli(["plan", str(mirror)])
        assert code == EXIT_FINDINGS
        assert "Documents/Sub/lost.txt" in text

    def test_a_refused_plan_exits_three(self, store, mirror):
        add_file(mirror, "kept.txt", b"kept")
        add_file(mirror, "lost.txt", b"lost")
        record_local(store, "kept.txt", "lost.txt")
        remote_scan(store, [], errors=[{"path": "/", "error": "boom"}])
        store.close()

        code, text = run_cli(["plan", str(mirror)])
        assert code == EXIT_REFUSED
        assert "REFUSED" in text

    def test_json_output_is_a_document_not_a_table(self, store, mirror):
        simple_mirror(store, mirror)
        store.close()
        code, text = run_cli(["plan", str(mirror), "--json"])
        payload = json.loads(text)
        assert code == EXIT_FINDINGS
        assert payload["upload_count"] == 1

    def test_report_writes_the_payload_to_a_file(self, store, mirror, tmp_path):
        simple_mirror(store, mirror)
        store.close()
        target = tmp_path / "out" / "uplink.json"
        run_cli(["plan", str(mirror), "--report", str(target)])
        assert json.loads(target.read_text())["upload_count"] == 1

    def test_csv_export_has_a_header_and_a_row(self, store, mirror, tmp_path):
        simple_mirror(store, mirror)
        store.close()
        target = tmp_path / "uplink.csv"
        run_cli(["plan", str(mirror), "--csv", str(target)])
        rows = list(csv.reader(target.open(encoding="utf-8")))
        assert rows[0][0] == "path"
        assert rows[1][0] == "Sub/lost.txt"

    def test_push_is_a_dry_run_by_default(self, store, mirror, monkeypatch):
        simple_mirror(store, mirror)
        store.close()
        called = []
        monkeypatch.setattr(
            "ifetch.uplink_cli._connect",
            lambda *a, **k: called.append("connect"),
        )

        code, text = run_cli(["push", str(mirror)])
        assert code == EXIT_FINDINGS
        assert called == []
        assert "dry run" in text
        assert "Bytes to send" in text
        assert "Documents/Sub/lost.txt" in text
        assert "--apply" in text
        # The gaps travel with the run report, not only with the plan report.
        assert "Not examined" in text
        assert "NOT CHECKED" in text

    def test_push_apply_uploads_and_exits_zero(self, store, mirror, monkeypatch):
        simple_mirror(store, mirror)
        store.close()
        drive = FakeDrive()
        monkeypatch.setattr("ifetch.uplink_cli._connect", lambda *a, **k: drive)
        monkeypatch.setattr("ifetch.uplink_cli.fetch_account_storage", lambda api: None)

        code, text = run_cli(["push", str(mirror), "--apply"])
        assert code == EXIT_OK
        assert drive.upload_names == ["lost.txt"]
        assert "uploaded" in text

    def test_push_apply_with_a_failure_exits_one(self, store, mirror, monkeypatch):
        simple_mirror(store, mirror)
        store.close()
        drive = FakeDrive()
        drive.upload_fails_for = {"lost.txt"}
        monkeypatch.setattr("ifetch.uplink_cli._connect", lambda *a, **k: drive)
        monkeypatch.setattr("ifetch.uplink_cli.fetch_account_storage", lambda api: None)

        code, _ = run_cli(["push", str(mirror), "--apply"])
        assert code == EXIT_FINDINGS

    def test_push_apply_refuses_when_quota_is_short(self, store, mirror, monkeypatch):
        simple_mirror(store, mirror)
        store.close()
        drive = FakeDrive()
        monkeypatch.setattr("ifetch.uplink_cli._connect", lambda *a, **k: drive)
        monkeypatch.setattr(
            "ifetch.uplink_cli.fetch_account_storage",
            lambda api: {"available_bytes": 0},
        )

        code, text = run_cli(["push", str(mirror), "--apply"])
        assert code == EXIT_REFUSED
        assert drive.uploads == []
        assert "REFUSED" in text

    def test_history_lists_what_was_uploaded(self, store, mirror, monkeypatch):
        simple_mirror(store, mirror)
        store.close()
        drive = FakeDrive()
        monkeypatch.setattr("ifetch.uplink_cli._connect", lambda *a, **k: drive)
        monkeypatch.setattr("ifetch.uplink_cli.fetch_account_storage", lambda api: None)
        run_cli(["push", str(mirror), "--apply"])

        code, text = run_cli(["history", str(mirror)])
        assert code == EXIT_OK
        assert "Documents/Sub/lost.txt" in text

    def test_history_on_a_fresh_mirror_exits_zero(self, store, mirror):
        store.close()
        code, text = run_cli(["history", str(mirror)])
        assert code == EXIT_OK
        assert "never uploaded anything" in text

    def test_a_directory_with_no_scan_is_an_error_not_a_traceback(self, tmp_path):
        code, _ = run_cli(["plan", str(tmp_path / "nope" / "deeper")])
        assert code == EXIT_ERROR


# ---------------------------------------------------------------------------
# The index migration
# ---------------------------------------------------------------------------

class TestIndexMigration:
    def test_a_fresh_index_is_at_the_current_version(self, mirror):
        with IndexStore(mirror) as store:
            assert store.get_meta("schema_version") == str(SCHEMA_VERSION)
            assert SCHEMA_VERSION >= 4

    def test_an_older_index_gains_the_uploads_table_without_losing_data(self, mirror):
        with IndexStore(mirror) as store:
            store.record_local_many([LocalItem(path="kept.txt", size=4)])
            store.set_meta("schema_version", "2")
            store._conn.execute("DROP TABLE uploads")
            store._conn.commit()

        with IndexStore(mirror) as store:
            assert store.get_meta("schema_version") == str(SCHEMA_VERSION)
            assert [r["path"] for r in store.iter_local()] == ["kept.txt"]
            store.record_upload("kept.txt", remote_path="Documents/kept.txt")
            assert store.get_upload("kept.txt")["state"] == UPLOAD_DONE

    def test_the_migration_is_additive_not_destructive(self, mirror):
        with IndexStore(mirror) as store:
            scan_id = store.begin_scan("Documents")
            store.record_remote_many([RemoteItem(path="kept.txt", size=4)], scan_id=scan_id)
            store.finish_scan(scan_id, errors=[{"path": "x", "error": "y"}])
            store.set_meta("schema_version", "1")

        with IndexStore(mirror) as store:
            latest = store.latest_scan()
            assert latest["error_count"] == 1
            assert store.remote_count() == 1

    def test_an_upload_row_survives_a_reopen(self, mirror):
        with IndexStore(mirror) as store:
            store.record_upload("a.txt", remote_path="Documents/a.txt", size=3)
        with IndexStore(mirror) as store:
            assert store.uploaded_paths() == ["a.txt"]

    def test_a_repeated_record_bumps_attempts_rather_than_duplicating(self, mirror):
        with IndexStore(mirror) as store:
            store.record_upload("a.txt", state=UPLOAD_FAILED, error="first")
            store.record_upload("a.txt", state=UPLOAD_DONE)
            row = store.get_upload("a.txt")
            assert row["attempts"] == 2
            assert row["state"] == UPLOAD_DONE
            assert len(list(store.iter_uploads())) == 1

    def test_forgetting_an_upload_makes_it_eligible_again(self, mirror):
        with IndexStore(mirror) as store:
            store.record_upload("a.txt")
            assert store.forget_upload("a.txt") is True
            assert store.uploaded_paths() == []
