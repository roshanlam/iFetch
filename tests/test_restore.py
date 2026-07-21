import json
import sys
from pathlib import Path

import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))

from ifetch import restore as restore_mod  # noqa: E402
from ifetch.restore import RestoreError, RestoreManager  # noqa: E402
from ifetch.versioning import VersionManager, compute_checksum  # noqa: E402


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _archive(vm, root, rel, content):
    """Write *content* to root/rel then archive it as a previous version."""
    path = root / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)
    entry = vm.record_version(Path(rel), compute_checksum(path), path)
    assert entry is not None
    return entry


def _read_meta(root):
    return json.loads((root / VersionManager.META_FILENAME).read_text())


def _write_meta(root, data):
    (root / VersionManager.META_FILENAME).write_text(json.dumps(data, indent=2))


def _set_epochs(root, rel, epochs):
    """Force deterministic epochs on the recorded versions of *rel*."""
    meta = _read_meta(root)
    versions = meta[rel]
    assert len(versions) == len(epochs)
    for version, epoch in zip(versions, epochs):
        version["epoch"] = epoch
    _write_meta(root, meta)


@pytest.fixture
def tree(tmp_path):
    """A root with notes.txt archived twice ("v1", "v2") and "v3" live."""
    root = tmp_path / "dl"
    root.mkdir()
    vm = VersionManager(root)
    _archive(vm, root, "notes.txt", "v1")
    _archive(vm, root, "notes.txt", "v2")
    (root / "notes.txt").write_text("v3")
    return root


# ---------------------------------------------------------------------------
# Listing
# ---------------------------------------------------------------------------
def test_list_versions_newest_first(tree):
    listing = RestoreManager(tree).list_versions()

    assert list(listing) == ["notes.txt"]
    entries = listing["notes.txt"]
    assert [e.version for e in entries] == [2, 1]
    assert [e.index for e in entries] == [0, 1]
    assert all(e.exists for e in entries)
    assert all(e.size == 2 for e in entries)
    assert all(e.checksum and e.timestamp for e in entries)


def test_list_versions_single_path_and_unknown_path(tree):
    manager = RestoreManager(tree)
    assert list(manager.list_versions("notes.txt")) == ["notes.txt"]

    with pytest.raises(RestoreError):
        manager.list_versions("nope.txt")


def test_list_versions_accepts_absolute_path_via_restore(tree):
    manager = RestoreManager(tree)
    result = manager.restore(tree / "notes.txt", index=0)
    assert result.success
    assert result.actions[0].rel_path == "notes.txt"


def test_absent_version_store_lists_nothing(tmp_path):
    manager = RestoreManager(tmp_path)
    assert manager.list_versions() == {}
    assert manager.has_history() is False
    # Listing must not create the .versions directory as a side effect.
    assert not (tmp_path / VersionManager.VERSIONS_DIRNAME).exists()


def test_corrupt_metadata_is_ignored(tmp_path):
    (tmp_path / VersionManager.META_FILENAME).write_text("{not json")
    assert RestoreManager(tmp_path).list_versions() == {}


# ---------------------------------------------------------------------------
# Restore by index / version / timestamp
# ---------------------------------------------------------------------------
def test_restore_by_index_replaces_current_file(tree):
    result = RestoreManager(tree).restore("notes.txt", index=0)

    assert result.success
    action = result.actions[0]
    assert action.status == "restored"
    assert action.version == 2
    assert (tree / "notes.txt").read_text() == "v2"


def test_restore_by_version_number(tree):
    result = RestoreManager(tree).restore("notes.txt", version=1)

    assert result.success
    assert (tree / "notes.txt").read_text() == "v1"


def test_restore_defaults_to_newest_version(tree):
    RestoreManager(tree).restore("notes.txt")
    assert (tree / "notes.txt").read_text() == "v2"


def test_restore_by_timestamp_content_mode(tree):
    # v1 archived at t=100, v2 archived at t=300; content live at t=200 is the
    # copy that was archived at 300.
    _set_epochs(tree, "notes.txt", [100, 300])

    result = RestoreManager(tree).restore("notes.txt", timestamp=200)

    assert result.success
    assert result.actions[0].version == 2
    assert (tree / "notes.txt").read_text() == "v2"


def test_restore_by_timestamp_archived_mode(tree):
    _set_epochs(tree, "notes.txt", [100, 300])

    result = RestoreManager(tree).restore(
        "notes.txt", timestamp=200, select="archived"
    )

    assert result.actions[0].version == 1
    assert (tree / "notes.txt").read_text() == "v1"


def test_restore_by_exact_timestamp_wins_in_both_modes(tree):
    _set_epochs(tree, "notes.txt", [100, 300])

    for mode in ("content", "archived"):
        (tree / "notes.txt").write_text("live")
        result = RestoreManager(tree).restore(
            "notes.txt", timestamp=100, select=mode
        )
        assert result.actions[0].version == 1, mode
        assert (tree / "notes.txt").read_text() == "v1"


def test_unparseable_timestamp_errors_cleanly(tree):
    with pytest.raises(RestoreError):
        RestoreManager(tree).restore("notes.txt", timestamp="not-a-date")


def test_timestamp_with_no_candidate_errors_cleanly(tree):
    _set_epochs(tree, "notes.txt", [100, 300])
    with pytest.raises(RestoreError):
        RestoreManager(tree).restore("notes.txt", timestamp=9999, select="content")


def test_missing_version_errors_cleanly(tree):
    manager = RestoreManager(tree)

    with pytest.raises(RestoreError):
        manager.restore("notes.txt", version=99)
    with pytest.raises(RestoreError):
        manager.restore("notes.txt", index=7)
    with pytest.raises(RestoreError):
        manager.restore("does-not-exist.txt", index=0)

    # Nothing was touched by any of the failed attempts.
    assert (tree / "notes.txt").read_text() == "v3"


def test_missing_archived_file_is_an_error_action(tree):
    entries = RestoreManager(tree).list_versions("notes.txt")["notes.txt"]
    Path(entries[0].archived_path).unlink()

    result = RestoreManager(tree).restore("notes.txt", index=0)

    assert not result.success
    assert "missing" in result.errors[0].message
    assert (tree / "notes.txt").read_text() == "v3"


# ---------------------------------------------------------------------------
# Alternate destination
# ---------------------------------------------------------------------------
def test_restore_to_alternate_directory_leaves_current_alone(tree, tmp_path):
    dest_dir = tmp_path / "inspect"
    dest_dir.mkdir()

    result = RestoreManager(tree).restore("notes.txt", index=1, to=dest_dir)

    assert result.success
    assert (dest_dir / "notes.txt").read_text() == "v1"
    assert (tree / "notes.txt").read_text() == "v3"


def test_restore_to_explicit_file_path(tree, tmp_path):
    target = tmp_path / "old-notes.txt"

    result = RestoreManager(tree).restore("notes.txt", version=1, to=target)

    assert result.success
    assert target.read_text() == "v1"
    assert (tree / "notes.txt").read_text() == "v3"


def test_restore_to_existing_outside_file_requires_force(tree, tmp_path):
    target = tmp_path / "old-notes.txt"
    target.write_text("precious")

    result = RestoreManager(tree).restore("notes.txt", version=1, to=target)
    assert not result.success
    assert "force" in result.errors[0].message
    assert target.read_text() == "precious"

    forced = RestoreManager(tree).restore(
        "notes.txt", version=1, to=target, force=True
    )
    assert forced.success
    assert target.read_text() == "v1"


# ---------------------------------------------------------------------------
# Archiving the current copy before overwriting
# ---------------------------------------------------------------------------
def test_current_file_is_archived_before_overwrite(tree):
    manager = RestoreManager(tree)
    result = manager.restore("notes.txt", version=1)

    action = result.actions[0]
    assert action.archives_current is True
    assert action.archived_current
    archived = Path(action.archived_current)
    assert archived.exists()
    assert archived.read_text() == "v3"

    # The restore is itself undoable: "v3" is now the newest version.
    entries = RestoreManager(tree).list_versions("notes.txt")["notes.txt"]
    assert entries[0].version == 3
    RestoreManager(tree).restore("notes.txt", version=3)
    assert (tree / "notes.txt").read_text() == "v3"


def test_restore_never_deletes_the_archive(tree):
    entries = RestoreManager(tree).list_versions("notes.txt")["notes.txt"]
    archived_paths = [Path(e.archived_path) for e in entries]

    RestoreManager(tree).restore("notes.txt", version=1)

    assert all(p.exists() for p in archived_paths)


def test_no_archive_when_destination_is_absent(tree):
    (tree / "notes.txt").unlink()

    result = RestoreManager(tree).restore("notes.txt", version=1)

    assert result.success
    assert result.actions[0].archives_current is False
    assert result.actions[0].archived_current is None
    assert (tree / "notes.txt").read_text() == "v1"


def test_restoring_the_live_baseline_entry_is_skipped(tree):
    """A v0 baseline entry points at the live file - restoring it is a no-op."""
    meta = _read_meta(tree)
    meta["notes.txt"].append({
        "version": 3,
        "checksum": compute_checksum(tree / "notes.txt"),
        "archived_path": str(tree / "notes.txt"),
        "timestamp": "20990101T000000",
    })
    _write_meta(tree, meta)

    manager = RestoreManager(tree)
    entries = manager.list_versions("notes.txt")["notes.txt"]
    assert entries[0].is_current is True

    result = manager.restore("notes.txt", index=0)
    assert result.success
    assert result.actions[0].status == "skipped"
    assert (tree / "notes.txt").read_text() == "v3"


# ---------------------------------------------------------------------------
# Dry run
# ---------------------------------------------------------------------------
def test_dry_run_touches_nothing(tree):
    before_meta = (tree / VersionManager.META_FILENAME).read_text()
    before_files = sorted(p.relative_to(tree).as_posix() for p in tree.rglob("*"))

    result = RestoreManager(tree, dry_run=True).restore("notes.txt", version=1)

    assert result.success
    assert result.dry_run
    action = result.actions[0]
    assert action.status == "planned"
    assert action.archives_current is True
    assert action.archived_current is None
    assert (tree / "notes.txt").read_text() == "v3"
    assert (tree / VersionManager.META_FILENAME).read_text() == before_meta
    assert sorted(p.relative_to(tree).as_posix() for p in tree.rglob("*")) == before_files


def test_dry_run_still_reports_checksum_problems(tree):
    entries = RestoreManager(tree).list_versions("notes.txt")["notes.txt"]
    Path(entries[0].archived_path).write_text("tampered")

    result = RestoreManager(tree, dry_run=True).restore("notes.txt", index=0)

    assert not result.success
    assert "checksum mismatch" in result.errors[0].message


# ---------------------------------------------------------------------------
# Integrity
# ---------------------------------------------------------------------------
def test_checksum_mismatch_aborts_without_writing(tree):
    entries = RestoreManager(tree).list_versions("notes.txt")["notes.txt"]
    Path(entries[0].archived_path).write_text("corrupted")

    result = RestoreManager(tree).restore("notes.txt", index=0)

    assert not result.success
    assert result.errors[0].status == "error"
    assert "refusing to restore" in result.errors[0].message
    # Current file untouched and not archived.
    assert (tree / "notes.txt").read_text() == "v3"
    assert len(_read_meta(tree)["notes.txt"]) == 2


def test_verification_can_be_disabled(tree):
    entries = RestoreManager(tree).list_versions("notes.txt")["notes.txt"]
    Path(entries[0].archived_path).write_text("corrupted")

    result = RestoreManager(tree, verify=False).restore("notes.txt", index=0)

    assert result.success
    assert (tree / "notes.txt").read_text() == "corrupted"


def test_post_write_mismatch_removes_the_bad_file(tree, monkeypatch):
    manager = RestoreManager(tree)
    calls = {"n": 0}
    real = restore_mod.compute_checksum

    def fake(path):
        calls["n"] += 1
        # 1st: source verify, 2nd: checksum of current file being archived,
        # 3rd: verification of what was written.
        if calls["n"] == 3:
            return "totally-different"
        return real(path)

    monkeypatch.setattr(restore_mod, "compute_checksum", fake)

    result = manager.restore("notes.txt", version=1)

    assert not result.success
    assert "checksum mismatch after writing" in result.errors[0].message
    assert not (tree / "notes.txt").exists()
    # The overwritten copy was archived first, so nothing was actually lost.
    assert Path(result.errors[0].archived_current).read_text() == "v3"


# ---------------------------------------------------------------------------
# Point-in-time restore-all
# ---------------------------------------------------------------------------
@pytest.fixture
def multi_tree(tmp_path):
    root = tmp_path / "dl"
    root.mkdir()
    vm = VersionManager(root)
    _archive(vm, root, "a.txt", "a-old")
    _archive(vm, root, "a.txt", "a-mid")
    _archive(vm, root, "docs/b.txt", "b-old")
    _archive(vm, root, "docs/b.txt", "b-mid")
    (root / "a.txt").write_text("a-bad")
    (root / "docs" / "b.txt").write_text("b-bad")
    _set_epochs(root, "a.txt", [100, 300])
    _set_epochs(root, "docs/b.txt", [150, 400])
    return root


def test_restore_all_point_in_time(multi_tree):
    result = RestoreManager(multi_tree).restore_all(timestamp=200)

    assert result.success
    assert len(result.restored) == 2
    assert (multi_tree / "a.txt").read_text() == "a-mid"
    assert (multi_tree / "docs" / "b.txt").read_text() == "b-mid"


def test_restore_all_archived_mode(multi_tree):
    result = RestoreManager(multi_tree).restore_all(timestamp=200, select="archived")

    assert result.success
    assert (multi_tree / "a.txt").read_text() == "a-old"
    assert (multi_tree / "docs" / "b.txt").read_text() == "b-old"


def test_restore_all_skips_files_without_a_candidate(multi_tree):
    result = RestoreManager(multi_tree).restore_all(timestamp=9999)

    assert result.success
    assert len(result.restored) == 0
    assert len(result.skipped) == 2
    assert (multi_tree / "a.txt").read_text() == "a-bad"


def test_restore_all_dry_run_changes_nothing(multi_tree):
    before = (multi_tree / VersionManager.META_FILENAME).read_text()

    result = RestoreManager(multi_tree, dry_run=True).restore_all(timestamp=200)

    assert result.success
    assert all(a.status == "planned" for a in result.actions)
    assert (multi_tree / "a.txt").read_text() == "a-bad"
    assert (multi_tree / VersionManager.META_FILENAME).read_text() == before


def test_restore_all_to_alternate_root(multi_tree, tmp_path):
    out = tmp_path / "snapshot"

    result = RestoreManager(multi_tree).restore_all(timestamp=200, to=out)

    assert result.success
    assert (out / "a.txt").read_text() == "a-mid"
    assert (out / "docs" / "b.txt").read_text() == "b-mid"
    assert (multi_tree / "a.txt").read_text() == "a-bad"


def test_restore_all_on_empty_store_is_a_no_op(tmp_path):
    result = RestoreManager(tmp_path).restore_all(timestamp=200)

    assert result.success
    assert result.actions == []


# ---------------------------------------------------------------------------
# Path traversal safety
# ---------------------------------------------------------------------------
def test_malicious_metadata_key_cannot_escape_the_root(tree, tmp_path):
    outside = tmp_path / "outside.txt"
    victim = "../outside.txt"
    meta = _read_meta(tree)
    meta[victim] = list(meta["notes.txt"])
    _write_meta(tree, meta)

    manager = RestoreManager(tree)
    with pytest.raises(RestoreError):
        manager.restore(victim, index=0)
    assert not outside.exists()

    # restore-all must contain the damage too, and still handle the good file.
    result = RestoreManager(tree).restore_all(timestamp=0)
    assert not result.success
    assert any("escapes" in e.message for e in result.errors)
    assert not outside.exists()


def test_malicious_archived_path_is_refused(tree, tmp_path):
    secret = tmp_path / "secret.txt"
    secret.write_text("secret")

    meta = _read_meta(tree)
    meta["notes.txt"][0]["archived_path"] = str(secret)
    _write_meta(tree, meta)

    result = RestoreManager(tree).restore("notes.txt", version=1)

    assert not result.success
    assert "outside" in result.errors[0].message
    assert (tree / "notes.txt").read_text() == "v3"


def test_malicious_key_cannot_escape_alternate_destination(tree, tmp_path):
    meta = _read_meta(tree)
    meta["../escape.txt"] = list(meta["notes.txt"])
    _write_meta(tree, meta)

    out = tmp_path / "out"
    out.mkdir()
    with pytest.raises(RestoreError):
        RestoreManager(tree).restore("../escape.txt", index=0, to=out)
    assert not (tmp_path / "escape.txt").exists()


def test_absolute_path_outside_root_is_refused(tree, tmp_path):
    with pytest.raises(RestoreError):
        RestoreManager(tree).restore(tmp_path / "elsewhere.txt", index=0)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def test_cli_help_exits_zero(capsys):
    with pytest.raises(SystemExit) as exc:
        restore_mod.main(["--help"])

    assert exc.value.code == 0
    out = capsys.readouterr().out
    assert "restore-all" in out


def test_cli_without_command_returns_two(capsys):
    assert restore_mod.main([]) == 2
    assert "usage" in capsys.readouterr().err.lower()


def test_cli_list_human_output(tree, capsys):
    code = restore_mod.main(["--root", str(tree), "list"])

    assert code == 0
    out = capsys.readouterr().out
    assert "notes.txt" in out
    assert "[0] v2" in out


def test_cli_list_json_output(tree, capsys):
    code = restore_mod.main(["--root", str(tree), "list", "--json"])

    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["success"] is True
    assert payload["files"][0]["path"] == "notes.txt"
    assert [v["version"] for v in payload["files"][0]["versions"]] == [2, 1]


def test_cli_list_limit(tree, capsys):
    restore_mod.main(["--root", str(tree), "list", "--limit", "1"])
    out = capsys.readouterr().out
    assert "[0] v2" in out
    assert "[1] v1" not in out
    assert "not shown" in out


def test_cli_list_unknown_path_returns_one(tree, capsys):
    code = restore_mod.main(["--root", str(tree), "list", "missing.txt"])

    assert code == 1
    assert "no version history" in capsys.readouterr().err


def test_cli_missing_root_returns_one(tmp_path, capsys):
    code = restore_mod.main(["--root", str(tmp_path / "nope"), "list"])

    assert code == 1
    assert "does not exist" in capsys.readouterr().err


def test_cli_restore_by_version(tree, capsys):
    code = restore_mod.main(
        ["--root", str(tree), "restore", "notes.txt", "--version", "1"]
    )

    assert code == 0
    assert (tree / "notes.txt").read_text() == "v1"
    assert "restored notes.txt" in capsys.readouterr().out


def test_cli_flags_accepted_after_subcommand(tree, capsys):
    code = restore_mod.main(
        ["restore", "notes.txt", "--index", "0", "--root", str(tree), "--json"]
    )

    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["success"] is True
    assert payload["actions"][0]["version"] == 2


def test_cli_dry_run_reports_without_changing(tree, capsys):
    code = restore_mod.main(
        ["--root", str(tree), "restore", "notes.txt", "--version", "1", "--dry-run"]
    )

    assert code == 0
    assert (tree / "notes.txt").read_text() == "v3"
    out = capsys.readouterr().out
    assert "[dry-run] would restore" in out


def test_cli_restore_to_destination(tree, tmp_path, capsys):
    out_dir = tmp_path / "peek"
    out_dir.mkdir()

    code = restore_mod.main(
        ["--root", str(tree), "restore", "notes.txt", "--version", "1",
         "--to", str(out_dir)]
    )

    assert code == 0
    assert (out_dir / "notes.txt").read_text() == "v1"
    assert (tree / "notes.txt").read_text() == "v3"


def test_cli_restore_all_dry_run_json(multi_tree, capsys):
    code = restore_mod.main(
        ["--root", str(multi_tree), "restore-all", "--at", "200",
         "--dry-run", "--json"]
    )

    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["dry_run"] is True
    assert payload["restored"] == 2
    assert (multi_tree / "a.txt").read_text() == "a-bad"


def test_cli_restore_all_applies(multi_tree, capsys):
    code = restore_mod.main(["--root", str(multi_tree), "restore-all", "--at", "200"])

    assert code == 0
    assert (multi_tree / "a.txt").read_text() == "a-mid"


def test_cli_returns_one_on_failed_action(tree, capsys):
    entries = RestoreManager(tree).list_versions("notes.txt")["notes.txt"]
    Path(entries[0].archived_path).write_text("corrupted")

    code = restore_mod.main(["--root", str(tree), "restore", "notes.txt", "--index", "0"])

    assert code == 1
    assert "checksum mismatch" in capsys.readouterr().err
    assert (tree / "notes.txt").read_text() == "v3"


def test_cli_json_error_payload(tree, capsys):
    code = restore_mod.main(
        ["--root", str(tree), "restore", "notes.txt", "--version", "42", "--json"]
    )

    assert code == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["success"] is False
    assert "42" in payload["error"]


def test_cli_no_verify_flag(tree, capsys):
    entries = RestoreManager(tree).list_versions("notes.txt")["notes.txt"]
    Path(entries[0].archived_path).write_text("corrupted")

    code = restore_mod.main(
        ["--root", str(tree), "restore", "notes.txt", "--index", "0", "--no-verify"]
    )

    assert code == 0
    assert (tree / "notes.txt").read_text() == "corrupted"


def test_cli_mutually_exclusive_selectors(tree):
    with pytest.raises(SystemExit) as exc:
        restore_mod.main(
            ["--root", str(tree), "restore", "notes.txt", "--index", "0", "--version", "1"]
        )
    assert exc.value.code == 2
