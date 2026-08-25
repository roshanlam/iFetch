"""Tests for the signed integrity manifest.

The claim being tested is a strong one: iFetch can prove, offline and years
later, that the bytes on disk are the bytes it downloaded. Apple publishes no
content hashes, so this record is the only evidence that exists.

The contract:

* corruption that preserves size **and** mtime is still detected;
* a manifest edited to match tampered data fails signature validation;
* "no signature" and "invalid signature" are never conflated - one is a
  configuration choice, the other is evidence of tampering;
* expanded package bundles are verifiable as single units;
* nothing here needs credentials or a network.
"""

import json
import os
import sys
import time
from pathlib import Path

import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))

from ifetch.manifest import (  # noqa: E402
    ENTRY_MISSING,
    ENTRY_MODIFIED,
    ENTRY_OK,
    ENTRY_TYPE_CHANGED,
    ENTRY_UNTRACKED,
    MANIFEST_FILENAME,
    Manifest,
    ManifestKeyError,
    canonical_bytes,
    is_artifact,
    load_signing_key,
    render_audit,
    sha256_directory,
    sha256_file,
)

KEY = b"correct-horse-battery-staple"
OTHER_KEY = b"a-different-key-entirely"


@pytest.fixture
def mirror(tmp_path):
    """A small local mirror with a few files already downloaded."""
    root = tmp_path / "mirror"
    (root / "sub").mkdir(parents=True)
    (root / "a.txt").write_bytes(b"alpha content")
    (root / "b.pdf").write_bytes(b"%PDF-1.4 bravo")
    (root / "sub" / "c.txt").write_bytes(b"charlie")
    return root


def recorded(root, key=None):
    """A manifest with every file in ``root`` recorded and saved."""
    manifest = Manifest(root, key=key)
    for path in sorted(root.rglob("*")):
        if path.is_file():
            manifest.record_file(path)
    manifest.save()
    return manifest


# ---------------------------------------------------------------------------
# Hashing primitives
# ---------------------------------------------------------------------------

class TestHashing:
    def test_file_hash_is_stable_and_content_dependent(self, tmp_path):
        f = tmp_path / "f"
        f.write_bytes(b"data")
        first = sha256_file(f)
        assert first == sha256_file(f)

        f.write_bytes(b"datb")
        assert sha256_file(f) != first

    def test_directory_hash_covers_contents(self, tmp_path):
        root = tmp_path / "d"
        root.mkdir()
        (root / "x").write_bytes(b"one")
        before = sha256_directory(root)

        (root / "x").write_bytes(b"two")
        assert sha256_directory(root) != before

    def test_directory_hash_covers_structure_not_just_bytes(self, tmp_path):
        """Renaming a member must change the digest even though bytes are equal."""
        root = tmp_path / "d"
        root.mkdir()
        (root / "x").write_bytes(b"same")
        before = sha256_directory(root)

        (root / "x").rename(root / "y")
        assert sha256_directory(root) != before

    def test_directory_hash_is_order_independent(self, tmp_path):
        """Two identical trees built in different orders must hash identically."""
        a, b = tmp_path / "a", tmp_path / "b"
        for root, order in ((a, ["1", "2", "3"]), (b, ["3", "1", "2"])):
            root.mkdir()
            for name in order:
                (root / name).write_bytes(name.encode())
        assert sha256_directory(a) == sha256_directory(b)

    def test_empty_directories_hash_equal(self, tmp_path):
        a, b = tmp_path / "a", tmp_path / "b"
        a.mkdir()
        b.mkdir()
        assert sha256_directory(a) == sha256_directory(b)


# ---------------------------------------------------------------------------
# Signing keys
# ---------------------------------------------------------------------------

class TestSigningKeys:
    def test_no_key_anywhere_is_a_valid_state(self):
        assert load_signing_key(env={}) is None

    def test_explicit_key_wins(self, tmp_path):
        key_file = tmp_path / "k"
        key_file.write_bytes(b"from-file")
        result = load_signing_key(
            key="explicit", key_file=key_file, env={"IFETCH_MANIFEST_KEY": "from-env"}
        )
        assert result == b"explicit"

    def test_key_file_beats_environment(self, tmp_path):
        key_file = tmp_path / "k"
        key_file.write_bytes(b"from-file\n")
        result = load_signing_key(
            key_file=key_file, env={"IFETCH_MANIFEST_KEY": "from-env"}
        )
        assert result == b"from-file"  # trailing newline stripped

    def test_environment_variable(self):
        assert load_signing_key(env={"IFETCH_MANIFEST_KEY": "envkey"}) == b"envkey"

    def test_empty_key_file_is_an_error_not_silently_unsigned(self, tmp_path):
        """An empty key file almost always means a secret failed to be written."""
        key_file = tmp_path / "k"
        key_file.write_bytes(b"   \n")
        with pytest.raises(ManifestKeyError) as excinfo:
            load_signing_key(key_file=key_file, env={})
        assert "empty" in str(excinfo.value)

    def test_unreadable_key_file_is_an_error(self, tmp_path):
        with pytest.raises(ManifestKeyError):
            load_signing_key(key_file=tmp_path / "does-not-exist", env={})

    def test_canonical_bytes_are_order_independent(self):
        a = canonical_bytes({"b": 1, "a": {"y": 2, "x": 3}})
        b = canonical_bytes({"a": {"x": 3, "y": 2}, "b": 1})
        assert a == b


# ---------------------------------------------------------------------------
# Recording
# ---------------------------------------------------------------------------

class TestRecording:
    def test_records_digest_size_and_remote_metadata(self, mirror):
        manifest = Manifest(mirror)
        entry = manifest.record_file(
            mirror / "a.txt", remote_size=13, remote_modified="date_modified=2026-01-01"
        )
        assert entry["sha256"] == sha256_file(mirror / "a.txt")
        assert entry["size"] == 13
        assert entry["remote_size"] == 13
        assert entry["remote_modified"] == "date_modified=2026-01-01"

    def test_supplied_digest_is_trusted_to_avoid_a_second_read(self, mirror):
        """The downloader already hashed the bytes; re-reading a 4 GB file is waste."""
        manifest = Manifest(mirror)
        entry = manifest.record_file(mirror / "a.txt", sha256="deadbeef")
        assert entry["sha256"] == "deadbeef"

    def test_recording_a_missing_path_records_nothing(self, mirror):
        """Recording an absent file would manufacture a future false 'missing'."""
        manifest = Manifest(mirror)
        assert manifest.record_file(mirror / "nope.txt") is None
        assert len(manifest) == 0

    def test_recording_a_directory_as_a_file_records_nothing(self, mirror):
        manifest = Manifest(mirror)
        assert manifest.record_file(mirror / "sub") is None

    def test_package_recording_requires_a_directory(self, mirror):
        manifest = Manifest(mirror)
        assert manifest.record_package(mirror / "a.txt") is None

    def test_keys_are_relative_posix_paths(self, mirror):
        manifest = Manifest(mirror)
        manifest.record_file(mirror / "sub" / "c.txt")
        assert manifest.paths() == ["sub/c.txt"]

    def test_forget_removes_an_entry(self, mirror):
        manifest = recorded(mirror)
        manifest.forget(mirror / "a.txt")
        assert "a.txt" not in manifest.paths()

    def test_round_trips_through_disk(self, mirror):
        recorded(mirror, key=KEY)
        reloaded = Manifest.load(mirror, key=KEY)
        assert set(reloaded.paths()) == {"a.txt", "b.pdf", "sub/c.txt"}

    def test_save_is_atomic_leaving_no_temp_file(self, mirror):
        recorded(mirror)
        assert not list(mirror.glob(f"{MANIFEST_FILENAME}.tmp"))

    def test_corrupt_manifest_loads_as_empty_rather_than_raising(self, mirror):
        (mirror / MANIFEST_FILENAME).write_text("{ not json", encoding="utf-8")
        manifest = Manifest.load(mirror)
        assert len(manifest) == 0

    def test_existing_entries_survive_a_partial_later_run(self, mirror):
        """A sync that only touches one file must not erase the rest of the record."""
        recorded(mirror, key=KEY)
        second = Manifest.load(mirror, key=KEY)
        second.record_file(mirror / "a.txt")
        second.save()
        assert set(Manifest.load(mirror, key=KEY).paths()) == {
            "a.txt", "b.pdf", "sub/c.txt"
        }


# ---------------------------------------------------------------------------
# Signatures
# ---------------------------------------------------------------------------

class TestSignatures:
    def test_signed_manifest_validates_with_the_right_key(self, mirror):
        recorded(mirror, key=KEY)
        assert Manifest.load(mirror, key=KEY).check_signature() == "valid"

    def test_wrong_key_reports_invalid(self, mirror):
        recorded(mirror, key=KEY)
        assert Manifest.load(mirror, key=OTHER_KEY).check_signature() == "invalid"

    def test_unsigned_manifest_is_unsigned_not_invalid(self, mirror):
        """Choosing not to sign is not the same as a forged signature."""
        recorded(mirror)
        assert Manifest.load(mirror).check_signature() == "unsigned"

    def test_signed_manifest_read_without_a_key_reports_no_key(self, mirror):
        recorded(mirror, key=KEY)
        assert Manifest.load(mirror).check_signature() == "no_key"

    def test_absent_manifest_reports_absent(self, mirror):
        assert Manifest.load(mirror, key=KEY).check_signature() == "absent"

    def test_editing_a_digest_in_the_manifest_breaks_the_signature(self, mirror):
        """The attack this defends against: rewrite the record to match bad data."""
        recorded(mirror, key=KEY)
        payload = json.loads((mirror / MANIFEST_FILENAME).read_text())
        payload["manifest"]["files"]["a.txt"]["sha256"] = "0" * 64
        (mirror / MANIFEST_FILENAME).write_text(json.dumps(payload))

        assert Manifest.load(mirror, key=KEY).check_signature() == "invalid"

    def test_adding_an_entry_breaks_the_signature(self, mirror):
        recorded(mirror, key=KEY)
        payload = json.loads((mirror / MANIFEST_FILENAME).read_text())
        payload["manifest"]["files"]["planted.txt"] = {
            "kind": "file", "sha256": "0" * 64, "size": 1
        }
        (mirror / MANIFEST_FILENAME).write_text(json.dumps(payload))

        assert Manifest.load(mirror, key=KEY).check_signature() == "invalid"

    def test_reformatting_the_json_does_not_break_the_signature(self, mirror):
        """Signing must survive whitespace and key-order changes."""
        recorded(mirror, key=KEY)
        path = mirror / MANIFEST_FILENAME
        payload = json.loads(path.read_text())
        path.write_text(json.dumps(payload, indent=8, sort_keys=False))

        assert Manifest.load(mirror, key=KEY).check_signature() == "valid"


# ---------------------------------------------------------------------------
# Offline audit
# ---------------------------------------------------------------------------

class TestAudit:
    def test_intact_mirror_passes(self, mirror):
        recorded(mirror, key=KEY)
        audit = Manifest.load(mirror, key=KEY).verify()
        assert audit.ok
        assert audit.counts()[ENTRY_OK] == 3

    def test_bit_rot_is_detected_even_when_size_and_mtime_are_preserved(self, mirror):
        """The headline claim. Size and mtime checks cannot catch this; hashes can."""
        recorded(mirror, key=KEY)
        target = mirror / "a.txt"
        original = target.stat()

        # Flip one byte, keeping length identical, then restore the timestamp.
        data = bytearray(target.read_bytes())
        data[0] ^= 0xFF
        target.write_bytes(bytes(data))
        os.utime(target, (original.st_atime, original.st_mtime))

        assert target.stat().st_size == original.st_size
        assert target.stat().st_mtime == original.st_mtime

        audit = Manifest.load(mirror, key=KEY).verify()
        assert not audit.ok
        failure = next(e for e in audit.entries if e.path == "a.txt")
        assert failure.status == ENTRY_MODIFIED
        assert failure.expected_sha256 != failure.actual_sha256

    def test_deleted_file_is_reported_as_missing(self, mirror):
        recorded(mirror, key=KEY)
        (mirror / "b.pdf").unlink()

        audit = Manifest.load(mirror, key=KEY).verify()
        assert not audit.ok
        assert next(e for e in audit.entries if e.path == "b.pdf").status == ENTRY_MISSING

    def test_truncated_file_is_reported_as_modified(self, mirror):
        recorded(mirror, key=KEY)
        (mirror / "a.txt").write_bytes(b"tr")

        audit = Manifest.load(mirror, key=KEY).verify()
        entry = next(e for e in audit.entries if e.path == "a.txt")
        assert entry.status == ENTRY_MODIFIED
        assert entry.actual_size == 2

    def test_a_file_replaced_by_a_directory_is_reported(self, mirror):
        recorded(mirror, key=KEY)
        (mirror / "a.txt").unlink()
        (mirror / "a.txt").mkdir()

        audit = Manifest.load(mirror, key=KEY).verify()
        entry = next(e for e in audit.entries if e.path == "a.txt")
        assert entry.status == ENTRY_TYPE_CHANGED

    def test_untracked_files_are_reported_but_do_not_fail_the_audit(self, mirror):
        """A user's own notes beside a mirror are not corruption."""
        recorded(mirror, key=KEY)
        (mirror / "my-notes.md").write_text("mine")

        audit = Manifest.load(mirror, key=KEY).verify()
        untracked = [e for e in audit.entries if e.status == ENTRY_UNTRACKED]
        assert [e.path for e in untracked] == ["my-notes.md"]
        assert audit.ok is True

    def test_ifetch_own_artifacts_are_never_reported_as_untracked(self, mirror):
        recorded(mirror, key=KEY)
        (mirror / ".ifetch_state.json").write_text("{}")
        (mirror / "download_report.json").write_text("{}")
        (mirror / ".versions").mkdir()
        (mirror / ".versions" / "old").write_text("archived")

        audit = Manifest.load(mirror, key=KEY).verify()
        assert [e.path for e in audit.entries if e.status == ENTRY_UNTRACKED] == []

    def test_untracked_reporting_can_be_disabled(self, mirror):
        recorded(mirror, key=KEY)
        (mirror / "extra.txt").write_text("x")
        audit = Manifest.load(mirror, key=KEY).verify(report_untracked=False)
        assert all(e.status != ENTRY_UNTRACKED for e in audit.entries)

    def test_an_unsigned_manifest_still_detects_bit_rot(self, mirror):
        """Signing is optional; without it the audit still does its main job."""
        recorded(mirror)
        (mirror / "a.txt").write_bytes(b"corrupted!!!")

        audit = Manifest.load(mirror).verify()
        assert audit.signature_status == "unsigned"
        assert audit.ok is False
        assert next(e for e in audit.entries if e.path == "a.txt").status == ENTRY_MODIFIED

    def test_an_intact_unsigned_mirror_passes(self, mirror):
        """An unsigned manifest is a weaker claim, not an automatic failure."""
        recorded(mirror)
        audit = Manifest.load(mirror).verify()
        assert audit.signature_status == "unsigned"
        assert audit.ok is True

    def test_a_manifest_rewritten_with_the_wrong_key_is_positively_rejected(self, mirror):
        """Files match the record, but the record is provably not ours."""
        recorded(mirror, key=KEY)
        (mirror / "a.txt").write_bytes(b"replaced entirely")

        forged = Manifest.load(mirror, key=OTHER_KEY)
        forged.record_file(mirror / "a.txt")
        forged.save()

        audit = Manifest.load(mirror, key=KEY).verify()
        assert audit.signature_status == "invalid"
        # Every file matches the forged record...
        assert all(e.status in (ENTRY_OK, ENTRY_UNTRACKED) for e in audit.entries)
        # ...and the audit still fails, because the record itself is not ours.
        assert audit.ok is False

    def test_stripping_a_signature_is_caught_by_require_signature(self, mirror):
        """The unsigned-forgery case is a policy decision, exercised at the CLI."""
        recorded(mirror, key=KEY)
        (mirror / "a.txt").write_bytes(b"replaced entirely")

        forged = Manifest.load(mirror)
        forged.record_file(mirror / "a.txt")
        forged.save()  # unsigned

        audit = Manifest.load(mirror, key=KEY).verify()
        assert audit.signature_status == "unsigned"
        # The audit alone cannot tell this from "never signed"...
        assert audit.ok is True
        # ...which is exactly why --require-signature exists (see test_verify_cli).
        assert audit.signature_status != "valid"

    def test_expanded_package_is_verified_as_one_unit(self, mirror):
        bundle = mirror / "Deck.key"
        (bundle / "Data").mkdir(parents=True)
        (bundle / "Index.zip").write_bytes(b"index")
        (bundle / "Data" / "img.jpg").write_bytes(b"jpeg")

        manifest = Manifest(mirror, key=KEY)
        manifest.record_package(bundle, remote_size=999)
        manifest.save()

        audit = Manifest.load(mirror, key=KEY).verify()
        assert audit.ok
        assert [e.path for e in audit.entries if e.status == ENTRY_OK] == ["Deck.key"]

    def test_a_corrupted_member_fails_the_whole_package(self, mirror):
        bundle = mirror / "Deck.key"
        bundle.mkdir()
        (bundle / "Index.zip").write_bytes(b"index")

        manifest = Manifest(mirror, key=KEY)
        manifest.record_package(bundle)
        manifest.save()

        (bundle / "Index.zip").write_bytes(b"CORRUPT")

        audit = Manifest.load(mirror, key=KEY).verify()
        entry = next(e for e in audit.entries if e.path == "Deck.key")
        assert entry.status == ENTRY_MODIFIED

    def test_package_members_are_not_reported_as_untracked(self, mirror):
        """One entry covers the bundle; its members must not each be flagged."""
        bundle = mirror / "Deck.key"
        (bundle / "Data").mkdir(parents=True)
        (bundle / "Index.zip").write_bytes(b"index")
        (bundle / "Data" / "img.jpg").write_bytes(b"jpeg")

        manifest = Manifest(mirror, key=KEY)
        for path in (mirror / "a.txt", mirror / "b.pdf", mirror / "sub" / "c.txt"):
            manifest.record_file(path)
        manifest.record_package(bundle)
        manifest.save()

        audit = Manifest.load(mirror, key=KEY).verify()
        assert [e.path for e in audit.entries if e.status == ENTRY_UNTRACKED] == []

    def test_a_package_flattened_into_a_file_is_reported(self, mirror):
        bundle = mirror / "Deck.key"
        bundle.mkdir()
        (bundle / "Index.zip").write_bytes(b"index")
        manifest = Manifest(mirror, key=KEY)
        manifest.record_package(bundle)
        manifest.save()

        import shutil

        shutil.rmtree(bundle)
        bundle.write_bytes(b"PK\x03\x04zip blob")

        audit = Manifest.load(mirror, key=KEY).verify()
        entry = next(e for e in audit.entries if e.path == "Deck.key")
        assert entry.status == ENTRY_TYPE_CHANGED

    def test_progress_callback_is_invoked_per_entry(self, mirror):
        recorded(mirror, key=KEY)
        seen = []
        Manifest.load(mirror, key=KEY).verify(
            progress=lambda done, total, result: seen.append((done, total))
        )
        assert seen == [(1, 3), (2, 3), (3, 3)]

    def test_audit_is_json_serialisable(self, mirror):
        recorded(mirror, key=KEY)
        json.dumps(Manifest.load(mirror, key=KEY).verify().to_dict())

    def test_rendered_report_names_the_problem_files(self, mirror):
        recorded(mirror, key=KEY)
        (mirror / "a.txt").write_bytes(b"changed!!!!!")

        text = render_audit(Manifest.load(mirror, key=KEY).verify())
        assert "a.txt" in text
        assert "PROBLEMS FOUND" in text

    def test_rendered_report_says_intact_when_clean(self, mirror):
        recorded(mirror, key=KEY)
        text = render_audit(Manifest.load(mirror, key=KEY).verify())
        assert "RESULT: intact" in text


class TestArtifactDetection:
    @pytest.mark.parametrize(
        "path",
        [
            ".ifetch_manifest.json", ".ifetch_state.json", "download_report.json",
            "verify_report.json", ".versions/old/file.txt",
        ],
    )
    def test_artifacts_are_recognised(self, path):
        assert is_artifact(path)

    @pytest.mark.parametrize("path", ["a.txt", "sub/b.pdf", "Deck.key/Index.zip"])
    def test_real_content_is_not_an_artifact(self, path):
        assert not is_artifact(path)
