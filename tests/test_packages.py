"""Tests for Apple package bundle expansion.

The contract under test:

* a ``.key``/``.pages``/``.numbers`` payload that is really a ZIP comes back as
  a usable *directory*, not a ZIP blob wearing a document's name;
* a genuine ``.zip`` file is left alone, and so is a package that for once
  arrived as a flat file;
* extraction treats the archive as hostile input - traversal, absolute paths,
  symlinks and decompression bombs are all refused;
* the swap into place is atomic: a failure leaves the previous bundle intact,
  never a half-written one.
"""

import os
import stat
import sys
import time
import zipfile
from pathlib import Path

import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))

from ifetch.packages import (  # noqa: E402
    PACKAGE_EXTENSIONS,
    PackageExpansionError,
    directory_fingerprint,
    expand_package,
    is_package_name,
    looks_like_zip,
    package_extension,
    should_expand,
)


def make_zip(path: Path, entries, mtimes=None):
    """Write a ZIP with ``{name: bytes}`` entries and optional per-entry mtimes."""
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as zf:
        for name, data in entries.items():
            info = zipfile.ZipInfo(name)
            if mtimes and name in mtimes:
                info.date_time = mtimes[name]
            else:
                info.date_time = (2026, 3, 14, 15, 9, 26)
            if data is None:  # directory entry
                info.external_attr = (stat.S_IFDIR | 0o755) << 16
                zf.writestr(info, b"")
            else:
                info.external_attr = (stat.S_IFREG | 0o644) << 16
                zf.writestr(info, data)
    return path


def keynote_archive(path: Path, root="Deck.key"):
    """A realistic iWork package: contents wrapped in a same-named directory."""
    return make_zip(
        path,
        {
            f"{root}/Index.zip": b"index-bytes",
            f"{root}/Metadata/Properties.plist": b"<plist/>",
            f"{root}/Data/image-1.jpg": b"\xff\xd8\xff\xe0jpeg",
            f"{root}/preview.jpg": b"preview",
        },
    )


# ---------------------------------------------------------------------------
# Detection
# ---------------------------------------------------------------------------

class TestDetection:
    @pytest.mark.parametrize(
        "name", ["Deck.key", "Report.pages", "Budget.numbers", "App.xcodeproj",
                 "Song.band", "Notes.rtfd", "Thing.app", "ast.idos"],
    )
    def test_recognises_apple_package_extensions(self, name):
        assert is_package_name(name)

    @pytest.mark.parametrize(
        "name", ["report.pdf", "photo.jpg", "notes.txt", "archive.zip",
                 "data.tar.gz", "noextension"],
    )
    def test_ordinary_files_are_not_packages(self, name):
        assert not is_package_name(name)

    @pytest.mark.parametrize("name", ["DECK.KEY", "Deck.Key", "report.PAGES"])
    def test_extension_matching_is_case_insensitive(self, name):
        """Apple is inconsistent about the case it reports."""
        assert is_package_name(name)

    def test_package_extension_returns_the_original_casing(self):
        assert package_extension("DECK.KEY") == "KEY"
        assert package_extension("report.pdf") is None

    def test_zip_magic_detection(self, tmp_path):
        zip_path = make_zip(tmp_path / "a.zip", {"f.txt": b"x"})
        assert looks_like_zip(zip_path)

    def test_non_zip_is_detected(self, tmp_path):
        plain = tmp_path / "b.bin"
        plain.write_bytes(b"not a zip at all")
        assert not looks_like_zip(plain)

    def test_empty_archive_still_reads_as_zip(self, tmp_path):
        empty = tmp_path / "empty.zip"
        with zipfile.ZipFile(empty, "w"):
            pass
        assert looks_like_zip(empty)

    def test_missing_file_is_not_a_zip(self, tmp_path):
        assert not looks_like_zip(tmp_path / "nope.zip")

    def test_should_expand_requires_both_name_and_content(self, tmp_path):
        """Name alone is not evidence, and neither is being a ZIP."""
        real_zip = make_zip(tmp_path / "payload", {"f.txt": b"x"})
        plain = tmp_path / "plain"
        plain.write_bytes(b"just bytes")

        assert should_expand("Deck.key", real_zip) is True
        assert should_expand("Archive.zip", real_zip) is False   # a real .zip file
        assert should_expand("Deck.key", plain) is False         # not actually a zip
        assert should_expand("report.pdf", real_zip) is False


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------

class TestExpansion:
    def test_keynote_bundle_becomes_a_real_directory(self, tmp_path):
        archive = keynote_archive(tmp_path / "payload")
        destination = tmp_path / "Deck.key"

        result = expand_package(archive, destination)

        assert destination.is_dir()
        assert (destination / "Index.zip").read_bytes() == b"index-bytes"
        assert (destination / "Metadata" / "Properties.plist").read_bytes() == b"<plist/>"
        assert (destination / "Data" / "image-1.jpg").read_bytes() == b"\xff\xd8\xff\xe0jpeg"
        assert result.entries == 4

    def test_the_redundant_wrapper_directory_is_stripped(self, tmp_path):
        """Without this you get Deck.key/Deck.key/Index.zip."""
        archive = keynote_archive(tmp_path / "payload")
        destination = tmp_path / "Deck.key"

        result = expand_package(archive, destination)

        assert result.stripped_root == "Deck.key"
        assert not (destination / "Deck.key").exists()

    def test_archive_without_a_common_root_is_not_stripped(self, tmp_path):
        archive = make_zip(
            tmp_path / "payload", {"Index.zip": b"a", "Data/x.jpg": b"b"}
        )
        destination = tmp_path / "Deck.key"

        result = expand_package(archive, destination)

        assert result.stripped_root is None
        assert (destination / "Index.zip").exists()
        assert (destination / "Data" / "x.jpg").exists()

    def test_a_single_top_level_file_is_not_mistaken_for_a_root(self, tmp_path):
        archive = make_zip(tmp_path / "payload", {"only.txt": b"data"})
        destination = tmp_path / "Thing.key"

        result = expand_package(archive, destination)

        assert result.stripped_root is None
        assert (destination / "only.txt").read_bytes() == b"data"

    def test_strip_root_can_be_disabled(self, tmp_path):
        archive = keynote_archive(tmp_path / "payload")
        destination = tmp_path / "Deck.key"

        expand_package(archive, destination, strip_root=False)

        assert (destination / "Deck.key" / "Index.zip").exists()

    def test_modification_times_are_preserved(self, tmp_path):
        """A bundle whose members all claim 'now' has lost real information."""
        archive = make_zip(
            tmp_path / "payload",
            {"a.txt": b"a", "b.txt": b"b"},
            mtimes={"a.txt": (2021, 6, 1, 12, 0, 0), "b.txt": (2024, 1, 2, 3, 4, 0)},
        )
        destination = tmp_path / "Doc.pages"

        expand_package(archive, destination)

        a = time.localtime((destination / "a.txt").stat().st_mtime)
        b = time.localtime((destination / "b.txt").stat().st_mtime)
        assert (a.tm_year, a.tm_mon, a.tm_mday) == (2021, 6, 1)
        assert (b.tm_year, b.tm_mon, b.tm_mday) == (2024, 1, 2)

    def test_nested_directories_are_created(self, tmp_path):
        archive = make_zip(
            tmp_path / "payload", {"deep/deeper/deepest/f.txt": b"found"}
        )
        destination = tmp_path / "X.key"
        expand_package(archive, destination)
        assert (destination / "deep/deeper/deepest/f.txt").read_bytes() == b"found"

    def test_explicit_directory_entries_are_created_even_when_empty(self, tmp_path):
        archive = make_zip(
            tmp_path / "payload", {"Data/": None, "Index.zip": b"i"}
        )
        destination = tmp_path / "X.key"
        expand_package(archive, destination)
        assert (destination / "Data").is_dir()

    def test_empty_archive_yields_an_empty_directory_not_an_error(self, tmp_path):
        archive = tmp_path / "payload"
        with zipfile.ZipFile(archive, "w"):
            pass
        destination = tmp_path / "Empty.key"

        result = expand_package(archive, destination)

        assert destination.is_dir()
        assert result.entries == 0

    def test_result_is_json_serialisable(self, tmp_path):
        import json

        archive = keynote_archive(tmp_path / "payload")
        result = expand_package(archive, tmp_path / "Deck.key")
        json.dumps(result.to_dict())


# ---------------------------------------------------------------------------
# Hostile archives
# ---------------------------------------------------------------------------

class TestArchiveSafety:
    """The archive arrives over the network; it is untrusted input."""

    def test_path_traversal_is_refused(self, tmp_path):
        archive = tmp_path / "payload"
        with zipfile.ZipFile(archive, "w") as zf:
            zf.writestr("../escaped.txt", b"pwned")
            zf.writestr("legit.txt", b"fine")
        destination = tmp_path / "sub" / "Deck.key"

        result = expand_package(archive, destination)

        assert not (tmp_path / "sub" / "escaped.txt").exists()
        assert not (tmp_path / "escaped.txt").exists()
        assert (destination / "legit.txt").exists()
        assert any("escape" in s for s in result.skipped)

    def test_deep_traversal_is_refused(self, tmp_path):
        archive = tmp_path / "payload"
        with zipfile.ZipFile(archive, "w") as zf:
            zf.writestr("a/b/../../../../escaped.txt", b"pwned")
        destination = tmp_path / "Deck.key"

        result = expand_package(archive, destination)

        assert not (tmp_path.parent / "escaped.txt").exists()
        assert result.entries == 0
        assert result.skipped

    def test_windows_style_traversal_is_refused(self, tmp_path):
        """Backslash separators must not slip past a POSIX-only check."""
        archive = tmp_path / "payload"
        with zipfile.ZipFile(archive, "w") as zf:
            zf.writestr("..\\escaped.txt", b"pwned")
        destination = tmp_path / "sub" / "Deck.key"

        result = expand_package(archive, destination)

        assert not (tmp_path / "sub" / "escaped.txt").exists()
        assert result.skipped

    def test_absolute_member_path_is_refused(self, tmp_path):
        archive = tmp_path / "payload"
        with zipfile.ZipFile(archive, "w") as zf:
            zf.writestr("/tmp/evil.txt", b"pwned")
            zf.writestr("ok.txt", b"fine")
        destination = tmp_path / "Deck.key"

        result = expand_package(archive, destination)

        assert (destination / "ok.txt").exists()
        assert any("absolute" in s for s in result.skipped)

    def test_symlink_entries_are_refused(self, tmp_path):
        """A symlink in the archive could redirect later writes outside the tree."""
        archive = tmp_path / "payload"
        with zipfile.ZipFile(archive, "w") as zf:
            info = zipfile.ZipInfo("link")
            info.external_attr = (stat.S_IFLNK | 0o777) << 16
            zf.writestr(info, b"/etc/passwd")
            zf.writestr("real.txt", b"fine")
        destination = tmp_path / "Deck.key"

        result = expand_package(archive, destination)

        assert not (destination / "link").exists()
        assert (destination / "real.txt").exists()
        assert any("non-regular" in s for s in result.skipped)

    def test_too_many_entries_is_refused(self, tmp_path):
        archive = make_zip(tmp_path / "payload", {f"f{i}.txt": b"x" for i in range(20)})
        with pytest.raises(PackageExpansionError) as excinfo:
            expand_package(archive, tmp_path / "Deck.key", max_entries=5)
        assert "entries" in str(excinfo.value)

    def test_decompression_bomb_is_refused(self, tmp_path):
        archive = make_zip(tmp_path / "payload", {"big.bin": b"\0" * 100_000})
        with pytest.raises(PackageExpansionError):
            expand_package(archive, tmp_path / "Deck.key", max_total_bytes=1000)

    def test_a_refused_archive_leaves_no_partial_directory(self, tmp_path):
        archive = make_zip(tmp_path / "payload", {f"f{i}.txt": b"x" for i in range(20)})
        destination = tmp_path / "Deck.key"

        with pytest.raises(PackageExpansionError):
            expand_package(archive, destination, max_entries=5)

        assert not destination.exists()
        assert list(tmp_path.glob(".Deck.key.ifetch-pkg*")) == []

    def test_corrupt_archive_raises_rather_than_producing_junk(self, tmp_path):
        bad = tmp_path / "payload"
        bad.write_bytes(b"PK\x03\x04 then garbage that is not a zip")
        with pytest.raises(PackageExpansionError):
            expand_package(bad, tmp_path / "Deck.key")


# ---------------------------------------------------------------------------
# Atomic replacement
# ---------------------------------------------------------------------------

class TestAtomicSwap:
    def test_replacing_an_existing_bundle_leaves_only_the_new_one(self, tmp_path):
        destination = tmp_path / "Deck.key"
        (destination / "Old").mkdir(parents=True)
        (destination / "Old" / "stale.txt").write_text("old data")

        archive = make_zip(tmp_path / "payload", {"New/fresh.txt": b"new data"})
        expand_package(archive, destination)

        assert (destination / "New" / "fresh.txt").read_bytes() == b"new data"
        assert not (destination / "Old").exists()

    def test_replacing_a_file_with_an_expanded_bundle_works(self, tmp_path):
        """The upgrade path: a previous run wrote the ZIP blob; now we expand it."""
        destination = tmp_path / "Deck.key"
        destination.write_bytes(b"PK\x03\x04 old zip blob")

        archive = keynote_archive(tmp_path / "payload")
        expand_package(archive, destination)

        assert destination.is_dir()
        assert (destination / "Index.zip").exists()

    def test_no_temporary_directories_are_left_behind(self, tmp_path):
        archive = keynote_archive(tmp_path / "payload")
        expand_package(archive, tmp_path / "Deck.key")

        leftovers = [p.name for p in tmp_path.iterdir() if "ifetch-pkg" in p.name
                     or "ifetch-old" in p.name]
        assert leftovers == []

    def test_concurrent_expansions_to_sibling_paths_do_not_collide(self, tmp_path):
        """Worker threads expand different bundles in the same directory."""
        import threading

        archives = {}
        for i in range(6):
            archives[f"Deck{i}.key"] = make_zip(
                tmp_path / f"payload{i}", {f"Deck{i}.key/f.txt": f"data{i}".encode()}
            )

        errors = []

        def run(name, archive):
            try:
                expand_package(archive, tmp_path / name)
            except Exception as exc:  # pragma: no cover
                errors.append(exc)

        threads = [
            threading.Thread(target=run, args=(name, archive))
            for name, archive in archives.items()
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert errors == []
        for i in range(6):
            assert (tmp_path / f"Deck{i}.key" / "f.txt").read_bytes() == f"data{i}".encode()


# ---------------------------------------------------------------------------
# Fingerprinting
# ---------------------------------------------------------------------------

class TestDirectoryFingerprint:
    def test_counts_files_and_sums_bytes_recursively(self, tmp_path):
        root = tmp_path / "bundle"
        (root / "sub").mkdir(parents=True)
        (root / "a.txt").write_bytes(b"12345")
        (root / "sub" / "b.txt").write_bytes(b"678")

        assert directory_fingerprint(root) == (2, 8)

    def test_empty_directory_fingerprints_as_zero(self, tmp_path):
        root = tmp_path / "empty"
        root.mkdir()
        assert directory_fingerprint(root) == (0, 0)

    def test_a_removed_member_changes_the_fingerprint(self, tmp_path):
        root = tmp_path / "bundle"
        root.mkdir()
        (root / "a.txt").write_bytes(b"aaa")
        (root / "b.txt").write_bytes(b"bbb")
        before = directory_fingerprint(root)

        (root / "b.txt").unlink()

        assert directory_fingerprint(root) != before

    def test_a_truncated_member_changes_the_fingerprint(self, tmp_path):
        root = tmp_path / "bundle"
        root.mkdir()
        (root / "a.txt").write_bytes(b"aaaaa")
        before = directory_fingerprint(root)

        (root / "a.txt").write_bytes(b"aa")

        assert directory_fingerprint(root) != before
