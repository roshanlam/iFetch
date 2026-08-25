"""End-to-end tests: a package bundle travelling through the download pipeline.

These drive the real :class:`DownloadManager` with a fake pyicloud node that
serves what Apple actually serves for a ``.key`` file - a ZIP archive, delivered
with no ``content-length``, whose byte count does not match the size the folder
listing reported. That mismatch is what makes other tools declare the transfer
corrupt and delete the file.

The contract:

* the bundle lands as a usable directory, and the size mismatch is not an error;
* ``--no-expand-packages`` still writes the raw archive, unchanged;
* a re-run skips the expanded bundle with no further downloads;
* a changed bundle is re-downloaded and replaces the old one atomically;
* an archive that cannot be expanded is never lost - it is stored verbatim.
"""

import io
import json
import sys
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))

from ifetch.downloader import DownloadManager, SyncState  # noqa: E402
from ifetch.manifest import Manifest  # noqa: E402

MTIME = datetime(2026, 3, 14, 15, 9, 26, tzinfo=timezone.utc)


def build_archive(entries, root="Deck.key"):
    """Bytes of a ZIP shaped the way Apple wraps a package."""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        for name, data in entries.items():
            zf.writestr(f"{root}/{name}" if root else name, data)
    return buffer.getvalue()


class PackageNode:
    """A pyicloud DriveNode serving a package: chunked, no content-length.

    ``size`` deliberately differs from ``len(payload)``: Apple reports the
    bundle's logical size in the listing while serving a compressed archive.
    """

    type = "file"

    def __init__(self, name="Deck.key", payload=b"", listed_size=None,
                 date_modified=MTIME):
        self.name = name
        self._payload = payload
        self.size = listed_size if listed_size is not None else len(payload) * 3
        self.date_modified = date_modified
        self.date_changed = None
        self.url = "https://example.invalid/package"
        self.open_calls = 0

    def open(self, stream=True):
        self.open_calls += 1
        payload = self._payload

        class _Response:
            def __init__(self):
                self.headers = {}  # no content-length, exactly as Apple sends
                self.url = "https://example.invalid/package"
                self.raw = io.BytesIO(payload)

            def iter_content(self, chunk_size=8192):
                stream_io = io.BytesIO(payload)
                while True:
                    chunk = stream_io.read(chunk_size)
                    if not chunk:
                        return
                    yield chunk

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

        return _Response()


@pytest.fixture
def manager(tmp_path, monkeypatch):
    """A DownloadManager wired to a temp destination, with no real auth."""
    monkeypatch.setenv("ICLOUD_EMAIL", "you@example.com")

    def build(**kwargs):
        mgr = DownloadManager(email="you@example.com", **kwargs)
        root = tmp_path / "dest"
        root.mkdir(exist_ok=True)
        mgr.root_path = root
        mgr.sync_state = SyncState(root)
        mgr.manifest = Manifest(root)
        return mgr, root

    return build


KEYNOTE = {
    "Index.zip": b"index-bytes",
    "Metadata/Properties.plist": b"<plist/>",
    "Data/image-1.jpg": b"\xff\xd8\xff\xe0jpeg-data",
}


class TestPackageExpansionEndToEnd:
    def test_keynote_file_arrives_as_a_usable_directory(self, manager):
        mgr, root = manager()
        node = PackageNode(payload=build_archive(KEYNOTE))
        destination = root / "Deck.key"

        assert mgr.download_drive_item(node, destination) is True

        assert destination.is_dir()
        assert (destination / "Index.zip").read_bytes() == b"index-bytes"
        assert (destination / "Metadata" / "Properties.plist").read_bytes() == b"<plist/>"
        assert (destination / "Data" / "image-1.jpg").read_bytes() == b"\xff\xd8\xff\xe0jpeg-data"

    def test_the_size_mismatch_is_not_treated_as_corruption(self, manager):
        """This is the exact condition that makes other tools delete the file."""
        payload = build_archive(KEYNOTE)
        mgr, root = manager()
        node = PackageNode(payload=payload, listed_size=len(payload) * 5)

        assert mgr.download_drive_item(node, root / "Deck.key") is True

        statuses = [r.status for r in mgr.download_results]
        assert statuses == ["completed"]
        assert (root / "Deck.key").is_dir()

    def test_the_downloaded_file_is_never_left_as_a_zip_blob(self, manager):
        """The failure mode this whole feature exists to prevent."""
        mgr, root = manager()
        node = PackageNode(payload=build_archive(KEYNOTE))
        destination = root / "Deck.key"

        mgr.download_drive_item(node, destination)

        assert not destination.is_file(), "wrote a ZIP where a bundle belongs"
        assert destination.is_dir()
        # And nothing inside it is the archive itself.
        members = {p.name for p in destination.rglob("*") if p.is_file()}
        assert members == {"Index.zip", "Properties.plist", "image-1.jpg"}

    def test_no_temporary_artefacts_are_left_behind(self, manager):
        mgr, root = manager()
        mgr.download_drive_item(PackageNode(payload=build_archive(KEYNOTE)), root / "Deck.key")

        leftovers = [p.name for p in root.iterdir() if "ifetch-pkg" in p.name
                     or "ifetch-old" in p.name or p.name.endswith(".temp")]
        assert leftovers == []

    def test_expansion_is_logged_with_the_entry_count(self, manager, caplog):
        mgr, root = manager()
        with caplog.at_level("INFO"):
            mgr.download_drive_item(PackageNode(payload=build_archive(KEYNOTE)), root / "Deck.key")

        events = [
            json.loads(r.message) for r in caplog.records
            if r.message.startswith("{") and "package_expanded" in r.message
        ]
        assert events and events[0]["entries"] == 3

    @pytest.mark.parametrize(
        "name", ["Deck.key", "Report.pages", "Budget.numbers", "App.xcodeproj"],
    )
    def test_every_iwork_and_developer_bundle_type_expands(self, manager, name):
        mgr, root = manager()
        node = PackageNode(name=name, payload=build_archive({"f.txt": b"x"}, root=name))
        mgr.download_drive_item(node, root / name)
        assert (root / name).is_dir()
        assert (root / name / "f.txt").read_bytes() == b"x"


class TestExpansionCanBeDisabled:
    def test_no_expand_packages_writes_the_raw_archive(self, manager):
        payload = build_archive(KEYNOTE)
        mgr, root = manager(expand_packages=False)
        destination = root / "Deck.key"

        mgr.download_drive_item(PackageNode(payload=payload), destination)

        assert destination.is_file()
        assert destination.read_bytes() == payload

    def test_a_real_zip_file_is_never_expanded_even_with_the_default_on(self, manager):
        """A user's own Archive.zip must stay a file."""
        payload = build_archive({"inner.txt": b"x"}, root="")
        mgr, root = manager()
        destination = root / "Archive.zip"

        mgr.download_drive_item(PackageNode(name="Archive.zip", payload=payload), destination)

        assert destination.is_file()
        assert destination.read_bytes() == payload

    def test_a_package_that_is_not_a_zip_is_left_alone(self, manager):
        """Name alone must not trigger expansion."""
        mgr, root = manager()
        destination = root / "Deck.key"

        mgr.download_drive_item(
            PackageNode(name="Deck.key", payload=b"just some flat bytes"), destination
        )

        assert destination.is_file()
        assert destination.read_bytes() == b"just some flat bytes"


class TestIncrementalReRuns:
    def test_an_unchanged_bundle_is_skipped_without_downloading(self, manager):
        mgr, root = manager()
        node = PackageNode(payload=build_archive(KEYNOTE))
        destination = root / "Deck.key"

        mgr.download_drive_item(node, destination)
        first_opens = node.open_calls
        mgr.download_results.clear()

        # Second run against the same remote metadata.
        assert mgr.download_drive_item(node, destination) is True

        assert node.open_calls == first_opens, "re-run opened a network stream"
        assert [r.status for r in mgr.download_results] == ["skipped"]
        assert (destination / "Index.zip").exists()

    def test_a_bundle_whose_remote_metadata_changed_is_re_downloaded(self, manager):
        mgr, root = manager()
        destination = root / "Deck.key"
        mgr.download_drive_item(PackageNode(payload=build_archive(KEYNOTE)), destination)

        changed = PackageNode(
            payload=build_archive({"Index.zip": b"NEW", "Only.txt": b"new file"}),
            date_modified=datetime(2026, 9, 9, tzinfo=timezone.utc),
        )
        mgr.download_results.clear()
        assert mgr.download_drive_item(changed, destination) is True

        assert (destination / "Index.zip").read_bytes() == b"NEW"
        assert (destination / "Only.txt").exists()
        # The stale members are gone: this is a replacement, not a merge.
        assert not (destination / "Metadata").exists()
        assert [r.status for r in mgr.download_results] == ["completed"]

    def test_a_locally_damaged_bundle_is_not_skipped(self, manager):
        """A deleted member must invalidate the skip, or backups silently rot."""
        mgr, root = manager()
        node = PackageNode(payload=build_archive(KEYNOTE))
        destination = root / "Deck.key"
        mgr.download_drive_item(node, destination)

        (destination / "Index.zip").unlink()
        mgr.download_results.clear()

        mgr.download_drive_item(node, destination)

        assert [r.status for r in mgr.download_results] == ["completed"]
        assert (destination / "Index.zip").read_bytes() == b"index-bytes"

    def test_a_truncated_member_is_not_skipped(self, manager):
        mgr, root = manager()
        node = PackageNode(payload=build_archive(KEYNOTE))
        destination = root / "Deck.key"
        mgr.download_drive_item(node, destination)

        (destination / "Index.zip").write_bytes(b"")
        mgr.download_results.clear()

        mgr.download_drive_item(node, destination)
        assert [r.status for r in mgr.download_results] == ["completed"]

    def test_a_deleted_bundle_is_re_downloaded(self, manager):
        import shutil

        mgr, root = manager()
        node = PackageNode(payload=build_archive(KEYNOTE))
        destination = root / "Deck.key"
        mgr.download_drive_item(node, destination)

        shutil.rmtree(destination)
        mgr.download_results.clear()

        mgr.download_drive_item(node, destination)
        assert destination.is_dir()
        assert [r.status for r in mgr.download_results] == ["completed"]

    def test_sync_state_records_the_package_kind(self, manager):
        mgr, root = manager()
        mgr.download_drive_item(PackageNode(payload=build_archive(KEYNOTE)), root / "Deck.key")

        entry = mgr.sync_state._data["Deck.key"]
        assert entry["status"] == SyncState.STATUS_PACKAGE
        assert entry["local_file_count"] == 3

    def test_sync_state_survives_a_save_and_reload(self, manager):
        mgr, root = manager()
        node = PackageNode(payload=build_archive(KEYNOTE))
        mgr.download_drive_item(node, root / "Deck.key")
        mgr.sync_state.save()

        reloaded = SyncState(root)
        mgr.sync_state = reloaded
        mgr.download_results.clear()
        opens = node.open_calls

        mgr.download_drive_item(node, root / "Deck.key")

        assert node.open_calls == opens
        assert [r.status for r in mgr.download_results] == ["skipped"]

    def test_upgrading_a_previously_stored_zip_blob_to_a_bundle(self, manager):
        """Users who ran an older iFetch have a ZIP where a directory belongs."""
        mgr, root = manager()
        destination = root / "Deck.key"
        destination.write_bytes(b"PK\x03\x04 stale blob from an older run")

        mgr.download_drive_item(PackageNode(payload=build_archive(KEYNOTE)), destination)

        assert destination.is_dir()
        assert (destination / "Index.zip").exists()


class TestFailureHandling:
    def test_an_unexpandable_archive_is_stored_verbatim_not_lost(self, manager, caplog):
        """Falling back to other tools' behaviour beats losing the download."""
        mgr, root = manager()
        destination = root / "Deck.key"
        broken = b"PK\x03\x04" + b"truncated garbage that is not a real zip"

        with caplog.at_level("WARNING"):
            assert mgr.download_drive_item(PackageNode(payload=broken), destination) is True

        assert destination.is_file()
        assert destination.read_bytes() == broken
        assert any("package_expansion_failed" in r.message for r in caplog.records)

    def test_a_hostile_archive_expands_only_its_safe_members(self, manager):
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w") as zf:
            zf.writestr("../escaped.txt", b"pwned")
            zf.writestr("Index.zip", b"safe")
        mgr, root = manager()
        destination = root / "Deck.key"

        mgr.download_drive_item(PackageNode(payload=buffer.getvalue()), destination)

        assert (destination / "Index.zip").read_bytes() == b"safe"
        assert not (root / "escaped.txt").exists()

    def test_an_empty_body_is_still_a_failure(self, manager):
        """Expansion must not paper over a transfer that returned nothing."""
        mgr, root = manager()
        node = PackageNode(payload=b"", listed_size=5000)

        assert mgr.download_drive_item(node, root / "Deck.key") is False
        assert [r.status for r in mgr.download_results] == ["failed"]


class TestManifestIntegration:
    def test_an_expanded_bundle_is_recorded_as_a_package(self, manager):
        mgr, root = manager()
        mgr.download_drive_item(PackageNode(payload=build_archive(KEYNOTE)), root / "Deck.key")

        entry = mgr.manifest.get(root / "Deck.key")
        assert entry["kind"] == "package"
        assert entry["file_count"] == 3
        assert len(entry["sha256"]) == 64

    def test_an_ordinary_file_is_recorded_as_a_file(self, manager):
        mgr, root = manager()
        mgr.download_drive_item(
            PackageNode(name="notes.txt", payload=b"plain text"), root / "notes.txt"
        )

        entry = mgr.manifest.get(root / "notes.txt")
        assert entry["kind"] == "file"

    def test_the_recorded_digest_verifies_against_the_expanded_bundle(self, manager):
        mgr, root = manager()
        mgr.download_drive_item(PackageNode(payload=build_archive(KEYNOTE)), root / "Deck.key")
        mgr.manifest.save()

        audit = Manifest.load(root).verify(report_untracked=False)
        assert audit.ok

    def test_corrupting_an_expanded_bundle_is_caught_by_the_manifest(self, manager):
        mgr, root = manager()
        mgr.download_drive_item(PackageNode(payload=build_archive(KEYNOTE)), root / "Deck.key")
        mgr.manifest.save()

        (root / "Deck.key" / "Index.zip").write_bytes(b"rotted")

        audit = Manifest.load(root).verify(report_untracked=False)
        assert not audit.ok
