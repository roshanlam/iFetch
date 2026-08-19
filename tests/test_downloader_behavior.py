import json
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from ifetch.downloader import DownloadManager  # noqa: E402
from ifetch.models import DownloadStatus  # noqa: E402


class _Folder(dict):
    def dir(self):
        return self


class _File:
    type = "file"
    size = 4

    def __init__(self, name="file.txt"):
        self.name = name

    def open(self, stream=True):
        raise AssertionError("open should not be called in these tests")


def test_process_item_parallel_skips_duplicate_active_download(monkeypatch, tmp_path):
    dm = DownloadManager(email="user@example.com")
    item = _File()
    local_path = tmp_path / "file.txt"
    dm._active_downloads.add(str(local_path))
    called = []

    monkeypatch.setattr(dm, "download_drive_item", lambda *args, **kwargs: called.append(True))

    dm.process_item_parallel(item, local_path, remote_path="Documents/file.txt")

    assert called == []


def test_process_item_parallel_does_not_log_success_for_skipped_file(monkeypatch, tmp_path):
    dm = DownloadManager(email="user@example.com", skip_existing=True)
    dm.root_path = tmp_path
    events = []
    item = _File()
    local_path = tmp_path / "file.txt"
    local_path.write_bytes(b"existing")  # file already on disk

    monkeypatch.setattr(dm.logger, "info", lambda payload: events.append(json.loads(payload)))

    dm.process_item_parallel(item, local_path, remote_path="Documents/file.txt")

    event_names = [e["event"] for e in events]
    assert "file_skipped" in event_names
    assert "download_success" not in event_names


def test_process_item_parallel_logs_failed_download(monkeypatch, tmp_path):
    dm = DownloadManager(email="user@example.com")
    events = []
    item = _File()
    local_path = tmp_path / "file.txt"

    monkeypatch.setattr(dm, "download_drive_item", lambda *args, **kwargs: False)
    monkeypatch.setattr(dm.logger, "error", lambda payload: events.append(json.loads(payload)))

    dm.process_item_parallel(item, local_path, remote_path="Documents/file.txt")

    assert any(event["event"] == "download_failed" for event in events)
    assert str(local_path) not in dm._active_downloads


def test_process_item_parallel_recurses_into_directories(monkeypatch, tmp_path):
    dm = DownloadManager(email="user@example.com", max_workers=2)
    folder = _Folder()
    folder["child.txt"] = _File("child.txt")
    seen = []

    monkeypatch.setattr(dm, "process_item_parallel", lambda item, path, remote_path=None: seen.append((path.name, remote_path)))

    DownloadManager.process_item_parallel(dm, folder, tmp_path / "Documents", remote_path="Documents")

    assert ("child.txt", "Documents/child.txt") in seen


def test_list_contents_logs_empty_directory(monkeypatch):
    dm = DownloadManager(email="user@example.com")
    events = []

    monkeypatch.setattr(dm, "get_drive_item", lambda path: _Folder())
    monkeypatch.setattr(dm.logger, "info", lambda payload: events.append(json.loads(payload)))

    dm.list_contents("Documents")

    assert events == [{"event": "empty_directory", "path": "Documents"}]


def test_list_contents_logs_file_item(monkeypatch):
    dm = DownloadManager(email="user@example.com")
    events = []

    monkeypatch.setattr(dm, "get_drive_item", lambda path: object())
    monkeypatch.setattr(dm.logger, "info", lambda payload: events.append(json.loads(payload)))

    dm.list_contents("Documents/file.txt")

    assert events == [{"event": "item_info", "path": "Documents/file.txt", "type": "file"}]


def test_list_contents_logs_errors(monkeypatch):
    dm = DownloadManager(email="user@example.com")
    errors = []

    monkeypatch.setattr(dm, "get_drive_item", lambda path: (_ for _ in ()).throw(Exception("boom")))
    monkeypatch.setattr(dm.logger, "error", lambda payload: errors.append(json.loads(payload)))

    dm.list_contents("Documents")

    assert errors == [{"event": "listing_error", "path": "Documents", "error": "boom"}]


def test_list_shared_roots_authenticates_and_handles_empty(monkeypatch):
    dm = DownloadManager(email="user@example.com")
    events = []

    class _Drive:
        shared = None

    def _authenticate():
        dm.api = type("API", (), {"drive": _Drive()})()

    monkeypatch.setattr(dm, "authenticate", _authenticate)
    monkeypatch.setattr(dm.logger, "info", lambda payload: events.append(json.loads(payload)))

    dm.list_shared_roots()

    assert events == [{"event": "no_shared_items"}]


def test_list_shared_roots_logs_errors(monkeypatch):
    dm = DownloadManager(email="user@example.com")
    errors = []

    class _BrokenShared:
        def dir(self):
            raise Exception("boom")

    dm.api = type("API", (), {"drive": type("Drive", (), {"shared": _BrokenShared()})()})()
    monkeypatch.setattr(dm.logger, "error", lambda payload: errors.append(json.loads(payload)))

    dm.list_shared_roots()

    assert errors == [{"event": "shared_listing_error", "error": "boom"}]


def test_generate_summary_report_counts_results():
    dm = DownloadManager(email="user@example.com")
    dm.download_results = [
        DownloadStatus(path="a", downloaded=5, status="completed", changes=2),
        DownloadStatus(path="b", downloaded=3, status="failed", changes=0),
    ]

    report = dm.generate_summary_report()

    assert report["summary"]["total_files"] == 2
    assert report["summary"]["successful"] == 1
    assert report["summary"]["failed"] == 1
    assert report["summary"]["total_bytes_transferred"] == 8
    assert report["summary"]["total_changed_chunks"] == 2


def test_download_expands_home_directory_in_local_path(monkeypatch, tmp_path):
    dm = DownloadManager(email="user@example.com")

    # Redirect the home directory so "~" expands under tmp_path on any platform.
    monkeypatch.setenv("USERPROFILE", str(tmp_path))
    monkeypatch.setenv("HOME", str(tmp_path))

    monkeypatch.setattr(dm, "authenticate", lambda: setattr(dm, "api", type("API", (), {"drive": object()})()))
    monkeypatch.setattr(dm, "get_drive_item", lambda path: "item")
    monkeypatch.setattr(dm, "process_item_parallel", lambda item, local_path, remote_path=None: None)
    monkeypatch.setattr(dm.plugin_manager, "dispatch", lambda *args, **kwargs: None)

    dm.download("Documents", "~/LocalBackup")

    # Report must land under the expanded home dir, not a literal "~" folder.
    assert (tmp_path / "LocalBackup" / "download_report.json").exists()
    assert not (Path("~") / "LocalBackup").exists()


def test_download_writes_report_and_dispatches_completion(monkeypatch, tmp_path):
    dm = DownloadManager(email="user@example.com")
    dispatched = []

    monkeypatch.setattr(dm, "authenticate", lambda: setattr(dm, "api", type("API", (), {"drive": object()})()))
    monkeypatch.setattr(dm, "get_drive_item", lambda path: "item")
    monkeypatch.setattr(dm, "process_item_parallel", lambda item, local_path, remote_path=None: dm.download_results.append(
        DownloadStatus(path=str(Path(local_path) / "file.txt"), downloaded=4, status="completed", changes=1)
    ))
    monkeypatch.setattr(dm.plugin_manager, "dispatch", lambda *args, **kwargs: dispatched.append((args, kwargs)))

    dm.download("Documents", tmp_path)

    report_path = tmp_path / "download_report.json"
    assert report_path.exists()
    payload = json.loads(report_path.read_text())
    assert payload["summary"]["successful"] == 1
    assert any(kwargs.get("name") == "download_session_completed" for _, kwargs in dispatched)
