import sys
from pathlib import Path

import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))

from ifetch import mirror  # noqa: E402
from ifetch.mirror import MirrorPipeline, MirrorResult  # noqa: E402


class _FakeDownloader:
    """Stand-in for DownloadManager tracking calls, following the interface
    the pipeline uses (authenticate/list_contents/download/summary)."""

    instances = []

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.api = None
        self.download_results = ["stale"]  # pipeline must reset this per cycle
        self.download_calls = []
        self.list_calls = []
        self.summary = {"successful": 2, "failed": 0}
        self.download_exc = None
        type(self).instances.append(self)

    def authenticate(self):
        self.api = object()

    def list_contents(self, path):
        self.list_calls.append(path)

    def download(self, icloud_path, local_path, log_file=None):
        if self.download_exc:
            raise self.download_exc
        self.download_calls.append((icloud_path, local_path, log_file))

    def generate_summary_report(self):
        return {"summary": dict(self.summary)}


class _FakeExporter:
    """Stand-in for GoogleDriveExporter."""

    instances = []

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.auth_calls = 0
        self.export_calls = []
        self.stats = {"uploaded": 5, "skipped": 3, "failed": 0}
        self.export_exc = None
        type(self).instances.append(self)

    def authenticate(self):
        self.auth_calls += 1

    def export_local_folders(self, folders, **kwargs):
        if self.export_exc:
            raise self.export_exc
        self.export_calls.append((folders, kwargs))
        return dict(self.stats)


@pytest.fixture(autouse=True)
def _patch_backends(monkeypatch):
    _FakeDownloader.instances = []
    _FakeExporter.instances = []
    monkeypatch.setattr(mirror, "DownloadManager", _FakeDownloader)
    monkeypatch.setattr(mirror, "GoogleDriveExporter", _FakeExporter)


def _make_pipeline(**overrides):
    kwargs = {
        "icloud_path": "Documents/Stuff",
        "local_path": "/nas/mirror",
        "email": "user@example.com",
    }
    kwargs.update(overrides)
    return MirrorPipeline(**kwargs)


def test_stage1_only_without_gdrive_folder():
    pipeline = _make_pipeline()

    result = pipeline.run_once()

    dm = _FakeDownloader.instances[0]
    assert dm.download_calls == [("Documents/Stuff", "/nas/mirror", None)]
    assert result.downloaded == 2
    assert result.download_failed == 0
    assert result.success is True
    assert result.stage2_ran is False
    assert result.stage2_skip_reason == "no --gdrive-folder configured"
    assert _FakeExporter.instances == []


def test_downloader_constructed_with_expected_options():
    pipeline = _make_pipeline(
        max_workers=8,
        max_retries=5,
        chunk_size=2048,
        include_patterns=["*.pdf"],
        exclude_patterns=["Archive/*"],
        log_file="/tmp/mirror.log",
    )

    pipeline.run_once()

    dm = _FakeDownloader.instances[0]
    assert dm.kwargs == {
        "email": "user@example.com",
        "max_workers": 8,
        "max_retries": 5,
        "chunk_size": 2048,
        "include_patterns": ["*.pdf"],
        "exclude_patterns": ["Archive/*"],
    }
    assert dm.download_calls == [("Documents/Stuff", "/nas/mirror", "/tmp/mirror.log")]


def test_both_stages_run_and_report_counts():
    pipeline = _make_pipeline(
        gdrive_folder="iCloud Mirror",
        credentials_file="/creds.json",
        token_file="/token.pickle",
    )

    result = pipeline.run_once()

    exporter = _FakeExporter.instances[0]
    assert exporter.kwargs == {
        "credentials_file": "/creds.json",
        "token_file": "/token.pickle",
        "root_folder_name": "iCloud Mirror",
    }
    assert exporter.auth_calls == 1
    assert exporter.export_calls == [(["/nas/mirror"], {})]
    assert result.stage2_ran is True
    assert result.uploaded == 5
    assert result.upload_skipped == 3
    assert result.upload_failed == 0
    assert result.success is True


def test_dry_run_lists_and_skips_uploads():
    pipeline = _make_pipeline(gdrive_folder="Backup", dry_run=True)

    result = pipeline.run_once()

    dm = _FakeDownloader.instances[0]
    assert dm.api is not None  # authenticated before listing
    assert dm.list_calls == ["Documents/Stuff"]
    assert dm.download_calls == []
    assert result.downloaded == 0
    assert result.stage2_ran is False
    assert result.stage2_skip_reason == "dry-run"
    assert _FakeExporter.instances == []
    assert result.success is True


def test_stage1_failures_skip_stage2(monkeypatch):
    # Documented behaviour: never mirror a partial local state onward.
    class _FailingDownloader(_FakeDownloader):
        def __init__(self, **kwargs):
            super().__init__(**kwargs)
            self.summary = {"successful": 1, "failed": 2}

    monkeypatch.setattr(mirror, "DownloadManager", _FailingDownloader)
    pipeline = _make_pipeline(gdrive_folder="Backup")

    result = pipeline.run_once()

    assert result.downloaded == 1
    assert result.download_failed == 2
    assert result.success is False
    assert result.stage2_ran is False
    assert result.stage2_skip_reason == "stage 1 had errors"
    assert _FakeExporter.instances == []


def test_stage1_exception_is_contained_and_skips_stage2(monkeypatch):
    class _ExplodingDownloader(_FakeDownloader):
        def __init__(self, **kwargs):
            super().__init__(**kwargs)
            self.download_exc = RuntimeError("boom")

    monkeypatch.setattr(mirror, "DownloadManager", _ExplodingDownloader)
    pipeline = _make_pipeline(gdrive_folder="Backup")

    result = pipeline.run_once()

    assert result.success is False
    assert len(result.errors) == 1
    assert "stage 1" in result.errors[0]
    assert "boom" in result.errors[0]
    assert result.stage2_ran is False
    assert result.stage2_skip_reason == "stage 1 had errors"
    assert _FakeExporter.instances == []


def test_stage2_exception_is_contained(monkeypatch):
    class _ExplodingExporter(_FakeExporter):
        def __init__(self, **kwargs):
            super().__init__(**kwargs)
            self.export_exc = RuntimeError("quota exceeded")

    monkeypatch.setattr(mirror, "GoogleDriveExporter", _ExplodingExporter)
    pipeline = _make_pipeline(gdrive_folder="Backup")

    result = pipeline.run_once()

    # Stage 1 succeeded, stage 2 failed but did not raise.
    assert result.downloaded == 2
    assert result.stage2_ran is False
    assert len(result.errors) == 1
    assert "stage 2" in result.errors[0]
    assert "quota exceeded" in result.errors[0]
    assert result.success is False


def test_stage2_upload_failures_mean_not_success(monkeypatch):
    class _PartialExporter(_FakeExporter):
        def __init__(self, **kwargs):
            super().__init__(**kwargs)
            self.stats = {"uploaded": 1, "skipped": 0, "failed": 4}

    monkeypatch.setattr(mirror, "GoogleDriveExporter", _PartialExporter)
    pipeline = _make_pipeline(gdrive_folder="Backup")

    result = pipeline.run_once()

    assert result.stage2_ran is True
    assert result.upload_failed == 4
    assert result.success is False


def test_backends_reused_across_cycles_and_results_reset():
    pipeline = _make_pipeline(gdrive_folder="Backup")

    first = pipeline.run_once(cycle=1)
    second = pipeline.run_once(cycle=2)

    # One downloader and one exporter for both cycles (no re-auth per cycle).
    assert len(_FakeDownloader.instances) == 1
    assert len(_FakeExporter.instances) == 1
    assert _FakeExporter.instances[0].auth_calls == 1
    assert len(_FakeDownloader.instances[0].download_calls) == 2

    # Per-cycle results reset: results list cleared before every download.
    assert _FakeDownloader.instances[0].download_results == []
    assert first.cycle == 1
    assert second.cycle == 2
    assert second.downloaded == 2


def test_missing_google_libraries_reported_as_stage2_error(monkeypatch):
    monkeypatch.setattr(mirror, "GoogleDriveExporter", None)
    pipeline = _make_pipeline(gdrive_folder="Backup")

    result = pipeline.run_once()

    assert result.stage2_ran is False
    assert len(result.errors) == 1
    assert "stage 2" in result.errors[0]
    assert result.success is False


def test_summary_line_format():
    result = MirrorResult(
        cycle=3,
        downloaded=4,
        download_failed=1,
        uploaded=2,
        upload_failed=0,
        errors=["stage 2 (local -> Google Drive): boom"],
        stage2_skip_reason=None,
        duration=1.234,
    )

    line = result.summary_line()

    assert line.startswith("[cycle 3]")
    assert "downloaded=4" in line
    assert "download_failed=1" in line
    assert "uploaded=2" in line
    assert "errors=1" in line
    assert "duration=1.2s" in line


def test_summary_line_mentions_skip_reason():
    result = MirrorResult(cycle=1, stage2_skip_reason="dry-run")

    assert "stage 2 skipped: dry-run" in result.summary_line()
