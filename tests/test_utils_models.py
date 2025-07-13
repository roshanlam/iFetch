import sys
from types import SimpleNamespace
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from ifetch.utils import can_read_file  # noqa: E402
from ifetch.models import DownloadStatus  # noqa: E402


def test_can_read_file_true_false():
    good = SimpleNamespace(type="file", size=10, open=lambda: None)
    bad = SimpleNamespace(type="folder")
    assert can_read_file(good) is True
    assert can_read_file(bad) is False


def test_download_status_fields():
    ds = DownloadStatus(path="/tmp/foo", size=100, downloaded=50, checksum="abc", status="pending", changes=2, error="")
    assert ds.path == "/tmp/foo"
    assert ds.size == 100
    assert ds.downloaded == 50
    assert ds.checksum == "abc"
    assert ds.status == "pending"
    assert ds.changes == 2
    assert ds.error == "" 