import sys
from pathlib import Path

import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))

from ifetch.downloader import DownloadManager  # noqa: E402


class _FakeResp:
    def __init__(self, content: bytes, status_code: int = 206):
        self.content = content
        self.status_code = status_code
        self.url = "http://example.com"

    def raise_for_status(self):
        if self.status_code >= 400:
            raise Exception("HTTP error")


def test_calculate_checksum(tmp_path):
    file_path = tmp_path / "file.txt"
    file_path.write_text("hello")

    dm = DownloadManager(email="user@example.com")
    checksum = dm.calculate_checksum(file_path)

    import hashlib
    expected = hashlib.sha256(b"hello").hexdigest()
    assert checksum == expected


def test_download_chunk_success(monkeypatch):
    dm = DownloadManager(email="user@example.com", max_retries=1)

    def _fake_get(url, headers, stream, timeout):
        return _FakeResp(b"abc", status_code=206)

    monkeypatch.setattr("ifetch.downloader.requests.get", _fake_get)

    data = dm.download_chunk("http://example.com/file", 0, 2)
    assert data == b"abc" 