import io
import sys
from pathlib import Path

import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))  # Ensure import

from ifetch.chunker import FileChunker  # noqa: E402


@pytest.fixture
def sample_file(tmp_path):
    content = b"abcdefghijklmn"  # 14 bytes
    file_path = tmp_path / "sample.bin"
    file_path.write_bytes(content)
    return file_path, content


def test_get_file_chunks(sample_file):
    file_path, content = sample_file
    chunker = FileChunker(chunk_size=5)  # deliberately small for test

    chunks = chunker.get_file_chunks(file_path)

    # Expect 3 chunks: 5 + 5 + 4 bytes
    assert len(chunks) == 3
    # Every hash maps to correct byte range lengths
    ranges = list(chunks.values())
    lengths = [end - start + 1 for start, end in ranges]
    assert lengths == [5, 5, 4]


def _make_response(data: bytes):
    """Create a fake requests.Response-like object for compute_download_ranges."""

    class _FakeRaw(io.BytesIO):
        def __init__(self, buf):
            super().__init__(buf)

    class _Resp:
        def __init__(self, buf: bytes):
            self.headers = {"content-length": str(len(buf))}
            self.raw = _FakeRaw(buf)

    return _Resp(data)


def test_compute_download_ranges_same_size_is_treated_as_unchanged(sample_file):
    """Documented limitation: equal size => assumed unchanged, content ignored."""
    file_path, content = sample_file
    chunker = FileChunker(chunk_size=5)

    # Remote content differs but has the SAME length as the local file.
    remote_content = content[:-4] + b"XXXX"
    response = _make_response(remote_content)

    ranges = chunker.compute_download_ranges(response, file_path)

    assert ranges == []


def test_compute_download_ranges_all_new(tmp_path):
    chunker = FileChunker(chunk_size=5)
    response = _make_response(b"abcde12345")

    ranges = chunker.compute_download_ranges(response, tmp_path / "missing.bin")

    assert ranges == [(0, 4), (5, 9)]


def test_compute_download_ranges_without_local_path(tmp_path):
    chunker = FileChunker(chunk_size=5)
    response = _make_response(b"abcde12345")

    assert chunker.compute_download_ranges(response) == [(0, 4), (5, 9)]


def test_compute_download_ranges_resume_from_local_size(tmp_path):
    file_path = tmp_path / "partial.bin"
    file_path.write_bytes(b"abcde")
    chunker = FileChunker(chunk_size=5)
    response = _make_response(b"abcdefghij")

    ranges = chunker.compute_download_ranges(response, file_path)

    assert ranges == [(5, 9)]


def test_compute_download_ranges_force_refetches_everything(sample_file):
    file_path, content = sample_file
    chunker = FileChunker(chunk_size=5)
    response = _make_response(content)

    assert chunker.compute_download_ranges(response, file_path) == []
    assert chunker.compute_download_ranges(response, file_path, force=True) == [
        (0, 4), (5, 9), (10, 13)
    ]


def test_compute_download_ranges_empty_local_file_downloads_everything(tmp_path):
    file_path = tmp_path / "empty.bin"
    file_path.write_bytes(b"")
    chunker = FileChunker(chunk_size=5)
    response = _make_response(b"abcdefghij")

    assert chunker.compute_download_ranges(response, file_path) == [(0, 4), (5, 9)]


def test_compute_download_ranges_longer_local_file_refetches_everything(tmp_path):
    file_path = tmp_path / "long.bin"
    file_path.write_bytes(b"abcdefghijklmno")
    chunker = FileChunker(chunk_size=5)
    response = _make_response(b"abcde")

    assert chunker.compute_download_ranges(response, file_path) == [(0, 4)]


def test_compute_download_ranges_zero_length_remote(tmp_path):
    chunker = FileChunker(chunk_size=5)
    response = _make_response(b"")

    assert chunker.compute_download_ranges(response, tmp_path / "x.bin") == []


class _HeaderlessResp:
    """Apple's package-bundle response: chunked, no content-length header."""

    def __init__(self, headers=None):
        self.headers = {"content-type": "application/octet-stream"} \
            if headers is None else headers


def test_remote_content_length_reads_the_header():
    assert FileChunker.remote_content_length(_make_response(b"abcde")) == 5
    assert FileChunker.remote_content_length(_make_response(b"")) == 0


@pytest.mark.parametrize("headers", [
    {},
    {"content-type": "application/octet-stream"},
    {"content-length": "not-a-number"},
    {"content-length": None},
    {"content-length": "-1"},
])
def test_remote_content_length_is_none_when_unusable(headers):
    assert FileChunker.remote_content_length(_HeaderlessResp(headers)) is None


def test_remote_content_length_is_none_without_headers():
    assert FileChunker.remote_content_length(object()) is None


def test_missing_content_length_is_not_zero_length(tmp_path):
    """The bug: an absent header used to be read as 'zero bytes, skip it'."""
    chunker = FileChunker(chunk_size=5)

    unknown = chunker.compute_download_ranges(_HeaderlessResp(), tmp_path / "x.app")
    empty = chunker.compute_download_ranges(_make_response(b""), tmp_path / "x.bin")

    assert unknown is None, "unknown length must be reported as unknown"
    assert empty == []
    assert unknown is not empty


def test_missing_content_length_stays_unknown_with_a_local_file(tmp_path):
    """No size to compare against -> still unknown, never 'unchanged'."""
    chunker = FileChunker(chunk_size=5)
    local = tmp_path / "x.app"
    local.write_bytes(b"abcde")

    assert chunker.compute_download_ranges(_HeaderlessResp(), local) is None
    assert chunker.compute_download_ranges(_HeaderlessResp(), local, force=True) is None
