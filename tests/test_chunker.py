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
    """Create a fake requests.Response-like object for find_changed_chunks."""

    class _FakeRaw(io.BytesIO):
        def __init__(self, buf):
            super().__init__(buf)

    class _Resp:
        def __init__(self, buf: bytes):
            self.headers = {"content-length": str(len(buf))}
            self.raw = _FakeRaw(buf)

    return _Resp(data)


def test_find_changed_chunks_detects_modification(sample_file, tmp_path):
    file_path, content = sample_file
    chunker = FileChunker(chunk_size=5)

    # Local chunks from existing file
    local_chunks = chunker.get_file_chunks(file_path)

    # Remote content with last chunk different
    remote_content = content[:-4] + b"XXXX"  # modify last 4 bytes
    response = _make_response(remote_content)

    ranges = chunker.find_changed_chunks(response, local_chunks)

    # Should flag only last chunk (offset 10-13)
    assert ranges == [(10, 13)]


def test_find_changed_chunks_all_new(tmp_path):
    chunker = FileChunker(chunk_size=5)
    remote_content = b"abcde12345"
    response = _make_response(remote_content)
    ranges = chunker.find_changed_chunks(response, existing_chunks={})
    assert ranges == [(0, len(remote_content) - 1)] 