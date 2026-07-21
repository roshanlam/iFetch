from pathlib import Path
from typing import List, Tuple, Dict, Optional, Any
import hashlib


class FileChunker:
    """Splits transfers into byte ranges and decides how much of a file to fetch.

    NOTE ON SCOPE: despite the historical name of this module, iFetch does **not**
    perform content-based (rolling-hash / MD5) chunk diffing against the remote
    file.  iCloud exposes no per-chunk digests and the download stream is not
    seekable, so the only cheap evidence available is the total size.  What this
    class actually implements is:

    * size-based change detection (same size -> assume unchanged), and
    * prefix resume (shorter local file -> fetch only the missing tail).

    Real content-based chunk diffing is deliberately deferred to 1.1.
    """

    def __init__(self, chunk_size: int = 1024 * 1024):
        """Initialize the chunker with a specific chunk size.

        Args:
            chunk_size: Size of each chunk in bytes (default: 1MB)
        """
        self.chunk_size = chunk_size

    def get_file_chunks(self, file_path: Path) -> Dict[str, Tuple[int, int]]:
        """
        Analyze an existing file and return its chunks with MD5 hashes.

        WARNING: this reads and hashes every byte of ``file_path``.  It is *not*
        used by the download path (nothing consumes the hashes today) and must
        not be reintroduced there — doing so costs a full-file hash per file on
        every single run.  Retained only as a utility / for future real chunk
        diffing work.

        Args:
            file_path: Path to the file to analyze

        Returns:
            Dictionary mapping chunk hashes to (start, end) positions
        """
        chunks = {}

        if not file_path.exists() or file_path.stat().st_size == 0:
            return chunks

        with file_path.open('rb') as f:
            position = 0
            while True:
                chunk_data = f.read(self.chunk_size)
                if not chunk_data:
                    break

                chunk_hash = hashlib.md5(chunk_data).hexdigest()
                chunk_size = len(chunk_data)
                chunks[chunk_hash] = (position, position + chunk_size - 1)
                position += chunk_size

        return chunks

    @staticmethod
    def remote_content_length(response: Any) -> Optional[int]:
        """Return the remote size from ``content-length``, or ``None`` if unknown.

        ``None`` and ``0`` mean completely different things and must never be
        conflated:

        * ``None`` -> the response carries **no** usable ``content-length``
          header (Apple serves package bundles such as ``*.app`` / ``*.logicx``
          with chunked transfer-encoding).  The total size is unknown, so range
          requests are impossible and the body must be streamed sequentially.
        * ``0``    -> the remote file is genuinely empty and an empty local file
          must be produced.

        Treating the first case as the second is what silently dropped files
        from backups: unknown-length responses produced "no ranges to fetch",
        which the caller read as "already up to date".
        """
        headers = getattr(response, 'headers', None)
        if headers is None:
            return None
        try:
            raw = headers.get('content-length')
        except Exception:
            return None
        if raw is None:
            return None
        try:
            value = int(str(raw).strip())
        except (TypeError, ValueError):
            return None
        return value if value >= 0 else None

    def _build_ranges(self, total_size: int, start_offset: int = 0) -> List[Tuple[int, int]]:
        """Build contiguous byte ranges using configured chunk size."""
        if total_size <= 0 or start_offset >= total_size:
            return []

        ranges: List[Tuple[int, int]] = []
        start = max(0, start_offset)
        while start < total_size:
            end = min(start + self.chunk_size - 1, total_size - 1)
            ranges.append((start, end))
            start = end + 1

        return ranges

    def compute_download_ranges(
        self,
        response: Any,
        local_path: Optional[Path] = None,
        force: bool = False,
    ) -> Optional[List[Tuple[int, int]]]:
        """
        Decide which byte ranges of the remote file still have to be fetched.

        This is SIZE-BASED CHANGE DETECTION PLUS PREFIX RESUME.  It is NOT
        content-based chunk diffing: no local or remote chunk digests are
        compared anywhere in this method.  Concretely:

        * no ``content-length`` header at all       -> ``None`` (size UNKNOWN;
          the caller must stream the whole body sequentially — see
          :meth:`remote_content_length`)
        * remote ``content-length`` is 0            -> nothing to download
        * ``force`` is set                          -> download everything
        * no local file (or an empty one)           -> download everything
        * local size == remote size                 -> assume UNCHANGED, skip
        * 0 < local size < remote size              -> resume from the prefix
        * local size > remote size                  -> download everything

        An empty list means "nothing needs fetching".  ``None`` means "I cannot
        answer, the total size is unknown" — these are NOT interchangeable and
        the caller must branch on ``None`` explicitly.

        The "same size means unchanged" assumption is the known accuracy limit
        of this strategy; a same-size in-place edit is invisible to it.  Real
        content diffing is deferred to 1.1.

        Args:
            response: The file download response (only ``headers`` are read)
            local_path: Path to the local file used for the size comparison
            force: Ignore the local file and re-fetch the whole thing

        Returns:
            List of (start, end) inclusive byte ranges that need downloading,
            or ``None`` when the remote length is unknown.
        """
        total_size = self.remote_content_length(response)

        if total_size is None:
            # Unknown length: ranges are meaningless, say so instead of
            # pretending the file is zero bytes / already up to date.
            return None

        if total_size <= 0:
            return []

        if force or local_path is None or not local_path.exists():
            return self._build_ranges(total_size)

        try:
            local_size = local_path.stat().st_size
        except OSError:
            return self._build_ranges(total_size)

        if local_size == 0:
            return self._build_ranges(total_size)
        if local_size == total_size:
            # Sizes match - assume unchanged.
            return []
        if local_size < total_size:
            # Resume from the already-downloaded prefix.
            return self._build_ranges(total_size, start_offset=local_size)

        # Local file is longer than remote: it definitely differs, refetch all.
        return self._build_ranges(total_size)
