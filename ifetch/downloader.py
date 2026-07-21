import os
import time
import shutil
import json
import threading
import traceback
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Set, Dict, Any, Union, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from requests.adapters import HTTPAdapter
from pyicloud import PyiCloudService
from pyicloud.exceptions import (
    PyiCloudFailedLoginException,
    PyiCloudNoStoredPasswordAvailableException
)
from .logger import setup_logging
from .models import DownloadStatus
from .chunker import FileChunker
from .tracker import DownloadTracker
from .utils import can_read_file
from .plugin import PluginManager
from .versioning import VersionManager


def remote_metadata(item: Any) -> Tuple[Optional[int], Optional[str]]:
    """Extract (size, modified-token) from a pyicloud DriveNode without I/O.

    Both values come from the folder listing payload that pyicloud already
    holds in ``DriveNode.data`` (``size`` / ``dateModified`` / ``dateChanged``),
    so reading them costs zero network round-trips.  Verified against pyicloud
    2.6.5: ``DriveNode.size`` -> ``Optional[int]``, ``DriveNode.date_modified``
    and ``DriveNode.date_changed`` -> ``Optional[datetime]`` (UTC).

    Either element is ``None`` when the attribute is missing or unusable; the
    caller must then treat the item as "unknown" and fall back to the network
    check.  The modified token is prefixed with its source so a value read from
    ``date_modified`` can never be mistaken for one read from ``date_changed``.
    """
    size: Optional[int] = None
    try:
        raw_size = getattr(item, 'size', None)
        if isinstance(raw_size, bool):
            raw_size = None
        if raw_size is not None:
            size = int(raw_size)
            if size < 0:
                size = None
    except Exception:  # property access or int() may fail on odd payloads
        size = None

    token: Optional[str] = None
    for attr in ('date_modified', 'date_changed'):
        try:
            value = getattr(item, attr, None)
        except Exception:
            value = None
        if value is None:
            continue
        if isinstance(value, datetime):
            token = f"{attr}={value.isoformat()}"
        else:
            text = str(value).strip()
            token = f"{attr}={text}" if text else None
        if token:
            break

    if token is None:
        data = getattr(item, 'data', None)
        if isinstance(data, dict):
            for key in ('dateModified', 'dateChanged'):
                value = data.get(key)
                if value:
                    token = f"{key}={value}"
                    break

    return size, token


class SyncState:
    """Thread-safe, on-disk record of what was last synced for each file.

    Persisted as ``.ifetch_state.json`` in the destination root.  Entries are
    keyed by POSIX-style path relative to that root and hold the remote size and
    remote modified token observed at completion time plus the local size and
    mtime written.  It exists solely to answer one question cheaply: "is this
    file *definitely* unchanged?".  Any uncertainty is reported as "unknown" and
    the caller performs the full network check.
    """

    STATE_FILENAME = ".ifetch_state.json"
    FLUSH_INTERVAL = 200

    def __init__(self, root: Path):
        self.root = Path(root).resolve()
        self.state_path = self.root / self.STATE_FILENAME
        self._data: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.RLock()
        self._dirty = 0
        self._load()

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------
    def _load(self) -> None:
        """Load state, tolerating a missing, unreadable or corrupt file."""
        if not self.state_path.exists():
            return
        try:
            payload = json.loads(self.state_path.read_text())
        except (json.JSONDecodeError, OSError, ValueError, UnicodeDecodeError):
            self._data = {}
            return

        files = payload.get("files") if isinstance(payload, dict) else None
        if not isinstance(files, dict):
            self._data = {}
            return

        # Drop anything that isn't a well-formed entry so a truncated or
        # hand-edited file can never produce a false "unchanged" verdict.
        self._data = {
            key: value for key, value in files.items()
            if isinstance(key, str) and isinstance(value, dict)
        }

    def save(self) -> None:
        """Atomically write the state file. Failures are non-fatal."""
        with self._lock:
            snapshot = {
                "version": 1,
                "updated": time.strftime("%Y-%m-%dT%H:%M:%S"),
                "files": {k: dict(v) for k, v in self._data.items()},
            }
            self._dirty = 0
        tmp_path = self.state_path.with_suffix(self.state_path.suffix + ".tmp")
        try:
            self.state_path.parent.mkdir(parents=True, exist_ok=True)
            with tmp_path.open("w") as fp:
                json.dump(snapshot, fp, indent=2)
            os.replace(tmp_path, self.state_path)
        except (OSError, RuntimeError, TypeError, ValueError):
            try:
                if tmp_path.exists():
                    tmp_path.unlink()
            except OSError:
                pass

    def _maybe_flush(self) -> None:
        with self._lock:
            should_flush = self._dirty >= self.FLUSH_INTERVAL
        if should_flush:
            self.save()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def key_for(self, local_path: Path) -> str:
        """Relative POSIX key for a local file (absolute path if outside root)."""
        try:
            return Path(local_path).resolve().relative_to(self.root).as_posix()
        except ValueError:
            return Path(local_path).as_posix()

    def is_unchanged(
        self,
        local_path: Path,
        remote_size: Optional[int],
        remote_token: Optional[str],
    ) -> bool:
        """Return True only when every piece of evidence agrees.

        Requires: remote metadata present, a completed entry recorded for this
        path, remote size AND remote modified token identical to what was
        recorded, and the local file still present with exactly the size we
        wrote (which must also equal the remote size).  Anything else -> False.
        """
        if remote_size is None or not remote_token:
            return False

        with self._lock:
            entry = self._data.get(self.key_for(local_path))
            entry = dict(entry) if isinstance(entry, dict) else None

        if not entry or entry.get("status") != "completed":
            return False
        if entry.get("remote_size") != remote_size:
            return False
        if entry.get("remote_modified") != remote_token:
            return False

        recorded_local_size = entry.get("local_size")
        if not isinstance(recorded_local_size, int):
            return False
        if recorded_local_size != remote_size:
            return False

        try:
            stat = local_path.stat()
        except OSError:
            return False
        if not local_path.is_file():
            return False
        return stat.st_size == recorded_local_size

    def record_completed(
        self,
        local_path: Path,
        remote_size: Optional[int],
        remote_token: Optional[str],
    ) -> None:
        """Record a fully-materialised local file. Missing metadata -> forget."""
        key = self.key_for(local_path)
        try:
            stat = local_path.stat()
        except OSError:
            self.invalidate(local_path)
            return

        if not local_path.is_file():
            # A directory (or anything that is not a regular file) is not a
            # completed download, whatever its stat() says.
            self.invalidate(local_path)
            return

        if remote_size is None or not remote_token or stat.st_size != remote_size:
            # We cannot prove this file is complete next run; don't claim we can.
            self.invalidate(local_path)
            return

        with self._lock:
            self._data[key] = {
                "remote_size": remote_size,
                "remote_modified": remote_token,
                "local_size": stat.st_size,
                "local_mtime": stat.st_mtime,
                "status": "completed",
                "synced_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
            }
            self._dirty += 1
        self._maybe_flush()

    def invalidate(self, local_path: Path) -> None:
        """Forget any entry for this path (failed/partial/unknown download)."""
        key = self.key_for(local_path)
        with self._lock:
            if key in self._data:
                del self._data[key]
                self._dirty += 1


class DownloadManager:
    """Enhanced iCloud file downloader with differential updates support."""
    def __init__(
        self,
        email: Optional[str] = None,
        max_workers: int = 4,
        max_retries: int = 3,
        chunk_size: int = 1024 * 1024,
        include_patterns: Optional[List[str]] = None,
        exclude_patterns: Optional[List[str]] = None,
        fast_scan: bool = True,
        force: bool = False,
    ):
        self.email = email or os.environ.get('ICLOUD_EMAIL')
        if not self.email:
            raise ValueError(
                "Email must be provided via argument or ICLOUD_EMAIL environment variable"
            )

        self.max_workers = max_workers
        self.max_retries = max_retries
        self.api: Optional[PyiCloudService] = None
        self.logger = setup_logging()
        self.download_results: List[DownloadStatus] = []
        self._active_downloads: Set[str] = set()
        self._download_lock = threading.Lock()
        self.chunker = FileChunker(chunk_size)
        self.http = requests.Session()
        adapter = HTTPAdapter(
            pool_connections=max(1, max_workers),
            pool_maxsize=max(1, max_workers * 2),
        )
        self.http.mount("http://", adapter)
        self.http.mount("https://", adapter)

        self.include_patterns = include_patterns or []
        self.exclude_patterns = exclude_patterns or []

        # Metadata fast-path: skip provably-unchanged files without opening an
        # HTTPS stream.  --no-fast-scan disables it; --force re-downloads all.
        self.fast_scan = fast_scan
        self.force = force
        self.sync_state: Optional[SyncState] = None

        # Load plugins once during instantiation
        self.plugin_manager = PluginManager()

        # Will be set when download() is invoked
        self.root_path: Optional[Path] = None
        self.version_manager: Optional[VersionManager] = None

    def authenticate(self) -> None:
        """Handle iCloud authentication including 2FA/2SA if needed."""
        if not self.email:
            raise ValueError("Email is required for authentication")

        try:
            params: Dict[str, Any] = {"apple_id": self.email.strip(), "password": None}
            if os.environ.get('ICLOUD_CHINA', '').lower() == 'true':
                params["china_mainland"] = True

            self.api = PyiCloudService(**params)

            if self.api.requires_2fa:
                print("\nTwo-factor authentication required.")
                code = input("Enter the verification code: ")
                if not self.api.validate_2fa_code(code):
                    raise Exception("Failed to verify 2FA code")
                if not self.api.is_trusted_session:
                    if not self.api.trust_session():
                        print("Warning: Failed to trust session.")

            elif self.api.requires_2sa:
                print("\nTwo-step authentication required.")
                devices = self.api.trusted_devices
                if not devices:
                    raise Exception("No trusted devices found")

                for i, device in enumerate(devices):
                    name = device.get('deviceName') or 'SMS to ' + device.get('phoneNumber', 'unknown')
                    print(f"{i}: {name}")

                idx = int(input("\nChoose a device: "))
                device = devices[idx]
                if not self.api.send_verification_code(device):
                    raise Exception("Failed to send verification code")
                code = input("Enter the verification code: ")
                if not self.api.validate_verification_code(device, code):
                    raise Exception("Failed to verify code")

        except PyiCloudFailedLoginException:
            raise Exception("Invalid credentials")
        except PyiCloudNoStoredPasswordAvailableException:
            raise Exception(
                "No stored password found. Run 'icloud auth login --username you@example.com' "
                "(requires: pip install \"pyicloud[cli]\") to store it in the system keyring."
            )
        except Exception as e:
            raise Exception(f"Authentication failed: {e}")

        # Notify plugins that authentication completed successfully
        self.plugin_manager.dispatch("on_authenticated", downloader=self)

    def get_drive_item(self, path: str) -> Any:
        """Navigate to a specific path in iCloud Drive."""
        if not self.api or not self.api.drive:
            raise Exception("Not authenticated or Drive service not available")

        # iCloud distinguishes between items you own (self.api.drive)
        # and items shared *with* you (self.api.drive.shared).  We first walk
        # through the owned drive; if the very first component is not found
        # there, we fall back to the shared root and continue the traversal
        # from that point.

        owned_root = self.api.drive
        shared_root = getattr(self.api.drive, "shared", None)

        parts = [p for p in path.strip("/").split("/") if p]
        if not parts:
            return owned_root

        # Try owned drive first
        try:
            item = self._walk_path(owned_root, parts)
            return item
        except (KeyError, AttributeError):
            # Fall back to shared items only when failing at the *first* segment
            if shared_root is None:
                available = self._list_child_names(owned_root)
                hint = f" Available top-level items: {', '.join(available)}" if available else ""
                raise Exception(f"Path not found: {path}.{hint}")

        # Restart traversal from shared root
        try:
            item = self._walk_path(shared_root, parts)
            return item
        except (KeyError, AttributeError):
            owned_available = self._list_child_names(owned_root)
            shared_available = self._list_child_names(shared_root)
            available = owned_available or shared_available
            hint = f" Available top-level items: {', '.join(available)}" if available else ""
            raise Exception(f"Path not found: {path}.{hint}")

    def _walk_path(self, root: Any, parts: List[str]) -> Any:
        """Walk a slash-separated path from a given root."""
        item = root
        for part in parts:
            if item is None:
                raise KeyError(part)
            item = self._resolve_child(item, part)
        return item

    def _resolve_child(self, item: Any, name: str) -> Any:
        """Resolve a child by exact or case-insensitive name."""
        try:
            return item[name]
        except (KeyError, TypeError, AttributeError):
            child_names = self._list_child_names(item)
            if not child_names:
                raise KeyError(name)

            normalized = name.casefold()
            for child_name in child_names:
                if child_name.casefold() == normalized:
                    return item[child_name]

            raise KeyError(name)

    @staticmethod
    def _list_child_names(item: Any) -> List[str]:
        """Return child names for dict-like or pyicloud directory nodes."""
        try:
            contents = item.dir()
        except Exception:
            contents = None

        if isinstance(contents, dict):
            return list(contents.keys())
        if isinstance(contents, list):
            return list(contents)
        if hasattr(item, "keys"):
            try:
                return list(item.keys())
            except Exception:
                return []
        return []

    def calculate_checksum(self, file_path: Path) -> str:
        """Calculate SHA-256 checksum of a file."""
        import hashlib
        sha256 = hashlib.sha256()
        with file_path.open('rb') as f:
            for chunk in iter(lambda: f.read(1024 * 1024), b''):
                sha256.update(chunk)
        return sha256.hexdigest()

    @staticmethod
    def _merge_ranges(ranges: List[tuple[int, int]]) -> List[tuple[int, int]]:
        """Collapse contiguous or overlapping byte ranges into fewer requests."""
        if not ranges:
            return []

        ordered = sorted(ranges)
        merged: List[tuple[int, int]] = [ordered[0]]

        for start, end in ordered[1:]:
            prev_start, prev_end = merged[-1]
            if start <= prev_end + 1:
                merged[-1] = (prev_start, max(prev_end, end))
            else:
                merged.append((start, end))

        return merged

    def download_chunk(self, url: str, start: int, end: int, item: Any = None) -> bytes:
        """Download a specific byte range with retries and backoff."""
        headers = {'Range': f'bytes={start}-{end}'}
        retries = 0
        last_error = None

        while retries < self.max_retries:
            try:
                resp = self.http.get(url, headers=headers, stream=True, timeout=30)
                resp.raise_for_status()  # Raise error for non-200/206 status codes
                if resp.status_code in (200, 206):
                    return resp.content
            except requests.HTTPError as e:
                last_error = e
                status = e.response.status_code if e.response is not None else None
                if status in (404, 410) and item is not None:
                    # Signed URL likely expired; refresh and retry.
                    try:
                        with self._open_with_retry(item, max_retries=1) as refreshed:
                            url = refreshed.url
                    except Exception:
                        pass
                retries += 1
                retry_after = None
                if e.response is not None:
                    retry_after = e.response.headers.get("Retry-After")
                if retry_after and retry_after.isdigit():
                    time.sleep(min(int(retry_after), 60))
                else:
                    time.sleep(2 ** retries)  # Exponential backoff
            except requests.RequestException as e:
                last_error = e
                retries += 1
                time.sleep(2 ** retries)  # Exponential backoff

        raise Exception(f"Failed to download chunk {start}-{end} after {self.max_retries} retries: {last_error}")

    def _try_shared_open(self, item: Any) -> Optional[Any]:
        """Fallback download path for files shared from another user's iCloud account.

        pyicloud's DriveNode.open() calls:
            get_file(docwsid, zone=self.data["zone"])
        which hits:
            /ws/{zone}/download/by_id?document_id={docwsid}

        For cross-user shared files Apple requires a 'shareID' parameter in that
        request — the same credential that timlaing/pyicloud v2.5 threads through
        retrieveItemDetailsInFolders to fix folder traversal (PR #152).  That PR
        does NOT update get_file(), so the download endpoint still 404s.

        Strategy 1 (most likely to work): include shareID in the download/by_id
        request, constructing it manually via the drive session.

        Strategy 2: try drivewsid as document_id (different ID space).

        Strategy 3: re-fetch node metadata with shareID; Apple may return a direct
        download URL in 'downloadURL'/'url'/'contentURL' fields.
        """
        data = getattr(item, 'data', None) or {}
        docwsid = data.get('docwsid')
        drivewsid = data.get('drivewsid')
        zone = data.get('zone', 'com.apple.CloudDocs')
        share_id = data.get('shareID')

        if not self.api or not hasattr(self.api, 'drive'):
            return None

        drive = self.api.drive

        # Strategy 1: replay the download/by_id request with shareID in params.
        # This is the missing piece in pyicloud v2.5 — get_file() never sends it.
        if (share_id and docwsid
                and hasattr(drive, 'params')
                and hasattr(drive, '_document_root')
                and hasattr(drive, 'session')):
            try:
                file_params: Dict[str, Any] = dict(drive.params)
                file_params.update({"document_id": docwsid, "shareID": share_id})
                resp = drive.session.get(
                    drive._document_root + f"/ws/{zone}/download/by_id",
                    params=file_params,
                )
                if resp.ok:
                    resp_json = resp.json()
                    data_token = resp_json.get("data_token")
                    package_token = resp_json.get("package_token")
                    if data_token and data_token.get("url"):
                        return drive.session.get(
                            data_token["url"], params=drive.params, stream=True
                        )
                    if package_token and package_token.get("url"):
                        return drive.session.get(
                            package_token["url"], params=drive.params, stream=True
                        )
            except Exception:
                pass

        # Strategy 2: try drivewsid as document_id
        if drivewsid and drivewsid != docwsid and hasattr(drive, 'get_file'):
            try:
                return drive.get_file(drivewsid, zone=zone, stream=True)
            except Exception:
                pass

        # Strategy 3: refresh node metadata (passing shareID if available);
        # Apple may embed a direct download URL in the response.
        if drivewsid and hasattr(drive, 'get_node_data'):
            try:
                import inspect
                gnd_sig = inspect.signature(drive.get_node_data)
                if share_id and 'share_id' in gnd_sig.parameters:
                    fresh = drive.get_node_data(drivewsid, share_id)
                else:
                    fresh = drive.get_node_data(drivewsid)
                for key in ('downloadURL', 'url', 'contentURL'):
                    url = fresh.get(key)
                    if url:
                        return self.http.get(url, stream=True, timeout=30)
            except Exception:
                pass

        return None

    def _open_with_retry(
        self,
        item: Any,
        max_retries: Optional[int] = None,
        remote_path: Optional[str] = None,
    ) -> Any:
        """Open item with retry logic for transient connection errors."""
        if max_retries is None:
            max_retries = self.max_retries

        last_error = None
        current_item = item
        for attempt in range(max_retries):
            try:
                return current_item.open(stream=True)
            except Exception as e:
                last_error = e
                error_str = str(e).lower()
                # Apple's "document not found in zone" — the docwsid belongs to
                # another user's iCloud account (shared-with-me file).  Refreshing
                # the item reference doesn't change the docwsid, so the usual
                # get_drive_item retry loop is useless here.
                is_apple_object_not_found = (
                    "wsobjectnotfound" in error_str or
                    "objectnotfoundexception" in error_str
                )
                # Generic 404 (signed URL expired, pyicloud cache stale, etc.)
                is_generic_404 = (
                    "404" in error_str or "not found" in error_str
                ) and not is_apple_object_not_found

                if is_apple_object_not_found:
                    fallback = self._try_shared_open(current_item)
                    if fallback is not None:
                        return fallback
                    self.logger.warning(json.dumps({
                        "event": "shared_file_download_attempted",
                        "file": getattr(item, 'name', 'unknown'),
                        "hint": (
                            "File not found via standard download endpoint. "
                            "This usually means the file was shared from another "
                            "iCloud account. Alternate download strategies also "
                            "failed. The file will be skipped."
                        ),
                    }))
                    raise

                if is_generic_404 and remote_path and attempt < max_retries - 1:
                    try:
                        current_item = self.get_drive_item(remote_path)
                        self.logger.warning(json.dumps({
                            "event": "item_refreshed",
                            "file": getattr(item, 'name', 'unknown'),
                            "remote_path": remote_path,
                            "attempt": attempt + 1,
                        }))
                        continue
                    except Exception as refresh_error:
                        last_error = refresh_error
                        raise

                # Retry on connection errors and server errors, not on auth or other permanent errors
                retryable = any(x in error_str for x in [
                    'connection', 'remote', 'timeout', 'reset',
                    '503', 'service unavailable', 'retry_needed', 'internal_failure'
                ])
                if retryable and attempt < max_retries - 1:
                    # Check if server specified retryAfter
                    wait_time = 2 ** (attempt + 1)  # Default: 2, 4, 8 seconds
                    if 'retryafter' in error_str:
                        import re
                        match = re.search(r'retryafter["\s:]+(\d+)', error_str)
                        if match:
                            wait_time = min(int(match.group(1)), 60)
                    self.logger.warning(json.dumps({
                        "event": "connection_retry",
                        "file": getattr(current_item, 'name', 'unknown'),
                        "attempt": attempt + 1,
                        "wait_seconds": wait_time,
                        "error": str(e)
                    }))
                    time.sleep(wait_time)
                    continue
                raise  # Non-retryable error
        raise last_error  # All retries exhausted

    def _can_fast_skip(self, item: Any, local_path: Path) -> bool:
        """Decide, using zero network I/O, whether a file is provably unchanged.

        Every one of the following must hold, otherwise we fall through to the
        normal open-and-check path:

        1. the fast path is enabled (``--no-fast-scan`` off) and ``--force`` off;
        2. a sync-state file was loaded for this destination root;
        3. the remote node exposes BOTH a size and a modified timestamp;
        4. a previous run recorded this path with ``status == "completed"``;
        5. the recorded remote size and remote modified token match exactly;
        6. the local file still exists with exactly the recorded size, which
           equals the remote size;
        7. no resume artefacts (``*.temp`` / ``*.download``) are lying around.
        """
        if not self.fast_scan or self.force or self.sync_state is None:
            return False

        remote_size, remote_token = remote_metadata(item)
        if remote_size is None or not remote_token:
            return False

        # A leftover partial download means the local file may be stale even if
        # its size looks right; never trust the fast path in that case.
        try:
            suffix = local_path.suffix
            if (local_path.with_suffix(suffix + '.temp').exists()
                    or local_path.with_suffix(suffix + '.download').exists()):
                return False
        except (OSError, ValueError):
            return False

        return self.sync_state.is_unchanged(local_path, remote_size, remote_token)

    def _record_sync_state(self, item: Any, local_path: Path) -> None:
        """Persist the metadata that lets the next run fast-skip this file."""
        if self.sync_state is None:
            return
        remote_size, remote_token = remote_metadata(item)
        try:
            self.sync_state.record_completed(local_path, remote_size, remote_token)
        except Exception:
            # State bookkeeping must never break a successful download.
            pass

    # ------------------------------------------------------------------
    # Local-file invariant helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _local_file_exists(local_path: Path) -> bool:
        """True only when a *regular file* is really present at ``local_path``.

        The core invariant of this downloader is: **never report success for a
        file that is not on disk afterwards.**  Every "unchanged"/"skipped"
        verdict is gated on this check.
        """
        try:
            return Path(local_path).is_file()
        except OSError:
            return False

    @staticmethod
    def _discard_temp(temp_path: Optional[Path]) -> None:
        """Remove a partial temp file so no half-written data survives."""
        try:
            if temp_path is not None and temp_path.exists():
                temp_path.unlink()
        except OSError:
            pass

    def _materialize_empty_file(
        self,
        item: Any,
        local_path: Path,
        temp_path: Path,
        tracker: DownloadTracker,
    ) -> bool:
        """Create a genuinely empty local file for a zero-byte remote file.

        ``content-length: 0`` means the remote file really is empty; an empty
        file in iCloud must produce an empty file locally, not nothing at all.
        """
        if self.sync_state is not None:
            self.sync_state.invalidate(local_path)

        local_path.parent.mkdir(parents=True, exist_ok=True)
        with temp_path.open('wb'):
            pass
        temp_path.replace(local_path)
        tracker.cleanup()

        self.logger.info(json.dumps({
            "event": "empty_file_created",
            "file": getattr(item, 'name', 'unknown'),
            "path": str(local_path),
        }))
        self.download_results.append(DownloadStatus(
            path=str(local_path),
            size=0,
            downloaded=0,
            checksum=self.calculate_checksum(local_path),
            status="completed",
            changes=1,
        ))
        self._record_sync_state(item, local_path)
        self.plugin_manager.dispatch(
            "after_download", remote_item=item, local_path=local_path, success=True
        )
        return True

    def _stream_body_to_temp(
        self,
        response: Any,
        temp_path: Path,
        item: Any,
        local_path: Path,
    ) -> int:
        """Write a whole response body to ``temp_path``; return bytes written."""
        iter_content = getattr(response, 'iter_content', None)
        if iter_content is None:
            raise Exception(
                "download stream does not support iter_content(); cannot fetch "
                "a file whose length the server did not report"
            )

        written = 0
        with temp_path.open('wb') as out_file:
            for chunk in iter_content(chunk_size=self.chunker.chunk_size):
                if not chunk:
                    continue
                out_file.write(chunk)
                written += len(chunk)
                self.plugin_manager.dispatch(
                    "on_event",
                    name="download_progress",
                    remote_item=item,
                    local_path=local_path,
                    downloaded=written,
                    total_size=None,
                )
            out_file.flush()
            try:
                os.fsync(out_file.fileno())
            except (OSError, AttributeError, ValueError):
                pass
        return written

    def _download_unknown_length(
        self,
        item: Any,
        response: Any,
        local_path: Path,
        temp_path: Path,
        tracker: DownloadTracker,
        remote_path: Optional[str] = None,
    ) -> bool:
        """Sequentially stream a response that carries no ``content-length``.

        Apple serves macOS package bundles (``*.app``, ``*.logicx``, ...) with
        chunked transfer-encoding and no ``content-length`` header even though
        the Drive listing reports a correct non-zero size.  Without a total
        size, ``Range`` requests cannot be constructed at all, so the only
        correct strategy is to stream the entire body into the temp file and
        then move it into place atomically — reusing the same temp-file,
        version-archiving, tracker and sync-state machinery as the ranged path.

        RESUME IS NOT POSSIBLE in this mode.  There is no way to know the total
        size or to ask the server for a suffix of an unknown-length body, so a
        failure restarts the transfer from byte 0.  Any stale range checkpoint
        is deleted up front so it can never be misapplied to a sequential
        stream.
        """
        remote_size, _ = remote_metadata(item)
        name = getattr(item, 'name', 'unknown')

        # The local copy is now known-unverifiable; drop any recorded state so
        # an interrupted run cannot leave behind a false "unchanged" entry.
        if self.sync_state is not None:
            self.sync_state.invalidate(local_path)

        local_path.parent.mkdir(parents=True, exist_ok=True)
        tracker.current_position = 0
        tracker.cleanup()

        self.logger.info(json.dumps({
            "event": "unknown_length_stream",
            "file": name,
            "path": str(local_path),
            "listed_size": remote_size,
            "resume": False,
        }))

        attempts = max(1, self.max_retries)
        written = 0
        last_error: Optional[Exception] = None

        for attempt in range(attempts):
            try:
                if attempt == 0:
                    written = self._stream_body_to_temp(
                        response, temp_path, item, local_path
                    )
                else:
                    # The original body is already partially consumed and is not
                    # seekable, so a retry must open a brand-new stream.
                    with self._open_with_retry(
                        item, remote_path=remote_path
                    ) as retry_response:
                        written = self._stream_body_to_temp(
                            retry_response, temp_path, item, local_path
                        )
                last_error = None
                break
            except Exception as exc:
                last_error = exc
                # Never leave a partial body behind; the destination file is
                # untouched until the atomic replace below.
                self._discard_temp(temp_path)
                if attempt >= attempts - 1:
                    break
                self.logger.warning(json.dumps({
                    "event": "unknown_length_retry",
                    "file": name,
                    "attempt": attempt + 1,
                    "error": str(exc),
                }))
                time.sleep(2 ** (attempt + 1))

        if last_error is not None:
            # Reported as a failure by download_drive_item's handler.
            raise last_error

        if written == 0 and remote_size:
            # The listing promised bytes but the body was empty: that is a
            # broken transfer, never a successful (empty) backup.
            self._discard_temp(temp_path)
            if self.sync_state is not None:
                self.sync_state.invalidate(local_path)
            error = (
                "remote listing reported "
                f"{remote_size} bytes but the download stream was empty"
            )
            self.logger.error(json.dumps({
                "event": "unknown_length_empty_body",
                "file": name,
                "path": str(local_path),
                "listed_size": remote_size,
            }))
            self.download_results.append(DownloadStatus(
                path=str(local_path),
                size=remote_size,
                downloaded=0,
                checksum="",
                status="failed",
                error=error,
            ))
            return False

        if remote_size and written != remote_size:
            # Package bundles are re-archived server-side, so the streamed byte
            # count legitimately differs from the listed size.  Worth recording,
            # but not an error.
            self.logger.warning(json.dumps({
                "event": "unknown_length_size_differs_from_listing",
                "file": name,
                "path": str(local_path),
                "listed_size": remote_size,
                "downloaded": written,
            }))

        # Archive the copy we are about to overwrite.
        if local_path.exists() and self.version_manager:
            try:
                old_checksum = self.calculate_checksum(local_path)
                rel_path = (
                    local_path.relative_to(self.root_path)
                    if self.root_path else local_path.name
                )
                self.version_manager.record_version(rel_path, old_checksum, local_path)
            except Exception:
                pass  # Non-fatal; continue with the download

        checksum = self.calculate_checksum(temp_path)
        temp_path.replace(local_path)
        tracker.cleanup()

        self.download_results.append(DownloadStatus(
            path=str(local_path),
            size=written,
            downloaded=written,
            checksum=checksum,
            status="completed",
            changes=1,
        ))
        self._record_sync_state(item, local_path)

        if self.version_manager:
            rel_path2 = (
                local_path.relative_to(self.root_path)
                if self.root_path else local_path.name
            )
            with self.version_manager._lock:
                if str(rel_path2) not in self.version_manager._data:
                    self.version_manager._data[str(rel_path2)] = [{
                        "version": 0,
                        "checksum": checksum,
                        "archived_path": str(local_path),
                        "timestamp": time.strftime("%Y%m%dT%H%M%S"),
                    }]
            self.version_manager._save()

        self.plugin_manager.dispatch(
            "after_download", remote_item=item, local_path=local_path, success=True
        )
        return True

    def download_drive_item(
        self,
        item: Any,
        local_path: Path,
        remote_path: Optional[str] = None,
    ) -> bool:
        """Download file with differential updates support and checkpointing."""
        if not hasattr(item, 'name') or not hasattr(item, 'open'):
            self.logger.warning(json.dumps({
                "event": "invalid_item",
                "error": "Item doesn't have name or open attributes"
            }))
            return False

        # ---- Metadata fast path: no HTTPS stream is opened at all ----------
        # The invariant guard is redundant with SyncState.is_unchanged (which
        # already stats the file) but stated explicitly here: a fast skip is
        # only ever allowed when the file demonstrably exists on disk.
        if self._can_fast_skip(item, local_path) and self._local_file_exists(local_path):
            remote_size, _ = remote_metadata(item)
            self.logger.info(json.dumps({
                "event": "file_skipped_fast_path",
                "file": item.name,
                "path": str(local_path),
            }))
            self.download_results.append(DownloadStatus(
                path=str(local_path),
                size=remote_size or 0,
                downloaded=0,
                checksum="",
                status="skipped",
                changes=0,
            ))
            return True

        tracker = DownloadTracker(local_path)
        temp_path: Optional[Path] = None
        total_size = 0

        try:
            with self._open_with_retry(item, remote_path=remote_path) as response:
                # None (header absent) and 0 (genuinely empty) are different
                # situations and must never be conflated.
                remote_length = self.chunker.remote_content_length(response)
                total_size = remote_length if remote_length is not None else 0
                temp_path = local_path.with_suffix(local_path.suffix + '.temp')
                changed_ranges = self.chunker.compute_download_ranges(
                    response, local_path, force=self.force
                )

                if changed_ranges is None:
                    # Unknown remote length -> ranged requests are impossible.
                    # Stream the whole body sequentially instead of silently
                    # concluding "nothing to do".
                    return self._download_unknown_length(
                        item, response, local_path, temp_path, tracker,
                        remote_path=remote_path,
                    )

                changed_ranges = self._merge_ranges(changed_ranges)

                if tracker.current_position > 0:
                    resumed_ranges = []
                    for start, end in changed_ranges:
                        if end < tracker.current_position:
                            continue
                        resumed_ranges.append((max(start, tracker.current_position), end))
                    changed_ranges = resumed_ranges

                if not changed_ranges and not self._local_file_exists(local_path):
                    # INVARIANT: "nothing to download" may only be reported as a
                    # success when the file is actually on disk.  It is not, so
                    # this verdict is wrong; download instead of lying.
                    if total_size <= 0:
                        return self._materialize_empty_file(
                            item, local_path, temp_path, tracker
                        )
                    self.logger.warning(json.dumps({
                        "event": "unchanged_verdict_without_local_file",
                        "file": getattr(item, 'name', 'unknown'),
                        "path": str(local_path),
                        "total_size": total_size,
                        "action": "forcing_full_download",
                    }))
                    tracker.current_position = 0
                    tracker.cleanup()
                    changed_ranges = self._merge_ranges(
                        self.chunker._build_ranges(total_size)
                    )

                if not changed_ranges:
                    if total_size == 0 and local_path.stat().st_size != 0:
                        # Remote says empty, local is not.  Never destroy local
                        # data on this signal alone, but do not hide it either.
                        self.logger.warning(json.dumps({
                            "event": "remote_empty_local_not_empty",
                            "file": getattr(item, 'name', 'unknown'),
                            "path": str(local_path),
                        }))
                    self.logger.info(json.dumps({
                        "event": "file_unchanged",
                        "file": item.name,
                        "path": str(local_path)
                    }))
                    self._record_sync_state(item, local_path)
                    self.download_results.append(DownloadStatus(
                        path=str(local_path),
                        size=total_size,
                        downloaded=0,
                        checksum="",
                        status="skipped",
                        changes=0,
                    ))
                    return True

                # From here on the local copy is known-stale (or absent).  Drop
                # any recorded state immediately so an interrupted run cannot
                # leave behind an entry that would trigger a false skip later.
                if self.sync_state is not None:
                    self.sync_state.invalidate(local_path)

                bytes_to_download = sum(end - start + 1 for start, end in changed_ranges)

                # Create parent directories if they don't exist
                local_path.parent.mkdir(parents=True, exist_ok=True)

                if temp_path.exists():
                    pass  # Reuse existing temp file for resumed download
                elif local_path.exists():
                    shutil.copy2(local_path, temp_path)
                else:
                    # Initialize the temp file with zeros
                    with temp_path.open('wb') as f:
                        f.seek(total_size - 1)
                        f.write(b'\0')

                # Backup current file after temp file is prepared so resumed data is preserved.
                if local_path.exists() and self.version_manager:
                    try:
                        old_checksum = self.calculate_checksum(local_path)
                        rel_path = local_path.relative_to(self.root_path) if self.root_path else local_path.name
                        self.version_manager.record_version(rel_path, old_checksum, local_path)
                    except Exception:
                        pass  # Non-fatal; continue with download

                with temp_path.open('r+b') as out_file:
                    for start, end in changed_ranges:
                        chunk = self.download_chunk(response.url, start, end, item=item)
                        out_file.seek(start)
                        out_file.write(chunk)
                        tracker.save_status(end + 1)

                        # Streaming progress event (generic)
                        self.plugin_manager.dispatch(
                            "on_event",
                            name="download_progress",
                            remote_item=item,
                            local_path=local_path,
                            downloaded=end,
                            total_size=total_size,
                        )

                # Only calculate checksum if temp_path exists and has content
                if temp_path.exists() and temp_path.stat().st_size > 0:
                    temp_checksum = self.calculate_checksum(temp_path)
                    temp_path.replace(local_path)
                else:
                    self.logger.error(json.dumps({
                        "event": "invalid_temp_file",
                        "file": item.name,
                        "error": "Temporary file is empty or doesn't exist"
                    }))
                    if self.sync_state is not None:
                        self.sync_state.invalidate(local_path)
                    return False

                self.download_results.append(DownloadStatus(
                    path=str(local_path),
                    size=total_size,
                    downloaded=bytes_to_download,
                    checksum=temp_checksum,
                    status="completed",
                    changes=len(changed_ranges)
                ))
                tracker.cleanup()
                self._record_sync_state(item, local_path)
                # Update version metadata with new checksum
                if self.version_manager:
                    new_checksum = temp_checksum
                    rel_path2 = local_path.relative_to(self.root_path) if self.root_path else local_path.name
                    # Only create baseline entry if none exists yet
                    with self.version_manager._lock:
                        if str(rel_path2) not in self.version_manager._data:
                            self.version_manager._data[str(rel_path2)] = [{
                                "version": 0,
                                "checksum": new_checksum,
                                "archived_path": str(local_path),
                                "timestamp": time.strftime("%Y%m%dT%H%M%S"),
                            }]
                    self.version_manager._save()

                # Notify plugins about successful download
                self.plugin_manager.dispatch(
                    "after_download",
                    remote_item=item,
                    local_path=local_path,
                    success=True,
                )
                return True

        except Exception as e:
            if self.sync_state is not None:
                self.sync_state.invalidate(local_path)
            error_str = str(e)
            is_shared_file_error = any(x in error_str.lower() for x in [
                'wsobjectnotfound', 'objectnotfoundexception',
            ])
            log_payload: Dict[str, Any] = {
                "event": "download_failed",
                "file": getattr(item, 'name', 'unknown'),
                "error": error_str,
                "traceback": traceback.format_exc(),
            }
            if is_shared_file_error:
                log_payload["hint"] = (
                    "This file appears to be shared from another iCloud account. "
                    "Downloading files shared by other users is not currently supported "
                    "by the underlying iCloud API. Only files you own can be downloaded."
                )
            self.logger.error(json.dumps(log_payload))
            if temp_path and temp_path.exists():
                self.logger.warning(json.dumps({
                    "event": "temp_file_retained",
                    "file": getattr(item, 'name', 'unknown'),
                    "path": str(temp_path)
                }))

            self.download_results.append(DownloadStatus(
                path=str(local_path),
                size=total_size,
                downloaded=0,
                checksum="",  # Empty string instead of None
                status="failed",
                error=str(e)
            ))
            return False

        # Notify plugins about before/after failures handled above

    def process_item_parallel(
        self,
        item: Any,
        local_path: Path,
        remote_path: Optional[str] = None,
    ) -> None:
        """Process files and directories in parallel."""
        try:
            # If file/dir is excluded by patterns, skip
            rel_path = local_path.relative_to(self.root_path) if self.root_path else local_path
            if not self._should_process(rel_path, is_dir=False if can_read_file(item) else True):
                return

            if can_read_file(item):
                with self._download_lock:
                    local_path_str = str(local_path)
                    if local_path_str in self._active_downloads:
                        return
                    self._active_downloads.add(local_path_str)

                try:
                    # before_download hook
                    self.plugin_manager.dispatch(
                        "before_download", remote_item=item, local_path=local_path
                    )
                    if self.download_drive_item(item, local_path, remote_path=remote_path):
                        self.logger.info(json.dumps({
                            "event": "download_success",
                            "file": getattr(item, 'name', 'unknown'),
                            "path": str(local_path)
                        }))
                    else:
                        self.logger.error(json.dumps({
                            "event": "download_failed",
                            "file": getattr(item, 'name', 'unknown'),
                            "path": str(local_path)
                        }))
                finally:
                    with self._download_lock:
                        self._active_downloads.remove(local_path_str)

            elif hasattr(item, 'dir'):
                contents = item.dir()
                if contents:
                    # Pre-resolve all child items BEFORE parallel execution to avoid
                    # "dictionary changed size during iteration" when pyicloud lazily
                    # loads items and modifies its internal cache
                    content_names = list(contents.keys()) if hasattr(contents, 'keys') else list(contents)
                    child_items = []
                    for name in content_names:
                        child_remote_path = name if not remote_path else f"{remote_path.rstrip('/')}/{name}"
                        child_items.append((item[name], local_path / name, child_remote_path))
                    local_path.mkdir(parents=True, exist_ok=True)
                    with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                        futures = [
                            executor.submit(self.process_item_parallel, child_item, child_path, child_remote_path)
                            for child_item, child_path, child_remote_path in child_items
                        ]
                        for future in as_completed(futures):
                            # Retrieve result or exception
                            try:
                                future.result()
                            except Exception as e:
                                self.logger.error(json.dumps({
                                    "event": "future_exception",
                                    "error": str(e),
                                    "traceback": traceback.format_exc()
                                }))

                        # after_download hook success/failure already inside download_drive_item,
                        # but make sure to send event even if function returned False
                        self.plugin_manager.dispatch(
                            "after_download",
                            remote_item=item,
                            local_path=local_path,
                            success=False,
                        )

        except Exception as e:
            self.logger.error(json.dumps({
                "event": "processing_error",
                "file": getattr(item, 'name', 'unknown'),
                "error": str(e),
                "traceback": traceback.format_exc()
            }))

    def list_contents(self, path: str) -> None:
        """List contents of a directory in iCloud Drive."""
        try:
            item = self.get_drive_item(path)
            if hasattr(item, 'dir'):
                contents = item.dir()
                if not contents:
                    self.logger.info(json.dumps({"event": "empty_directory", "path": path}))
                    return
                self.logger.info(json.dumps({
                    "event": "listing_contents",
                    "path": path,
                    "contents": [
                        {"name": name, "type": "file" if can_read_file(item[name]) else "folder"}
                        for name in contents
                    ]
                }))

                # Notify plugins about listing event
                self.plugin_manager.dispatch(
                    "on_list_contents", path=path, contents=[
                        {"name": name, "type": "file" if can_read_file(item[name]) else "folder"}
                        for name in contents
                    ]
                )
            else:
                self.logger.info(json.dumps({"event": "item_info", "path": path, "type": "file"}))
        except Exception as e:
            self.logger.error(json.dumps({"event": "listing_error", "path": path, "error": str(e)}))

    # ------------------------------------------------------------------
    # Shared-drive helpers
    # ------------------------------------------------------------------
    def list_shared_roots(self) -> None:
        """List the top-level items that have been shared *with* the user."""
        if not self.api:
            self.authenticate()

        shared_root = getattr(self.api.drive, "shared", None)
        if not shared_root:
            self.logger.info(json.dumps({"event": "no_shared_items"}))
            return

        try:
            contents = shared_root.dir()
            if not contents:
                self.logger.info(json.dumps({"event": "no_shared_items"}))
                return

            self.logger.info(json.dumps({
                "event": "listing_shared_roots",
                "contents": [
                    {"name": name, "type": "file" if can_read_file(shared_root[name]) else "folder"}
                    for name in contents
                ]
            }))
        except Exception as e:
            self.logger.error(json.dumps({"event": "shared_listing_error", "error": str(e)}))

    def generate_summary_report(self) -> Dict[str, Any]:
        """Generate a summary report of the download operation."""
        total_files = len(self.download_results)
        successful = sum(1 for r in self.download_results if r.status == "completed")
        failed = sum(1 for r in self.download_results if r.status == "failed")
        # Files proven unchanged (metadata fast path or size match) are neither
        # downloads nor failures; they get their own bucket.
        skipped = sum(1 for r in self.download_results if r.status == "skipped")
        total_bytes = sum(r.downloaded for r in self.download_results)
        total_changes = sum(getattr(r, 'changes', 0) for r in self.download_results)

        return {
            "summary": {
                "total_files": total_files,
                "successful": successful,
                "failed": failed,
                "skipped": skipped,
                "total_bytes_transferred": total_bytes,
                "total_changed_chunks": total_changes,
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
            },
            "details": [r.__dict__ for r in self.download_results]
        }

    def download(
        self,
        icloud_path: str,
        local_path: Union[str, Path] = '.',
        log_file: Optional[str] = None
    ) -> None:
        """Main download method with parallel processing and logging."""
        if log_file:
            self.logger = setup_logging(log_file)

        if not self.api:
            self.authenticate()

        if not self.api or not self.api.drive:
            raise Exception("iCloud Drive service not available")

        # Convert local_path to Path if it's a string
        local_path_obj = Path(local_path).resolve()

        # Initialise version manager for this download session
        self.root_path = local_path_obj
        self.version_manager = VersionManager(local_path_obj)
        # Sync state lives in the destination root; a missing or corrupt file
        # simply yields an empty state, i.e. first-run behaviour.
        self.sync_state = SyncState(local_path_obj)

        item = self.get_drive_item(icloud_path)

        self.logger.info(json.dumps({
            "event": "download_started",
            "icloud_path": icloud_path,
            "local_path": str(local_path_obj),
            "max_workers": self.max_workers,
            "chunk_size": self.chunker.chunk_size,
            "fast_scan": self.fast_scan,
            "force": self.force
        }))

        remote_path = icloud_path.strip("/") or None
        try:
            self.process_item_parallel(item, local_path_obj, remote_path=remote_path)
        finally:
            # Persist whatever we learned even if the run was interrupted.
            self.sync_state.save()

        report = self.generate_summary_report()
        self.logger.info(json.dumps({"event": "download_completed", "summary": report}))

        # Notify plugins with generic completion event
        self.plugin_manager.dispatch("on_event", name="download_session_completed", summary=report)

        # Create report file in the same location as downloads
        report_path = local_path_obj / "download_report.json"
        with report_path.open('w') as f:
            json.dump(report, f, indent=2)

    def _should_process(self, rel_path: Path, is_dir: bool) -> bool:
        """Return True if path should be downloaded/traversed based on include/exclude patterns."""
        from fnmatch import fnmatch

        path_str = rel_path.as_posix()

        # Exclude check first
        for pat in self.exclude_patterns:
            if fnmatch(path_str, pat):
                return False

        # Files can be filtered aggressively; directories must stay traversable
        # or include globs like "*.pdf" will prune matching descendants.
        if is_dir:
            return True

        # Include logic: if include list empty -> include all; else must match one
        if not self.include_patterns:
            return True
        return any(fnmatch(path_str, pat) for pat in self.include_patterns)
