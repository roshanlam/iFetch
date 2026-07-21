import os
import time
import shutil
import json
import threading
import traceback
from pathlib import Path
from typing import Optional, List, Set, Dict, Any, Union
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

        tracker = DownloadTracker(local_path)
        temp_path: Optional[Path] = None
        total_size = 0

        try:
            with self._open_with_retry(item, remote_path=remote_path) as response:
                total_size = int(response.headers.get('content-length', 0))
                temp_path = local_path.with_suffix(local_path.suffix + '.temp')
                existing_chunks = self.chunker.get_file_chunks(local_path)
                changed_ranges = self.chunker.find_changed_chunks(response, existing_chunks, local_path)
                changed_ranges = self._merge_ranges(changed_ranges)

                if tracker.current_position > 0:
                    resumed_ranges = []
                    for start, end in changed_ranges:
                        if end < tracker.current_position:
                            continue
                        resumed_ranges.append((max(start, tracker.current_position), end))
                    changed_ranges = resumed_ranges

                if not changed_ranges:
                    self.logger.info(json.dumps({
                        "event": "file_unchanged",
                        "file": item.name,
                        "path": str(local_path)
                    }))
                    return True

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
        total_bytes = sum(r.downloaded for r in self.download_results)
        total_changes = sum(getattr(r, 'changes', 0) for r in self.download_results)

        return {
            "summary": {
                "total_files": total_files,
                "successful": successful,
                "failed": failed,
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

        item = self.get_drive_item(icloud_path)

        self.logger.info(json.dumps({
            "event": "download_started",
            "icloud_path": icloud_path,
            "local_path": str(local_path_obj),
            "max_workers": self.max_workers,
            "chunk_size": self.chunker.chunk_size
        }))

        remote_path = icloud_path.strip("/") or None
        self.process_item_parallel(item, local_path_obj, remote_path=remote_path)

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
