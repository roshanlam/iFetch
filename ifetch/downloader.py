import os
import time
import shutil
import json
import threading
from pathlib import Path
from typing import Optional, List, Set, Dict, Any, Union
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from tqdm import tqdm
from pyicloud import PyiCloudService
from pyicloud.exceptions import (
    PyiCloudFailedLoginException,
    PyiCloudNoStoredPasswordAvailableException
)
from logger import setup_logging
from models import DownloadStatus
from chunker import FileChunker
from tracker import DownloadTracker
from utils import can_read_file


class DownloadManager:
    """Enhanced iCloud file downloader with differential updates support."""
    def __init__(
        self,
        email: Optional[str] = None,
        max_workers: int = 4,
        max_retries: int = 3,
        chunk_size: int = 1024 * 1024
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
            raise Exception("No stored password found. Please run 'icloud --username=you@example.com'")
        except Exception as e:
            raise Exception(f"Authentication failed: {e}")

    def get_drive_item(self, path: str) -> Any:
        """Navigate to a specific path in iCloud Drive."""
        if not self.api or not self.api.drive:
            raise Exception("Not authenticated or Drive service not available")

        item = self.api.drive
        for part in path.strip('/').split('/'):
            if not part:
                continue
            try:
                if item is None:
                    raise Exception(f"Invalid path component: {part} in {path}")
                item = item[part]
            except (KeyError, AttributeError):
                raise Exception(f"Path not found: {path}")
        return item

    def calculate_checksum(self, file_path: Path) -> str:
        """Calculate SHA-256 checksum of a file."""
        import hashlib
        sha256 = hashlib.sha256()
        with file_path.open('rb') as f:
            for chunk in iter(lambda: f.read(8192), b''):
                sha256.update(chunk)
        return sha256.hexdigest()

    def download_chunk(self, url: str, start: int, end: int) -> bytes:
        """Download a specific byte range with retries and backoff."""
        headers = {'Range': f'bytes={start}-{end}'}
        retries = 0
        last_error = None

        while retries < self.max_retries:
            try:
                resp = requests.get(url, headers=headers, stream=True, timeout=30)
                resp.raise_for_status()  # Raise error for non-200/206 status codes
                if resp.status_code in (200, 206):
                    return resp.content
            except requests.RequestException as e:
                last_error = e
                retries += 1
                time.sleep(2 ** retries)  # Exponential backoff

        raise Exception(f"Failed to download chunk {start}-{end} after {self.max_retries} retries: {last_error}")

    def download_drive_item(self, item: Any, local_path: Path) -> bool:
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
            with item.open(stream=True) as response:
                total_size = int(response.headers.get('content-length', 0))
                existing_chunks = self.chunker.get_file_chunks(local_path)
                changed_ranges = self.chunker.find_changed_chunks(response, existing_chunks)

                if not changed_ranges:
                    self.logger.info(json.dumps({
                        "event": "file_unchanged",
                        "file": item.name,
                        "path": str(local_path)
                    }))
                    return True

                bytes_to_download = sum(end - start + 1 for start, end in changed_ranges)
                temp_path = local_path.with_suffix(local_path.suffix + '.temp')

                # Create parent directories if they don't exist
                local_path.parent.mkdir(parents=True, exist_ok=True)

                if local_path.exists():
                    shutil.copy2(local_path, temp_path)
                else:
                    # Initialize the temp file with zeros
                    with temp_path.open('wb') as f:
                        f.seek(total_size - 1)
                        f.write(b'\0')

                with temp_path.open('r+b') as out_file, tqdm(
                    desc=f"Updating {item.name}",
                    total=bytes_to_download,
                    unit='B',
                    unit_scale=True,
                    unit_divisor=1024
                ) as pbar:
                    for start, end in changed_ranges:
                        chunk = self.download_chunk(response.url, start, end)
                        out_file.seek(start)
                        out_file.write(chunk)
                        pbar.update(len(chunk))
                        tracker.save_status(end + 1)

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
                return True

        except Exception as e:
            self.logger.error(json.dumps({
                "event": "download_failed",
                "file": getattr(item, 'name', 'unknown'),
                "error": str(e)
            }))
            if temp_path and temp_path.exists():
                try:
                    temp_path.unlink()
                except OSError as unlink_error:
                    self.logger.error(json.dumps({
                        "event": "temp_file_cleanup_error",
                        "error": str(unlink_error)
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

    def process_item_parallel(self, item: Any, local_path: Path) -> None:
        """Process files and directories in parallel."""
        try:
            if can_read_file(item):
                with self._download_lock:
                    local_path_str = str(local_path)
                    if local_path_str in self._active_downloads:
                        return
                    self._active_downloads.add(local_path_str)

                try:
                    if self.download_drive_item(item, local_path):
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
                    local_path.mkdir(parents=True, exist_ok=True)
                    with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                        futures = [
                            executor.submit(self.process_item_parallel, item[name], local_path / name)
                            for name in contents
                        ]
                        for future in as_completed(futures):
                            # Retrieve result or exception
                            try:
                                future.result()
                            except Exception as e:
                                self.logger.error(json.dumps({
                                    "event": "future_exception",
                                    "error": str(e)
                                }))

        except Exception as e:
            self.logger.error(json.dumps({
                "event": "processing_error",
                "file": getattr(item, 'name', 'unknown'),
                "error": str(e)
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
            else:
                self.logger.info(json.dumps({"event": "item_info", "path": path, "type": "file"}))
        except Exception as e:
            self.logger.error(json.dumps({"event": "listing_error", "path": path, "error": str(e)}))

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
        item = self.get_drive_item(icloud_path)

        self.logger.info(json.dumps({
            "event": "download_started",
            "icloud_path": icloud_path,
            "local_path": str(local_path_obj),
            "max_workers": self.max_workers,
            "chunk_size": self.chunker.chunk_size
        }))

        self.process_item_parallel(item, local_path_obj)

        report = self.generate_summary_report()
        self.logger.info(json.dumps({"event": "download_completed", "summary": report}))

        # Create report file in the same location as downloads
        report_path = local_path_obj / "download_report.json"
        with report_path.open('w') as f:
            json.dump(report, f, indent=2)
