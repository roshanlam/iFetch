"""
iFetch Mirror Pipeline.

Orchestrates a two-stage, delta-aware mirror:

    Stage 1: iCloud Drive  -> local folder (DownloadManager)
    Stage 2: local folder  -> Google Drive (GoogleDriveExporter, optional)

Semantics:
    - Stage 1 always runs.
    - Stage 2 runs only when a Google Drive folder name is configured.
    - Stage 2 is SKIPPED when stage 1 had any errors, so a partial local
      state is never mirrored to Google Drive.
    - In dry-run mode stage 1 lists what would be downloaded and stage 2
      uploads are skipped (the plan is printed instead).

The heavy lifting is delegated to the existing DownloadManager and
GoogleDriveExporter; this module only composes them so the whole pipeline
is testable without the CLI.
"""

import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

from .downloader import DownloadManager

try:  # Google Drive support is optional; stage 2 needs the google libraries.
    from .exporters.googledrive import GoogleDriveExporter
except ImportError:  # pragma: no cover - depends on installed extras
    GoogleDriveExporter = None  # type: ignore[assignment]


@dataclass
class MirrorResult:
    """Summary of a single mirror cycle."""

    cycle: int = 1
    dry_run: bool = False
    downloaded: int = 0
    download_failed: int = 0
    uploaded: int = 0
    upload_skipped: int = 0
    upload_failed: int = 0
    stage2_ran: bool = False
    stage2_skip_reason: Optional[str] = None
    errors: List[str] = field(default_factory=list)
    duration: float = 0.0

    @property
    def success(self) -> bool:
        """True when no stage reported any failure."""
        return not self.errors and self.download_failed == 0 and self.upload_failed == 0

    def summary_line(self) -> str:
        """One-line, per-cycle summary suitable for logging."""
        line = (
            f"[cycle {self.cycle}] "
            f"downloaded={self.downloaded} "
            f"download_failed={self.download_failed} "
            f"uploaded={self.uploaded} "
            f"upload_failed={self.upload_failed} "
            f"errors={len(self.errors)} "
            f"duration={self.duration:.1f}s"
        )
        if self.stage2_skip_reason:
            line += f" (stage 2 skipped: {self.stage2_skip_reason})"
        return line


class MirrorPipeline:
    """Mirrors an iCloud Drive path to a local folder and, optionally, on to
    Google Drive. Both hops are delta-aware because the underlying tools
    already are (chunked diff downloads / upload index)."""

    def __init__(
        self,
        icloud_path: str,
        local_path: str,
        gdrive_folder: Optional[str] = None,
        email: Optional[str] = None,
        max_workers: int = 4,
        max_retries: int = 3,
        chunk_size: int = 1024 * 1024,
        include_patterns: Optional[List[str]] = None,
        exclude_patterns: Optional[List[str]] = None,
        log_file: Optional[str] = None,
        credentials_file: str = 'credentials.json',
        token_file: str = '.gdrive_token.json',
        dry_run: bool = False,
    ):
        self.icloud_path = icloud_path
        self.local_path = str(local_path)
        self.gdrive_folder = gdrive_folder
        self.email = email
        self.max_workers = max_workers
        self.max_retries = max_retries
        self.chunk_size = chunk_size
        self.include_patterns = include_patterns or []
        self.exclude_patterns = exclude_patterns or []
        self.log_file = log_file
        self.credentials_file = credentials_file
        self.token_file = token_file
        self.dry_run = dry_run

        # Lazily created and then reused across cycles so watch mode does not
        # re-authenticate on every iteration.
        self._downloader: Optional[DownloadManager] = None
        self._exporter: Optional[Any] = None

    # ------------------------------------------------------------------
    # Stage helpers
    # ------------------------------------------------------------------
    def _get_downloader(self) -> DownloadManager:
        if self._downloader is None:
            self._downloader = DownloadManager(
                email=self.email,
                max_workers=self.max_workers,
                max_retries=self.max_retries,
                chunk_size=self.chunk_size,
                include_patterns=self.include_patterns,
                exclude_patterns=self.exclude_patterns,
            )
        return self._downloader

    def _get_exporter(self) -> Any:
        if self._exporter is None:
            if GoogleDriveExporter is None:
                raise RuntimeError(
                    "Google Drive export requires the Google API packages "
                    "(google-api-python-client, google-auth-oauthlib)."
                )
            self._exporter = GoogleDriveExporter(
                credentials_file=self.credentials_file,
                token_file=self.token_file,
                root_folder_name=self.gdrive_folder,
            )
            self._exporter.authenticate()
        return self._exporter

    def _run_stage1(self, result: MirrorResult) -> bool:
        """Sync iCloud -> local. Returns True when the stage is clean."""
        try:
            downloader = self._get_downloader()
            # Reset per-cycle results so watch mode reports deltas, not totals.
            downloader.download_results = []

            if self.dry_run:
                print(f"[dry-run] would download '{self.icloud_path}' to '{self.local_path}'")
                if not downloader.api:
                    downloader.authenticate()
                downloader.list_contents(self.icloud_path)
                return True

            downloader.download(
                self.icloud_path,
                self.local_path,
                log_file=self.log_file,
            )
            summary = downloader.generate_summary_report()["summary"]
            result.downloaded = summary.get("successful", 0)
            result.download_failed = summary.get("failed", 0)
            return result.download_failed == 0
        except Exception as e:  # noqa: BLE001 - contained so watch mode survives
            result.errors.append(f"stage 1 (iCloud -> local): {e}")
            return False

    def _run_stage2(self, result: MirrorResult, stage1_clean: bool) -> None:
        """Upload local -> Google Drive, unless skipped."""
        if not self.gdrive_folder:
            result.stage2_skip_reason = "no --gdrive-folder configured"
            return
        if self.dry_run:
            print(
                f"[dry-run] would upload '{self.local_path}' to Google Drive "
                f"folder '{self.gdrive_folder}'"
            )
            result.stage2_skip_reason = "dry-run"
            return
        if not stage1_clean:
            # Never mirror a partial local state to Google Drive.
            result.stage2_skip_reason = "stage 1 had errors"
            return

        try:
            exporter = self._get_exporter()
            stats: Dict[str, Any] = exporter.export_local_folders(
                folders=[self.local_path],
            )
            result.stage2_ran = True
            result.uploaded = stats.get("uploaded", 0)
            result.upload_skipped = stats.get("skipped", 0)
            result.upload_failed = stats.get("failed", 0)
        except Exception as e:  # noqa: BLE001 - contained so watch mode survives
            result.errors.append(f"stage 2 (local -> Google Drive): {e}")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def run_once(self, cycle: int = 1) -> MirrorResult:
        """Run one full mirror cycle and return its summary."""
        result = MirrorResult(cycle=cycle, dry_run=self.dry_run)
        start = time.time()

        stage1_clean = self._run_stage1(result)
        self._run_stage2(result, stage1_clean)

        result.duration = time.time() - start
        return result
