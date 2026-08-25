"""
Google Drive Exporter for iFetch.

This module provides functionality to export local folders and iCloud Drive
data to Google Drive.
"""

import os
import pickle
import hashlib
import json
from pathlib import Path
from typing import Optional, List, Dict, Any
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import time

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from googleapiclient.errors import HttpError

from .index_manager import UploadIndexManager

# If modifying these scopes, delete the token file.
SCOPES = ['https://www.googleapis.com/auth/drive.file']

# Default (JSON) location for the cached OAuth token.
DEFAULT_TOKEN_FILE = '.gdrive_token.json'

# Result codes returned by the internal upload helper.
UPLOAD_UPLOADED = 'uploaded'
UPLOAD_SKIPPED = 'skipped'
UPLOAD_FAILED = 'failed'
UPLOAD_IGNORED = 'ignored'


def escape_query_value(value: str) -> str:
    """Escape a value for interpolation into a Google Drive API query string.

    Drive's query language uses single-quoted string literals in which a
    backslash escapes the following character.  Names containing ``'`` (very
    common, e.g. "Roshan's Files") or ``\\`` would otherwise break or distort
    the query.

    Args:
        value: Raw, user-controlled value (file name, folder name, file ID...)

    Returns:
        The value with ``\\`` and ``'`` backslash-escaped.
    """
    return str(value).replace('\\', '\\\\').replace("'", "\\'")


class GoogleDriveExporter:
    """Exports local files and iCloud Drive data to Google Drive."""

    def __init__(
        self,
        credentials_file: str = 'credentials.json',
        token_file: str = DEFAULT_TOKEN_FILE,
        root_folder_name: str = 'MacOS Data',
        chunk_size: int = 10 * 1024 * 1024,  # 10 MB chunks for resumable uploads
        ignore_file: Optional[str] = '.gdriveexportignore',
        index_file: str = '.gdrive_upload_index.json',
        use_index: bool = True,
        upload_workers: int = 4,  # Number of parallel upload workers
    ):
        """
        Initialize Google Drive Exporter.

        Args:
            credentials_file: Path to Google OAuth2 credentials JSON file
            token_file: Path to the cached authentication token.  Tokens are
                stored as JSON; a legacy ``.pickle`` token (from older iFetch
                releases) is read once and migrated to the JSON format.
            root_folder_name: Name of the root folder in Google Drive to store exports
            chunk_size: Chunk size for resumable uploads (default: 10 MB)
            ignore_file: Path to ignore file (like .gitignore) for excluding files/folders
            index_file: Path to index file for tracking uploaded files
            use_index: Enable incremental uploads using index (default: True)
            upload_workers: Number of parallel upload workers (default: 4)
        """
        self.credentials_file = credentials_file
        self.token_file = token_file
        self.json_token_file, self.legacy_token_file = self._resolve_token_paths(token_file)
        self.root_folder_name = root_folder_name
        self.chunk_size = chunk_size
        self.ignore_file = ignore_file
        self.use_index = use_index
        self.upload_workers = upload_workers
        self.service = None  # Main service for authentication
        self.root_folder_id = None
        self._credentials = None  # Store credentials for thread-local services
        self._folder_cache: Dict[str, str] = {}  # Cache folder IDs
        self._file_cache: Dict[str, Dict[str, Any]] = {}  # Cache file metadata
        self._ignore_patterns: List[str] = []  # Ignore patterns from file
        self._load_ignore_patterns()

        # Initialize index manager
        self.index_manager = UploadIndexManager(index_file) if use_index else None

        # Thread-local storage for per-thread Google Drive service instances
        self._thread_local = threading.local()

        # Thread-safe locks for parallel operations
        self._index_lock = threading.Lock()
        self._print_lock = threading.Lock()
        self._folder_cache_lock = threading.Lock()

        # Upload statistics tracking
        self._upload_start_time = None
        self._total_uploaded_bytes = 0
        self._bytes_lock = threading.Lock()

    @staticmethod
    def _resolve_token_paths(token_file: str):
        """Work out the JSON token path and the legacy pickle path.

        Passing an explicit ``*.pickle`` path (older CLI defaults) still works:
        the pickle is read, then transparently migrated to a sibling ``.json``
        file which is used from then on.

        Returns:
            Tuple of ``(json_token_path, legacy_pickle_path)``
        """
        path = Path(token_file)
        if path.suffix == '.pickle':
            return str(path.with_suffix('.json')), str(path)
        return str(path), str(path.with_suffix('.pickle'))

    def _load_legacy_pickle_token(self, path: Path):
        """Read a legacy pickled credential, tolerating any failure."""
        try:
            with open(path, 'rb') as token:
                return pickle.load(token)
        except Exception as error:  # noqa: BLE001 - never let a bad file abort auth
            print(f"Warning: could not read legacy token '{path}': {error}")
            return None

    def _save_token(self, creds) -> None:
        """Persist credentials as JSON with 0600 permissions."""
        path = Path(self.json_token_file)
        payload = creds.to_json()

        parent = path.parent
        if str(parent) and not parent.exists():
            parent.mkdir(parents=True, exist_ok=True)

        fd = os.open(str(path), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, 'w') as token:
            token.write(payload)

        # Enforce restrictive permissions even if the file already existed.
        try:
            os.chmod(str(path), 0o600)
        except OSError:
            pass

    def _load_token(self):
        """Load cached credentials, migrating a legacy pickle token if needed."""
        json_path = Path(self.json_token_file)
        if json_path.exists():
            try:
                return Credentials.from_authorized_user_file(str(json_path), SCOPES)
            except Exception as error:  # noqa: BLE001 - fall back to re-auth
                print(f"Warning: could not read token '{json_path}': {error}")

        legacy_path = Path(self.legacy_token_file)
        if legacy_path.exists():
            creds = self._load_legacy_pickle_token(legacy_path)
            if creds is not None:
                print(f"Migrating legacy token '{legacy_path}' to '{json_path}'...")
                try:
                    self._save_token(creds)
                except Exception as error:  # noqa: BLE001 - migration is best-effort
                    print(f"Warning: could not migrate token to '{json_path}': {error}")
                return creds

        return None

    def authenticate(self) -> None:
        """Authenticate with Google Drive API."""
        creds = self._load_token()

        # If no valid credentials, let user log in
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                print("Refreshing Google Drive authentication...")
                creds.refresh(Request())
            else:
                if not os.path.exists(self.credentials_file):
                    raise FileNotFoundError(
                        f"Credentials file not found: {self.credentials_file}\n"
                        "Please download OAuth 2.0 credentials from Google Cloud Console:\n"
                        "1. Go to https://console.cloud.google.com/\n"
                        "2. Create a project and enable Google Drive API\n"
                        "3. Create OAuth 2.0 credentials (Desktop app)\n"
                        "4. Download and save as 'credentials.json'"
                    )

                print("Opening browser for Google Drive authentication...")
                flow = InstalledAppFlow.from_client_secrets_file(
                    self.credentials_file, SCOPES
                )
                creds = flow.run_local_server(port=0)

            # Save credentials for future use (JSON, owner-readable only)
            self._save_token(creds)
            print("Authentication successful!")

        # Store credentials for thread-local services
        self._credentials = creds

        # Build main service
        self.service = build('drive', 'v3', credentials=creds)

        # Create or find root folder
        self.root_folder_id = self._get_or_create_folder(self.root_folder_name)
        print(f"Using root folder: {self.root_folder_name} (ID: {self.root_folder_id})")

    def _get_thread_local_service(self):
        """
        Get a thread-local Google Drive service instance.
        Each thread gets its own service object to avoid SSL/connection issues.
        """
        if not hasattr(self._thread_local, 'service') or self._thread_local.service is None:
            # Create a new service instance for this thread
            self._thread_local.service = build('drive', 'v3', credentials=self._credentials)
        return self._thread_local.service

    def _load_ignore_patterns(self) -> None:
        """Load ignore patterns from the ignore file."""
        if not self.ignore_file:
            return

        ignore_path = Path(self.ignore_file)
        if not ignore_path.exists():
            # Try to find it in the current directory
            ignore_path = Path.cwd() / self.ignore_file
            if not ignore_path.exists():
                print(f"Note: Ignore file '{self.ignore_file}' not found. No patterns will be ignored.")
                return

        try:
            with open(ignore_path, 'r') as f:
                for line in f:
                    line = line.strip()
                    # Skip comments and empty lines
                    if not line or line.startswith('#'):
                        continue
                    self._ignore_patterns.append(line)

            if self._ignore_patterns:
                print(f"Loaded {len(self._ignore_patterns)} ignore patterns from {ignore_path}")
        except Exception as e:
            print(f"Warning: Could not load ignore file '{ignore_path}': {e}")

    def _should_ignore(self, path: Path, base_path: Optional[Path] = None) -> bool:
        """
        Check if a path should be ignored based on ignore patterns.

        Args:
            path: Path to check
            base_path: Base path to calculate relative path from (optional)

        Returns:
            True if path should be ignored, False otherwise
        """
        if not self._ignore_patterns:
            return False

        # Convert to Path object if needed
        if not isinstance(path, Path):
            path = Path(path)

        # Get path name and relative path
        path_name = path.name

        # Calculate relative path if base_path provided
        if base_path:
            try:
                rel_path = path.relative_to(base_path)
            except ValueError:
                # Path is not relative to base_path, use absolute
                rel_path = path
        else:
            rel_path = path

        # Convert to POSIX-style path (forward slashes)
        rel_path_str = str(rel_path).replace('\\', '/')

        # Check against each pattern
        for pattern in self._ignore_patterns:
            # Clean pattern
            pattern_clean = pattern.rstrip('/')

            # Exact name match (e.g., ".DS_Store")
            if path_name == pattern_clean:
                return True

            # Directory pattern (ends with /)
            if pattern.endswith('/'):
                # Check if this is a directory and matches
                if path.is_dir() and (path_name == pattern_clean or rel_path_str == pattern_clean):
                    return True
                # Check if any parent directory matches
                if f"/{pattern_clean}/" in f"/{rel_path_str}/":
                    return True

            # Glob pattern matching
            # Match against relative path
            if self._match_pattern(rel_path_str, pattern_clean):
                return True

            # Match against just the filename
            if self._match_pattern(path_name, pattern_clean):
                return True

            # For patterns with **, match against full path
            if '**' in pattern_clean:
                if self._match_pattern(rel_path_str, pattern_clean):
                    return True

        return False

    @staticmethod
    def _match_pattern(path_str: str, pattern: str) -> bool:
        """
        Match a path string against a glob pattern.

        Args:
            path_str: Path string to match
            pattern: Glob pattern

        Returns:
            True if matches, False otherwise
        """
        import fnmatch

        # Handle ** pattern (match any number of directories)
        if '**' in pattern:
            # Convert ** to regex-like pattern
            # e.g., "**/*.pyc" should match "a/b/c.pyc"
            parts = pattern.split('**')
            if len(parts) == 2:
                prefix, suffix = parts
                prefix = prefix.strip('/')
                suffix = suffix.strip('/')

                # If no prefix, just match suffix anywhere
                if not prefix:
                    if suffix:
                        # Check if path ends with the pattern after **
                        if fnmatch.fnmatch(path_str, f"*{suffix}"):
                            return True
                        # Also check each path component
                        if '/' in path_str:
                            for part in path_str.split('/'):
                                if fnmatch.fnmatch(part, suffix):
                                    return True
                    return False

                # Match prefix and suffix
                if fnmatch.fnmatch(path_str, f"{prefix}*{suffix}"):
                    return True

        # Standard glob matching
        return fnmatch.fnmatch(path_str, pattern)

    def _get_or_create_folder(
        self,
        folder_name: str,
        parent_id: Optional[str] = None
    ) -> str:
        """
        Get folder ID if exists, otherwise create it.
        Thread-safe for parallel operations.

        Args:
            folder_name: Name of the folder
            parent_id: Parent folder ID (None for root)

        Returns:
            Folder ID
        """
        # Check cache first (thread-safe)
        cache_key = f"{parent_id or 'root'}:{folder_name}"
        with self._folder_cache_lock:
            if cache_key in self._folder_cache:
                return self._folder_cache[cache_key]

        # Get thread-local service
        service = self._get_thread_local_service()

        # Search for existing folder (values escaped for Drive query syntax)
        query = (
            f"name='{escape_query_value(folder_name)}' "
            "and mimeType='application/vnd.google-apps.folder' and trashed=false"
        )
        if parent_id:
            query += f" and '{escape_query_value(parent_id)}' in parents"
        else:
            query += " and 'root' in parents"

        try:
            results = service.files().list(
                q=query,
                spaces='drive',
                fields='files(id, name)',
                pageSize=1
            ).execute()

            files = results.get('files', [])

            if files:
                folder_id = files[0]['id']
                with self._folder_cache_lock:
                    self._folder_cache[cache_key] = folder_id
                return folder_id
        except HttpError as error:
            with self._print_lock:
                print(f"Error searching for folder: {error}")

        # Folder doesn't exist, create it
        file_metadata = {
            'name': folder_name,
            'mimeType': 'application/vnd.google-apps.folder'
        }
        if parent_id:
            file_metadata['parents'] = [parent_id]

        try:
            folder = service.files().create(
                body=file_metadata,
                fields='id'
            ).execute()
            folder_id = folder['id']
            with self._folder_cache_lock:
                self._folder_cache[cache_key] = folder_id
            with self._print_lock:
                print(f"Created folder: {folder_name}")
            return folder_id
        except HttpError as error:
            raise Exception(f"Error creating folder '{folder_name}': {error}")

    def _get_file_info(self, file_path: Path) -> Dict[str, Any]:
        """Get file information including size and MD5 checksum."""
        file_size = file_path.stat().st_size
        md5_hash = self._calculate_md5(file_path)

        return {
            'size': file_size,
            'md5': md5_hash,
            'modified': datetime.fromtimestamp(file_path.stat().st_mtime).isoformat()
        }

    @staticmethod
    def _calculate_md5(file_path: Path) -> str:
        """Calculate MD5 checksum of a file."""
        hash_md5 = hashlib.md5()
        with open(file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()

    def _file_exists_in_drive(
        self,
        file_name: str,
        parent_id: str,
        local_md5: str
    ) -> Optional[str]:
        """
        Check if file exists in Google Drive with same MD5.
        Thread-safe for parallel operations.

        Args:
            file_name: Name of the file
            parent_id: Parent folder ID
            local_md5: MD5 checksum of local file

        Returns:
            File ID if exists and unchanged, None otherwise
        """
        # Get thread-local service
        service = self._get_thread_local_service()

        query = (
            f"name='{escape_query_value(file_name)}' "
            f"and '{escape_query_value(parent_id)}' in parents and trashed=false"
        )

        try:
            results = service.files().list(
                q=query,
                spaces='drive',
                fields='files(id, name, md5Checksum, size)',
                pageSize=1
            ).execute()

            files = results.get('files', [])

            if files:
                file = files[0]
                drive_md5 = file.get('md5Checksum', '')

                # Compare checksums
                if drive_md5.lower() == local_md5.lower():
                    return file['id']  # File exists and unchanged
        except HttpError as error:
            with self._print_lock:
                print(f"Error checking file existence: {error}")

        return None

    def upload_file(
        self,
        file_path: Path,
        gdrive_parent_id: str,
        force: bool = False,
        base_path: Optional[Path] = None
    ) -> Optional[str]:
        """
        Upload a file to Google Drive.

        Args:
            file_path: Path to local file
            gdrive_parent_id: Parent folder ID in Google Drive
            force: Force upload even if file exists
            base_path: Base path for calculating relative paths (for ignore patterns)

        Returns:
            File ID if uploaded or already present, None if skipped/failed
        """
        return self._upload_file_with_status(
            file_path, gdrive_parent_id, force, base_path
        )[1]

    def _upload_file_with_status(
        self,
        file_path: Path,
        gdrive_parent_id: str,
        force: bool = False,
        base_path: Optional[Path] = None
    ):
        """
        Upload a file and report what actually happened.

        Identical to :meth:`upload_file` but returns a
        ``(status, file_id)`` tuple where *status* is one of
        ``uploaded`` / ``skipped`` / ``failed`` / ``ignored``.  Callers use
        this to keep accurate statistics without re-hashing the file or
        issuing extra Drive API calls.
        """
        if not file_path.exists() or not file_path.is_file():
            with self._print_lock:
                print(f"Skipping: {file_path} (not a file)")
            return UPLOAD_FAILED, None

        # Check if file should be ignored
        if self._should_ignore(file_path, base_path):
            with self._print_lock:
                print(f"Ignored: {file_path}")
            return UPLOAD_IGNORED, None

        file_name = file_path.name
        file_info = self._get_file_info(file_path)

        # Calculate relative path for index tracking
        if base_path:
            try:
                rel_path = str(file_path.relative_to(base_path))
            except ValueError:
                rel_path = str(file_path)
        else:
            rel_path = str(file_path)

        # Check index first (faster than checking Google Drive)
        # CRITICAL OPTIMIZATION: Trust the index and skip API call!
        if self.index_manager and not force:
            with self._index_lock:
                if not self.index_manager.file_needs_upload(
                    rel_path,
                    file_info['size'],
                    file_path.stat().st_mtime,
                    file_info['md5']
                ):
                    # File is in index and hasn't changed - SKIP IT!
                    # Don't make API call to Google Drive, just trust the index
                    gdrive_file_id = self.index_manager.get_gdrive_file_id(rel_path)
                    with self._print_lock:
                        print(f"Skipped: {file_path} (unchanged since last upload)")
                    return UPLOAD_SKIPPED, gdrive_file_id

        # Only check Google Drive if index says file needs upload OR force=True
        # This dramatically reduces API calls!
        if not force:
            existing_file_id = self._file_exists_in_drive(
                file_name,
                gdrive_parent_id,
                file_info['md5']
            )
            if existing_file_id:
                with self._print_lock:
                    print(f"Skipped: {file_path} (already exists in Google Drive, unchanged)")
                # Update index with this file
                if self.index_manager:
                    with self._index_lock:
                        self.index_manager.add_file_entry(
                            rel_path,
                            file_info['size'],
                            file_path.stat().st_mtime,
                            file_info['md5'],
                            existing_file_id
                        )
                return UPLOAD_SKIPPED, existing_file_id

        # Prepare file metadata
        file_metadata = {
            'name': file_name,
            'parents': [gdrive_parent_id]
        }

        # Upload file
        try:
            media = MediaFileUpload(
                str(file_path),
                resumable=True,
                chunksize=self.chunk_size
            )

            with self._print_lock:
                print(f"Uploading: {file_path} ({file_info['size']} bytes)")

            upload_start = time.time()

            # Get thread-local service for thread-safe API calls
            service = self._get_thread_local_service()
            request = service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id, name, size, md5Checksum'
            )

            response = None
            last_progress = 0
            while response is None:
                status, response = request.next_chunk()
                if status:
                    progress = int(status.progress() * 100)
                    # Only print progress updates every 20% to reduce output noise
                    if progress - last_progress >= 20 or progress == 100:
                        with self._print_lock:
                            print(f"  {file_path.name}: {progress}%")
                        last_progress = progress

            upload_time = time.time() - upload_start
            upload_speed = file_info['size'] / upload_time / (1024 * 1024) if upload_time > 0 else 0

            with self._print_lock:
                print(f"✓ Uploaded: {file_path.name} ({file_info['size']/(1024*1024):.2f} MB in {upload_time:.1f}s, {upload_speed:.2f} MB/s)")

            # Track uploaded bytes
            with self._bytes_lock:
                self._total_uploaded_bytes += file_info['size']

            # Update index with successful upload
            if self.index_manager:
                with self._index_lock:
                    self.index_manager.add_file_entry(
                        rel_path,
                        file_info['size'],
                        file_path.stat().st_mtime,
                        file_info['md5'],
                        response['id']
                    )

            return UPLOAD_UPLOADED, response['id']

        except HttpError as error:
            with self._print_lock:
                print(f"✗ Error uploading {file_path}: {error}")
            return UPLOAD_FAILED, None

    def upload_directory(
        self,
        local_path: Path,
        gdrive_parent_id: Optional[str] = None,
        force: bool = False,
        include_patterns: Optional[List[str]] = None,
        exclude_patterns: Optional[List[str]] = None,
        base_path: Optional[Path] = None
    ) -> Dict[str, Any]:
        """
        Recursively upload a directory to Google Drive.

        Args:
            local_path: Path to local directory
            gdrive_parent_id: Parent folder ID in Google Drive (None for root folder)
            force: Force upload even if files exist
            include_patterns: List of patterns to include (e.g., ['*.pdf', '*.txt'])
            exclude_patterns: List of patterns to exclude
            base_path: Base path for calculating relative paths (for ignore patterns)

        Returns:
            Dictionary with upload statistics
        """
        if gdrive_parent_id is None:
            gdrive_parent_id = self.root_folder_id

        if not local_path.exists() or not local_path.is_dir():
            raise ValueError(f"Invalid directory: {local_path}")

        # Set base_path on first call
        if base_path is None:
            base_path = local_path.parent

        stats = {
            'total_files': 0,
            'uploaded': 0,
            'skipped': 0,
            'failed': 0,
            'ignored': 0,
            'total_bytes': 0
        }

        # Create folder in Google Drive with same name as local directory
        folder_name = local_path.name
        current_folder_id = self._get_or_create_folder(folder_name, gdrive_parent_id)

        # Collect files and directories
        files_to_upload = []
        subdirs_to_process = []

        for item in local_path.iterdir():
            # Check if item should be ignored (applies to both files and directories)
            if self._should_ignore(item, base_path):
                with self._print_lock:
                    print(f"Ignored: {item}")
                stats['ignored'] += 1
                continue

            if item.is_file():
                # Check include/exclude patterns
                if include_patterns and not any(item.match(p) for p in include_patterns):
                    continue
                if exclude_patterns and any(item.match(p) for p in exclude_patterns):
                    continue

                stats['total_files'] += 1
                stats['total_bytes'] += item.stat().st_size
                files_to_upload.append(item)

            elif item.is_dir():
                subdirs_to_process.append(item)

        # Upload files in parallel
        if files_to_upload:
            with ThreadPoolExecutor(max_workers=self.upload_workers) as executor:
                # Submit all upload tasks
                future_to_file = {
                    executor.submit(
                        self._upload_file_with_status,
                        file_path,
                        current_folder_id,
                        force,
                        base_path,
                    ): file_path
                    for file_path in files_to_upload
                }

                # Process completed uploads - the worker reports what it did,
                # so no re-hashing or extra API calls are needed here.
                for future in as_completed(future_to_file):
                    file_path = future_to_file[future]
                    try:
                        status, _file_id = future.result()
                        if status in (UPLOAD_UPLOADED, UPLOAD_SKIPPED, UPLOAD_IGNORED):
                            stats[status] += 1
                        else:
                            stats['failed'] += 1
                    except Exception as e:
                        with self._print_lock:
                            print(f"✗ Exception uploading {file_path}: {e}")
                        stats['failed'] += 1

        # Process subdirectories recursively (sequential to avoid too many threads)
        for subdir in subdirs_to_process:
            sub_stats = self.upload_directory(
                subdir,
                current_folder_id,
                force,
                include_patterns,
                exclude_patterns,
                base_path  # Pass base_path through recursive calls
            )
            # Merge statistics
            for key in stats:
                stats[key] += sub_stats[key]

        return stats

    def export_local_folders(
        self,
        folders: List[str],
        force: bool = False,
        include_patterns: Optional[List[str]] = None,
        exclude_patterns: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Export multiple local folders to Google Drive.

        Args:
            folders: List of folder paths to export (e.g., ['~/Documents', '~/Downloads'])
            force: Force upload even if files exist
            include_patterns: List of patterns to include
            exclude_patterns: List of patterns to exclude

        Returns:
            Dictionary with combined upload statistics
        """
        # Start tracking upload time
        self._upload_start_time = time.time()
        self._total_uploaded_bytes = 0

        total_stats = {
            'total_files': 0,
            'uploaded': 0,
            'skipped': 0,
            'failed': 0,
            'ignored': 0,
            'total_bytes': 0,
            'uploaded_bytes': 0,
            'folders': {}
        }

        print(f"\nUpload configuration:")
        print(f"  Parallel workers: {self.upload_workers}")
        print(f"  Chunk size: {self.chunk_size / (1024*1024):.0f} MB")
        print(f"  Index enabled: {'Yes' if self.index_manager else 'No'}\n")

        for folder in folders:
            folder_path = Path(folder).expanduser().resolve()

            if not folder_path.exists():
                print(f"Warning: Folder not found: {folder_path}")
                continue

            print(f"\n{'='*60}")
            print(f"Exporting: {folder_path}")
            print(f"{'='*60}\n")

            try:
                stats = self.upload_directory(
                    folder_path,
                    self.root_folder_id,
                    force,
                    include_patterns,
                    exclude_patterns
                )

                total_stats['folders'][str(folder_path)] = stats

                # Merge statistics
                for key in ['total_files', 'uploaded', 'skipped', 'failed', 'ignored', 'total_bytes']:
                    total_stats[key] += stats[key]

            except Exception as e:
                print(f"Error exporting {folder_path}: {e}")
                total_stats['folders'][str(folder_path)] = {'error': str(e)}

        # Calculate upload statistics
        total_stats['uploaded_bytes'] = self._total_uploaded_bytes
        if self._upload_start_time:
            total_stats['upload_time'] = time.time() - self._upload_start_time
            if total_stats['upload_time'] > 0 and total_stats['uploaded_bytes'] > 0:
                total_stats['avg_speed_mbps'] = (total_stats['uploaded_bytes'] / total_stats['upload_time']) / (1024 * 1024)

        # Save index after all uploads (force save to ensure all changes are persisted)
        if self.index_manager:
            if self.index_manager._pending_saves > 0:
                print(f"\nSaving upload index ({self.index_manager._pending_saves} pending changes)...")
                self.index_manager.save_index(force=True)
                print("Index saved successfully")
            else:
                print("\nNo index changes to save")

        return total_stats

    def print_summary(self, stats: Dict[str, Any]) -> None:
        """Print upload summary statistics."""
        print(f"\n{'='*60}")
        print("EXPORT SUMMARY")
        print(f"{'='*60}\n")

        print(f"Total files processed: {stats['total_files']}")
        print(f"  Uploaded: {stats['uploaded']}")
        print(f"  Skipped (unchanged): {stats['skipped']}")
        print(f"  Ignored: {stats.get('ignored', 0)}")
        print(f"  Failed: {stats['failed']}")
        print(f"Total size: {stats['total_bytes'] / (1024**3):.2f} GB")

        # Show upload statistics
        if stats.get('uploaded_bytes', 0) > 0:
            print(f"Uploaded: {stats['uploaded_bytes'] / (1024**3):.2f} GB")
            if stats.get('upload_time'):
                print(f"Upload time: {stats['upload_time']:.1f} seconds")
                if stats.get('avg_speed_mbps'):
                    print(f"Average speed: {stats['avg_speed_mbps']:.2f} MB/s")

        if 'folders' in stats:
            print(f"\nPer-folder breakdown:")
            for folder, folder_stats in stats['folders'].items():
                if 'error' in folder_stats:
                    print(f"  {folder}: ERROR - {folder_stats['error']}")
                else:
                    print(f"  {folder}:")
                    print(f"    Files: {folder_stats['total_files']}, "
                          f"Uploaded: {folder_stats['uploaded']}, "
                          f"Skipped: {folder_stats['skipped']}, "
                          f"Ignored: {folder_stats.get('ignored', 0)}, "
                          f"Failed: {folder_stats['failed']}")

        print(f"\n{'='*60}\n")
