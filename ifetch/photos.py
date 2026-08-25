"""
iCloud Photos downloader for iFetch.

This module mirrors the conventions established by :mod:`ifetch.downloader`
(structured JSON logging, exponential backoff honouring ``Retry-After``,
``ThreadPoolExecutor`` parallelism, a JSON run report) but targets the
iCloud *Photos* service instead of iCloud Drive.

Authentication is *composed* rather than re-implemented: a
:class:`~ifetch.downloader.DownloadManager` is created internally and its
``authenticate()`` performs the keyring password / 2FA / trusted-session
dance exactly once, in exactly one place.

Design notes about the underlying pyicloud API
----------------------------------------------
Two generations of the pyicloud Photos service are in the wild and this
module supports both:

* pyicloud 2.0.x (``pyicloud.services.photos``)
    - ``api.photos.albums`` is an ``AlbumContainer`` that **iterates album
      names** (``str``) and is indexed by name.
    - ``PhotoAsset.download(version)`` returns a streaming ``requests``
      ``Response``.
    - ``PhotoAsset.versions`` exposes ``original`` / ``medium`` / ``thumb``
      only, so the Live Photo video component has to be read straight off
      the ``resOriginalVidCompl*`` CloudKit fields.
* pyicloud >= 2.5 (``pyicloud.services.photos_cloudkit``)
    - ``api.photos.albums`` **iterates album objects** and offers
      ``find(name)``.
    - ``PhotoAsset.download(version)`` returns ``bytes``.
    - ``PhotoAsset.is_live_photo`` exists and ``versions`` includes
      ``original_video`` / ``medium_video`` / ``sidecar`` / ``alternative``.

Everything below is written defensively against those differences.
"""

from __future__ import annotations

import json
import os
import re
import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Tuple, Union

from .downloader import DownloadManager
from .logger import setup_logging
from .models import DownloadStatus

#: Name of the local index that makes re-runs cheap.
INDEX_FILENAME = ".ifetch_photos_index.json"

#: Filename written next to the downloads summarising the run.
REPORT_FILENAME = "photos_report.json"

#: Supported ``--folder-structure`` values.
FOLDER_STRUCTURES = ("flat", "year", "date", "day", "album")

#: Version keys we try, in order, when the requested one is unavailable.
VERSION_FALLBACKS = ("original", "medium", "thumb")

#: Version keys that hold the video half of a Live Photo, best first.
LIVE_VERSION_KEYS = ("original_video", "medium_video", "thumb_video", "live")

#: Album name pyicloud uses for the recently-deleted smart album.
RECENTLY_DELETED_ALBUM = "Recently Deleted"

_ILLEGAL_FILENAME_CHARS = re.compile(r'[\x00-\x1f<>:"/\\|?*]')
_RETRY_AFTER_RE = re.compile(r"retry[-_ ]?after[\"'\s:=]+(\d+)", re.IGNORECASE)
_NON_RETRYABLE = (
    "unauthorized",
    "forbidden",
    "invalid credentials",
    "not activated",
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def parse_date(value: str) -> datetime:
    """Parse a ``YYYY-MM-DD`` (or full ISO-8601) string into an aware datetime."""
    if isinstance(value, datetime):
        return _as_utc(value)

    text = str(value).strip()
    if not text:
        raise ValueError("Empty date value")

    candidates = ("%Y-%m-%d", "%Y/%m/%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S")
    for fmt in candidates:
        try:
            return datetime.strptime(text, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue

    try:
        return _as_utc(datetime.fromisoformat(text.replace("Z", "+00:00")))
    except ValueError:
        raise ValueError(
            f"Unrecognised date: {value!r} (expected YYYY-MM-DD or ISO-8601)"
        )


def _as_utc(value: datetime) -> datetime:
    """Return an aware UTC datetime for naive or aware input."""
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def sanitize_filename(name: str) -> str:
    """Make an Apple-supplied filename safe to use as a single path segment."""
    cleaned = _ILLEGAL_FILENAME_CHARS.sub("_", str(name or "")).strip().strip(".")
    return cleaned or "photo"


@dataclass
class AssetPlan:
    """A resolved decision about one asset, made before any download starts."""

    asset: Any
    asset_id: str
    filename: str
    local_path: Path
    version: str
    size: Optional[int] = None
    created: Optional[datetime] = None
    live_path: Optional[Path] = None
    live_version: Optional[str] = None
    live_url: Optional[str] = None
    need_photo: bool = True
    skipped: bool = False
    skip_reason: str = ""
    album: str = ""

    @property
    def needs_work(self) -> bool:
        return self.need_photo or self.live_path is not None


@dataclass
class PhotosIndex:
    """Local asset index: the reason a second run costs (almost) nothing."""

    path: Path
    entries: Dict[str, Dict[str, Any]] = field(default_factory=dict)

    @classmethod
    def load(cls, root: Union[str, Path]) -> "PhotosIndex":
        index_path = Path(root) / INDEX_FILENAME
        entries: Dict[str, Dict[str, Any]] = {}
        if index_path.exists():
            try:
                with index_path.open("r") as handle:
                    raw = json.load(handle)
                if isinstance(raw, dict):
                    loaded = raw.get("assets", raw)
                    if isinstance(loaded, dict):
                        entries = {
                            str(k): v for k, v in loaded.items() if isinstance(v, dict)
                        }
            except (json.JSONDecodeError, OSError):
                entries = {}
        return cls(path=index_path, entries=entries)

    def get(self, asset_id: str) -> Optional[Dict[str, Any]]:
        return self.entries.get(str(asset_id))

    def record(self, asset_id: str, data: Dict[str, Any]) -> None:
        self.entries[str(asset_id)] = data

    def save(self) -> None:
        payload = {
            "version": 1,
            "updated": datetime.now(timezone.utc).isoformat(),
            "assets": self.entries,
        }
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(self.path.suffix + ".tmp")
        with tmp.open("w") as handle:
            json.dump(payload, handle, indent=2)
        tmp.replace(self.path)


class PhotosDownloader:
    """Download an iCloud Photos library (or a slice of it) to local disk."""

    def __init__(
        self,
        email: Optional[str] = None,
        max_workers: int = 4,
        max_retries: int = 3,
        version: str = "original",
        folder_structure: str = "date",
        live_photos: bool = False,
        include_deleted: bool = False,
        dry_run: bool = False,
        set_mtime: bool = True,
    ):
        if folder_structure not in FOLDER_STRUCTURES:
            raise ValueError(
                f"Unsupported folder structure: {folder_structure!r} "
                f"(choose from {', '.join(FOLDER_STRUCTURES)})"
            )

        # Credential handling is delegated wholesale to DownloadManager so
        # there is exactly one implementation of the keyring/2FA flow.
        self._auth_manager = DownloadManager(
            email=email,
            max_workers=max_workers,
            max_retries=max_retries,
        )
        self.email = self._auth_manager.email

        self.max_workers = max(1, int(max_workers))
        self.max_retries = max(1, int(max_retries))
        self.version = version
        self.folder_structure = folder_structure
        self.live_photos = bool(live_photos)
        self.include_deleted = bool(include_deleted)
        self.dry_run = bool(dry_run)
        self.set_mtime = bool(set_mtime)

        self.api: Optional[Any] = None
        self.logger = setup_logging()
        self.results: List[DownloadStatus] = []
        self._results_lock = threading.Lock()
        self.index: Optional[PhotosIndex] = None
        self.root_path: Optional[Path] = None

    # ------------------------------------------------------------------
    # Authentication
    # ------------------------------------------------------------------
    def authenticate(self) -> None:
        """Authenticate with iCloud, reusing DownloadManager's flow."""
        self._auth_manager.authenticate()
        self.api = self._auth_manager.api

    def _ensure_api(self) -> Any:
        if self.api is None:
            self.authenticate()
        if self.api is None:
            raise Exception("Authentication did not yield an iCloud session")
        return self.api

    @property
    def photos_service(self) -> Any:
        """The pyicloud photos service."""
        api = self._ensure_api()
        service = getattr(api, "photos", None)
        if service is None:
            raise Exception("iCloud Photos service not available for this account")
        return service

    # ------------------------------------------------------------------
    # Albums
    # ------------------------------------------------------------------
    def _album_objects(self) -> List[Any]:
        """Return album objects across both pyicloud generations."""
        container = self.photos_service.albums
        albums: List[Any] = []
        for item in container:
            if isinstance(item, str):
                try:
                    albums.append(container[item])
                except Exception:
                    continue
            else:
                albums.append(item)
        return albums

    @staticmethod
    def _album_name(album: Any) -> str:
        for attr in ("fullname", "name", "title"):
            value = getattr(album, attr, None)
            if isinstance(value, str) and value:
                return value
        return str(album)

    def list_albums(self) -> List[Dict[str, Any]]:
        """List every album with its (best-effort) asset count."""
        albums: List[Dict[str, Any]] = []
        for album in self._album_objects():
            count: Optional[int]
            try:
                count = len(album)
            except Exception:
                count = None
            albums.append(
                {
                    "name": getattr(album, "name", None) or self._album_name(album),
                    "fullname": self._album_name(album),
                    "count": count,
                }
            )

        self.logger.info(json.dumps({"event": "albums_listed", "albums": albums}))
        return albums

    def get_album(self, name: Optional[str] = None) -> Any:
        """Resolve an album by name; ``None`` means the whole library."""
        service = self.photos_service
        if not name:
            return service.all

        container = service.albums

        finder = getattr(container, "find", None)
        if callable(finder):
            try:
                found = finder(name)
                if found is not None:
                    return found
            except Exception:
                pass

        try:
            found = container[name]
            if found is not None:
                return found
        except Exception:
            pass

        wanted = str(name).casefold()
        available: List[str] = []
        for album in self._album_objects():
            candidates = {
                str(getattr(album, "name", "") or ""),
                self._album_name(album),
            }
            available.extend(c for c in candidates if c)
            if any(c.casefold() == wanted for c in candidates if c):
                return album

        hint = f" Available albums: {', '.join(sorted(set(available)))}" if available else ""
        raise ValueError(f"Album not found: {name}.{hint}")

    # ------------------------------------------------------------------
    # Asset enumeration
    # ------------------------------------------------------------------
    @staticmethod
    def _iter_album_photos(album: Any) -> Iterator[Any]:
        photos = getattr(album, "photos", None)
        if photos is not None:
            return iter(photos)
        return iter(album)

    def iter_assets(
        self,
        album: Optional[str] = None,
        since: Optional[datetime] = None,
        until: Optional[datetime] = None,
    ) -> Iterator[Tuple[str, Any]]:
        """Yield ``(album_name, asset)`` pairs matching the selection."""
        sources: List[Any] = [self.get_album(album)]
        if self.include_deleted and not album:
            try:
                sources.append(self.get_album(RECENTLY_DELETED_ALBUM))
            except Exception as exc:
                self.logger.warning(
                    json.dumps(
                        {"event": "recently_deleted_unavailable", "error": str(exc)}
                    )
                )

        since_utc = _as_utc(since) if since else None
        until_utc = _as_utc(until) if until else None

        seen: set = set()
        for source in sources:
            album_name = self._album_name(source)
            for asset in self._iter_album_photos(source):
                created = self._asset_created(asset)
                if created is not None:
                    if since_utc and created < since_utc:
                        continue
                    if until_utc and created > until_utc:
                        continue
                asset_id = self._asset_id(asset)
                if asset_id in seen:
                    continue
                seen.add(asset_id)
                yield album_name, asset

    @staticmethod
    def _asset_id(asset: Any) -> str:
        for attr in ("id", "asset_id", "master_id"):
            value = getattr(asset, attr, None)
            if value:
                return str(value)
        return str(id(asset))

    def _asset_created(self, asset: Any) -> Optional[datetime]:
        for attr in ("created", "asset_date", "added_date"):
            try:
                value = getattr(asset, attr, None)
            except Exception:
                value = None
            if isinstance(value, datetime):
                return _as_utc(value)
        return None

    @staticmethod
    def _asset_versions(asset: Any) -> Dict[str, Dict[str, Any]]:
        try:
            versions = asset.versions
        except Exception:
            return {}
        return versions if isinstance(versions, dict) else {}

    def _resolve_version(self, asset: Any) -> str:
        """Pick the best available derivative, preferring the requested one."""
        versions = self._asset_versions(asset)
        if not versions:
            # Nothing to introspect (mock or trimmed record): trust the request.
            return self.version
        if self.version in versions:
            return self.version
        for candidate in VERSION_FALLBACKS:
            if candidate in versions:
                self.logger.warning(
                    json.dumps(
                        {
                            "event": "version_fallback",
                            "asset_id": self._asset_id(asset),
                            "requested": self.version,
                            "using": candidate,
                        }
                    )
                )
                return candidate
        return next(iter(versions))

    # ------------------------------------------------------------------
    # Live Photos
    # ------------------------------------------------------------------
    def _live_component(self, asset: Any) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        """Return ``(version_key, url, filename)`` for the Live Photo video.

        pyicloud >= 2.5 surfaces the paired video as the ``original_video``
        version; pyicloud 2.0.x does not expose it at all, so the raw
        ``resOriginalVidComplRes`` CloudKit field is read as a fallback.
        """
        versions = self._asset_versions(asset)
        for key in LIVE_VERSION_KEYS:
            entry = versions.get(key)
            if isinstance(entry, dict) and entry.get("url"):
                return key, entry.get("url"), entry.get("filename")

        master = getattr(asset, "_master_record", None)
        if isinstance(master, dict):
            fields = master.get("fields") or {}
            res = fields.get("resOriginalVidComplRes")
            if isinstance(res, dict):
                value = res.get("value") or {}
                url = value.get("downloadURL")
                if url:
                    return "original_video", url, None
        return None, None, None

    @staticmethod
    def _live_filename(photo_name: str, version_filename: Optional[str]) -> str:
        if version_filename and Path(version_filename).suffix:
            suffix = Path(version_filename).suffix
        else:
            suffix = ".MOV"
        return Path(photo_name).stem + suffix

    # ------------------------------------------------------------------
    # Path planning
    # ------------------------------------------------------------------
    def _target_directory(
        self, destination: Path, created: Optional[datetime], album_name: str
    ) -> Path:
        if self.folder_structure == "flat":
            return destination
        if self.folder_structure == "album":
            return destination / sanitize_filename(album_name or "Library")

        stamp = created or datetime.fromtimestamp(0, timezone.utc)
        if self.folder_structure == "year":
            return destination / f"{stamp.year:04d}"
        if self.folder_structure == "day":
            return destination / f"{stamp.year:04d}" / f"{stamp.month:02d}" / f"{stamp.day:02d}"
        # "date" -> YYYY/MM
        return destination / f"{stamp.year:04d}" / f"{stamp.month:02d}"

    @staticmethod
    def _disambiguate(path: Path, asset_id: str, attempt: int) -> Path:
        """Deterministically derive a collision-free variant of ``path``."""
        token = re.sub(r"[^A-Za-z0-9]", "", str(asset_id))[-8:] or "asset"
        suffix = f"_{token}" if attempt == 0 else f"_{token}_{attempt + 1}"
        return path.with_name(f"{path.stem}{suffix}{path.suffix}")

    def _claim_path(
        self,
        claimed: Dict[str, str],
        candidate: Path,
        asset_id: str,
        owned: Optional[str],
    ) -> Path:
        """Reserve a path for ``asset_id``, never overwriting another asset.

        ``owned`` is the path (as recorded in the index) that already belongs
        to this asset, if any: reclaiming it is always allowed.
        """
        attempt = 0
        path = candidate
        while True:
            key = path.as_posix()
            owner = claimed.get(key)
            if owner is None:
                belongs_to_us = owned is not None and Path(owned) == path
                if path.exists() and not belongs_to_us:
                    path = self._disambiguate(candidate, asset_id, attempt)
                    attempt += 1
                    continue
                claimed[key] = asset_id
                return path
            if owner == asset_id:
                return path
            path = self._disambiguate(candidate, asset_id, attempt)
            attempt += 1

    # ------------------------------------------------------------------
    # Planning
    # ------------------------------------------------------------------
    def plan(
        self,
        destination: Union[str, Path],
        album: Optional[str] = None,
        since: Optional[datetime] = None,
        until: Optional[datetime] = None,
    ) -> List[AssetPlan]:
        """Decide, serially and deterministically, what each asset needs."""
        dest = Path(destination).resolve()
        if self.index is None or self.root_path != dest:
            self.root_path = dest
            self.index = PhotosIndex.load(dest)

        claimed: Dict[str, str] = {}
        plans: List[AssetPlan] = []

        for album_name, asset in self.iter_assets(album=album, since=since, until=until):
            try:
                plans.append(self._plan_asset(asset, album_name, dest, claimed))
            except Exception as exc:  # one unreadable record must not abort planning
                self.logger.error(
                    json.dumps(
                        {
                            "event": "plan_failed",
                            "asset_id": self._asset_id(asset),
                            "error": str(exc),
                        }
                    )
                )
        return plans

    def _plan_asset(
        self,
        asset: Any,
        album_name: str,
        destination: Path,
        claimed: Dict[str, str],
    ) -> AssetPlan:
        asset_id = self._asset_id(asset)
        created = self._asset_created(asset)
        version = self._resolve_version(asset)
        versions = self._asset_versions(asset)
        version_info = versions.get(version) or {}

        raw_name = version_info.get("filename") or getattr(asset, "filename", None) or asset_id
        filename = sanitize_filename(raw_name)

        size = version_info.get("size")
        if size is None:
            size = getattr(asset, "size", None)
        if not isinstance(size, int):
            size = None

        entry = self.index.get(asset_id) if self.index else None
        owned = None
        if entry and entry.get("path"):
            owned = str(destination / entry["path"])

        target_dir = self._target_directory(destination, created, album_name)
        local_path = self._claim_path(
            claimed, target_dir / filename, asset_id, owned
        )

        plan = AssetPlan(
            asset=asset,
            asset_id=asset_id,
            filename=filename,
            local_path=local_path,
            version=version,
            size=size,
            created=created,
            album=album_name,
        )

        # Live Photo pairing
        if self.live_photos:
            live_version, live_url, live_name = self._live_component(asset)
            if live_version:
                live_candidate = local_path.parent / self._live_filename(
                    local_path.name, live_name
                )
                owned_live = None
                if entry and entry.get("live_path"):
                    owned_live = str(destination / entry["live_path"])
                plan.live_path = self._claim_path(
                    claimed, live_candidate, asset_id + ":live", owned_live
                )
                plan.live_version = live_version
                plan.live_url = live_url

        # Skip decision -------------------------------------------------
        if entry and entry.get("version") == version:
            recorded = destination / entry.get("path", "")
            same_size = (
                size is None
                or entry.get("size") is None
                or int(entry["size"]) == int(size)
            )
            if recorded.exists() and same_size:
                plan.need_photo = False
                plan.local_path = recorded
                if plan.live_path is not None:
                    live_recorded = entry.get("live_path")
                    if live_recorded and (destination / live_recorded).exists():
                        plan.live_path = None
                        plan.live_version = None
                if plan.live_path is None:
                    plan.skipped = True
                    plan.skip_reason = "already_downloaded"

        return plan

    # ------------------------------------------------------------------
    # Retry / transport
    # ------------------------------------------------------------------
    @staticmethod
    def _retry_after_seconds(error: Exception) -> Optional[int]:
        response = getattr(error, "response", None)
        headers = getattr(response, "headers", None)
        if headers is not None:
            try:
                value = headers.get("Retry-After")
            except Exception:
                value = None
            if value is not None and str(value).strip().isdigit():
                return int(str(value).strip())

        match = _RETRY_AFTER_RE.search(str(error))
        if match:
            return int(match.group(1))
        return None

    @staticmethod
    def _is_retryable(error: Exception) -> bool:
        text = str(error).lower()
        return not any(token in text for token in _NON_RETRYABLE)

    def _with_retry(self, func, description: str):
        """Run ``func`` with exponential backoff honouring ``Retry-After``."""
        last_error: Optional[Exception] = None
        for attempt in range(self.max_retries):
            try:
                return func()
            except Exception as exc:
                last_error = exc
                if not self._is_retryable(exc) or attempt >= self.max_retries - 1:
                    break
                delay = self._retry_after_seconds(exc)
                wait = min(delay, 60) if delay is not None else 2 ** (attempt + 1)
                self.logger.warning(
                    json.dumps(
                        {
                            "event": "photo_retry",
                            "target": description,
                            "attempt": attempt + 1,
                            "wait_seconds": wait,
                            "error": str(exc),
                        }
                    )
                )
                time.sleep(wait)
        raise Exception(
            f"Failed after {self.max_retries} attempts ({description}): {last_error}"
        )

    def _write_payload(self, payload: Any, path: Path) -> int:
        """Persist a pyicloud download result (bytes or streaming Response)."""
        path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = path.with_suffix(path.suffix + ".part")
        written = 0

        if payload is None:
            raise Exception("iCloud returned no data for this version")

        if isinstance(payload, (bytes, bytearray)):
            with temp_path.open("wb") as handle:
                handle.write(payload)
            written = len(payload)
        elif hasattr(payload, "iter_content"):
            raise_for_status = getattr(payload, "raise_for_status", None)
            if callable(raise_for_status):
                raise_for_status()
            with temp_path.open("wb") as handle:
                for chunk in payload.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        handle.write(chunk)
                        written += len(chunk)
        elif hasattr(payload, "content"):
            data = payload.content
            with temp_path.open("wb") as handle:
                handle.write(data)
            written = len(data)
        elif hasattr(payload, "read"):
            data = payload.read()
            with temp_path.open("wb") as handle:
                handle.write(data)
            written = len(data)
        else:
            raise Exception(f"Unsupported download payload: {type(payload)!r}")

        temp_path.replace(path)
        return written

    def _apply_mtime(self, path: Path, created: Optional[datetime]) -> None:
        if not (self.set_mtime and created):
            return
        try:
            stamp = created.timestamp()
            os.utime(path, (stamp, stamp))
        except Exception:
            pass  # non-fatal; the bytes matter more than the timestamp

    def _download_version(self, asset: Any, version: str, path: Path) -> int:
        payload = self._with_retry(
            lambda: asset.download(version=version), f"{path.name} ({version})"
        )
        return self._write_payload(payload, path)

    def _download_live(self, plan: AssetPlan) -> int:
        """Fetch the video half of a Live Photo."""
        asset = plan.asset
        versions = self._asset_versions(asset)
        if plan.live_version in versions:
            return self._download_version(asset, plan.live_version, plan.live_path)

        # pyicloud 2.0.x: the paired video is not a known version, so hit the
        # CloudKit downloadURL directly using the authenticated session.
        url = plan.live_url
        if not url:
            raise Exception("Live Photo video component has no download URL")
        session = getattr(self.photos_service, "session", None) or self._auth_manager.http
        payload = self._with_retry(
            lambda: session.get(url, stream=True), f"{plan.live_path.name} (live)"
        )
        return self._write_payload(payload, plan.live_path)

    # ------------------------------------------------------------------
    # Execution
    # ------------------------------------------------------------------
    def _record(self, status: DownloadStatus) -> None:
        with self._results_lock:
            self.results.append(status)

    def _execute_plan(self, plan: AssetPlan) -> bool:
        """Download one asset. Failures are contained, never raised."""
        downloaded = 0
        try:
            if plan.need_photo:
                downloaded += self._download_version(
                    plan.asset, plan.version, plan.local_path
                )
                self._apply_mtime(plan.local_path, plan.created)

            if plan.live_path is not None:
                downloaded += self._download_live(plan)
                self._apply_mtime(plan.live_path, plan.created)

            self._record(
                DownloadStatus(
                    path=str(plan.local_path),
                    size=plan.size or downloaded,
                    downloaded=downloaded,
                    status="completed",
                )
            )
            self.logger.info(
                json.dumps(
                    {
                        "event": "photo_downloaded",
                        "asset_id": plan.asset_id,
                        "path": str(plan.local_path),
                        "version": plan.version,
                        "live_path": str(plan.live_path) if plan.live_path else None,
                        "bytes": downloaded,
                    }
                )
            )
            self._update_index(plan, downloaded)
            return True
        except Exception as exc:
            self._record(
                DownloadStatus(
                    path=str(plan.local_path),
                    size=plan.size or 0,
                    downloaded=0,
                    status="failed",
                    error=str(exc),
                )
            )
            self.logger.error(
                json.dumps(
                    {
                        "event": "photo_download_failed",
                        "asset_id": plan.asset_id,
                        "path": str(plan.local_path),
                        "error": str(exc),
                        "traceback": traceback.format_exc(),
                    }
                )
            )
            return False

    def _relative(self, path: Path) -> str:
        if self.root_path:
            try:
                return path.relative_to(self.root_path).as_posix()
            except ValueError:
                pass
        return path.as_posix()

    def _update_index(self, plan: AssetPlan, downloaded: int) -> None:
        if self.index is None:
            return
        existing = self.index.get(plan.asset_id) or {}
        size = plan.size
        if size is None:
            try:
                size = plan.local_path.stat().st_size
            except OSError:
                size = downloaded
        entry = {
            "filename": plan.filename,
            "path": self._relative(plan.local_path),
            "size": size,
            "created": plan.created.isoformat() if plan.created else None,
            "version": plan.version,
            "album": plan.album,
            "downloaded_at": datetime.now(timezone.utc).isoformat(),
        }
        live_path = plan.live_path or (
            self.root_path / existing["live_path"]
            if existing.get("live_path") and self.root_path
            else None
        )
        if live_path is not None:
            entry["live_path"] = self._relative(Path(live_path))
        with self._results_lock:
            self.index.record(plan.asset_id, entry)

    # ------------------------------------------------------------------
    # Reporting
    # ------------------------------------------------------------------
    def generate_summary_report(
        self, skipped: int = 0, planned: int = 0
    ) -> Dict[str, Any]:
        """Summarise the run, mirroring DownloadManager's report shape."""
        successful = sum(1 for r in self.results if r.status == "completed")
        failed = sum(1 for r in self.results if r.status == "failed")
        total_bytes = sum(r.downloaded for r in self.results)

        return {
            "summary": {
                "total_assets": planned or (successful + failed + skipped),
                "downloaded": successful,
                "skipped": skipped,
                "failed": failed,
                "total_bytes_transferred": total_bytes,
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            },
            "details": [r.__dict__ for r in self.results],
        }

    # ------------------------------------------------------------------
    # Entry point
    # ------------------------------------------------------------------
    def download(
        self,
        destination: Union[str, Path] = ".",
        album: Optional[str] = None,
        since: Optional[datetime] = None,
        until: Optional[datetime] = None,
        log_file: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Download the selected assets and return the run report."""
        if log_file:
            self.logger = setup_logging(log_file)

        dest = Path(destination).resolve()
        if not self.dry_run:
            dest.mkdir(parents=True, exist_ok=True)

        self.root_path = dest
        self.index = PhotosIndex.load(dest)
        self.results = []

        self.logger.info(
            json.dumps(
                {
                    "event": "photos_download_started",
                    "album": album or "All Photos",
                    "destination": str(dest),
                    "version": self.version,
                    "folder_structure": self.folder_structure,
                    "live_photos": self.live_photos,
                    "max_workers": self.max_workers,
                    "dry_run": self.dry_run,
                }
            )
        )

        plans = self.plan(dest, album=album, since=since, until=until)
        pending = [p for p in plans if p.needs_work]
        skipped = len(plans) - len(pending)

        if self.dry_run:
            for plan in pending:
                self.logger.info(
                    json.dumps(
                        {
                            "event": "would_download",
                            "asset_id": plan.asset_id,
                            "path": str(plan.local_path),
                            "version": plan.version,
                            "live_path": str(plan.live_path)
                            if plan.live_path
                            else None,
                            "size": plan.size,
                        }
                    )
                )
            report = self.generate_summary_report(skipped=skipped, planned=len(plans))
            report["summary"]["dry_run"] = True
            report["summary"]["would_download"] = len(pending)
            report["plan"] = [
                {
                    "asset_id": p.asset_id,
                    "path": str(p.local_path),
                    "version": p.version,
                    "live_path": str(p.live_path) if p.live_path else None,
                }
                for p in pending
            ]
            self.logger.info(
                json.dumps({"event": "photos_download_completed", "summary": report["summary"]})
            )
            return report

        if pending:
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = {
                    executor.submit(self._execute_plan, plan): plan for plan in pending
                }
                for future in as_completed(futures):
                    try:
                        future.result()
                    except Exception as exc:  # defensive: _execute_plan swallows
                        self.logger.error(
                            json.dumps(
                                {
                                    "event": "future_exception",
                                    "asset_id": futures[future].asset_id,
                                    "error": str(exc),
                                }
                            )
                        )

        try:
            self.index.save()
        except OSError as exc:
            self.logger.error(
                json.dumps({"event": "index_save_failed", "error": str(exc)})
            )

        report = self.generate_summary_report(skipped=skipped, planned=len(plans))
        self.logger.info(
            json.dumps({"event": "photos_download_completed", "summary": report["summary"]})
        )

        try:
            with (dest / REPORT_FILENAME).open("w") as handle:
                json.dump(report, handle, indent=2)
        except OSError as exc:
            self.logger.error(
                json.dumps({"event": "report_write_failed", "error": str(exc)})
            )

        return report
