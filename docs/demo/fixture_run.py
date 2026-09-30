#!/usr/bin/env python3
"""Fixture-backed local demo of the first-run download path.

Drives the real DownloadManager against in-process stream nodes — the same
package-expand and metadata fast-path code paths a live iCloud run uses —
without contacting Apple. Used to record docs/assets/ifetch-demo.gif.
"""

from __future__ import annotations

import io
import json
import logging
import os
import sys
import tempfile
import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO))

os.environ.setdefault("ICLOUD_EMAIL", "you@example.com")
os.environ.setdefault("NO_COLOR", "1")

from ifetch.downloader import DownloadManager, SyncState  # noqa: E402
from ifetch.manifest import Manifest  # noqa: E402
from ifetch.render import human_bytes  # noqa: E402

MTIME = datetime(2026, 3, 14, 15, 9, 26, tzinfo=timezone.utc)
DEST_LABEL = "~/icloud-backup"
REMOTE = "Documents"


class StreamNode:
    """Serves bytes via iter_content with no content-length.

    Matches Apple's package delivery path and is enough for flat files in this
    demo — DownloadManager never opens a real network socket.
    """

    type = "file"

    def __init__(self, name: str, payload: bytes, listed_size: int | None = None):
        self.name = name
        self._payload = payload
        self.size = listed_size if listed_size is not None else len(payload)
        self.date_modified = MTIME
        self.date_changed = None
        self.url = f"fixture://{name}"
        self.open_calls = 0

    def open(self, stream=True):
        self.open_calls += 1
        payload = self._payload
        url = self.url

        class _Response:
            def __init__(self):
                self.headers = {}
                self.url = url
                self.raw = io.BytesIO(payload)

            def iter_content(self, chunk_size=8192):
                bio = io.BytesIO(payload)
                while True:
                    chunk = bio.read(chunk_size)
                    if not chunk:
                        return
                    yield chunk

            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

        return _Response()


def build_keynote_archive() -> bytes:
    entries = {
        "Index.zip": b"index-bytes",
        "Metadata/Properties.plist": b"<plist/>",
        "Data/image-1.jpg": b"\xff\xd8\xff\xe0jpeg-data",
    }
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for name, data in entries.items():
            zf.writestr(f"Deck.key/{name}", data)
    return buf.getvalue()


def pause(seconds: float) -> None:
    time.sleep(seconds)


def banner(local: Path) -> None:
    print("=" * 70)
    print("iCloud Drive Downloader")
    print(f"Remote Path: {REMOTE}")
    print(f"Local Path: {DEST_LABEL}")
    print("Parallel Workers: 4")
    print("=" * 70)
    sys.stdout.flush()


def print_summary(summary: dict, heading: str = "Download Summary") -> None:
    print(f"\n{heading}:")
    print(f"- Total files: {summary['total_files']}")
    print(f"- Successfully downloaded: {summary['successful']}")
    print(f"- Skipped (unchanged): {summary['skipped']}")
    print(f"- Failed: {summary['failed']}")
    print(f"- Total data transferred: {human_bytes(summary['total_bytes_transferred'])}")
    print(f"- Changed chunks: {summary['total_changed_chunks']}")
    sys.stdout.flush()


class DemoLogHandler(logging.Handler):
    """Surface the events visitors care about; keep the rest quiet."""

    INTERESTING = {
        "package_expanded",
        "file_skipped_fast_path",
        "unknown_length_size_differs_from_listing",
        "download_failed",
    }

    def emit(self, record: logging.LogRecord) -> None:
        msg = record.getMessage()
        if not msg.startswith("{"):
            return
        try:
            payload = json.loads(msg)
        except json.JSONDecodeError:
            return
        event = payload.get("event")
        if event not in self.INTERESTING:
            return
        if event == "package_expanded":
            print(
                f"  → expanded {payload['file']} "
                f"({payload['entries']} entries → usable directory)"
            )
        elif event == "file_skipped_fast_path":
            print(f"  → skipped {payload['file']} (metadata fast path, 0 bytes)")
        elif event == "unknown_length_size_differs_from_listing":
            print(
                f"  · {payload['file']}: listing size {payload['listed_size']} B, "
                f"archive {payload['downloaded']} B — expanding package"
            )
        elif event == "download_failed":
            print(f"  ✗ {payload.get('file')}: {payload.get('error')}")
        sys.stdout.flush()


def attach_quiet_logger(mgr: DownloadManager) -> None:
    mgr.logger.handlers.clear()
    mgr.logger.addHandler(DemoLogHandler())
    mgr.logger.setLevel(logging.INFO)
    mgr.logger.propagate = False


def show_tree(root: Path) -> None:
    print("\nLocal tree after first run:")
    for path in sorted(root.rglob("*")):
        if path.name.startswith("."):
            continue
        rel = path.relative_to(root)
        if path.is_dir():
            print(f"  {rel}/")
        else:
            print(f"  {rel}  ({path.stat().st_size} B)")
    sys.stdout.flush()


def build_manager(dest: Path) -> DownloadManager:
    mgr = DownloadManager(email="you@example.com", max_retries=1, max_workers=4)
    mgr.root_path = dest
    mgr.sync_state = SyncState(dest)
    mgr.manifest = Manifest(dest)
    attach_quiet_logger(mgr)
    return mgr


def build_nodes() -> list[StreamNode]:
    key_bytes = build_keynote_archive()
    return [
        StreamNode("notes.txt", b"Meeting notes for the Q3 deck.\n"),
        StreamNode("photo.jpg", b"\xff\xd8\xff\xe0" + b"jpeg-bytes" * 200),
        StreamNode("Deck.key", key_bytes, listed_size=len(key_bytes) * 3),
    ]


def run_pass(mgr: DownloadManager, nodes: list[StreamNode], dest: Path) -> dict:
    for node in nodes:
        print(f"\n[{node.name}]")
        sys.stdout.flush()
        pause(0.22)
        mgr.download_drive_item(node, dest / node.name)
        pause(0.15)
    return mgr.generate_summary_report()["summary"]


def main() -> int:
    dest = Path(tempfile.mkdtemp(prefix="ifetch-demo-"))
    nodes = build_nodes()
    mgr = build_manager(dest)

    banner(dest)
    pause(0.28)
    print("Authenticating with iCloud...")
    pause(0.22)
    print("Authentication successful! (session already trusted)")
    pause(0.28)
    print(f"\nDownloading from '{REMOTE}' to '{DEST_LABEL}'")
    print("This may take some time depending on the size of the content...")
    pause(0.18)

    summary1 = run_pass(mgr, nodes, dest)
    show_tree(dest)
    print_summary(summary1)
    print(f"\nDetailed report saved to '{DEST_LABEL}/download_report.json'")
    print("\nOperation completed.")
    pause(0.45)

    print("\n" + "-" * 70)
    print("Re-run (nothing changed on iCloud) — metadata fast path")
    print("-" * 70)
    pause(0.22)
    banner(dest)
    pause(0.18)
    print("Authenticating with iCloud...")
    pause(0.18)
    print("Authentication successful! (session already trusted)")
    pause(0.18)
    print(f"\nDownloading from '{REMOTE}' to '{DEST_LABEL}'")
    pause(0.18)

    mgr.download_results.clear()
    summary2 = run_pass(mgr, nodes, dest)
    print_summary(summary2)
    print(f"\nDetailed report saved to '{DEST_LABEL}/download_report.json'")
    print("\nOperation completed.")
    pause(0.28)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
