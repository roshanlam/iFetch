#!/usr/bin/env python3
"""Offline fixture benchmark for iFetch (no Apple ID / network).

The repo's benchmarks/benchmark.py measures cold / warm / resume against a live
iCloud folder and requires an authenticated session. This companion harness
exercises the same three scenarios against in-process FakeNode fixtures so
reviewers can get before/after numbers without signing in.

Scenarios
---------
  cold   - first download of a synthetic tree into an empty directory
  warm   - immediate re-run (metadata fast path; expects 0 bytes transferred)
  resume - half-written files with a proven journal prefix, then completed

Usage:
    python benchmarks/offline_benchmark.py [--runs 5] [--files 64] [--file-bytes 524288]
                                           [--json out.json] [--quiet]

Emits a human-readable table (median ± spread) and optional JSON.
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import os
import shutil
import statistics
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

# Make sure a worktree install is preferred when compare.sh points PYTHONPATH,
# but fall back to the ambient package.
os.environ.setdefault("NO_COLOR", "1")
os.environ.setdefault("ICLOUD_EMAIL", "bench@example.com")

from ifetch.downloader import DownloadManager, SyncState  # noqa: E402
from ifetch.index import IndexStore  # noqa: E402
from ifetch.manifest import Manifest  # noqa: E402
from ifetch.transfers import TransferJournal  # noqa: E402

MTIME = datetime(2026, 3, 14, 15, 9, 26, tzinfo=timezone.utc)


class FakeNode:
    """Minimal pyicloud DriveNode stand-in with ranged payload access."""

    type = "file"

    def __init__(self, name: str, content: bytes, date_modified=MTIME):
        self.name = name
        self._content = content
        self.size = len(content)
        self.date_modified = date_modified
        self.date_changed = None
        self.url = f"https://example.invalid/{name}"
        self.open_calls = 0

    def open(self, stream=True):
        self.open_calls += 1
        outer = self

        class _Ctx:
            def __init__(self):
                self.headers = {"content-length": str(len(outer._content))}
                self.url = outer.url

            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

        return _Ctx()


def _build_nodes(n_files: int, file_bytes: int) -> list[FakeNode]:
    # Deterministic but non-trivial payloads (avoid sparse-file shortcuts).
    base = bytes((i * 17 + 31) % 256 for i in range(4096))
    reps = math.ceil(file_bytes / len(base))
    blob = (base * reps)[:file_bytes]
    return [FakeNode(f"f{i:04d}.bin", blob) for i in range(n_files)]


def _manager(dest: Path) -> DownloadManager:
    dm = DownloadManager(email="bench@example.com", max_retries=1, max_workers=4)
    dm.root_path = dest
    dm.sync_state = SyncState(dest)
    dm.manifest = Manifest(dest)
    # Ranged fetches never hit the network — serve from the FakeNode payload.
    dm.download_chunk = lambda url, start, end, item=None: item._content[start : end + 1]
    dm.calculate_checksum = lambda p: "dummy"
    # Quiet on a private logger so we never mutate the shared
    # ``icloud_downloader`` logger used by the rest of the test suite.
    quiet = logging.getLogger(f"ifetch.offline_bench.{id(dm)}")
    quiet.handlers.clear()
    quiet.addHandler(logging.NullHandler())
    quiet.setLevel(logging.CRITICAL)
    quiet.propagate = False
    dm.logger = quiet
    return dm


def _summary(dm: DownloadManager) -> dict:
    return dm.generate_summary_report()["summary"]


def run_cold(nodes: list[FakeNode], dest: Path) -> dict:
    if dest.exists():
        shutil.rmtree(dest)
    dest.mkdir(parents=True)
    dm = _manager(dest)
    t0 = time.perf_counter()
    for node in nodes:
        ok = dm.download_drive_item(node, dest / node.name)
        if not ok:
            raise RuntimeError(f"cold download failed for {node.name}")
    dm.sync_state.save()
    elapsed = time.perf_counter() - t0
    s = _summary(dm)
    total = s["total_bytes_transferred"]
    return {
        "seconds": elapsed,
        "bytes_transferred": total,
        "files": s["total_files"],
        "successful": s["successful"],
        "skipped": s["skipped"],
        "MiB_per_s": (total / 1024 / 1024) / elapsed if elapsed else 0.0,
    }


def run_warm(nodes: list[FakeNode], dest: Path) -> dict:
    """Re-run over an already-populated tree (expects fast-path skips)."""
    dm = _manager(dest)
    # Reload sync state written by cold.
    dm.sync_state = SyncState(dest)
    t0 = time.perf_counter()
    for node in nodes:
        ok = dm.download_drive_item(node, dest / node.name)
        if not ok:
            raise RuntimeError(f"warm download failed for {node.name}")
    elapsed = time.perf_counter() - t0
    s = _summary(dm)
    return {
        "seconds": elapsed,
        "bytes_transferred": s["total_bytes_transferred"],
        "files": s["total_files"],
        "successful": s["successful"],
        "skipped": s["skipped"],
        "open_calls": sum(n.open_calls for n in nodes),
    }


def run_resume(nodes: list[FakeNode], dest: Path) -> dict:
    """Seed half of each file with a proven journal row, then finish."""
    if dest.exists():
        shutil.rmtree(dest)
    dest.mkdir(parents=True)
    # Proven partials: write prefix + journal begin(resume_from=half).
    with IndexStore(dest) as store:
        journal = TransferJournal(store, dest)
        for node in nodes:
            half = len(node._content) // 2
            path = dest / node.name
            path.write_bytes(node._content[:half])
            journal.begin(path, total_bytes=len(node._content), resume_from=half)

    dm = _manager(dest)
    with IndexStore(dest) as store:
        dm.journal = TransferJournal(store, dest)
        t0 = time.perf_counter()
        for node in nodes:
            ok = dm.download_drive_item(node, dest / node.name)
            if not ok:
                raise RuntimeError(f"resume download failed for {node.name}")
        elapsed = time.perf_counter() - t0

    # Verify final bytes match fixtures.
    mismatches = 0
    for node in nodes:
        if (dest / node.name).read_bytes() != node._content:
            mismatches += 1
    s = _summary(dm)
    return {
        "seconds": elapsed,
        "bytes_transferred": s["total_bytes_transferred"],
        "files": s["total_files"],
        "successful": s["successful"],
        "skipped": s["skipped"],
        "mismatches": mismatches,
        "MiB_per_s": (s["total_bytes_transferred"] / 1024 / 1024) / elapsed if elapsed else 0.0,
    }


def _median(xs: list[float]) -> float:
    return float(statistics.median(xs))


def _spread(xs: list[float]) -> float:
    """Half-range (max-min)/2 — simple, reviewer-friendly noise measure."""
    if not xs:
        return 0.0
    return (max(xs) - min(xs)) / 2.0


def _aggregate(runs: list[dict], keys: list[str]) -> dict:
    out = {"n": len(runs)}
    for key in keys:
        vals = [float(r[key]) for r in runs]
        out[key] = {
            "median": _median(vals),
            "spread": _spread(vals),
            "min": min(vals),
            "max": max(vals),
            "runs": vals,
        }
    return out


def fmt_secs(s: float) -> str:
    if s < 0.001:
        return f"{s * 1e6:.0f}µs"
    if s < 1:
        return f"{s * 1000:.1f}ms"
    return f"{s:.3f}s"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--runs", type=int, default=5, help="repetitions per scenario (default 5)")
    ap.add_argument("--files", type=int, default=64, help="fixture file count (default 64)")
    ap.add_argument("--file-bytes", type=int, default=512 * 1024,
                    help="bytes per fixture file (default 512 KiB)")
    ap.add_argument("--json", type=Path, help="write machine-readable results here")
    ap.add_argument("--quiet", action="store_true")
    args = ap.parse_args()

    if args.runs < 1:
        sys.exit("--runs must be >= 1")

    nodes = _build_nodes(args.files, args.file_bytes)
    total_bytes = args.files * args.file_bytes
    if not args.quiet:
        print(f"Offline fixture: {args.files} files × {args.file_bytes} B "
              f"= {total_bytes / 1024 / 1024:.1f} MiB; runs={args.runs}")
        print("Note: official benchmarks/benchmark.py (live iCloud) is NOT run here.\n")

    cold_runs, warm_runs, resume_runs = [], [], []
    workspace = Path(tempfile.mkdtemp(prefix="ifetch-offline-bench-"))
    try:
        for i in range(args.runs):
            cold_dir = workspace / f"cold-{i}"
            resume_dir = workspace / f"resume-{i}"
            # Reset open_calls between runs.
            for n in nodes:
                n.open_calls = 0
            cold = run_cold(nodes, cold_dir)
            opens_after_cold = sum(n.open_calls for n in nodes)
            warm = run_warm(nodes, cold_dir)
            # warm.open_calls currently includes cold; report delta.
            warm["open_calls"] = sum(n.open_calls for n in nodes) - opens_after_cold
            resume = run_resume(nodes, resume_dir)
            cold_runs.append(cold)
            warm_runs.append(warm)
            resume_runs.append(resume)
            if not args.quiet:
                print(f"  run {i + 1}/{args.runs}: "
                      f"cold={fmt_secs(cold['seconds'])} "
                      f"({cold['MiB_per_s']:.1f} MiB/s), "
                      f"warm={fmt_secs(warm['seconds'])} "
                      f"({warm['bytes_transferred']} B, "
                      f"{warm['skipped']} skipped, "
                      f"{warm['open_calls']} opens), "
                      f"resume={fmt_secs(resume['seconds'])} "
                      f"({resume['MiB_per_s']:.1f} MiB/s, "
                      f"mismatches={resume['mismatches']})")
    finally:
        shutil.rmtree(workspace, ignore_errors=True)

    results = {
        "mode": "offline-fixtures",
        "tip_note": "no Apple ID; FakeNode ranged payloads",
        "fixture": {
            "files": args.files,
            "file_bytes": args.file_bytes,
            "total_bytes": total_bytes,
        },
        "runs": args.runs,
        "cold": _aggregate(cold_runs, ["seconds", "MiB_per_s", "bytes_transferred"]),
        "warm": _aggregate(warm_runs, ["seconds", "bytes_transferred", "skipped", "open_calls"]),
        "resume": _aggregate(resume_runs, ["seconds", "MiB_per_s", "bytes_transferred", "mismatches"]),
        "raw": {"cold": cold_runs, "warm": warm_runs, "resume": resume_runs},
    }

    # Human table
    print("\n## Offline baseline (median ± spread)\n")
    print(f"Dataset: fixture {args.files} files, {total_bytes / 1024 / 1024:.1f} MiB "
          f"(no network)\n")
    print("| Scenario | Metric | Median | Spread (±) |")
    print("|---|---|---|---|")

    def row(scenario, metric, block, fmt=lambda x: f"{x:.4f}"):
        m = block[metric]
        print(f"| {scenario} | {metric} | {fmt(m['median'])} | {fmt(m['spread'])} |")

    row("cold (full download)", "seconds", results["cold"], fmt_secs)
    row("cold (full download)", "MiB_per_s", results["cold"], lambda x: f"{x:.2f}")
    row("warm (fast-path re-run)", "seconds", results["warm"], fmt_secs)
    row("warm (fast-path re-run)", "bytes_transferred", results["warm"], lambda x: f"{x:.0f}")
    row("warm (fast-path re-run)", "open_calls", results["warm"], lambda x: f"{x:.0f}")
    row("resume (proven prefix)", "seconds", results["resume"], fmt_secs)
    row("resume (proven prefix)", "MiB_per_s", results["resume"], lambda x: f"{x:.2f}")
    row("resume (proven prefix)", "mismatches", results["resume"], lambda x: f"{x:.0f}")

    if args.json:
        args.json.write_text(json.dumps(results, indent=2))
        print(f"\nJSON: {args.json}")

    # Soft sanity checks — do not fail the harness on timing, only on correctness.
    if any(r["mismatches"] for r in resume_runs):
        print("ERROR: resume produced content mismatches", file=sys.stderr)
        return 2
    if any(r["bytes_transferred"] != 0 for r in warm_runs):
        print("ERROR: warm re-run transferred bytes (fast path broken?)", file=sys.stderr)
        return 3
    if any(r["open_calls"] != 0 for r in warm_runs):
        print("ERROR: warm re-run opened remote streams (fast path broken?)", file=sys.stderr)
        return 4
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
