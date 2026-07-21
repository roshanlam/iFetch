#!/usr/bin/env python3
"""Reproducible performance benchmark for iFetch.

Measures, against a real iCloud Drive folder you own:

  1. cold    - full download into an empty directory (optionally at several
               --max-workers settings), wall time and effective throughput
  2. warm    - immediate re-run over the same directory (delta sync); wall
               time and how little was re-fetched
  3. resume  - a cold download killed partway through, then restarted;
               verifies the final tree matches the reference run

Usage:
    python benchmarks/benchmark.py Documents/SomeFolder --email you@example.com \
        [--workers 4 8] [--kill-after 20] [--ifetch /path/to/ifetch]

Requires an authenticated session (run any `ifetch ... --list` once,
interactively, so the keyring password and trusted session exist).

Results are printed as a Markdown table and written to benchmark_results.json.
Network speed varies run to run — treat single runs as indicative, and rerun
at a quiet hour for publishable numbers.
"""

import argparse
import json
import shutil
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path

REPORT_NAME = "download_report.json"


def tree_stats(root: Path):
    """(file_count, total_bytes, {relpath: size}) for all files under root."""
    files = {}
    for p in root.rglob("*"):
        if p.is_file() and p.name != REPORT_NAME and ".versions" not in p.parts:
            files[str(p.relative_to(root))] = p.stat().st_size
    return len(files), sum(files.values()), files


def run_ifetch(ifetch, icloud_path, dest, email, workers, timeout=None):
    cmd = [ifetch, icloud_path, str(dest), "--email", email,
           "--max-workers", str(workers)]
    start = time.monotonic()
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    elapsed = time.monotonic() - start
    if proc.returncode != 0:
        sys.exit(f"ifetch failed (exit {proc.returncode}):\n{proc.stderr[-2000:]}")
    return elapsed


def fmt_bytes(n):
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if n < 1024 or unit == "TiB":
            return f"{n:.1f} {unit}" if unit != "B" else f"{n} B"
        n /= 1024


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("icloud_path", help="iCloud Drive folder to benchmark against")
    ap.add_argument("--email", required=True)
    ap.add_argument("--workers", nargs="+", type=int, default=[4],
                    help="worker counts to benchmark cold pulls at (default: 4)")
    ap.add_argument("--kill-after", type=float, default=20.0,
                    help="seconds before interrupting the resume-test download")
    ap.add_argument("--ifetch", default="ifetch", help="ifetch executable to use")
    ap.add_argument("--keep", action="store_true",
                    help="keep the temporary download directories")
    args = ap.parse_args()

    base = Path(tempfile.mkdtemp(prefix="ifetch-bench-"))
    results = {"icloud_path": args.icloud_path, "cold": [], "warm": None,
               "resume": None}
    print(f"Benchmark workspace: {base}\n")

    try:
        # --- cold pulls at each worker count -------------------------------
        reference = None
        for w in args.workers:
            dest = base / f"cold-w{w}"
            dest.mkdir()
            print(f"[cold] full download, --max-workers {w} ...")
            elapsed = run_ifetch(args.ifetch, args.icloud_path, dest,
                                 args.email, w)
            count, total, files = tree_stats(dest)
            mbps = (total / 1024 / 1024) / elapsed if elapsed else 0.0
            results["cold"].append({"workers": w, "seconds": round(elapsed, 1),
                                    "files": count, "bytes": total,
                                    "MiB_per_s": round(mbps, 2)})
            print(f"       {count} files, {fmt_bytes(total)} in {elapsed:.1f}s "
                  f"({mbps:.2f} MiB/s)")
            if reference is None:
                reference = (dest, files)

        # --- warm re-run over the first cold directory ---------------------
        ref_dest, ref_files = reference
        w = args.workers[0]
        print(f"[warm] re-run over already-synced copy, --max-workers {w} ...")
        elapsed = run_ifetch(args.ifetch, args.icloud_path, ref_dest,
                             args.email, w)
        cold_secs = results["cold"][0]["seconds"]
        speedup = cold_secs / elapsed if elapsed else float("inf")
        results["warm"] = {"seconds": round(elapsed, 1),
                           "speedup_vs_cold": round(speedup, 1)}
        print(f"       finished in {elapsed:.1f}s "
              f"({speedup:.1f}x faster than the cold pull)")

        # --- resume: kill a cold pull partway, restart, verify -------------
        dest = base / "resume"
        dest.mkdir()
        print(f"[resume] starting download, killing after {args.kill_after}s ...")
        cmd = [args.ifetch, args.icloud_path, str(dest), "--email", args.email,
               "--max-workers", str(w)]
        proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL,
                                stderr=subprocess.DEVNULL)
        time.sleep(args.kill_after)
        interrupted = proc.poll() is None
        if interrupted:
            proc.send_signal(signal.SIGINT)
            proc.wait(timeout=60)
            _, partial_bytes, _ = tree_stats(dest)
            print(f"       interrupted with {fmt_bytes(partial_bytes)} on disk; "
                  f"restarting ...")
            elapsed = run_ifetch(args.ifetch, args.icloud_path, dest,
                                 args.email, w)
        else:
            partial_bytes = None
            elapsed = 0.0
            print("       download finished before the kill window — "
                  "use a bigger folder or smaller --kill-after for a real "
                  "resume test")
        count, total, files = tree_stats(dest)
        complete = files == ref_files
        results["resume"] = {"interrupted": interrupted,
                             "partial_bytes": partial_bytes,
                             "restart_seconds": round(elapsed, 1),
                             "matches_reference": complete}
        print(f"       final tree matches reference: {complete}")

        # --- report --------------------------------------------------------
        total = results["cold"][0]["bytes"]
        count = results["cold"][0]["files"]
        print("\n## Results — copy-paste ready\n")
        print(f"Dataset: `{args.icloud_path}` — {count} files, "
              f"{fmt_bytes(total)}\n")
        print("| Scenario | Time | Throughput |")
        print("|---|---|---|")
        for c in results["cold"]:
            print(f"| Full download ({c['workers']} workers) | "
                  f"{c['seconds']}s | {c['MiB_per_s']} MiB/s |")
        warm = results["warm"]
        print(f"| Re-run, nothing changed (delta sync) | {warm['seconds']}s | "
              f"{warm['speedup_vs_cold']}x faster |")
        r = results["resume"]
        if r["interrupted"]:
            print(f"| Killed mid-download, restarted | "
                  f"+{r['restart_seconds']}s to finish | resumed, tree "
                  f"verified {'identical' if r['matches_reference'] else 'MISMATCH'} |")

        out = Path(__file__).with_name("benchmark_results.json")
        out.write_text(json.dumps(results, indent=2))
        print(f"\nRaw numbers: {out}")
    finally:
        if args.keep:
            print(f"Downloads kept in {base}")
        else:
            shutil.rmtree(base, ignore_errors=True)


if __name__ == "__main__":
    main()
