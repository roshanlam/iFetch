#!/usr/bin/env python3
"""Render benchmark_results.json as a share-ready PNG (1200x1200).

Usage:
    python benchmarks/visualize.py [results.json] [--out chart.png] [--sample]

--sample renders built-in placeholder numbers with a SAMPLE watermark, for
previewing the layout before real results exist. Never post the sample.
"""

import argparse
import json
import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

# Palette: validated single-hue + ink roles (light mode; LinkedIn feed is light)
SURFACE = "#fcfcfb"
INK = "#0b0b0b"
INK_2 = "#52514e"
MUTED = "#898781"
GRID = "#e1e0d9"
BASELINE = "#c3c2b7"
BLUE = "#2a78d6"
GOOD = "#006300"  # success text (light-mode delta-good)

SAMPLE = {
    "icloud_path": "Documents/SampleFolder",
    "cold": [
        {"workers": 4, "seconds": 186.4, "files": 412,
         "bytes": 2_469_606_195, "MiB_per_s": 12.6},
        {"workers": 8, "seconds": 112.9, "files": 412,
         "bytes": 2_469_606_195, "MiB_per_s": 20.9},
    ],
    "warm": {"seconds": 3.9, "speedup_vs_cold": 47.8,
             "bytes_transferred": 0, "changed_chunks": 0},
    "resume": {"interrupted": True, "partial_bytes": 388_002_611,
               "restart_seconds": 151.2, "matches_reference": True,
               "verification": "sha256"},
}


def fmt_gib(n):
    return f"{n / 1024**3:.1f} GiB"


def fmt_secs(s):
    return f"{s:.0f}s" if s >= 10 else f"{s:.1f}s"


def render(results, out_path, is_sample):
    cold = results["cold"]
    warm = results["warm"]
    resume = results.get("resume") or {}
    ref = cold[0]

    rows = [(f"Full download · {c['workers']} workers", c["seconds"],
             f"{fmt_secs(c['seconds'])}  ·  {c['MiB_per_s']} MiB/s")
            for c in cold]
    warm_note = fmt_secs(warm["seconds"])
    if warm.get("bytes_transferred") == 0:
        warm_note += "  ·  0 bytes transferred"
    rows.append(("Re-run, nothing changed", warm["seconds"], warm_note))

    fig = plt.figure(figsize=(12, 12), dpi=100)
    fig.patch.set_facecolor(SURFACE)
    sans = ["system-ui", "-apple-system", "Helvetica Neue", "Arial",
            "sans-serif"]
    plt.rcParams["font.family"] = "sans-serif"
    plt.rcParams["font.sans-serif"] = sans

    # --- header ------------------------------------------------------------
    fig.text(0.07, 0.945, "iFetch — real numbers, one command to reproduce",
             fontsize=21, color=INK, fontweight="bold")
    fig.text(0.07, 0.912,
             f"Dataset: {results['icloud_path']}  ·  {ref['files']} files  ·  "
             f"{fmt_gib(ref['bytes'])} from iCloud Drive",
             fontsize=14, color=INK_2)

    # --- hero stat ---------------------------------------------------------
    if warm.get("bytes_transferred") == 0:
        hero, hero_label = "0 bytes", "re-downloaded when nothing changed"
        hero_size = 64
    else:
        hero = f"{warm['speedup_vs_cold']:.0f}×"
        hero_label = "faster when nothing changed"
        hero_size = 88
    fig.text(0.07, 0.80, hero, fontsize=hero_size, color=BLUE,
             fontweight="bold")
    fig.text(0.07, 0.756, hero_label, fontsize=19, color=INK,
             fontweight="bold")
    fig.text(0.07, 0.712,
             f"a re-run over the same {fmt_gib(ref['bytes'])} checks every "
             f"file in {fmt_secs(warm['seconds'])} and downloads\n"
             "only what changed — here, nothing had",
             fontsize=13.5, color=INK_2, linespacing=1.5)

    # --- bar chart ---------------------------------------------------------
    ax = fig.add_axes([0.07, 0.36, 0.86, 0.33])
    ax.set_facecolor(SURFACE)
    labels = [r[0] for r in rows]
    values = [r[1] for r in rows]
    y = list(range(len(rows)))[::-1]

    ax.set_axisbelow(True)
    ax.xaxis.grid(True, color=GRID, linewidth=1)
    for spine in ax.spines.values():
        spine.set_visible(False)
    ax.spines["left"].set_visible(True)
    ax.spines["left"].set_color(BASELINE)

    max_v = max(values)
    for yi, (label, val, annot) in zip(y, rows):
        width = max(val, max_v * 0.004)  # keep tiny bars visible
        ax.barh(yi, width, height=0.34, color=BLUE, edgecolor="none",
                zorder=3)
        ax.text(-max_v * 0.015, yi, label, ha="right", va="center",
                fontsize=13.5, color=INK)
        ax.text(width + max_v * 0.015, yi, annot, ha="left", va="center",
                fontsize=13, color=INK_2)

    ax.set_xlim(0, max_v * 1.32)
    ax.set_ylim(-0.6, len(rows) - 0.4)
    ax.set_yticks([])
    ax.tick_params(axis="x", colors=MUTED, labelsize=11, length=0)
    ax.set_xlabel("wall time — seconds (shorter is better)", fontsize=11.5,
                  color=MUTED, loc="left")

    # --- resume verification line ------------------------------------------
    if resume.get("interrupted"):
        verified_how = ("SHA-256-identical to an uninterrupted download"
                        if resume.get("verification") == "sha256"
                        else "verified identical")
        verdict = (f"resumed and finished — {verified_how}"
                   if resume.get("matches_reference")
                   else "resumed — VERIFICATION FAILED")
        color = GOOD if resume.get("matches_reference") else "#d03b3b"
        fig.text(0.07, 0.287, r"$\checkmark$", fontsize=20, color=color,
                 fontweight="bold")
        fig.text(0.10, 0.287,
                 f"Killed mid-download, restarted: {verdict}",
                 fontsize=14.5, color=INK)

    # --- footer ------------------------------------------------------------
    fig.text(0.07, 0.225,
             "Reproduce it yourself:", fontsize=12.5, color=INK_2,
             fontweight="bold")
    fig.text(0.07, 0.196,
             "pip install ifetch   ·   python benchmarks/benchmark.py",
             fontsize=13.5, color=INK, family="monospace")
    fig.text(0.07, 0.16, "github.com/roshanlam/iFetch  ·  MIT licensed",
             fontsize=12.5, color=MUTED)

    if is_sample:
        fig.text(0.5, 0.5, "SAMPLE DATA — DO NOT POST", fontsize=52,
                 color="#d03b3b", alpha=0.28, ha="center", va="center",
                 rotation=30, fontweight="bold")

    fig.savefig(out_path, facecolor=SURFACE, bbox_inches="tight",
                pad_inches=0.35)
    print(f"wrote {out_path}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("results", nargs="?", default="benchmarks/benchmark_results.json")
    ap.add_argument("--out", default="benchmarks/benchmark_chart.png")
    ap.add_argument("--sample", action="store_true")
    args = ap.parse_args()

    if args.sample:
        results = SAMPLE
    else:
        path = Path(args.results)
        if not path.exists():
            sys.exit(f"{path} not found — run benchmarks/benchmark.py first, "
                     f"or preview the layout with --sample")
        results = json.loads(path.read_text())
    render(results, args.out, args.sample)


if __name__ == "__main__":
    main()
