#!/usr/bin/env bash
# Compare offline iFetch benchmark scenarios across two git refs.
#
# Usage (mode bits may not survive some commit paths — prefer bash):
#   bash benchmarks/compare.sh <base-ref> <head-ref> [--runs N] [--files N] [--file-bytes N]
#
# Creates clean worktrees for each ref under .bench-worktrees/, installs the
# package into a per-ref venv, runs benchmarks/offline_benchmark.py
# (fixture-based; no Apple ID), and prints a side-by-side median±spread table
# with percent change.
#
# The offline runner always comes from this clone, so uncommitted harness
# improvements apply to both sides; only the ifetch package code is swapped
# per ref. Official live benchmarks/benchmark.py is not invoked.
#
# Env:
#   PYTHON            - interpreter used to create per-ref venvs (default: python
#                       from PATH / active env, else python3)
#   IFETCH_BENCH_KEEP - set to 1 to leave worktrees under .bench-worktrees/
set -euo pipefail

# Locate repo root from this script's location, then confirm via git.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(git -C "$SCRIPT_DIR" rev-parse --show-toplevel)"
RUNNER="${ROOT}/benchmarks/offline_benchmark.py"
WT_ROOT="${ROOT}/.bench-worktrees"
RUNS=5
FILES=64
FILE_BYTES=$((512 * 1024))

# Prefer active-env `python`, then $PYTHON, then python3.
if [[ -n "${PYTHON:-}" ]]; then
  : # keep caller-provided PYTHON
elif command -v python >/dev/null 2>&1; then
  PYTHON="$(command -v python)"
else
  PYTHON="$(command -v python3)"
fi

usage() {
  cat <<'USAGE'
Compare offline iFetch benchmark scenarios across two git refs.

Usage:
  bash benchmarks/compare.sh <base-ref> <head-ref> [--runs N] [--files N] [--file-bytes N]

Works from any cwd. Creates clean worktrees under .bench-worktrees/, installs
the package into a per-ref venv, runs benchmarks/offline_benchmark.py
(fixture-based; no Apple ID), and prints a side-by-side median±spread table
with percent change.

The offline runner always comes from this clone, so harness improvements
apply to both sides; only the ifetch package code is swapped per ref.
Official live benchmarks/benchmark.py is not invoked.

Env:
  PYTHON            - interpreter for per-ref venvs (default: active python)
  IFETCH_BENCH_KEEP - set to 1 to leave worktrees for debugging
USAGE
  exit 2
}

[[ $# -ge 2 ]] || usage
BASE_REF="$1"
HEAD_REF="$2"
shift 2

while [[ $# -gt 0 ]]; do
  case "$1" in
    --runs) RUNS="$2"; shift 2 ;;
    --files) FILES="$2"; shift 2 ;;
    --file-bytes) FILE_BYTES="$2"; shift 2 ;;
    -h|--help) usage ;;
    *) echo "Unknown arg: $1" >&2; usage ;;
  esac
done

if [[ ! -f "$RUNNER" ]]; then
  echo "Missing offline runner: $RUNNER" >&2
  exit 1
fi

# Resolve refs up front so bad names fail before worktree setup.
BASE_SHA="$(git -C "$ROOT" rev-parse --verify "${BASE_REF}^{commit}")"
HEAD_SHA="$(git -C "$ROOT" rev-parse --verify "${HEAD_REF}^{commit}")"
BASE_SHORT="${BASE_SHA:0:7}"
HEAD_SHORT="${HEAD_SHA:0:7}"

stamp="$(date +%Y%m%d%H%M%S)"
BASE_DIR="${WT_ROOT}/base-${BASE_SHORT}-${stamp}"
HEAD_DIR="${WT_ROOT}/head-${HEAD_SHORT}-${stamp}"
mkdir -p "$WT_ROOT"

cleanup() {
  # Best-effort: leave worktrees if IFETCH_BENCH_KEEP=1 for debugging.
  if [[ "${IFETCH_BENCH_KEEP:-}" == "1" ]]; then
    echo "Keeping worktrees under $WT_ROOT (IFETCH_BENCH_KEEP=1)"
    return
  fi
  git -C "$ROOT" worktree remove --force "$BASE_DIR" 2>/dev/null || true
  git -C "$ROOT" worktree remove --force "$HEAD_DIR" 2>/dev/null || true
  rm -rf "$BASE_DIR" "$HEAD_DIR"
}
trap cleanup EXIT

echo "==> Worktree base  ${BASE_REF} @ ${BASE_SHORT}"
git -C "$ROOT" worktree add --detach "$BASE_DIR" "$BASE_SHA"
echo "==> Worktree head  ${HEAD_REF} @ ${HEAD_SHORT}"
git -C "$ROOT" worktree add --detach "$HEAD_DIR" "$HEAD_SHA"

setup_venv() {
  local dir="$1"
  "$PYTHON" -m venv "${dir}/.venv"
  # shellcheck disable=SC1091
  source "${dir}/.venv/bin/activate"
  pip -q install -U pip
  pip -q install -e "${dir}"
  deactivate
}

echo "==> Installing base package (python=$PYTHON)"
setup_venv "$BASE_DIR"
echo "==> Installing head package"
setup_venv "$HEAD_DIR"

BASE_JSON="${WT_ROOT}/base-${BASE_SHORT}.json"
HEAD_JSON="${WT_ROOT}/head-${HEAD_SHORT}.json"

run_bench() {
  local dir="$1" out="$2" label="$3"
  echo "==> Running offline scenarios on ${label}"
  "${dir}/.venv/bin/python" "$RUNNER" \
    --runs "$RUNS" \
    --files "$FILES" \
    --file-bytes "$FILE_BYTES" \
    --json "$out"
}

run_bench "$BASE_DIR" "$BASE_JSON" "base ${BASE_SHORT}"
run_bench "$HEAD_DIR" "$HEAD_JSON" "head ${HEAD_SHORT}"

# Side-by-side table via a tiny Python aggregator (stdlib only).
"${BASE_DIR}/.venv/bin/python" - "$BASE_JSON" "$HEAD_JSON" "$BASE_SHORT" "$HEAD_SHORT" <<'PY'
import json, sys

base_path, head_path, base_short, head_short = sys.argv[1:5]
base = json.loads(open(base_path).read())
head = json.loads(open(head_path).read())

# (scenario, metric, lower_is_better)
METRICS = [
    ("cold", "seconds", True),
    ("cold", "MiB_per_s", False),
    ("warm", "seconds", True),
    ("warm", "bytes_transferred", True),
    ("warm", "open_calls", True),
    ("resume", "seconds", True),
    ("resume", "MiB_per_s", False),
    ("resume", "mismatches", True),
]

def fmt(metric, v):
    if metric in ("seconds",):
        if v < 0.001:
            return f"{v*1e6:.0f}µs"
        if v < 1:
            return f"{v*1000:.2f}ms"
        return f"{v:.4f}s"
    if metric in ("MiB_per_s",):
        return f"{v:.2f}"
    return f"{v:.0f}"

def pct(old, new):
    if old == 0:
        return "n/a" if new == 0 else "inf"
    return f"{((new - old) / old) * 100:+.1f}%"

print()
print(f"## Compare  base={base_short}  vs  head={head_short}")
fx = base["fixture"]
print(f"Fixture: {fx['files']} files × {fx['file_bytes']} B "
      f"({fx['total_bytes']/1024/1024:.1f} MiB), runs={base['runs']}")
print()
print("| Scenario | Metric | Base median ± spread | Head median ± spread | Δ% (median) |")
print("|---|---|---|---|---|")
for scenario, metric, _ in METRICS:
    b = base[scenario][metric]
    h = head[scenario][metric]
    b_cell = f"{fmt(metric, b['median'])} ± {fmt(metric, b['spread'])}"
    h_cell = f"{fmt(metric, h['median'])} ± {fmt(metric, h['spread'])}"
    print(f"| {scenario} | {metric} | {b_cell} | {h_cell} | {pct(b['median'], h['median'])} |")

print()
print("Spread is (max-min)/2 across runs. Negative Δ% on seconds / bytes / opens is an improvement;")
print("positive Δ% on MiB_per_s is an improvement.")
print(f"Raw JSON: {base_path}")
print(f"          {head_path}")
PY
