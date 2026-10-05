# Benchmarks

## Live iCloud (`benchmark.py`)

`benchmark.py` measures cold / warm / resume against a real iCloud Drive folder.
It needs a signed-in Apple ID and network access, so reviewers generally cannot
reproduce those numbers locally.

## Offline harness (`offline_benchmark.py`)

Fixture-based companion that exercises the same three scenarios against
in-process `FakeNode` payloads — no Apple ID, no network.

### What it measures

- **Cold**: first download of a synthetic tree into an empty directory
  (code-path / CPU overhead of the download pipeline).
- **Warm**: immediate re-run over the populated tree (metadata fast-path skip;
  expects 0 bytes transferred and 0 remote `open` calls).
- **Resume**: half-written files with a proven journal prefix, then completed
  (resume correctness + time to finish the remainder).

### What it does **not** measure

- Network MiB/s or Apple CDN throughput
- Apple rate-limit / backoff behavior
- Auth / session reuse timing
- Live mid-download kill-and-restart resume

Those remain live-only follow-ups via `benchmark.py`.

### Noise guidance

At the default fixture (64 × 512 KiB = 32 MiB, 5 runs):

- **Warm** and **resume** medians are usually stable enough for PR gates.
- **Cold** differences under ~20% are noise at this fixture size. Enlarge with
  `--files` / `--file-bytes`, or bump `--runs`, before treating a cold delta
  as a real regression.

### Usage

```bash
# Single-ref offline baseline
python benchmarks/offline_benchmark.py --runs 5 --files 64 --file-bytes 524288
python benchmarks/offline_benchmark.py --json /tmp/out.json   # machine-readable

# Before/after across two git refs (worktrees under .bench-worktrees/)
bash benchmarks/compare.sh main HEAD --runs 5
bash benchmarks/compare.sh main HEAD --runs 3 --files 128 --file-bytes 1048576

# Keep worktrees for debugging
IFETCH_BENCH_KEEP=1 bash benchmarks/compare.sh main HEAD --runs 1
```

`compare.sh` resolves the repo root via `git rev-parse`, so it works from any
cwd. Prefer `bash benchmarks/compare.sh …` so executable mode bits do not
matter. Override the interpreter used for per-ref venvs with `PYTHON=…`.
