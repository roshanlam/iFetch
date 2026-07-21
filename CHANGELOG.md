# Changelog

All notable changes to iFetch will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

> **Note:** This is the first structured changelog for iFetch. The entries
> below summarize the project's git history to date; earlier changes were not
> tracked in changelog form.

## [Unreleased]

### Added

- **Metadata fast path.** A sync-state file (`.ifetch_state.json`) is written in
  the destination root recording, per file, the remote size and remote modified
  timestamp plus the local size and mtime at completion. On later runs a file is
  skipped with **zero network round-trips** when all of those still agree and no
  `.temp`/`.download` artifact is present; any uncertainty falls back to the
  full network check. Previously every file cost a full HTTPS open just to read
  `content-length` (~39 ms/file, over an hour of pure round-trips on a 100k-file
  drive).
- `ifetch --no-fast-scan` to bypass the sync state and force a network check per
  file, and `ifetch --force` to re-download everything regardless of local state.
- **`ifetch-verify`** (`ifetch/verify.py`): read-only integrity checking of a
  local mirror against iCloud Drive. Levels `size` (default, fast), `checksum`
  (hashes local files) and `redownload` (streams remote files and hashes them in
  memory to prove byte-for-byte equality). Flags: `--email`, `--level`,
  `--max-workers`, `--report`, `--quiet`. Exit codes: 0 verified, 1 verification
  failure, 2 operational error. Statuses: `verified`, `size_mismatch`,
  `missing_local`, `extra_local`, `checksum_mismatch`, `checksum_unavailable`,
  `error`. Verification never modifies the user's files.
- Run reports and the CLI summary now include a `skipped` count; files proven
  unchanged are recorded with `status="skipped"` and `downloaded: 0` instead of
  being counted as downloads.
- `auth` extra (`pip install "ifetch[auth]"`) that pulls in `pyicloud[cli]`,
  providing the `icloud` command used to store your password in the keyring.
- Benchmark suite (`benchmarks/benchmark.py`) measuring cold download,
  unchanged re-run, and kill-and-resume with integrity verification, plus a
  chart generator (`benchmarks/visualize.py`).

### Changed

- `FileChunker.find_changed_chunks` renamed to
  `FileChunker.compute_download_ranges`, and documented for what it actually
  does: size-based change detection plus prefix resume.
- Documentation honesty pass on the sync engine. The README and docs previously
  advertised "chunk-level delta sync — changed files only re-download the byte
  ranges that differ". That was never implemented and has been removed
  everywhere. The documented behavior is now the real behavior: equal remote and
  local size means the file is skipped; a local file that is a shorter prefix of
  the remote resumes from that offset; **any other difference re-downloads the
  entire file**. The known blind spot — a modification that leaves the file size
  unchanged is not detected by size comparison — is now stated explicitly, along
  with a pointer to `ifetch-verify --level redownload`.
- README documents that `ifetch-verify --level checksum` does **not** verify
  against Apple: iCloud Drive item metadata exposes no content checksum
  (verified against pyicloud 2.6.5; `etag` is a mutation counter, not a digest),
  so those files report `checksum_unavailable` rather than `verified`.
- Added a Roadmap: genuine content-based chunk diffing is scoped for 1.1, and
  two-way sync is deliberately not implemented because safe bidirectional sync
  requires conflict-resolution and delete-propagation semantics that risk
  destroying the cloud copy if rushed.

### Fixed

- Removed the false claim in `docs/mirror.md` that hop 1 skips unchanged files
  "via stored checksums (`.ifetch_versions.json`)". That file is the version
  history; skip decisions are made from the sync state and file sizes.

- Auth instructions updated for pyicloud 2.x: the CLI is now
  `icloud auth login --username ...` (the 1.x `icloud --username ...` syntax
  no longer exists). Updated the README, docs, and iFetch's own
  "No stored password found" error message; added troubleshooting entries
  for the macOS `SSLCertVerificationError` and 2FA-request failures.

## [1.0.0] - 2026-07-20

First release on PyPI: `pip install ifetch`.

### Added

- Google Drive export CLI with indexed upload support.
- Listing and downloading of items shared with the user, with comprehensive
  shared-file download tests and hint detection fixes.
- Plugin system (`ifetch/plugin.py`): auto-discovered `BasePlugin` subclasses
  can hook download lifecycle events (`on_authenticated`, `on_list_contents`,
  `before_download`, `after_download`, `on_event`).
- JSON profile support for include/exclude sync patterns.
- Persistent file history, with metadata protected from mutation on failed
  downloads.
- Retry/backoff logic for transient connection errors, extended to cover 503
  and other server errors.
- Parallel downloads, incremental updates (skip on size match, resume from a
  partial prefix), and JSON logging. Note: contemporaneous docs described this
  as "chunk-based differential" downloading; it never diffed content — see the
  Unreleased "Changed" entry.
- Automated test suite (pytest + pytest-cov) with expanded coverage across
  downloader flows.
- Buy Me a Coffee funding option; MIT license.
- CI workflows (test matrix across Python 3.10–3.13 on Ubuntu/macOS, ruff
  lint), PyPI publish workflow, issue/PR templates, CONTRIBUTING guide, and
  this changelog.

### Changed

- Switched from the abandoned `picklepete/pyicloud` to the maintained
  `timlaing/pyicloud` fork (shared-drive/shareID support), now consumed from
  PyPI as `pyicloud>=2.5.0`.
- Refactored the original monolith into modules: logger, models, utils,
  chunker, tracker, downloader, cli.
- Improved download latency and hardened downloader flows.

### Fixed

- Shared-file downloads and hint detection.
- iCloud Drive path resolution for listed folders.
- Large-file iCloud resume and chunked retries.
- Thread-safety issues in versioning and error logging.
- "seek" error when comparing chunks on HTTP streams; dictionary-iteration
  bug fixes.
- Bug where everything was downloaded as a folder.
- Documentation fixes: download command and virtual environment activation
  instructions in ReadMe.md.
