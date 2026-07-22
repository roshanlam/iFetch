# Changelog

All notable changes to iFetch will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

> **Note:** This is the first structured changelog for iFetch. The entries
> below summarize the project's git history to date; earlier changes were not
> tracked in changelog form.

## [Unreleased]

### Added

- **`ifetch snapshot`** (`create`, `list`, `diff`, `restore`, `delete`) - dated
  states of a mirror. A snapshot is metadata, not a copy, so taking one is
  instant and costs kilobytes no matter how large the mirror is; that is what
  makes taking one before every risky sync practical. `diff` compares two
  points in time **by digest**, so a file whose size did not change is still
  reported. `restore` is a dry run by default and prints where each file's
  bytes would come from: `.versions` (the exact recorded bytes, preferred),
  iCloud (the current remote copy, which may differ from the snapshot), or
  nowhere - and files in the last category are **named rather than omitted**,
  because quietly dropping them would imply a complete restore is possible.
  Applying a restore archives what it replaces, so the restore is itself
  reversible.
- **`ifetch recover placeholders`** - find local files whose contents are not
  actually on this disk. A file evicted to iCloud still appears in Finder with
  a size, but holds nothing; copying it to a backup drive copies nothing, and
  every byte total and checksum over such a tree is wrong. Two signals are used
  and each is reported with its evidence: a `.name.ext.icloud` stub (certain,
  and detectable on any OS because the stub is a real file - which matters when
  reading a drive pulled out of a Mac), and zero-block-but-nonzero-size
  (likely, macOS/APFS only). **Signals that cannot be evaluated on the current
  platform are named explicitly rather than silently skipped**, so the report
  never says "none found" when it means "I could not look".
- **`ifetch recover missing`** - what iCloud has that this disk does not really
  have, separated into three kinds because they call for different actions:
  never downloaded, disappeared (recorded by an earlier run and now gone -
  local data loss), and placeholder-only. A file absent from disk *and* from
  the latest iCloud scan is called out with a pointer to `.versions`.
- **`ifetch recover inventory`** - where the space went, aggregated by folder,
  by file type and by largest item, with optional Apple quota figures. CSV
  export throughout, because "which folders should I stop paying for?" is a
  sorting question.
- **`ifetch plan`** - a dry run that reports exactly what a download would do
  before it does any of it: how many files would be fetched and overwritten,
  the byte total, whether the destination has enough free space (with
  headroom), what is being skipped and why, which local files are not in iCloud
  (never deleted, only reported), and an estimated duration. Transfers nothing.
  The time estimate is produced **only** from throughput measured on previous
  runs or supplied via `--throughput`; with neither, it is reported as unknown
  rather than guessed.
- **`ifetch audit`** - the same reconciliation presented as "what exists
  remotely versus locally", exiting non-zero when the two sides disagree so it
  is usable from a monitoring job.
- **Metadata-only scanning** (`ifetch/scanner.py`). Traversal and transfer used
  to be a single pass, which made it impossible to say anything about a job
  before running it. `RemoteScanner` walks a drive tree reading only the
  metadata Apple already returns in folder listings - one request per directory,
  no file content - and `LocalScanner` indexes the destination, reusing recorded
  digests where size and mtime still agree so a rescan does not re-hash a
  terabyte. A folder that cannot be listed is recorded as an error and the scan
  continues, rather than costing the inventory of everything else.
- **Persistent SQLite index** (`.ifetch_index.db`, `ifetch/index.py`). The flat
  JSON files answered "is this one file unchanged?" but cannot support the
  workflows built on top of them: totalling a tree before downloading, set
  differences between remote and local, dated snapshots, digest lookups for
  rename/move detection, or per-file transfer rows that survive a crash. All of
  those are now single queries against one store, using only the standard
  library. Existing `.ifetch_state.json` and `.ifetch_manifest.json` are
  imported automatically on first open, exactly once, and are left byte-for-byte
  untouched so an older iFetch keeps working. The manifest continues to be
  written as the human-readable, signable integrity export.
- **`--password-command`** (and `$IFETCH_PASSWORD_COMMAND`), sourcing the Apple
  ID password from `pass`, `1password-cli`, a mounted secret or any command that
  prints it. A system keyring is precisely what Docker, systemd and NAS boxes
  lack, which previously left the password as the one part of authentication
  that still required a human. The command is split with `shlex` and run
  **without a shell**, so quoted paths containing spaces work and the option is
  not an injection vector.
- **`--portable-names`**, sanitizing filenames for the strictest common
  filesystem rather than only the current one — for destinations that are exFAT,
  a NAS share, or will later be read from Windows.
- **`--normalize-names {preserve,nfc}`** (default `preserve`). `nfc` writes
  composed Unicode locally, which is more natural to type on Linux, at the cost
  of renaming anything already mirrored under the decomposed spelling.
- **`ifetch auth` command tree** (`doctor`, `renew`, `status`), also installed as
  `ifetch-auth`. `doctor` reports which authentication precondition failed and
  what to do about it — Apple's `HTTP 423 Missing PCS cookies`, `400 Invalid
  Session Token`, the `409`-on-a-valid-code case and the China redirect are each
  translated into a named cause with a remedy. Exit codes are `0` healthy,
  `1` needs attention soon, `2` broken now, so a scheduled job can distinguish
  "renew eventually" from "act now".
- **Non-interactive two-factor authentication.** A code can come from
  `--2fa-code`, `$IFETCH_2FA_CODE`, a polled file (`--2fa-file`), a polled HTTP
  endpoint (`--2fa-webhook`) or piped stdin. The file and webhook sources are
  retried until `--2fa-timeout`. iFetch never reads a TTY when stdin is a
  terminal it cannot prompt on, so a daemon fails with an actionable message
  instead of hanging forever.
- **Proactive session-expiry warnings.** The real expiry is read from the stored
  web-auth cookie (falling back to the session file's mtime plus 30 days, always
  labelled as an estimate). Every download run warns if the session expires
  within `--warn-days` (default 7); `ifetch auth status` exposes the same as a
  one-line, script-friendly report, and `ifetch auth renew --if-expiring-within
  N` is a no-op on a healthy session.
- **Apple package bundle expansion**, on by default. `.key`, `.pages`,
  `.numbers`, `.band`, `.xcodeproj` and other Apple packages are directories that
  Apple serves as ZIP archives whose byte count never matches the size reported
  in the folder listing. iFetch now expands them into real directories,
  preserving per-member modification times, and records them in the sync state
  by file count and total size so re-runs still skip unchanged bundles.
  `--no-expand-packages` restores the previous raw-archive behaviour. Expansion
  requires *both* a known package extension and a payload that really is a ZIP,
  so an ordinary `Archive.zip` is untouched.
- **Signed integrity manifest** (`.ifetch_manifest.json`). Apple exposes no
  content hashes, so iFetch records its own SHA-256 for every file at download
  time. `ifetch-verify --offline` re-hashes a mirror and reports drift with no
  credentials and no network, detecting corruption that preserves both size and
  mtime. `--sign-key`/`--sign-key-file`/`$IFETCH_MANIFEST_KEY` add an
  HMAC-SHA256 over the manifest's canonical form; `--require-signature` makes a
  valid signature mandatory. Expanded bundles are verified as single units.
- **`--region china`** (and `$ICLOUD_REGION`) for Apple IDs registered in China
  Mainland, which are served by `iCloud.com.cn`. The legacy `ICLOUD_CHINA=true`
  environment variable continues to work.
- **Contract-test harness for cross-account shared folders**
  (`tests/test_shared_folder_contract.py`), replaying recorded Apple responses
  including the nested-subdirectory `HTTP 400`, plus
  `docs/shared-folder-validation.md` — the manual procedure for validating the
  path against a real share between two Apple IDs.


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

- `DownloadManager.authenticate()` accepts an optional `two_factor` resolver.
  The keyword is optional and the CLI falls back to the no-argument form, so
  existing subclasses and plugins that override `authenticate()` keep working.
- `ifetch-verify` no longer requires a remote path when `--offline` is given;
  a single positional argument is then read as the local mirror directory.
- Package downloads whose streamed byte count differs from the listed size are
  reported as expanded bundles rather than as a size anomaly.


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

- **Folders with accents in their names no longer report "Path not found".**
  Apple returns filenames in Unicode NFD while users and shells produce NFC;
  these are different strings that render identically, and the previous
  `casefold()` comparison did not normalize, so `Café`, `Résumé`, `Übungen` and
  most non-English paths failed to resolve. Lookup now normalizes both sides.
- Remote names that are not legal filenames on the target filesystem are now
  sanitized instead of failing or escaping the destination directory, and two
  remote names that sanitize to the same local name are given deterministic
  distinct names rather than silently overwriting one another. Only names the
  current platform genuinely cannot represent are changed, so existing mirrors
  are not moved.
- `ifetch-verify` derives local names through the same sanitizer as the
  downloader, so sanitized or de-collided files are no longer reported missing.
- ZIP members carrying permission bits but no file-type bits (as written by
  Python's own `zipfile.writestr`) are no longer misclassified as non-regular
  entries and skipped during package expansion.
- An unrecognised failure during `ifetch auth renew` now shows the underlying
  error text instead of being replaced by a generic "not recognised" message.


- Removed the false claim in `docs/mirror.md` that hop 1 skips unchanged files
  "via stored checksums (`.ifetch_versions.json`)". That file is the version
  history; skip decisions are made from the sync state and file sizes.

- Auth instructions updated for pyicloud 2.x: the CLI is now
  `icloud auth login --username ...` (the 1.x `icloud --username ...` syntax
  no longer exists). Updated the README, docs, and iFetch's own
  "No stored password found" error message; added troubleshooting entries
  for the macOS `SSLCertVerificationError` and 2FA-request failures.

### Security

- Package archives are treated as untrusted input: member paths that are
  absolute, contain `..` (in either separator style), or resolve outside the
  destination are refused, as are symlink and device entries. Extraction is
  bounded by entry-count and total-size limits, and the expanded directory is
  swapped into place atomically so a failure leaves the previous bundle intact.

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
