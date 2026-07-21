# Changelog

All notable changes to iFetch will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

> **Note:** This is the first structured changelog for iFetch. The entries
> below summarize the project's git history to date; earlier changes were not
> tracked in changelog form.

## [Unreleased]

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
- Parallel downloads, differential (chunk-based) updates, and JSON logging.
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
