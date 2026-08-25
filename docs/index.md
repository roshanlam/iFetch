# iFetch documentation

iFetch is a Python CLI for bulk-downloading iCloud Drive content with incremental re-runs (unchanged files are skipped, interrupted ones resume), retries, read-only integrity verification, version history, plugins, and an iCloud → NAS → Google Drive mirror pipeline.

For exactly what "unchanged" means — and what iFetch deliberately does *not* do (there is no content-based chunk diffing; a changed file is re-downloaded in full) — see [How re-runs decide what to download](../ReadMe.md#how-re-runs-decide-what-to-download) in the README.

- [Scheduling](scheduling.md) — run iFetch automatically with launchd, cron, or systemd, or keep it always-on with `ifetch-mirror --watch`
- [Monitoring](monitoring.md) — Healthchecks dead-man's switch, ntfy push, generic webhooks, and what iFetch will and will not claim about a run
- [Docker](docker.md) — the container image, volume layout, headless 2FA, and the expired-session failure unattended containers actually hit
- [Plugins](plugins.md) — plugin authoring guide with complete examples
- [Mirror pipeline](mirror.md) — iCloud → NAS → Google Drive, including Google OAuth setup
- [Troubleshooting](troubleshooting.md) — 2FA, sessions, keyring, rate limits, ADP, shared folders
- [`ifetch-verify`](../ReadMe.md#ifetch-verify--read-only-integrity-checking) — read-only integrity checking of a local mirror, and which level actually proves what

For installation and the full CLI reference, see the [project README](../ReadMe.md).
