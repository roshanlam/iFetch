# iFetch 1.1.0

iFetch grows from a focused downloader into a full iCloud Drive backup and
recovery toolkit. Everything here is backward-compatible with 1.0.0 — same
commands, same defaults — with one thing worth knowing before you upgrade.

**Heads-up (upgrading from 1.0.0):** Apple package bundles (`.key`, `.pages`,
`.numbers`, `.xcodeproj`, …) now come back as real, usable folders by default
instead of the raw ZIP archive Apple serves. Pass `--no-expand-packages` if you
want the old ZIP behavior.

## The headline changes

- **Crash-safe resume and repair.** A download cut off by a closed laptop, a
  dropped connection or a power cut can be finished without starting over.
  `ifetch resume` fetches only the unfinished files, without re-listing your
  whole drive; `ifetch repair` reports what was left behind and, with
  `--check-digests`, files that no longer match the checksum recorded when they
  were downloaded.
- **A recovery toolkit, not just a downloader.** `ifetch plan` dry-runs a sync
  before it touches anything, `ifetch audit` reconciles iCloud against your
  disk, `ifetch recover` finds what is missing or evicted, `ifetch snapshot`
  keeps dated states you can diff and restore, and `ifetch conflicts` spots
  renamed, moved or duplicated files instead of downloading them again.
- **Advanced Data Protection and shared folders.** ADP-enabled accounts can now
  reach iCloud Drive, and files inside a folder shared by another Apple ID
  download correctly instead of failing with 404.

## Also new

- **Web UI** — `ifetch serve`, for sign-in, browsing and downloading without a terminal.
- **Backup safety** — `ifetch guard` finds files that exist only on Apple's servers and are missing from every backup of your Mac; `ifetch vanish` flags files deleted from iCloud.
- **Uploads** — `ifetch uplink` restores files iCloud is missing, and never overwrites, renames or deletes anything.
- **Ops** — `--bwlimit` (rclone-style bandwidth timetables), run-outcome notifications (Healthchecks.io / ntfy / webhooks), and an official Docker image.
- **Faster on large drives** — big folders no longer slow down as the run goes on.
- **From the community** — `--skip-existing` and `--retry-failed`, with thanks to external contributors.

Full details are in the [CHANGELOG](https://github.com/roshanlam/iFetch/blob/main/CHANGELOG.md).
