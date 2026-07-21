# iFetch

**Bulk-download your iCloud Drive from the command line — with delta sync, resume, version history, and a pipeline that mirrors iCloud to your NAS and Google Drive.**

[![CI](https://github.com/roshanlam/iFetch/actions/workflows/ci.yml/badge.svg)](https://github.com/roshanlam/iFetch/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Python 3.10+](https://img.shields.io/badge/python-3.10%2B-blue.svg)](https://www.python.org/)

Apple gives you two ways to get your data out of iCloud Drive: drag files around in Finder, or wait days for a privacy export. iFetch gives you a third: a scriptable CLI that downloads exactly what you want, only re-fetches what changed, survives interruptions, and keeps a local version history so an accidental overwrite in the cloud never costs you a file.

<!-- TODO: demo GIF here — record with vhs/asciinema: `ifetch Documents ~/icloud-backup` showing delta sync + summary report -->

## Why iFetch?

| | iFetch | [icloudpd](https://github.com/icloud-photos-downloader/icloud_photos_downloader) | [rclone](https://rclone.org/) | Apple privacy export |
|---|---|---|---|---|
| iCloud **Drive** files/folders | Yes | No — Photos only | No iCloud Drive backend | Yes |
| Delta sync (only changed data) | Yes, chunk-level | — | — | No, full dump every time |
| Resume interrupted transfers | Yes | — | — | No |
| Scriptable / schedulable | Yes | Yes | Yes | No — manual web request, takes days |
| Local version history | Yes | No | No | No |
| Shared-with-you items | Yes | No | — | No |
| Continue to Google Drive / NAS | Yes (`ifetch-mirror`) | No | Partially (no iCloud hop) | No |

If you want your **photos**, use icloudpd — it is excellent at that. If you want your **iCloud Drive**, that is what iFetch is for.

## 60-second quickstart

```sh
# 1. Install
pip install "ifetch[gdrive,auth]"   # core + Google Drive export + auth CLI (or: pip install ifetch)

# 2. Store your iCloud password in the system keyring (one time)
icloud auth login --username you@example.com

# 3. Download a folder (you'll be prompted for a 2FA code on first run)
ifetch Documents ~/icloud-backup
```

That's it. Run the same command tomorrow and iFetch only fetches the chunks that changed.

To work from source instead:

```sh
git clone https://github.com/roshanlam/iFetch.git
cd iFetch
pip install -e ".[gdrive]"
```

> **Note on pyicloud:** iFetch is built on [pyicloud](https://github.com/timlaing/pyicloud), which is actively maintained again on PyPI (v2.5.0+ adds the shared-drive support iFetch relies on). It is installed automatically.

## Features

- **Secure authentication** — password lives in your OS keyring (via pyicloud), full 2FA/2SA support, trusted sessions so you are not re-prompted every run
- **Chunk-level delta sync** — unchanged files are skipped; changed files only re-download the byte ranges that differ
- **Resume-capable** — checkpointed downloads pick up where they left off after an interruption
- **Parallel downloads** — configurable worker pool (`--max-workers`)
- **Retries with exponential backoff** — transient 5xx/timeout errors are retried, `Retry-After` headers respected
- **Version history** — before overwriting a changed file, the previous copy is archived to a local `.versions/` directory (see [Version history](#version-history--rollback))
- **Shared items** — list and download files/folders shared *with* you (`--list-shared`)
- **Profiles** — named include/exclude pattern sets for repeatable sync jobs
- **Plugins** — hook into auth, listing, and download lifecycle events ([docs/plugins.md](docs/plugins.md))
- **Structured JSON logging** and a per-run `download_report.json` summary
- **Google Drive export** (`ifetch-export`) and an **iCloud → NAS → Google Drive mirror** (`ifetch-mirror`)

## Benchmarks

Measured against a real iCloud Drive folder (414 files, 2.8 GiB) on a residential connection, July 2026:

| Scenario | Time | Result |
|---|---|---|
| Full download (4 workers) | 42.8s | 67.9 MiB/s |
| Full download (8 workers) | 50.2s | 57.9 MiB/s |
| Re-run, nothing changed (delta sync) | 16.2s | **0 bytes re-transferred** |
| Killed mid-download, restarted | +17.6s to finish | final tree **SHA-256-identical** to an uninterrupted download |

![iFetch benchmark chart](benchmarks/benchmark_chart.png)

The delta-sync and resume rows are the point: re-runs verify everything and download nothing, and an interrupted download resumes to a byte-identical result. Throughput varies with your connection and Apple's rate limiting (note 8 workers was *slower* than 4 here — Apple throttles aggressive parallelism; 4 is a good default).

Don't take these numbers on faith — the harness ships in the repo:

```sh
python benchmarks/benchmark.py "Documents/SomeFolder" --email you@example.com --workers 4 8
```

## CLI reference

iFetch installs three commands:

| Command | Purpose |
|---|---|
| `ifetch` | Download/list iCloud Drive content locally |
| `ifetch-export` | Upload local folders to Google Drive (delta-aware) |
| `ifetch-mirror` | One-way pipeline: iCloud → local folder/NAS → Google Drive |

### `ifetch` — iCloud Drive downloader

```sh
ifetch <icloud_path> [local_path] [options]
```

| Argument / flag | Description | Default |
|---|---|---|
| `icloud_path` | Remote iCloud Drive path, e.g. `Documents/MyFolder`. Required unless `--list-shared` | — |
| `local_path` | Local destination directory | current directory |
| `--email` | iCloud account email (or set `ICLOUD_EMAIL`) | env var / error |
| `--max-workers N` | Concurrent download threads | `4` |
| `--max-retries N` | Retry attempts for failed chunks (exponential backoff) | `3` |
| `--chunk-size BYTES` | Chunk size for differential downloads | `1048576` (1 MiB) |
| `--log-file PATH` | Write structured JSON logs to a file | console only |
| `--list` | List directory contents instead of downloading | off |
| `--list-shared` | List top-level items shared *with* you (no path needed) | off |
| `--profile NAME` | Apply include/exclude patterns from a profile | no filter |
| `--profile-file PATH` | Custom profile JSON path | `~/.ifetch_profiles.json` |

Environment variables: `ICLOUD_EMAIL` (account email), `ICLOUD_CHINA=true` (use iCloud China mainland endpoints), `IFETCH_PLUGIN_PATH` (extra plugin directory).

Examples:

```sh
ifetch Documents --list                          # list a folder
ifetch --list-shared --email you@example.com     # list items shared with you
ifetch Documents/Photos ~/Downloads/icloud-photos
ifetch Documents/Programming ~/Work/Code \
  --email you@example.com --max-workers 8 --max-retries 5 --log-file download.log
```

After each download run, a summary is printed and a detailed `download_report.json` is written into the destination directory.

### `ifetch-export` — local folders → Google Drive

```sh
ifetch-export [options]
```

Uploads local folders (by default `~/Documents`, `~/Downloads`, `~/Desktop`, `~/Pictures`, `~/LocalDoc` — whichever exist) to a folder in your Google Drive, skipping anything that hasn't changed since the last run (MD5 + a local upload index). Asks for confirmation before uploading. Requires Google OAuth credentials — see [docs/mirror.md](docs/mirror.md) for the setup walkthrough.

| Flag | Description | Default |
|---|---|---|
| `--folders PATH...` | Folders to export | Documents, Downloads, Desktop, Pictures, LocalDoc |
| `--gdrive-folder NAME` | Destination folder name in Google Drive | `MacOS Data` |
| `--credentials PATH` | Google OAuth2 client credentials JSON | `credentials.json` |
| `--token PATH` | Where the OAuth token is cached | `.gdrive_token.pickle` |
| `--force` | Re-upload everything, even unchanged files | off |
| `--include PAT...` | Only upload files matching patterns (e.g. `*.pdf *.docx`) | all |
| `--exclude PAT...` | Skip files matching patterns (e.g. `*.tmp`) | none |
| `--chunk-size MB` | Resumable-upload chunk size in MB | `10` |
| `--upload-workers N` | Parallel upload workers (max recommended: 8) | `4` |
| `--ignore-file PATH` | `.gitignore`-style ignore file | `.gdriveexportignore` |
| `--no-ignore` | Ignore file disabled — upload everything | off |
| `--index-file PATH` | Upload-tracking index file | `.gdrive_upload_index.json` |
| `--no-index` | Disable the index (slower; rescans everything) | off |
| `--rebuild-index` | Clear the index and exit | off |
| `--show-index-stats` | Print index statistics and exit | off |
| `--list-defaults` | Print the default folder list and exit | off |

### `ifetch-mirror` — iCloud → NAS → Google Drive

```sh
ifetch-mirror <icloud_path> <local_path> --gdrive-folder NAME [options]
```

| Argument / flag | Description |
|---|---|
| `icloud_path` | Source path in iCloud Drive |
| `local_path` | Local staging folder — typically a NAS mount |
| `--gdrive-folder NAME` | Destination folder in Google Drive |
| `--watch SECONDS` | Keep running, repeating the pipeline on an interval |
| `--dry-run` | Show what would be transferred without transferring |
| `--email` | iCloud account email (or `ICLOUD_EMAIL`) |

See the [Mirror section](#mirror-icloud--nas--google-drive) below and the full guide in [docs/mirror.md](docs/mirror.md).

## Mirror: iCloud → NAS → Google Drive

A frequently requested workflow: keep a copy of your iCloud Drive on a NAS **and** in Google Drive, without ever re-transferring unchanged data. `ifetch-mirror` chains both hops into one command, delta-aware at each stage:

```sh
# One shot: pull iCloud Documents to the NAS, then push to Google Drive
ifetch-mirror Documents /Volumes/nas/icloud-mirror --gdrive-folder "iCloud Mirror"

# Always-on: re-run the pipeline every 15 minutes
ifetch-mirror Documents /Volumes/nas/icloud-mirror \
  --gdrive-folder "iCloud Mirror" --watch 900

# Preview what would happen
ifetch-mirror Documents /Volumes/nas/icloud-mirror \
  --gdrive-folder "iCloud Mirror" --dry-run
```

- Hop 1 (iCloud → local) uses iFetch's chunk-level delta sync — only changed byte ranges cross the wire.
- Hop 2 (local → Google Drive) uses the export engine's MD5 + upload index — only changed files are re-uploaded.
- `--watch` makes it a lightweight always-on daemon; alternatively schedule single runs with launchd/cron/systemd ([docs/scheduling.md](docs/scheduling.md)).

The pipeline is **one-way** (iCloud is the source of truth). Two-way sync is on the roadmap.

## Profiles

Profiles are named include/exclude pattern sets stored in `~/.ifetch_profiles.json`:

```json
{
  "pdf_backup": {
    "include": ["Documents/**/*.pdf"],
    "exclude": ["Documents/Private/*"]
  }
}
```

```sh
ifetch Documents ~/PDFs --profile pdf_backup
ifetch Documents ~/PDFs --profile pdf_backup --profile-file ./my_profiles.json
```

Patterns are glob-style (`fnmatch`) and are matched against the remote path. An empty `include` list means "everything".

## Plugins

Drop a Python file into the `plugins/` directory next to the `ifetch` package (or point `IFETCH_PLUGIN_PATH` at any directory) and subclass `BasePlugin`:

```python
from ifetch.plugin import BasePlugin

class Notify(BasePlugin):
    def after_download(self, remote_item, local_path, success, **kwargs):
        if success:
            print(f"Downloaded {remote_item.name} -> {local_path}")
```

Available hooks: `on_authenticated`, `on_list_contents`, `before_download`, `after_download`, and a generic `on_event` (fires for `download_progress` and `download_session_completed`, among others). Plugins are auto-discovered at startup, and a crashing plugin never takes down a transfer.

Full authoring guide with two complete example plugins (webhook/ntfy notifications, checksum manifest verifier): [docs/plugins.md](docs/plugins.md).

## Version history & rollback

Every time iFetch is about to overwrite a file that changed in iCloud, it first archives your existing local copy:

```
~/icloud-backup/
├── report.pdf                     # current version
├── .ifetch_versions.json          # version metadata (checksums, timestamps)
└── .versions/
    └── report.pdf.v1_20260718T093012   # previous version, timestamped
```

Nothing is ever silently destroyed by a sync. To roll back, copy the archived version over the current file:

```sh
cp ~/icloud-backup/.versions/report.pdf.v1_20260718T093012 ~/icloud-backup/report.pdf
```

A `ifetch restore` convenience command is on the roadmap; today rollback is a manual copy from `.versions/`.

## Scheduling

Run iFetch on a schedule with launchd (macOS), cron (Linux/NAS), or systemd timers — worked examples for all three, plus notes on keyring access in non-interactive sessions, are in [docs/scheduling.md](docs/scheduling.md). For an always-on process instead of a scheduler, use `ifetch-mirror --watch`.

## Troubleshooting

Common issues — the 2FA flow, expired sessions, per-OS keyring problems, rate limiting/503s, the Advanced Data Protection caveat, and shared-folder quirks — are covered in [docs/troubleshooting.md](docs/troubleshooting.md). Quick hits:

- **"No stored password found"** — run `icloud auth login --username you@example.com` once to store your password in the keyring (the `icloud` CLI comes with `pip install "ifetch[auth]"`).
- **Repeated 2FA prompts** — your session expired; run `ifetch` interactively once to re-trust the session.
- **503 / rate limited** — iFetch backs off automatically; lower `--max-workers` if it persists.
- **Advanced Data Protection** — with ADP enabled, Apple blocks web/API access to Drive data unless you enable "Access iCloud Data on the Web" in your ADP settings.

## Contributing

Contributions are welcome — bug reports, docs, and PRs alike. Please open an issue to discuss larger changes first, and make sure `pytest` passes.

## License

MIT — see [LICENSE](LICENSE).

## Acknowledgments

- [timlaing/pyicloud](https://github.com/timlaing/pyicloud) — the maintained iCloud API wrapper iFetch is built on
- [tqdm](https://github.com/tqdm/tqdm) — progress bars
