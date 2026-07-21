# Mirror pipeline: iCloud → NAS → Google Drive

`ifetch-mirror` chains iFetch's two engines into a single one-way pipeline:

```
iCloud Drive  ──(delta download)──▶  local folder / NAS mount  ──(delta upload)──▶  Google Drive
```

Typical use case: your source of truth is iCloud, you want a durable copy on your NAS, and a second off-site copy in Google Drive — without ever re-transferring unchanged data at either hop.

```sh
ifetch-mirror <icloud_path> <local_path> --gdrive-folder NAME [--watch SECONDS] [--dry-run] [--email EMAIL]
```

| Argument / flag | Description |
|---|---|
| `icloud_path` | Source path in iCloud Drive (e.g. `Documents`) |
| `local_path` | Local staging directory — typically a NAS mount (`/Volumes/nas/...`, `/mnt/nas/...`) |
| `--gdrive-folder NAME` | Destination folder name in Google Drive |
| `--watch SECONDS` | Keep running and repeat the pipeline on this interval |
| `--dry-run` | Report what would be transferred at each hop without transferring |
| `--email` | iCloud account email (or set `ICLOUD_EMAIL`) |

The pipeline is **one-way**: changes flow iCloud → local → Google Drive. Deleting or editing files on the NAS or in Google Drive does not propagate back to iCloud. Two-way sync is on the roadmap.

## How each hop stays "delta"

**Hop 1 — iCloud → local** uses the `ifetch` download engine:

- unchanged files are skipped via stored checksums (`.ifetch_versions.json`);
- changed files re-download only the differing byte ranges (chunk-level diff);
- interrupted transfers resume from their checkpoint;
- when a file changed in iCloud, your previous local copy is archived under `.versions/` before being replaced — the NAS mirror doubles as a version history.

**Hop 2 — local → Google Drive** uses the export engine (`ifetch-export` under the hood):

- a local upload index (`.gdrive_upload_index.json`) records size, mtime, and MD5 of everything uploaded; unchanged files are skipped without any API call;
- files missing from the index are compared against Drive by MD5 before uploading;
- uploads are resumable and chunked, several files in parallel.

Net effect: a `--watch` pass where nothing changed costs one iCloud listing and zero uploads.

## Prerequisites

1. **iCloud auth set up** — password in the keyring (`icloud --username you@example.com`) and one interactive run completed for 2FA. See [troubleshooting](troubleshooting.md#authentication-and-2fa).
2. **Google OAuth credentials** — a `credentials.json` file, set up as follows.

## Google OAuth credential setup

iFetch's Google Drive exporter uses OAuth 2.0 for **installed (desktop) apps**. You create your own OAuth client in Google Cloud — this means the access token belongs to you and only you; iFetch talks directly to Google with no third-party server involved.

One-time setup (about 5 minutes):

1. Go to [console.cloud.google.com](https://console.cloud.google.com/) and create a project (any name, e.g. "ifetch-export").
2. Enable the **Google Drive API**: *APIs & Services → Library → Google Drive API → Enable*.
3. Configure the OAuth consent screen (*APIs & Services → OAuth consent screen*): choose **External**, fill in the app name and your email, and add yourself as a **test user**. You do not need to publish or verify the app.
4. Create credentials: *APIs & Services → Credentials → Create Credentials → OAuth client ID → Application type: **Desktop app***.
5. Download the JSON and save it as `credentials.json` in the directory where you run iFetch (or pass its path with `--credentials`).

### What happens at first run

When the exporter authenticates it:

1. Looks for a cached token at `.gdrive_token.pickle` (configurable with `--token`). If present and valid, no interaction is needed; if expired, it is refreshed silently using the refresh token.
2. Otherwise it starts a **local-server OAuth flow**: your browser opens to Google's consent page, you sign in and approve, and Google redirects back to a temporary local port to hand iFetch the token.
3. The token is pickled to `.gdrive_token.pickle` for future runs.

The requested scope is **`drive.file`** — iFetch can only see and manage files/folders **it created itself**. It cannot read the rest of your Google Drive. If you ever change scopes, delete `.gdrive_token.pickle` to force a fresh consent.

**Headless machines** (NAS without a browser): run the first authentication on a desktop machine, then copy `.gdrive_token.pickle` (and `credentials.json`) to the headless box. Refreshes after that are non-interactive.

**Token errors** ("invalid_grant", repeated re-consent): delete `.gdrive_token.pickle` and authenticate again. Test-mode OAuth apps can have refresh tokens expire after 7 days — publish the consent screen to "In production" (no verification needed for your own use with the `drive.file` scope) to get long-lived refresh tokens.

## Recipes

One-shot mirror:

```sh
ifetch-mirror Documents /mnt/nas/icloud-mirror --gdrive-folder "iCloud Mirror"
```

Preview without transferring:

```sh
ifetch-mirror Documents /mnt/nas/icloud-mirror --gdrive-folder "iCloud Mirror" --dry-run
```

Always-on, every 15 minutes:

```sh
ifetch-mirror Documents /mnt/nas/icloud-mirror --gdrive-folder "iCloud Mirror" --watch 900
```

Run it under a supervisor for resilience — a systemd unit with `Restart=on-failure` is shown in [scheduling.md](scheduling.md#the-always-on-alternative-ifetch-mirror---watch). For point-in-time runs instead of an always-on process, schedule the one-shot form with launchd/cron/systemd.

## NAS notes

- **Mount first.** If the NAS mount is down, the local path looks like an empty directory. Guard scheduled runs with a mount check, e.g. `mountpoint -q /mnt/nas && ifetch-mirror ...` on Linux.
- **SMB/NFS metadata**: hop 2's change detection uses size + mtime (falling back to MD5), so filesystems with coarse mtime resolution are fine — worst case a file is re-hashed, not re-uploaded.
- **Keep the housekeeping files** (`.ifetch_versions.json`, `.versions/`, `.gdrive_upload_index.json`, `.gdrive_token.pickle`) with the mirror. Deleting the upload index does not cause duplicates — files are re-verified against Drive by MD5 — but it makes the next pass much slower.
- **Snapshots**: if your NAS does snapshots (btrfs/ZFS/Synology), snapshotting the mirror directory gives you an extra safety net on top of iFetch's `.versions/` history.

## Roadmap

- Two-way sync (local edits propagating back to iCloud) — planned, not yet available.
- `ifetch restore` command for one-step rollback from `.versions/`.
