# Troubleshooting

## Authentication and 2FA

### How the 2FA flow works

iFetch authenticates through [pyicloud](https://github.com/timlaing/pyicloud) using the password stored in your OS keyring — it never asks you to type your password into iFetch itself:

1. Store the password once: `icloud --username you@example.com` (prompts for the password and saves it to the keyring).
2. On the first `ifetch` run, Apple sends a verification code to your trusted devices. iFetch prompts: `Enter the verification code:`.
3. After a valid code, iFetch asks Apple to **trust the session**, so subsequent runs skip 2FA until the session expires.

Accounts still on legacy two-step authentication (2SA) get a device picker instead — choose a trusted device, receive the code, enter it.

Common failures:

- **`No stored password found`** — step 1 was skipped, or the password was stored under a different email than the one you passed via `--email`/`ICLOUD_EMAIL`. Re-run `icloud --username you@example.com`.
- **`Invalid credentials`** — wrong password in the keyring (did you change your Apple ID password recently?). Re-run `icloud --username ...` to overwrite it.
- **`Failed to verify 2FA code`** — codes are short-lived; request a fresh one and type it promptly.
- **`Warning: Failed to trust session`** — authentication worked, but you will be re-prompted for 2FA on the next run. Usually transient; try again later.

### iCloud China

Set `ICLOUD_CHINA=true` to use the China-mainland iCloud endpoints.

## Session expiry

Trusted sessions do not last forever — Apple expires them (typically on the order of a couple of months, sometimes sooner after a password change or security event). Symptoms: a previously working scheduled job starts failing, or `ifetch` prompts for 2FA again.

Fix: run `ifetch <path> --list` **interactively** once, enter the new 2FA code, and the session is re-trusted. Scheduled/unattended runs then work again. There is no way to answer a 2FA prompt from cron/launchd, so plan for an occasional interactive re-auth (see [scheduling.md](scheduling.md)).

If re-auth loops or behaves strangely, delete pyicloud's cached session/cookie data and log in fresh. The cache location depends on your pyicloud version and platform — look for a `pyicloud` directory under `~/.pyicloud`, `~/Library/Caches`/`~/.cache`, or your system temp directory — then re-run `ifetch` interactively.

## Keyring issues (per OS)

pyicloud stores your Apple ID password via the Python `keyring` library, which delegates to the OS.

### macOS

The backend is the macOS **Keychain** — no extra packages needed.

- If a scheduled (launchd) run hangs or fails reading the password, the Keychain may be prompting for access invisibly. Run the same command once in a normal terminal session and click **Always Allow** on the Keychain dialog.
- If the login keychain is locked (e.g. over SSH): `security unlock-keychain ~/Library/Keychains/login.keychain-db`.

### Linux (desktop)

The backend is the Secret Service API — provided by **gnome-keyring** or **KWallet**. Install one:

```sh
sudo apt install gnome-keyring        # Debian/Ubuntu (GNOME)
# or
sudo apt install kwalletmanager       # KDE
```

`keyring.errors.NoKeyringError` / "No recommended backend was available" means no Secret Service daemon is running in your session.

### Linux (headless / NAS / servers)

There is no desktop session to provide a keyring, so either:

- Run a keyring daemon manually with a fixed password (works in cron/systemd):

  ```sh
  sudo apt install gnome-keyring dbus-x11
  eval "$(printf 'yourpass' | gnome-keyring-daemon --unlock --components=secrets)"
  ```

- Or install the plain-text fallback backend (understand the tradeoff: the password is stored obfuscated-but-recoverable on disk):

  ```sh
  pip install keyrings.alt
  ```

  and configure `~/.config/python_keyring/keyringrc.cfg`:

  ```ini
  [backend]
  default-keyring=keyrings.alt.file.PlaintextKeyring
  ```

Then run `icloud --username you@example.com` again to store the password in the now-working backend.

### Windows

The backend is Windows Credential Locker and generally just works. If `icloud --username` succeeds but `ifetch` cannot find the password, check that both run under the same user account (and both inside/outside the same virtualenv is fine — the keyring is per-OS-user, not per-venv).

## Rate limiting and 503 errors

Apple's iCloud endpoints throttle aggressive clients. iFetch already:

- retries transient errors (`503 Service Unavailable`, connection resets, `RETRY_NEEDED`, internal failures) with **exponential backoff**, up to `--max-retries` times, and
- honors the server's `Retry-After` header when one is sent.

If you still see persistent 503s or `Too Many Requests`:

1. Lower concurrency: `--max-workers 2` (or even 1).
2. Raise `--max-retries` for flaky connections.
3. Space out scheduled runs — hourly is usually fine, every minute is asking for throttling.
4. Wait. Throttling windows typically clear within minutes to an hour.

Failures are recorded per-file in `download_report.json` and in the `--log-file` JSON log, so a partially throttled run can simply be re-run — delta sync skips everything that already succeeded.

## Advanced Data Protection (ADP) caveat

If your Apple ID has **Advanced Data Protection** enabled, iCloud Drive data is end-to-end encrypted and Apple's web/API endpoints — which pyicloud (and therefore iFetch) uses — cannot read it by default. Symptoms: authentication succeeds but Drive listings come back empty or fail with access errors.

Workaround: on an Apple device go to **Settings → [your name] → iCloud** and enable **"Access iCloud Data on the Web"**. This allows web/API access to your (still encrypted at rest) data and lets iFetch work. If you are not willing to enable that, iFetch cannot download ADP-protected Drive content — that is an Apple platform restriction, not an iFetch bug.

## Shared-folder quirks

iFetch supports items shared *with* you, but Apple's shared-drive API has rough edges:

- **Listing**: `ifetch --list-shared` shows top-level shared items. When you pass a path that isn't found in your own Drive, iFetch automatically retries the same path against the shared-items root, so `ifetch SharedProjectFolder ~/backup` usually just works.
- **Cross-account shared files** need a `shareID` parameter on Apple's download endpoint. iFetch detects this case and constructs the request itself (this is why `pyicloud>=2.5.0` — the maintained [timlaing/pyicloud](https://github.com/timlaing/pyicloud), now on PyPI — is required; older releases cannot traverse shared folders).
- **Some shared items still fail**: if the owner's share settings restrict downloads, or the item type isn't supported by the fallback, iFetch logs a `shared_file_download_attempted` event with a hint and marks the file failed rather than aborting the run. The rest of the sync continues.
- Items **you** shared with others live in your own Drive as normal and download normally.

If a shared item fails persistently, check the JSON log (`--log-file`) for the `hint` field — it distinguishes "shared from another account and not downloadable" from ordinary network errors.

## Still stuck?

Run with `--log-file debug.log`, reproduce the problem, and open an issue at [github.com/roshanlam/iFetch/issues](https://github.com/roshanlam/iFetch/issues) with the relevant (redacted) log lines, your OS, and your Python version.
