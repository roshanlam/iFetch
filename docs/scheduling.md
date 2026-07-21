# Scheduling iFetch

iFetch is a one-shot CLI, which makes it easy to schedule with whatever your platform already provides. This page shows worked examples for launchd (macOS), cron (Linux/NAS), and systemd (Linux servers), plus the built-in alternative: `ifetch-mirror --watch`.

## Before you schedule anything

1. **Do one interactive run first.** The first `ifetch` run prompts for a 2FA code and trusts the session. Scheduled runs cannot answer that prompt — they reuse the trusted session established interactively. When the session eventually expires (typically after a couple of months), run `ifetch` once by hand to re-authenticate. See [troubleshooting](troubleshooting.md#session-expiry).
2. **Store the password in the keyring**: `icloud --username you@example.com`. Scheduled jobs read it from there; nothing is stored in plain text.
3. **Set `ICLOUD_EMAIL`** in the job's environment so you don't need `--email` on every invocation.
4. **Log to a file** with `--log-file` so failures in unattended runs are diagnosable.

## macOS: launchd

Create `~/Library/LaunchAgents/com.ifetch.backup.plist`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
  "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.ifetch.backup</string>

    <key>ProgramArguments</key>
    <array>
        <string>/usr/local/bin/ifetch</string>
        <string>Documents</string>
        <string>/Users/you/icloud-backup</string>
        <string>--log-file</string>
        <string>/Users/you/icloud-backup/ifetch.log</string>
    </array>

    <key>EnvironmentVariables</key>
    <dict>
        <key>ICLOUD_EMAIL</key>
        <string>you@example.com</string>
    </dict>

    <!-- Run every day at 02:30 -->
    <key>StartCalendarInterval</key>
    <dict>
        <key>Hour</key>
        <integer>2</integer>
        <key>Minute</key>
        <integer>30</integer>
    </dict>

    <key>StandardOutPath</key>
    <string>/tmp/ifetch.out</string>
    <key>StandardErrorPath</key>
    <string>/tmp/ifetch.err</string>
</dict>
</plist>
```

Adjust the `ifetch` path to wherever it is installed (`which ifetch` — if you use a virtualenv, point at `venv/bin/ifetch`). Then load it:

```sh
launchctl load ~/Library/LaunchAgents/com.ifetch.backup.plist
# test it immediately:
launchctl start com.ifetch.backup
```

Because this is a user LaunchAgent (not a system daemon), it runs in your login session and can read your Keychain, which is where the iCloud password lives. If macOS shows a Keychain permission dialog on the first scheduled run, click "Always Allow".

To run every N seconds instead of at a fixed time, replace `StartCalendarInterval` with:

```xml
<key>StartInterval</key>
<integer>3600</integer>
```

## Linux / NAS: cron

```sh
crontab -e
```

```cron
# m h dom mon dow  command
30 2 * * *  ICLOUD_EMAIL=you@example.com /home/you/venv/bin/ifetch Documents /mnt/backup/icloud --log-file /var/log/ifetch.log
```

Notes for cron environments:

- cron runs with a minimal environment — use absolute paths for both `ifetch` and the destination, and set `ICLOUD_EMAIL` inline (or in a wrapper script).
- On headless boxes the keyring needs a Secret Service daemon (e.g. `gnome-keyring-daemon`) or an alternative backend; see [keyring issues](troubleshooting.md#keyring-issues) for headless setups.
- Many NAS systems (Synology, QNAP) expose cron through their task-scheduler UI — paste the same command there.

A more robust wrapper script (`/usr/local/bin/ifetch-backup.sh`):

```sh
#!/usr/bin/env bash
set -euo pipefail
export ICLOUD_EMAIL=you@example.com
/home/you/venv/bin/ifetch Documents /mnt/backup/icloud \
  --log-file /var/log/ifetch.log \
  || echo "ifetch failed at $(date)" >> /var/log/ifetch-failures.log
```

## Linux: systemd service + timer

`/etc/systemd/system/ifetch-backup.service`:

```ini
[Unit]
Description=iFetch iCloud Drive backup
Wants=network-online.target
After=network-online.target

[Service]
Type=oneshot
User=you
Environment=ICLOUD_EMAIL=you@example.com
ExecStart=/home/you/venv/bin/ifetch Documents /mnt/backup/icloud --log-file /var/log/ifetch.log
```

`/etc/systemd/system/ifetch-backup.timer`:

```ini
[Unit]
Description=Run iFetch backup daily

[Timer]
OnCalendar=*-*-* 02:30:00
Persistent=true
RandomizedDelaySec=10m

[Install]
WantedBy=timers.target
```

Enable and verify:

```sh
sudo systemctl daemon-reload
sudo systemctl enable --now ifetch-backup.timer
systemctl list-timers ifetch-backup.timer
journalctl -u ifetch-backup.service   # logs
```

`Persistent=true` makes systemd run a missed job at next boot — useful for machines that aren't always on. If your keyring requires an unlocked user session, consider running this as a user unit instead (`systemctl --user`), combined with `loginctl enable-linger you`.

## The always-on alternative: `ifetch-mirror --watch`

If you want continuous mirroring rather than point-in-time runs, skip the scheduler entirely:

```sh
ifetch-mirror Documents /mnt/nas/icloud-mirror \
  --gdrive-folder "iCloud Mirror" \
  --watch 900        # repeat every 15 minutes
```

This keeps a single process running that repeats the full iCloud → local → Google Drive pipeline on the given interval. Both hops are delta-aware, so an interval pass where nothing changed transfers (almost) nothing. Pair it with a process supervisor for resilience — e.g. a systemd service with `Restart=on-failure` (no timer needed):

```ini
[Service]
User=you
Environment=ICLOUD_EMAIL=you@example.com
ExecStart=/home/you/venv/bin/ifetch-mirror Documents /mnt/nas/icloud-mirror --gdrive-folder "iCloud Mirror" --watch 900
Restart=on-failure
RestartSec=60
```

See [mirror.md](mirror.md) for the full pipeline guide.
