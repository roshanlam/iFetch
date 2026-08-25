# Running iFetch in Docker

The image is a thin wrapper: `ifetch` is the entrypoint, so everything after
the image name is ordinary iFetch arguments and every flag in the
[README](../ReadMe.md) works unchanged.

```
ghcr.io/roshanlam/ifetch:latest
```

Built for `linux/amd64` and `linux/arm64`, so it runs on a Raspberry Pi and on
ARM NAS boxes as well as on x86.

## Read this before you start

**The first run must be interactive.** Apple requires a six-digit code the
first time an account signs in from a new client, and it will not accept one
from a machine that cannot receive it. You run once by hand, answer the
prompt, and Apple then *trusts* the session for roughly two months. Every run
after that is unattended. There is no way around this and any tool that claims
otherwise is either storing your session or lying.

**Two volumes, and the second one is the one people forget.**

| Mount | Holds | If you lose it |
| --- | --- | --- |
| `/data` | your mirrored files | you re-download everything |
| `/config` | the trusted iCloud session | **every start asks for a 2FA code it cannot get** |

pyicloud stores the trusted session under `$TMPDIR/pyicloud/$USER`. In a
container `$TMPDIR` is `/tmp`, which dies with the container — so the image
sets `TMPDIR=/config` and `USER=ifetch`, which puts the session on the mounted
volume at `/config/pyicloud/ifetch/`. Do not change either variable unless you
are moving the volume, and if you do change `USER`, change it consistently or
the next run will not find the session it wrote last time.

## Quickstart

### 1. Lay out the directories

```sh
mkdir -p ~/ifetch/{icloud,config,secrets}
cd ~/ifetch
printf '%s' 'your-apple-id-password' > secrets/icloud_password
chmod 600 secrets/icloud_password
```

There is no system keyring inside a container, so the password comes from a
command that prints it — `IFETCH_PASSWORD_COMMAND`. `cat` on a mounted secret
is the simplest form; `pass`, `age -d`, or an `op` call work identically. The
command is split with `shlex` and run **without a shell**, so no pipes or
redirection: wrap anything complex in a script.

### 2. Fetch the compose file

Copy [`docker-compose.yml`](../docker-compose.yml) from the repository into
`~/ifetch/` and change the four values marked `CHANGE ME`: your e-mail, the
iCloud folder to mirror, and the two host paths.

### 3. Do the interactive first run

```sh
docker compose run --rm -it ifetch
```

`-it` is what makes this work: iFetch only prompts for a code when stdin is a
real terminal, precisely so that a daemon fails fast instead of hanging on a
prompt nobody can see. Type the code from your Apple device when asked. The
session is written to `/config` and trusted.

Verify it landed:

```sh
ls ~/ifetch/config/pyicloud/ifetch/
# youexamplecom.session  youexamplecom.cookiejar
```

If that directory is empty, the session did **not** persist — jump to
[the session is not persisting](#the-session-is-not-persisting).

### 4. Run it unattended from now on

```sh
docker compose run --rm ifetch
```

Note `run --rm`, not `up`. iFetch performs one download and exits; it is not a
daemon. Scheduling is the host's job:

```cron
30 2 * * *  cd /home/you/ifetch && /usr/bin/docker compose run --rm ifetch >> /var/log/ifetch.log 2>&1
```

Or as a systemd timer — see [scheduling.md](scheduling.md) for the unit files;
swap the `ExecStart` for the `docker compose run` line above.

### 5. Turn on monitoring

An unattended container with no notifications is a backup you are guessing
about. Uncomment the notification block in `docker-compose.yml`:

```yaml
    environment:
      IFETCH_HEALTHCHECKS_URL: "https://hc-ping.com/<your-check-uuid>"
      IFETCH_NTFY_URL: "https://ntfy.sh/my-private-ifetch-topic"
```

Healthchecks is the one that matters here, because it alerts on the run that
*didn't happen* — including the run that could not start because the session
expired, which is the failure this setup actually hits. See
[monitoring.md](monitoring.md).

## Without compose

```sh
docker run --rm -it \
  -e ICLOUD_EMAIL=you@example.com \
  -e IFETCH_PASSWORD_COMMAND='cat /run/secrets/icloud_password' \
  -e IFETCH_HEALTHCHECKS_URL='https://hc-ping.com/<uuid>' \
  -v ~/ifetch/icloud:/data \
  -v ~/ifetch/config:/config \
  -v ~/ifetch/secrets/icloud_password:/run/secrets/icloud_password:ro \
  ghcr.io/roshanlam/ifetch:latest \
  Documents /data --log-file /config/ifetch.log
```

`TMPDIR` and `USER` are baked into the image, so they do not need repeating.

## The other CLIs

The image ships every iFetch entry point; override the entrypoint to reach
them:

```sh
# How much life is left in the trusted session?
docker compose run --rm --entrypoint ifetch-auth ifetch status

# Full diagnosis, including an online check
docker compose run --rm --entrypoint ifetch-auth ifetch doctor --online

# What would a run do? No transfer.
docker compose run --rm --entrypoint ifetch-plan ifetch Documents /data

# Prove the mirror is intact
docker compose run --rm --entrypoint ifetch-verify ifetch /data
```

## Troubleshooting

### The session expired

**This is the failure you will actually hit.** Apple's trust token lasts about
two months. When it lapses, an unattended container has nothing to answer with,
and the run fails with something like `Invalid session token` or a 2FA prompt
into a terminal that does not exist.

Check before it bites:

```sh
docker compose run --rm --entrypoint ifetch-auth ifetch status
# you@example.com [global]: Session expires in 6.0 days (read from the stored
# session cookie). Renew it now so the next scheduled run does not fail.
```

Exit code `0` is healthy, `1` is a warning, `2` means expired — usable directly
in a wrapper script.

There are three ways to get a new trusted session, in order of how much you
have to be present:

**(a) Re-run interactively.** The simplest, and the one to reach for first:

```sh
docker compose run --rm -it --entrypoint ifetch-auth ifetch renew
```

Type the code. Done for another two months. If the stored session is corrupt
rather than merely expired, add `--reset` to discard it first.

**(b) Pass the code in for one run.** If you can be at a keyboard but not at a
terminal — say you are doing this from a phone over your NAS's web UI:

```sh
docker compose run --rm -e IFETCH_2FA_CODE=123456 --entrypoint ifetch-auth ifetch renew
```

Codes are single-use and expire in minutes, so this only works if you kick it
off right after the code arrives.

**(c) Drop the code into a watched file.** The genuinely hands-off-ish option,
and the one suited to a NAS: iFetch polls a file until it contains a six-digit
code, for up to `--2fa-timeout` seconds (default 300).

```sh
docker compose run --rm \
  --entrypoint ifetch-auth ifetch renew \
  --2fa-file /config/2fa-code --2fa-timeout 600 --no-stdin
```

Then, from anywhere that can write to that directory — an iOS Shortcut writing
to an SMB share, an `ssh` one-liner, your NAS's file manager:

```sh
echo 123456 > ~/ifetch/config/2fa-code
```

The file may contain a whole SMS; iFetch extracts the code, and refuses rather
than guesses if the text contains two different six-digit numbers. Delete the
file afterwards.

There is also `--2fa-webhook <url>`, which polls a URL with `GET` and accepts
the bare code or JSON containing one, for setups where the code arrives at a
service rather than a filesystem.

All four sources (`--2fa-code`, `$IFETCH_2FA_CODE`, `--2fa-file`,
`--2fa-webhook`) work on the main `ifetch` command too, not just
`ifetch-auth renew` — so a normal scheduled run can pick up a code you left for
it.

### The session is not persisting

Every run asks for a code, and `~/ifetch/config/pyicloud/` is empty or missing.

1. **Check the volume is actually mounted.** `docker compose config` should
   show `/config` in the volume list. A typo in the host path creates the
   directory silently rather than erroring.
2. **Check `TMPDIR`.** `docker compose run --rm --entrypoint sh ifetch -c 'echo $TMPDIR'`
   must print `/config`. If something in your compose file overrode it, the
   session went to `/tmp` and vanished with the container.
3. **Check `USER`.** The session directory is named after
   `getpass.getuser()`. If you set `user: "1000:1000"` in compose *and* that
   uid has no `/etc/passwd` entry, Python falls back through `$USER` — which
   the image sets to `ifetch` for exactly this reason. Removing that variable
   makes `getpass.getuser()` raise and authentication fail before it starts.
4. **Check write permissions** — see below.

### Permission denied writing to /data

The container runs as the non-root user `ifetch`. With a bind mount, the host's
ownership wins, so a directory owned by your host user is not writable by the
container's.

Either give the container your identity:

```yaml
    user: "1000:1000"     # your `id -u`:`id -g`
```

(keep `USER: "ifetch"` in `environment:` — it names the session directory, not
the process owner), or hand the directories to the image's uid:

```sh
sudo chown -R 100999:100999 ~/ifetch/icloud ~/ifetch/config   # check the real uid first
docker compose run --rm --entrypoint id ifetch
```

Named volumes avoid this entirely — Docker copies the image's ownership into an
empty volume — at the cost of the files not being directly visible on the host.

### `getpass.getuser()` raised, or the run dies before authenticating

You are running with `--user` at a uid that has no passwd entry, and something
removed the `USER` environment variable. Put it back:

```sh
docker run -e USER=ifetch ...
```

### The container downloads everything again on every run

`/data` is not persisting, or it is a different `/data` each time. iFetch
decides what to skip from the manifest and sync state in the destination root;
an empty destination is a first run by definition. Confirm
`~/ifetch/icloud/.ifetch_manifest.json` exists on the host after a run.

### Rate limits and 503s from Apple

Not container-specific — see
[troubleshooting.md](troubleshooting.md#rate-limiting-and-503-errors). But note
that `restart: "no"` in the compose file is deliberate: a restart loop against
a rate-limited account makes the problem permanent.

### Advanced Data Protection

If your account has ADP enabled, the container hits the same `HTTP 423 Missing
PCS cookies` wall as any other client, and clearing it needs an approval tapped
on a trusted device. Run `ifetch-auth doctor --online` to confirm that is what
you are looking at, then see
[the ADP caveat](troubleshooting.md#advanced-data-protection-adp-caveat).

## What is in the image

- Multi-stage build: wheels are compiled in a builder stage and only the
  installed tree is copied forward. The runtime layer has no compiler, no
  `git`, no `curl`.
- `python:3.12-slim-bookworm` base, plus `ca-certificates` (for self-hosted
  Healthchecks or ntfy behind a private CA) and `tini` (so `docker stop`
  interrupts a download cleanly rather than killing it mid-write).
- Runs as the non-root user `ifetch`; `WORKDIR` is `/data`.
- `cap_drop: ALL` and `no-new-privileges` in the compose file. iFetch writes
  files and makes outbound HTTPS connections; that is the whole of its
  privilege.

Images are published on every `v*` tag as `:X.Y.Z`, `:X.Y`, `:X` and `:latest`,
and on every push to `main` as `:edge`. Pin a version in production —
`:latest` moving under a scheduled job is not a surprise you want at 02:30.

## Related

- [Monitoring](monitoring.md) — Healthchecks, ntfy and webhooks by environment
  variable
- [Scheduling](scheduling.md) — cron and systemd, including the `docker compose
  run` form
- [Troubleshooting](troubleshooting.md) — the non-container failure modes
