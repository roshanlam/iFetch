# Monitoring a scheduled iFetch run

A backup you never look at is a backup you are guessing about. The failure that
actually happens is not a crash — it is silence: the job stops running in March
and nobody notices until July.

iFetch can tell three different systems what happened, and the point of having
three is that they answer different questions:

| Backend | Answers | When you want it |
| --- | --- | --- |
| **Healthchecks.io** | "did the run happen at all?" | Always. It is the only one that alerts on a run that *didn't* start. |
| **ntfy** | "what happened, on my phone" | When you want to be told, not to go looking. |
| **Generic webhook** | "here is JSON, do what you like" | Home Assistant, n8n, Discord relays, your own script. |

All three can be on at once. A backend that is down, misconfigured, slow or
returning garbage is logged as a warning and skipped — **a notification never
fails a run**.

## The one rule

`success`, `failure` and `anomaly` are three different things and iFetch keeps
them apart:

- **success** — the run completed and the summary had nothing in it.
- **failure** — the run did not complete. Authentication failed, the disk
  filled, the process died.
- **anomaly** — the run *completed* and found something: individual files that
  failed, a failed integrity check, findings from a recovery command.

An anomaly is not a failure. Sending both down the same channel is how people
learn to ignore the channel.

## Healthchecks.io (dead man's switch)

Healthchecks alerts when a ping *doesn't* arrive. That is the only signal that
survives the NAS being off, the disk being full, the cron entry being deleted,
or Python failing to import.

1. Create a check at [healthchecks.io](https://healthchecks.io) (or on your own
   instance — self-hosting is fully supported, nothing here is hardcoded to
   `hc-ping.com`).
2. Set its period to your schedule and give it a grace window longer than a
   normal run takes.
3. Hand iFetch the ping URL:

```sh
export IFETCH_HEALTHCHECKS_URL="https://hc-ping.com/0f9d1b2e-4c3a-4f1d-9a7b-2c8e5d6f0a11"
ifetch Documents /mnt/backup/icloud
```

Or with a flag:

```sh
ifetch Documents /mnt/backup/icloud \
  --healthchecks-url "https://hc-ping.com/0f9d1b2e-..."
```

Self-hosted, either form works:

```sh
export IFETCH_HEALTHCHECKS_URL="https://hc.lan.example.org/ping/0f9d1b2e-..."
# or, splitting the check UUID from the server:
export IFETCH_HEALTHCHECKS_BASE_URL="https://hc.lan.example.org/ping"
export IFETCH_HEALTHCHECKS_UUID="0f9d1b2e-4c3a-4f1d-9a7b-2c8e5d6f0a11"
```

### What gets pinged when

| Event | Request |
| --- | --- |
| run start | `POST <url>/start` |
| run success | `POST <url>` |
| run failure | `POST <url>/fail` |
| anomaly | `POST <url>/log` |

The `/start` ping is what makes Healthchecks able to show run durations, and
what makes "the run started but never finished" visible as a distinct state
from "the run never started".

`/log` records the message against the check **without turning it red**. That
is the honest mapping for "finished, but read this": Healthchecks has two
states and an anomaly is a third fact. If you would rather be paged for
anomalies too:

```sh
export IFETCH_HEALTHCHECKS_ANOMALY_FAILS=1
```

Every ping carries a short run summary in the body, which is what Healthchecks
shows in its UI and in the alert e-mail:

```
iFetch run finished

1204 files seen, 12 downloaded, 1192 unchanged, 0 failed, 214.7 MB transferred

total files: 1204
successful: 12
failed: 0
duration: 3m 41s

host: nas
run: 3d1c9f2a-...
```

## ntfy (push to a phone)

```sh
export IFETCH_NTFY_URL="https://ntfy.sh/my-private-ifetch-topic"
ifetch Documents /mnt/backup/icloud
```

On ntfy.sh, **the topic name is the whole of your security** — anyone who knows
it can read your notifications. Use a long random one, or self-host:

```sh
export IFETCH_NTFY_SERVER="https://ntfy.lan.example.org"
export IFETCH_NTFY_TOPIC="icloud-backup"
export IFETCH_NTFY_TOKEN="tk_..."     # for access-controlled topics
export IFETCH_NTFY_TAGS="nas,backup"  # appended to the per-event tags
```

Priority and tags follow the event, so a nightly success does not buzz the same
way a failure does:

| Event | Priority | Tag |
| --- | --- | --- |
| start | `min` | ⏳ |
| success | `default` | ✅ |
| anomaly | `high` | ⚠️ |
| failure | `urgent` | 🚨 |

Set `IFETCH_NTFY_PRIORITY` to pin every message to one priority instead.

## Generic webhook

Posts the event as JSON:

```sh
export IFETCH_WEBHOOK_URL="https://automation.lan/hooks/ifetch"
export IFETCH_WEBHOOK_HEADERS='{"X-Api-Key": "..."}'
```

```json
{
  "source": "ifetch",
  "event": "anomaly",
  "title": "iFetch run finished with findings",
  "message": "2 file(s) failed to download",
  "run_id": "3d1c9f2a-...",
  "host": "nas",
  "timestamp": 1753670000.0,
  "timestamp_iso": "2026-07-28T02:33:20Z",
  "details": {
    "total_files": 1204,
    "successful": 10,
    "failed": 2,
    "skipped": 1192,
    "total_bytes_transferred": 225116160,
    "duration": "3m 41s",
    "findings": ["2 file(s) failed to download"]
  }
}
```

`IFETCH_WEBHOOK_HEADERS` accepts a JSON object, or repeated `--webhook-header
'Key: Value'` flags. A header that cannot be parsed is dropped with a warning
rather than guessed at — sending an authorization header you did not write
would be worse than sending none. `IFETCH_WEBHOOK_METHOD` changes the verb if
your endpoint insists on `PUT`.

## Configuration reference

Flags win over environment variables; environment variables win over defaults.
Nothing configured means nothing is sent, silently — you are not warned about a
feature you did not ask for.

| Environment variable | Flag | Meaning |
| --- | --- | --- |
| `IFETCH_HEALTHCHECKS_URL` | `--healthchecks-url` | Full ping URL, any host |
| `IFETCH_HEALTHCHECKS_UUID` | `--healthchecks-uuid` | Check UUID, combined with the base URL |
| `IFETCH_HEALTHCHECKS_BASE_URL` | `--healthchecks-base-url` | Base for the UUID form (default `https://hc-ping.com`) |
| `IFETCH_HEALTHCHECKS_ANOMALY_FAILS` | `--healthchecks-anomaly-fails` | Report anomalies as check failures instead of log entries |
| `IFETCH_NTFY_URL` | `--ntfy-url` | Full ntfy topic URL |
| `IFETCH_NTFY_TOPIC` | `--ntfy-topic` | Topic name, combined with the server |
| `IFETCH_NTFY_SERVER` | `--ntfy-server` | ntfy server (default `https://ntfy.sh`) |
| `IFETCH_NTFY_TOKEN` | `--ntfy-token` | Access token, sent as `Authorization: Bearer` |
| `IFETCH_NTFY_PRIORITY` | `--ntfy-priority` | Pin every message to one priority |
| `IFETCH_NTFY_TAGS` | `--ntfy-tags` | Extra comma-separated tags |
| `IFETCH_WEBHOOK_URL` | `--webhook-url` | POST the event JSON here |
| `IFETCH_WEBHOOK_HEADERS` | `--webhook-header` (repeatable) | Extra headers; the env form takes a JSON object |
| `IFETCH_WEBHOOK_METHOD` | — | HTTP verb for the webhook (default `POST`) |
| `IFETCH_NOTIFY_TIMEOUT` | `--notify-timeout` | Seconds before a request is abandoned (default 5) |
| `IFETCH_NOTIFY_RETRIES` | `--notify-retries` | Retries after the first attempt (default 2) |
| `IFETCH_NOTIFY_BACKOFF` | — | Base seconds for exponential backoff (default 1) |
| `IFETCH_NOTIFY_DISABLED` | `--no-notify` | Send nothing, whatever else is set |

## What "must never fail the run" actually means

Every request has a short timeout (5 seconds by default) and a bounded number
of retries. Connection errors, timeouts, 5xx and 429 are retried with
exponential backoff; 4xx is not, because a 404 means the check does not exist
and asking three times will not create it.

Beyond that, every possible outcome — a dead endpoint, a hung socket, an HTML
error page where JSON was expected, an outright bug in a backend — is caught,
logged as one warning, and recorded. Worst case a notification is missed; the
download is never affected.

Anything that could not be delivered is said out loud rather than silently
dropped. The run's JSON report carries the outcome of every attempt:

```json
"notifications": {
  "enabled": true,
  "run_id": "3d1c9f2a-...",
  "backends": ["healthchecks -> https://hc-ping.com/***", "ntfy -> https://ntfy.sh/***"],
  "deliveries": [
    {"backend": "healthchecks", "event": "start", "delivered": true, "attempts": 1, "status_code": 200},
    {"backend": "ntfy", "event": "start", "delivered": false, "attempts": 3, "error": "HTTP 503"}
  ],
  "undelivered": 1
}
```

## Your ping URLs are credentials

A Healthchecks ping URL is not a location, it is an authorisation: anyone
holding it can mark your check up and suppress your alerts forever. The same
goes for an ntfy topic URL on a public server, a Slack-style webhook URL, and
any token you set.

iFetch treats all of them as secrets. They never appear in a log line, in
`download_report.json`, or in an exception message — including the exception
messages `requests` builds, which embed the full request URL and are the path
that actually leaks in most tools. What you see instead keeps the scheme and
host, because "my self-hosted box is unreachable" needs to stay diagnosable,
and destroys the rest:

```
Notification not delivered (healthchecks, event=start, attempts=3): ConnectionError: ... url: ***
```

Store them the way you store the Apple ID password — in an env file with
restrictive permissions, a Docker secret, or your secret manager — not on the
command line, where they land in shell history and `ps`.

## Putting it together

A cron entry that is actually monitored:

```cron
30 2 * * * ICLOUD_EMAIL=you@example.com \
  IFETCH_HEALTHCHECKS_URL=https://hc-ping.com/0f9d1b2e-... \
  IFETCH_NTFY_URL=https://ntfy.sh/my-private-ifetch-topic \
  /home/you/venv/bin/ifetch Documents /mnt/backup/icloud --log-file /var/log/ifetch.log
```

For systemd, put the variables in an `EnvironmentFile=` with mode `600` rather
than inline in the unit — unit files are world-readable. See
[scheduling.md](scheduling.md) for the full unit, and [docker.md](docker.md)
for the container equivalent.

## Related

- [Scheduling](scheduling.md) — launchd, cron, systemd, `ifetch-mirror --watch`
- [Docker](docker.md) — the same configuration, as environment variables
- [Troubleshooting](troubleshooting.md#session-expiry) — the failure most
  likely to trigger your first alert
