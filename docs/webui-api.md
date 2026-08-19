# Web UI API contract

The contract between `ifetch/webui/server.py` (backend) and
`ifetch/webui/static/` (frontend). Both sides are written against this page;
changing it means changing both.

JSON in, JSON out, everything under `/api`. All responses carry
`Content-Type: application/json`. Errors are an HTTP status plus
`{"ok": false, "error": "<one sentence a person can act on>"}`.

## Access control

`ifetch serve` prints a URL containing a random token:

```
http://127.0.0.1:8765/?t=k3nf9a2b...
```

- Loading `/` with a valid `?t=` sets an `HttpOnly`, `SameSite=Strict` cookie
  and redirects to `/`. Every later request is authorised by that cookie.
- A request with no valid token gets **404**, not 401. A 401 confirms the
  server is there; a 404 tells a port scanner nothing.
- Token comparison uses `hmac.compare_digest`.
- The server binds `127.0.0.1` unless `--host` is passed, which prints a
  warning naming what is being exposed.
- Requests whose `Host` header is not a loopback name are rejected. Without
  that, any web page you visit can drive this server through your browser
  (DNS rebinding), and it holds an authenticated iCloud session.
- No `Access-Control-Allow-Origin` header is ever sent.
- **The Apple ID password is never sent to the browser and never accepted from
  it.** The server resolves it from the OS keyring or `--password-command`,
  exactly as the CLI does. The browser only ever sends the 2FA code.

## `GET /`

The single page. `Content-Type: text/html`.

## `GET /api/state`

The one endpoint the page polls. Everything the UI renders comes from here.

```json
{
  "version": "1.0.0",
  "auth": {
    "state": "signed_out | needs_2fa | signed_in | error",
    "email": "you@example.com or null",
    "message": "human sentence, may be empty",
    "expires_in_days": 12
  },
  "job": null,
  "last": null,
  "paths": {"default_local": "/Users/you/icloud-backup",
            "icloud_drive": "/Users/you/Library/Mobile Documents/com~apple~CloudDocs or null"}
}
```

`job` is the run in progress, `last` the most recently finished one. Both use
this shape:

```json
{
  "id": "j3",
  "kind": "download | guard | vanish",
  "state": "running | done | failed | cancelled",
  "label": "Documents -> ~/icloud-backup",
  "started_at": 1785270000.0,
  "finished_at": null,
  "message": "",
  "progress": {
    "files_done": 312, "files_total": 400,
    "bytes_done": 1503238553, "bytes_total": 2362232012,
    "skipped": 47, "failed": 0,
    "current": "Documents/Photos/IMG_0042.heic"
  },
  "result": null
}
```

`files_total` and `bytes_total` are `null` until known — the frontend must show
an indeterminate bar rather than inventing a denominator. `result` is populated
only when `state` is `done`, and holds the same payload the matching CLI writes
with `--json`.

## Sign in

`POST /api/auth/start` `{"email": "you@example.com"}`

→ `{"ok": true, "state": "needs_2fa" | "signed_in", "message": "..."}`

`POST /api/auth/2fa` `{"code": "123456"}`

→ `{"ok": true, "state": "signed_in" | "error", "message": "..."}`

A wrong code returns `{"ok": false, "error": ...}` with the state still
`needs_2fa`, so the user can retry without starting over.

`POST /api/auth/signout` → `{"ok": true}`

## Browse

`GET /api/browse?path=Documents` (omit `path` for the root)

```json
{
  "path": "Documents",
  "parent": "",
  "entries": [
    {"name": "Photos", "kind": "dir",     "size": null},
    {"name": "Deck.key", "kind": "package", "size": 4820213},
    {"name": "notes.txt", "kind": "file",  "size": 1024}
  ]
}
```

Directories sort before files, each alphabetically, case-insensitive. `parent`
is `null` at the root.

## Start work

Each returns `{"ok": true, "job_id": "j4"}` and 409 with an explanatory error
when a job is already running.

| Endpoint | Body |
|---|---|
| `POST /api/download` | `{"icloud_path": "Documents", "local_path": "/Users/you/backup"}` |
| `POST /api/guard` | `{"local_path": "/Users/you/Library/Mobile Documents/com~apple~CloudDocs"}` |
| `POST /api/vanish` | `{"local_path": "/Users/you/backup"}` |

`POST /api/cancel` → `{"ok": true}`. Cancelling a download stops it between
files; partial files stay resumable, which is what the existing transfer
journal is for.

## Reading a report

Reports are returned in `job.result` and are the same JSON the CLIs emit.
The two the UI renders specially:

- **guard** — `ifetch.guard.GuardReport.to_dict()`. The headline is the byte
  count that exists only on Apple's servers. If `complete` is false the UI must
  say the check was partial rather than showing a reassuring number.
- **vanish** — `ifetch.vanished.VanishedReport.to_dict()`. If the circuit
  breaker tripped the UI must show the refusal instead of the findings, because
  those findings are not deletions and presenting them as such is the exact
  mistake the breaker exists to prevent.
