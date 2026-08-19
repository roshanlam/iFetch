# The web UI

```sh
ifetch serve
```

```
iFetch web UI: http://127.0.0.1:8765/?t=rHdG_oDvW0g-U-feMkHAAd0...
The token is in that URL and nowhere else. Ctrl-C to stop.
```

Open the link. Everything happens in the browser: signing in (including the
two-factor code), picking a folder, watching the download, and reading the two
reports that are awkward to read in a terminal.

No extra dependencies — it is Python's own `http.server` and a single HTML file.
Nothing is fetched from the internet, so it works on a machine with no route
out.

## What it covers

| In the UI | CLI only |
|---|---|
| Sign in, including 2FA | `uplink`, `snapshot`, `conflicts` |
| Browse iCloud Drive | `repair`, `resume`, `sharecheck` |
| Download, with live progress | `export`, `mirror`, `photos` |
| `guard` — what your backups are missing | |
| `vanish` — what disappeared from iCloud | |

The write path (`ifetch uplink`) is deliberately not in the UI. Putting the one
operation that modifies your iCloud behind a one-click button deserves more care
than a first version can give it.

## Running it on a NAS

The server binds `127.0.0.1`, so the safe way to reach it from another machine
is an SSH tunnel:

```sh
# on the NAS
ifetch serve

# on your laptop
ssh -N -L 8765:127.0.0.1:8765 you@nas
```

Then open the URL the NAS printed, on your laptop, unchanged.

You *can* bind an interface directly with `--host 0.0.0.0`, and iFetch will warn
you about what that exposes. Two things it will still do, which you have to work
around deliberately:

- requests are refused unless the `Host` header is a loopback name, so you need
  `--allow-host nas.local` for whatever name you actually type;
- the connection is plain HTTP, so the token crosses the network in the clear.

The tunnel is better. It costs one command and neither problem applies.

## How access works

- The token is random per run and printed once. It is not stored, and there is
  no password to set.
- Opening the URL exchanges the token for an `HttpOnly`, `SameSite=Strict`
  cookie and drops it from the address bar, so the token does not linger in
  browser history or leak through a `Referer`.
- A request without the cookie gets **404**, not 401 — a 401 would confirm to a
  port scanner that something is here.
- Requests whose `Host` is not a loopback name are refused. Without that, any
  web page you happen to visit could drive this server through your browser
  while it holds a live iCloud session.
- **Your Apple ID password is never sent to the browser and never accepted from
  it.** The server reads it from your OS keyring, exactly as the CLI does. The
  page has no password field at all; the only credential it ever sends is the
  six-digit 2FA code.

Stopping the server invalidates the token. Restarting prints a new one.

## Reading the reports

Two things the UI is careful about, because a tidier screen would be a
misleading one:

**A total it does not know is shown as unknown.** Early in a download the file
count and byte total are genuinely not known yet, so the bar is indeterminate
and there is no percentage. It will not invent a denominator to have something
to draw.

**A check that could not finish is labelled, not rounded off.** The zero-blocks
test `guard` uses only works on macOS, folders can be unreadable, and some files
record no size. When any of that happens the report says so and the headline
number is described as a floor. Likewise, if `vanish` refuses a result because
the scan behind it recorded errors, the UI shows the refusal — not the file
list. Those files are not confirmed deletions, and showing them as a tidy list
of deletions is the exact mistake the refusal exists to prevent.

## Troubleshooting

**"This link has expired."** The token changed, which means `ifetch serve` was
restarted. Use the URL from the current run.

**The page loads but says it lost contact.** The server stopped. It retries on
its own, so restarting `ifetch serve` on the same port is enough.

**Sign-in asks for a 2FA code every time.** The trusted session is not being
kept. In Docker that is usually the session volume; see
[docker.md](docker.md).

**Port already in use.** `ifetch serve --port 0` takes any free port and prints
which one it got.
