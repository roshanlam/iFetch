# Writing iFetch plugins

iFetch has a deliberately small plugin system: subclass `ifetch.plugin.BasePlugin`, override the callbacks you care about, drop the file in a plugin directory, done. Plugins let you send notifications, index downloads, verify integrity, or bridge iFetch into other systems without touching core code.

## How plugins are discovered

At startup, iFetch's `PluginManager` scans these directories for `*.py` files:

1. The `plugins/` directory that sits **next to the `ifetch` package** (i.e. the project root — this repo ships `plugins/example_local_indexer.py` as a working example).
2. The directory named by the `IFETCH_PLUGIN_PATH` environment variable, if set.

For each file, the first `BasePlugin` subclass found is instantiated (with no arguments). Important consequences:

- **One plugin class per file.** If a file defines several subclasses, only the first is used — put each plugin in its own file.
- **Constructors take no arguments.** Read configuration from environment variables or a config file in `__init__`.
- A file that fails to import, or a class that fails to instantiate, is silently skipped — iFetch keeps running.

## The hooks

All hooks are optional no-ops on `BasePlugin`; override only what you need. **Hooks are invoked with keyword arguments**, and every hook receives a `**kwargs` catch-all so future iFetch versions can add fields without breaking your plugin — always keep `**kwargs` in your signatures.

| Hook | Signature | Fires when |
|---|---|---|
| `on_authenticated` | `(self, downloader, **kwargs)` | iCloud authentication (incl. 2FA) succeeded. `downloader` is the live `DownloadManager` — you can reach `downloader.api` (the pyicloud session) from here. |
| `on_list_contents` | `(self, path, contents, **kwargs)` | A directory was listed (`--list`). `contents` is a list of `{"name": ..., "type": "file"|"folder"}` dicts. |
| `before_download` | `(self, remote_item, local_path, **kwargs)` | A file is about to start downloading. `remote_item` is the pyicloud drive node (has `.name`, `.size`, ...); `local_path` is a `pathlib.Path`. |
| `after_download` | `(self, remote_item, local_path, success, **kwargs)` | A file finished downloading — or failed (`success=False`). |
| `on_event` | `(self, name, **payload)` | Generic events not covered above. |

### `on_event` names currently emitted

| `name` | Payload |
|---|---|
| `download_progress` | `remote_item`, `local_path`, `downloaded` (bytes so far), `total_size` — emitted as chunks stream in |
| `download_session_completed` | `summary` — the same dict written to `download_report.json` |

Handle `on_event` defensively (`payload.get(...)`) since names and fields can grow over time.

### Error handling

Exceptions raised inside a plugin callback are **swallowed by the dispatcher** so a buggy plugin can never corrupt or abort a transfer. The flip side: you will not see a traceback. During development, wrap your hook bodies in your own `try/except` and log to a file.

## Example 1: ntfy / webhook notifications

Sends a push notification via [ntfy.sh](https://ntfy.sh) (or any webhook that accepts a POST) when a download session finishes, plus an immediate alert on every failed file. Configure with environment variables so no code changes are needed.

`plugins/ntfy_notifier.py`:

```python
"""Push notifications for iFetch via ntfy.sh (or any POST webhook).

Configuration (environment variables):
    IFETCH_NTFY_URL    e.g. https://ntfy.sh/my-ifetch-topic  (required to activate)
"""
import os
import requests

from ifetch.plugin import BasePlugin


class NtfyNotifier(BasePlugin):
    def __init__(self):
        self.url = os.environ.get("IFETCH_NTFY_URL")

    def _post(self, title: str, message: str, priority: str = "default"):
        if not self.url:
            return  # not configured -> stay silent
        try:
            requests.post(
                self.url,
                data=message.encode("utf-8"),
                headers={"Title": title, "Priority": priority},
                timeout=10,
            )
        except requests.RequestException:
            pass  # never let notification failures matter

    def after_download(self, remote_item, local_path, success, **kwargs):
        if not success:
            name = getattr(remote_item, "name", "unknown")
            self._post("iFetch: download failed", f"{name} -> {local_path}", "high")

    def on_event(self, name, **payload):
        if name == "download_session_completed":
            s = payload.get("summary", {}).get("summary", payload.get("summary", {}))
            msg = (
                f"Files: {s.get('total_files', '?')}, "
                f"ok: {s.get('successful', '?')}, "
                f"failed: {s.get('failed', '?')}, "
                f"transferred: {s.get('total_bytes_transferred', 0) / (1024*1024):.1f} MB"
            )
            self._post("iFetch: sync finished", msg)
```

Usage:

```sh
export IFETCH_NTFY_URL=https://ntfy.sh/your-secret-topic
ifetch Documents ~/icloud-backup
```

For Slack/Discord/other webhooks, swap the `requests.post` body for the JSON payload your endpoint expects.

## Example 2: checksum manifest verifier

Maintains a SHA-256 manifest of everything iFetch downloads. On each run it (a) records fresh checksums for newly downloaded files, and (b) alerts if a file that iFetch did *not* re-download has changed on disk since the last run — catching local corruption or tampering between syncs.

`plugins/checksum_verifier.py`:

```python
"""SHA-256 manifest + verification for iFetch downloads.

Writes ~/.ifetch_checksums.json mapping local paths to checksums.
On session completion, re-verifies previously recorded files and
appends any mismatches to ~/.ifetch_checksum_alerts.log.
"""
import hashlib
import json
from pathlib import Path

from ifetch.plugin import BasePlugin

MANIFEST = Path.home() / ".ifetch_checksums.json"
ALERT_LOG = Path.home() / ".ifetch_checksum_alerts.log"


def sha256_of(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


class ChecksumVerifier(BasePlugin):
    def __init__(self):
        self.manifest = {}
        if MANIFEST.exists():
            try:
                self.manifest = json.loads(MANIFEST.read_text())
            except json.JSONDecodeError:
                self.manifest = {}
        self.updated_this_run = set()

    def after_download(self, remote_item, local_path, success, **kwargs):
        if not success:
            return
        p = Path(local_path)
        try:
            self.manifest[str(p)] = sha256_of(p)
            self.updated_this_run.add(str(p))
            MANIFEST.write_text(json.dumps(self.manifest, indent=2))
        except OSError:
            pass

    def on_event(self, name, **payload):
        if name != "download_session_completed":
            return
        # Verify files we did NOT touch this run: their on-disk checksum
        # should still match the manifest from the previous run.
        for path_str, expected in list(self.manifest.items()):
            if path_str in self.updated_this_run:
                continue
            p = Path(path_str)
            if not p.exists():
                continue  # deleted locally; not an integrity failure
            try:
                actual = sha256_of(p)
            except OSError:
                continue
            if actual != expected:
                with ALERT_LOG.open("a") as f:
                    f.write(
                        f"CHECKSUM MISMATCH {path_str}: "
                        f"expected {expected}, got {actual}\n"
                    )
```

Run any sync and then check `~/.ifetch_checksum_alerts.log` — an entry there means a file changed on disk without iFetch downloading it.

## Tips

- **Keep hooks fast.** `before_download`/`after_download` run on the download worker threads; slow hooks slow the transfer. Offload heavy work to a queue/thread if needed.
- **Hooks may run concurrently** (iFetch downloads with a thread pool) — guard shared state with a lock if you mutate it from `before_download`/`after_download`/`download_progress`.
- **Test in isolation**: `IFETCH_PLUGIN_PATH=/path/to/dev-plugins ifetch Documents --list` loads your plugin without installing it into the repo's `plugins/` directory.
- The shipped example plugin, `plugins/example_local_indexer.py`, appends every successful download to `~/.ifetch_index.txt` — a minimal template to copy from.
