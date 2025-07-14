# iFetch

A robust Python utility for efficiently downloading files and folders from iCloud Drive, designed for bulk data recovery and migration. This tool helps users easily retrieve their data from iCloud Drive when Apple's native solutions are insufficient.

## Features

- 🔐 **Secure authentication** with 2FA/2SA support
- 📁 **Recursive directory listing & downloading**
- ⚡ **Parallel downloads** with configurable worker count
- 🔄 **Differential (“delta”) updates**: only changed chunks are fetched
- ⏸️ **Resume-capable downloads** with checkpointing
- 🔁 **Exponential backoff & retry logic** for robust transfers
- 📝 **Structured JSON logging** (console + optional file)
- 📊 **Download summary report** (successes, failures, stats)
- 🔍 **Directory listing mode** (without downloading)
- 🤝 **Shared-folder support** (`--list-shared`, download shared items)
- 🧩 **Plugin system** – hook into authentication, progress & completion events
- 🗂 **Profile-based include/exclude filters** for personalised sync sets
- 🗄 **On-disk version history & rollback** with automatic archiving of prior versions

## Why This Tool?

While iCloud Drive provides seamless cloud storage on Apple platforms, bulk-downloading entire folders or massive archives can be cumbersome or unreliable. iFetch addresses common scenarios such as:

- Recovering data after disabling iCloud Drive
- Migrating large datasets between accounts
- Creating local backups of selected directories
- Efficiently syncing only what’s changed

## Installation

1. Create and activate a virtual environment:
```sh
python3 -m venv ivenv
source venv/bin/activate
```

2. Install Python dependencies:
```sh
pip install pyicloud tqdm requests keyring
```

3. Install system keyring dependencies:
For Ubuntu/Debian:
```sh
sudo apt-get install python3-keyring
```

For macOS:
```sh
brew install python-keyring
```

4. Configuration
Store your iCloud credentials securely:
```sh
icloud --username=your@email.com
```
The tool will prompt for your password and store it securely in your system's keyring.


## Usage
```sh
python ifetch/cli.py "<icloud_path>" "[local_path]" [options]
```
* <icloud_path>: remote iCloud Drive path, e.g. Documents/MyFolder

* [local_path]: local destination directory (default: current directory)

### List Directory Contents
View the contents of an iCloud Drive directory:
```sh
python ifetch/cli.py Documents --list
```

### Download Files/Folders
Download a specific directory or file:
```sh
python ifetch/cli.py Documents/Photos ~/Downloads/icloud-photos
python ifetch/cli.py Documents/Programming ~/LocalDoc/Programming
```

### Download with custom settings
Download a specific directory or file with custom settings:
```sh
python cli.py Documents/Programming ~/Work/Code \
  --email=you@apple.com \
  --max-workers=8 \
  --max-retries=5 \
  --chunk-size=2097152 \
  --log-file=download.log
```

## Available options
| Flag                    | Description                                                      | Default         |
| ----------------------- | ---------------------------------------------------------------- | --------------- |
| `--email`               | iCloud account email (or set `ICLOUD_EMAIL` env var)             | (env / prompt)  |
| `--max-workers N`       | Number of concurrent download threads                            | 4               |
| `--max-retries N`       | Retry attempts per failed chunk (with exponential backoff)       | 3               |
| `--chunk-size BYTES`    | Byte size for each differential-download chunk                   | 1 MB            |
| `--log-file PATH`       | Path to save structured JSON logs                                | (console only)  |
| `--list`                | List contents only (no downloads)                                | off             |
| `--list-shared`         | List top-level items shared *with* you                            | off             |
| `--profile NAME`        | Apply include/exclude patterns from profile file                  | (no filter)     |
| `--profile-file PATH`   | Custom path to profile JSON (defaults to `~/.ifetch_profiles.json`) | default path    |

### List Shared Items
List everything that has been shared with your account (no path needed):
```sh
python ifetch/cli.py --list-shared --email you@apple.com
```

### Use a Profile
Create a profile file (JSON) – default location `~/.ifetch_profiles.json`:

```json
{
  "pdf_backup": {
    "include": ["Documents/**/*.pdf"],
    "exclude": ["Documents/Private/*"]
  }
}
```

Download only PDFs according to the above profile:
```sh
python ifetch/cli.py Documents ~/PDFs \
  --profile pdf_backup \
  --email you@apple.com
```

Specify another profile file:
```sh
python ifetch/cli.py Documents ~/PDFs \
  --profile pdf_backup \
  --profile-file ./my_profiles.json \
  --email you@apple.com
```

### Extend with Plugins
Drop a Python file inside a `plugins/` folder:

```python
from ifetch.plugin import BasePlugin

class Notify(BasePlugin):
    def after_download(self, remote_item, local_path, success, **kw):
        if success:
            print(f"Downloaded {remote_item.name} → {local_path}")
```
iFetch auto-discovers plugins on startup.

## Contributing
Contributions are welcome! Please feel free to submit a Pull Request.
License
This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments
pyicloud for the excellent iCloud API wrapper
tqdm for the progress bar functionality

## Troubleshooting

### Authentication Issues
* Ensure your Apple ID and password are correct
* For 2FA, make sure you have access to your trusted devices


### Download Problems
* Check your internet connection
* Verify you have sufficient local storage
* Ensure your iCloud Drive is properly synced
