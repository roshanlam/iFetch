# Changelog

All notable changes to iFetch will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

> **Note:** This is the first structured changelog for iFetch. The entries
> below summarize the project's git history to date; earlier changes were not
> tracked in changelog form.

## [Unreleased]

### Fixed

- **Files in a folder shared by someone else now download**
  ([#15](https://github.com/roshanlam/iFetch/issues/15)). Before this, the
  shared folder itself opened fine and every file inside it failed with a 404.

  Apple attaches the share's ID to the top folder only, and leaves it off
  everything inside. iFetch was reading that ID off each file, found nothing,
  and asked Apple for the file without saying which share it belonged to.
  iFetch now passes the ID down as it walks into the folder. The same gap is
  why rclone can only read the top level of a shared folder
  ([rclone#9477](https://github.com/rclone/rclone/issues/9477)).

  Still tested against saved copies of Apple's replies, not a real second
  account. `docs/shared-folder-validation.md` is the check that would confirm
  it for real.

- **A failed shared download now says what it tried.** iFetch has three ways to
  fetch a shared file and used to discard the reason each one failed, so a bug
  report of "still 404" gave nothing to work with. Each attempt is now logged
  with its result.

### Added

- **`ifetch guard` — find the files your backups are quietly skipping.** With
  "Optimize Mac Storage" turned on, macOS removes the contents of files you
  have not opened recently and leaves the name, date and size behind. Finder
  still shows a 4 GB video; the disk holds nothing. Time Machine, Backblaze,
  Arq and rsync all copy those empty files and report success, and you find out
  when a restore comes back blank.

  `ifetch guard` shows how much of the folder is really on your disk and how
  much only exists in iCloud. `--materialize --apply` downloads the missing
  files from iCloud directly.

  It will not tell you a folder is fine if it could not check properly: the
  zero-blocks test only works on macOS, so on Linux and Windows it says which
  check it skipped instead of reporting zero. Folders it could not read are
  listed. And because `brctl download` reports success for work it has only
  queued, every file is re-checked afterwards and anything still missing is
  named.

- **`ifetch vanish` — find out when files disappear from iCloud.** iCloud empties
  the Trash after about 30 days, so a bad sync or an accidental delete is
  usually noticed too late. Since iFetch only downloads, its copy keeps
  everything it has ever seen, which is what makes this possible.

  It sorts what is gone into three groups, because each needs a different
  response: gone from iCloud but still on your disk, gone from both, or gone
  from iCloud with only an empty placeholder left locally — which looks fine in
  Finder and is not.

  The purge date is a deadline at the latest, not an exact date, and the report
  says so: iFetch knows when it first noticed a file missing, not when it was
  deleted. If a lot of files vanish at once it refuses to call them deletions,
  because a failed listing or a drive that did not mount looks exactly the
  same. A scan that hit errors triggers that refusal even when nothing appears
  to be missing.

- **Advanced Data Protection accounts now work.** If you have ADP turned on,
  iFetch does Apple's approval handshake, saves the result with your session so
  later runs do not ask again, and tells you exactly what went wrong instead of
  showing `HTTP 423` — whether "Access iCloud Data on the Web" is off, an
  approval was never tapped, or the session needs renewing. It gives up after a
  set time rather than waiting forever, so a scheduled backup cannot hang.

  Tested against saved copies of Apple's replies. Turning ADP on needs a
  physical device and a recovery key, so this has not been run against a real
  ADP account.

- **`--bwlimit` — stop overnight runs from using the whole connection.** Takes a
  single speed (`512k`, `10M`) or a schedule
  (`08:00,512k 13:00,off 18:00,30M`), same format as rclone. The limit covers
  the whole run rather than each worker separately, and changes as the clock
  passes each time you set. A typo is rejected when you start, not hours in.

- **Notifications** for scheduled and NAS setups: Healthchecks.io, ntfy, and
  plain webhooks, set by flag or environment variable. A run that finished with
  some files failing is reported separately from a run that did not finish, so
  the alerts stay worth reading. Ping URLs and tokens are kept out of logs and
  reports. A notification service being down can never fail your backup.

- **`ifetch serve` — a web UI, for people who would rather not use a terminal.**
  Run it and it prints a link; open the link and you can sign in (2FA included),
  browse iCloud Drive, download a folder while watching the progress, and read
  the guard and vanish reports. No terminal after the first command.

  It uses Python's own web server and a single HTML file, so it adds no
  dependencies and fetches nothing from the internet — it works on a machine
  with no route out. That also means it runs fine on a headless NAS; reach it
  over an SSH tunnel. See [docs/webui.md](docs/webui.md).

  It binds to localhost only and prints a one-time access token in the URL.
  **Your Apple ID password is never sent to the browser and never accepted from
  it** — the server reads it from your keyring exactly as the CLI does, and the
  page has no password field at all. The only credential it ever sends is the
  six-digit code.

  The screen is careful in the same way the reports are. A total it does not
  know yet is shown as unknown, with an indeterminate bar and no percentage,
  rather than a made-up denominator. A guard check that could not examine
  everything is labelled partial instead of showing a reassuring number. And if
  vanish refuses a result because the scan behind it recorded errors, the UI
  shows the refusal rather than the file list — those files are not confirmed
  deletions, and listing them as though they were is the mistake the refusal
  exists to prevent.

  `ifetch uplink` is deliberately not in the UI. The one operation that writes
  to your iCloud deserves more care than a first version can give a button.

- **`ifetch uplink` — put files back into iCloud when iCloud lost them.** A
  backup you cannot restore from is a filing cabinet, and until now the answer
  to "iCloud lost my folder" was "drag it back in Finder".

  This is deliberately narrow: **it uploads only files iCloud does not have, and
  never overwrites, deletes or renames anything.** It is not two-way sync and is
  not a step toward it. `ifetch uplink plan` shows what would be sent and
  transfers nothing; `push --apply` sends it.

  The failure it guards hardest against is the one that would hurt most: if the
  listing of your iCloud folder fails or comes back short, *every* local file
  looks missing, and a naive version would upload your whole mirror back into
  the account. So a scan that recorded errors refuses the entire run, even for a
  single file, and so does a disappearance large enough to be suspicious on its
  own.

  It also refuses, and names, each of these rather than sending them: a file
  that has been emptied by "Optimize Mac Storage" (uploading it would put an
  empty file in iCloud under a real name), a file that no longer matches the
  checksum recorded when it was downloaded, anything resolving outside your
  folder, and Apple packages like `.key` and `.pages`, which are folders on disk
  that Apple expects as a single archive — a mangled Keynote is worse than an
  honest refusal.

  Everything is re-checked in the moment before each file is sent, because a
  plan can be minutes old: if the file has reappeared in iCloud meanwhile it is
  skipped, not overwritten. One file failing does not stop the rest, and every
  success is recorded so an interrupted run picks up where it stopped and a
  repeated one does nothing.

- **`ifetch-sharecheck` — validate shared folders in one command.** Checking
  that iFetch really works with a folder someone else shared with you needs a
  second Apple ID, so it has always been a manual fifteen-minute checklist in
  `docs/shared-folder-validation.md`. A checklist that long does not get run.
  This is the same procedure as one command, ending with a verdict and a row to
  paste into that document.

  It is read-only: it lists and downloads into a temporary folder and deletes it
  afterwards. Nothing is written to your iCloud.

  Two things it is careful about. **A step that did not run is never counted as
  a pass** — it exits 3, not 0, so a skipped check cannot be mistaken for a
  clean one. And **pointing it at a folder you own stops the run** instead of
  passing every step against your own files and reporting a validation it did
  not earn; `--assume-shared` overrides that.

  It also checks the fix directly, not just the symptom. Downloading a subfolder
  successfully could happen for the wrong reason, so one step asks whether the
  share ID actually reached a file two levels down and whether it was inherited.
  If Apple happened to supply it, the step passes and says the fix was never
  exercised.

- **Docker image** — `Dockerfile`, `docker-compose.yml`, and a workflow that
  publishes to GHCR. See [docs/docker.md](docs/docker.md); the volume setup
  matters, or the container asks for a 2FA code every time it starts.

- **`ifetch repair` and `ifetch resume`** — finish a download that was cut off,
  without starting over. A download now keeps a running record of what it is
  transferring, written to disk as it goes, so a run that is killed — a closed
  laptop, a dropped connection, a power cut — leaves a trace of exactly what was
  in flight. `ifetch resume` picks up only those unfinished files and opens each
  one directly, instead of re-listing your whole drive to find the three that
  did not finish.

  `ifetch repair` is the part you can run with no connection at all. It reports
  what was interrupted, what failed and why, files that have now failed several
  times over, leftover partial files from an older run, and — with
  `--check-digests` — files whose contents no longer match the checksum recorded
  when they were downloaded. `--apply` queues those files for a fresh fetch;
  iFetch cannot rebuild a missing piece locally, so repairing means letting the
  next run get it. A half-downloaded file that is provably wrong is thrown away
  rather than resumed from, and a file that no longer matches its checksum is
  never quietly overwritten — `ifetch-restore` can bring back an archived copy.
  The record is best-effort: a folder where it cannot be kept still downloads
  exactly as before.

- **`ifetch conflicts`** (`renames`, `duplicates`, `moves`) — spot files that
  only changed name, so they are not downloaded or stored twice. Renaming a
  folder in iCloud used to mean downloading all of it again;
  `ifetch conflicts renames --apply` moves your local copies instead.

  The three checks differ in how sure they can be, and the reports say which is
  which. `duplicates` and `moves` compare checksums, so they are certain.
  `renames` cannot be: Apple publishes no checksums, so a file in iCloud can
  only be matched on name and size. Each match is labelled `strong` (same name
  and size, unique on both sides) or `weak` (same size only), and if two
  answers fit equally well it lists both instead of picking one. `--apply` acts
  on strong matches only.

  Files it could not check — packages, files iCloud gave no size for, empty
  files — are counted and explained, so "none found" never quietly means "I
  could not look". Before each move it re-checks the disk, refuses to overwrite
  anything already there, refuses any path leading outside your folder, and
  updates its records so `ifetch-verify` does not then report the file missing.

- **`ifetch snapshot`** (`create`, `list`, `diff`, `restore`, `delete`) — dated
  records of what your copy looked like. A snapshot stores names and checksums,
  not the files themselves, so taking one is instant and costs kilobytes no
  matter how big the folder is. That is what makes taking one before every
  risky sync practical.

  `diff` compares two dates by checksum, so a file that changed without
  changing size still shows up. `restore` is a preview by default and says
  where each file would come from: `.versions` (the exact old copy), iCloud
  (the current version, which may differ), or nowhere — and files in that last
  group are named rather than left out, because quietly dropping them would
  suggest a full restore is possible. Applying a restore keeps what it
  replaces, so it can be undone.

- **`ifetch recover placeholders`** — find local files whose contents are not
  actually on the disk. A file iCloud has emptied still shows a size in Finder
  but holds nothing, so copying it to a backup drive copies nothing and any
  size total or checksum over that folder is wrong.

  Two signs are checked and each is reported with how sure it is: a
  `.name.ext.icloud` stub (certain, and works on any operating system because
  the stub is a real file — which matters when reading a drive pulled out of a
  Mac), and a file with a size but no disk blocks (likely, macOS only). **Checks
  that cannot run on your system are named**, so the report never says "none
  found" when it means "I could not look".

- **`ifetch recover missing`** — what iCloud has that your disk does not really
  have, split into three kinds because each needs a different response: never
  downloaded, gone (an earlier run recorded it and now it is missing — you lost
  data), and placeholder only. A file missing from both the disk and the last
  iCloud scan is called out, with a pointer to `.versions`.

- **`ifetch recover inventory`** — where the space went, grouped by folder, by
  file type and by largest item, with your Apple storage figures if available.
  Everything exports to CSV, because "which folders should I stop paying for?"
  is a sorting question.

- **`ifetch plan`** — a preview of exactly what a download would do before it
  does any of it: how many files would be fetched and overwritten, the total
  size, whether the destination has room, what is being skipped and why, which
  local files are not in iCloud (only reported, never deleted), and roughly how
  long it will take. It transfers nothing. The time estimate comes only from
  speeds measured on earlier runs or from `--throughput`; with neither, it says
  unknown instead of guessing.

- **`ifetch audit`** — the same comparison shown as "what is in iCloud versus
  what is here", exiting non-zero when the two disagree so you can run it from a
  monitoring job.

- **Scanning without downloading** (`ifetch/scanner.py`). Listing and downloading
  used to be one pass, which made it impossible to say anything about a job
  before running it. `RemoteScanner` walks iCloud reading only the details Apple
  already includes in folder listings — one request per folder, no file contents
  — and `LocalScanner` indexes your copy, reusing checksums it already has where
  size and date still match, so a rescan does not re-read a terabyte. A folder
  that fails to list is recorded as an error and the scan carries on, rather
  than costing you the inventory of everything else.

- **SQLite index** (`.ifetch_index.db`, `ifetch/index.py`). The old JSON files
  could answer "has this one file changed?" but nothing built on top of that:
  totalling a folder before downloading, comparing iCloud against your disk,
  dated snapshots, checksum lookups for rename detection, or per-file records
  that survive a crash. Those are now single lookups in one place, using only
  the standard library. Existing `.ifetch_state.json` and
  `.ifetch_manifest.json` are imported automatically the first time, once, and
  left untouched so an older iFetch keeps working. The manifest is still written
  as the readable, signable proof of integrity.

- **`--password-command`** (and `$IFETCH_PASSWORD_COMMAND`), for taking your
  Apple ID password from `pass`, the 1Password CLI, a mounted secret, or any
  command that prints it. Docker, systemd and NAS boxes have no system keyring,
  which used to leave the password as the one part of signing in that still
  needed a person. The command is split with `shlex` and run **without a
  shell**, so quoted paths with spaces work and it cannot be used for injection.

- **`--portable-names`**, cleaning up filenames for the strictest filesystem
  rather than just the one you are on — for destinations that are exFAT, a NAS
  share, or will later be read from Windows.

- **`--normalize-names {preserve,nfc}`** (default `preserve`). `nfc` writes
  accented characters the way Linux expects, at the cost of renaming anything
  already downloaded under the other spelling.

- **`ifetch auth`** (`doctor`, `renew`, `status`), also installed as
  `ifetch-auth`. `doctor` says which part of signing in failed and what to do
  about it — Apple's `HTTP 423 Missing PCS cookies`, `400 Invalid Session
  Token`, the `409`-on-a-valid-code case and the China redirect each get a named
  cause and a fix. Exit codes are `0` fine, `1` needs attention soon, `2` broken
  now, so a scheduled job can tell "renew sometime" from "act now".

- **Two-factor codes without a prompt.** A code can come from `--2fa-code`,
  `$IFETCH_2FA_CODE`, a file (`--2fa-file`), an HTTP endpoint
  (`--2fa-webhook`), or piped input. The file and endpoint are re-checked until
  `--2fa-timeout`. iFetch never tries to prompt on a terminal it cannot use, so
  a background job fails with a useful message instead of hanging forever.

- **Early warning before your session expires.** The real expiry is read from the
  stored login cookie (falling back to the session file's date plus 30 days,
  always labelled an estimate). Every run warns if the session expires within
  `--warn-days` (default 7); `ifetch auth status` reports the same in one line
  for scripts, and `ifetch auth renew --if-expiring-within N` does nothing on a
  healthy session.

- **Apple packages come back as folders**, on by default. `.key`, `.pages`,
  `.numbers`, `.band`, `.xcodeproj` and similar are really folders, and Apple
  sends them as ZIP files whose size never matches what the folder listing said.
  iFetch now unpacks them into real folders, keeps each item's date, and records
  them by file count and total size so later runs still skip unchanged ones.
  `--no-expand-packages` keeps the old raw-archive behaviour. Unpacking needs
  *both* a known package extension and a payload that really is a ZIP, so an
  ordinary `Archive.zip` is left alone.

- **Signed integrity manifest** (`.ifetch_manifest.json`). Apple publishes no
  checksums, so iFetch records its own SHA-256 for every file as it downloads.
  `ifetch-verify --offline` re-checks your copy and reports anything that
  drifted, with no password and no internet — catching corruption that leaves
  both size and date unchanged. `--sign-key`, `--sign-key-file` and
  `$IFETCH_MANIFEST_KEY` add a signature over the manifest;
  `--require-signature` makes a valid one mandatory. Unpacked packages are
  checked as a whole.

- **`--region china`** (and `$ICLOUD_REGION`) for Apple IDs registered in China
  Mainland, which are served by `iCloud.com.cn`. The old `ICLOUD_CHINA=true`
  environment variable still works.

- **Test harness for folders shared by another Apple ID**
  (`tests/test_shared_folder_contract.py`), replaying saved copies of Apple's
  replies including the `HTTP 400` on subfolders, plus
  `docs/shared-folder-validation.md` — the manual check against a real share
  between two accounts.

- **Metadata fast path.** A sync-state file (`.ifetch_state.json`) is written in
  the destination root recording, per file, the remote size and remote modified
  timestamp plus the local size and mtime at completion. On later runs a file is
  skipped with **zero network round-trips** when all of those still agree and no
  `.temp`/`.download` artifact is present; any uncertainty falls back to the
  full network check. Previously every file cost a full HTTPS open just to read
  `content-length` (~39 ms/file, over an hour of pure round-trips on a 100k-file
  drive).
- `ifetch --no-fast-scan` to bypass the sync state and force a network check per
  file, and `ifetch --force` to re-download everything regardless of local state.
- **`ifetch-verify`** (`ifetch/verify.py`): read-only integrity checking of a
  local mirror against iCloud Drive. Levels `size` (default, fast), `checksum`
  (hashes local files) and `redownload` (streams remote files and hashes them in
  memory to prove byte-for-byte equality). Flags: `--email`, `--level`,
  `--max-workers`, `--report`, `--quiet`. Exit codes: 0 verified, 1 verification
  failure, 2 operational error. Statuses: `verified`, `size_mismatch`,
  `missing_local`, `extra_local`, `checksum_mismatch`, `checksum_unavailable`,
  `error`. Verification never modifies the user's files.
- Run reports and the CLI summary now include a `skipped` count; files proven
  unchanged are recorded with `status="skipped"` and `downloaded: 0` instead of
  being counted as downloads.
- `auth` extra (`pip install "ifetch[auth]"`) that pulls in `pyicloud[cli]`,
  providing the `icloud` command used to store your password in the keyring.
- Benchmark suite (`benchmarks/benchmark.py`) measuring cold download,
  unchanged re-run, and kill-and-resume with integrity verification, plus a
  chart generator (`benchmarks/visualize.py`).

### Changed

- `DownloadManager.authenticate()` accepts an optional `two_factor` resolver.
  The keyword is optional and the CLI falls back to the no-argument form, so
  existing subclasses and plugins that override `authenticate()` keep working.
- `ifetch-verify` no longer requires a remote path when `--offline` is given;
  a single positional argument is then read as the local mirror directory.
- Package downloads whose streamed byte count differs from the listed size are
  reported as expanded bundles rather than as a size anomaly.


- `FileChunker.find_changed_chunks` renamed to
  `FileChunker.compute_download_ranges`, and documented for what it actually
  does: size-based change detection plus prefix resume.
- Documentation honesty pass on the sync engine. The README and docs previously
  advertised "chunk-level delta sync — changed files only re-download the byte
  ranges that differ". That was never implemented and has been removed
  everywhere. The documented behavior is now the real behavior: equal remote and
  local size means the file is skipped; a local file that is a shorter prefix of
  the remote resumes from that offset; **any other difference re-downloads the
  entire file**. The known blind spot — a modification that leaves the file size
  unchanged is not detected by size comparison — is now stated explicitly, along
  with a pointer to `ifetch-verify --level redownload`.
- README documents that `ifetch-verify --level checksum` does **not** verify
  against Apple: iCloud Drive item metadata exposes no content checksum
  (verified against pyicloud 2.6.5; `etag` is a mutation counter, not a digest),
  so those files report `checksum_unavailable` rather than `verified`.
- Added a Roadmap: genuine content-based chunk diffing is scoped for 1.1, and
  two-way sync is deliberately not implemented because safe bidirectional sync
  requires conflict-resolution and delete-propagation semantics that risk
  destroying the cloud copy if rushed.

### Fixed

- **Folders with accents in their names no longer report "Path not found".**
  Apple returns filenames in Unicode NFD while users and shells produce NFC;
  these are different strings that render identically, and the previous
  `casefold()` comparison did not normalize, so `Café`, `Résumé`, `Übungen` and
  most non-English paths failed to resolve. Lookup now normalizes both sides.
- Remote names that are not legal filenames on the target filesystem are now
  sanitized instead of failing or escaping the destination directory, and two
  remote names that sanitize to the same local name are given deterministic
  distinct names rather than silently overwriting one another. Only names the
  current platform genuinely cannot represent are changed, so existing mirrors
  are not moved.
- `ifetch-verify` derives local names through the same sanitizer as the
  downloader, so sanitized or de-collided files are no longer reported missing.
- ZIP members carrying permission bits but no file-type bits (as written by
  Python's own `zipfile.writestr`) are no longer misclassified as non-regular
  entries and skipped during package expansion.
- An unrecognised failure during `ifetch auth renew` now shows the underlying
  error text instead of being replaced by a generic "not recognised" message.


- Removed the false claim in `docs/mirror.md` that hop 1 skips unchanged files
  "via stored checksums (`.ifetch_versions.json`)". That file is the version
  history; skip decisions are made from the sync state and file sizes.

- Auth instructions updated for pyicloud 2.x: the CLI is now
  `icloud auth login --username ...` (the 1.x `icloud --username ...` syntax
  no longer exists). Updated the README, docs, and iFetch's own
  "No stored password found" error message; added troubleshooting entries
  for the macOS `SSLCertVerificationError` and 2FA-request failures.

### Security

- Package archives are treated as untrusted input: member paths that are
  absolute, contain `..` (in either separator style), or resolve outside the
  destination are refused, as are symlink and device entries. Extraction is
  bounded by entry-count and total-size limits, and the expanded directory is
  swapped into place atomically so a failure leaves the previous bundle intact.

## [1.0.0] - 2026-07-20

First release on PyPI: `pip install ifetch`.

### Added

- Google Drive export CLI with indexed upload support.
- Listing and downloading of items shared with the user, with comprehensive
  shared-file download tests and hint detection fixes.
- Plugin system (`ifetch/plugin.py`): auto-discovered `BasePlugin` subclasses
  can hook download lifecycle events (`on_authenticated`, `on_list_contents`,
  `before_download`, `after_download`, `on_event`).
- JSON profile support for include/exclude sync patterns.
- Persistent file history, with metadata protected from mutation on failed
  downloads.
- Retry/backoff logic for transient connection errors, extended to cover 503
  and other server errors.
- Parallel downloads, incremental updates (skip on size match, resume from a
  partial prefix), and JSON logging. Note: contemporaneous docs described this
  as "chunk-based differential" downloading; it never diffed content — see the
  Unreleased "Changed" entry.
- Automated test suite (pytest + pytest-cov) with expanded coverage across
  downloader flows.
- Buy Me a Coffee funding option; MIT license.
- CI workflows (test matrix across Python 3.10–3.13 on Ubuntu/macOS, ruff
  lint), PyPI publish workflow, issue/PR templates, CONTRIBUTING guide, and
  this changelog.

### Changed

- Switched from the abandoned `picklepete/pyicloud` to the maintained
  `timlaing/pyicloud` fork (shared-drive/shareID support), now consumed from
  PyPI as `pyicloud>=2.5.0`.
- Refactored the original monolith into modules: logger, models, utils,
  chunker, tracker, downloader, cli.
- Improved download latency and hardened downloader flows.

### Fixed

- Shared-file downloads and hint detection.
- iCloud Drive path resolution for listed folders.
- Large-file iCloud resume and chunked retries.
- Thread-safety issues in versioning and error logging.
- "seek" error when comparing chunks on HTTP streams; dictionary-iteration
  bug fixes.
- Bug where everything was downloaded as a folder.
- Documentation fixes: download command and virtual environment activation
  instructions in ReadMe.md.
