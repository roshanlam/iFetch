# Validating folders shared by another Apple ID

iFetch can download from a folder someone else shared with you. That path is
covered by [`tests/test_shared_folder_contract.py`](../tests/test_shared_folder_contract.py),
which replays saved copies of Apple's replies — so it proves iFetch reacts
correctly to the replies we *believe* Apple sends. It cannot prove Apple sends
them.

Closing that gap needs a second Apple ID, which no test suite can create. This
page is how to close it.

## The short version

```sh
ifetch-sharecheck SharedTest --email you@example.com
```

That runs the whole procedure and prints a verdict plus a row to paste into the
[results table](#results) below. You still need the two-account setup described
under [Setup](#setup); the command handles everything after that.

Exit codes:

| Code | Meaning |
|---|---|
| `0` | every step passed |
| `1` | a step failed — something is wrong |
| `2` | the run could not start (usually authentication) |
| `3` | steps were skipped, so this proves less than a pass |

Code `3` is deliberately not `0`. A run that never reached the important step
has not validated anything, and a CI job treating "incomplete" as success would
defeat the point of running it.

It is read-only: it lists and downloads into a temporary directory and deletes
it afterwards. Nothing is written to your iCloud.

## What is actually being tested

When you are a participant in a shared folder rather than its owner, the files
belong to the owner's account, and every request about them has to name the
share. Apple puts that ID (`shareID`) on the folder that was shared and leaves
it off the items it returns for that folder's contents.

A client that reads the ID off each file therefore loses it one level down, and
from there every request goes out without saying which share it is about. The
symptom is distinctive: **the shared folder opens fine and everything inside it
fails** — HTTP 400 on a subfolder listing, 404 on a download. It is why rclone's
iCloud backend can only work at the top level of a share
([rclone#9477](https://github.com/rclone/rclone/issues/9477)), and it is what
iFetch issue [#15](https://github.com/roshanlam/iFetch/issues/15) was.

iFetch passes the ID down as it walks into the folder. **Step 4 below is what
catches it if that stops working.**

Step 5 then checks the mechanism directly, because step 4 passing is necessary
but not sufficient: it could pass because Apple happened to include the ID on
those particular files. Step 5 asks whether the ID actually reached a file two
levels down, and whether it got there by inheritance. If it was the file's own
ID, step 5 says so — it passes, but it tells you the fix was never exercised.

## Setup

You need two accounts:

- **Account A** — the owner. Creates and shares the folder.
- **Account B** — the participant. Runs iFetch.

On **Account A**, in iCloud Drive:

```
SharedTest/
├── root-file.txt          (any small text file)
├── Deck.key               (a real Keynote file — also tests the package path)
└── nested/
    ├── nested-file.txt
    └── deeper/
        └── deepest.txt
```

Share `SharedTest` with Account B. Accept the invitation on Account B and check
the folder appears in its iCloud Drive.

Then, signed in as **Account B**:

```sh
ifetch auth doctor --email accountB@example.com --online
ifetch-sharecheck SharedTest --email accountB@example.com
```

Do the `auth doctor` run first. A shared-folder failure and a sign-in failure
look alike in logs, and ruling the second one out takes seconds.

If the folder you point at is not actually shared with you, the command stops
and reports `incomplete` rather than passing every step against your own files.
`--assume-shared` overrides that if you are certain.

## The steps

| # | Step | What a failure means |
|---|---|---|
| 2 | The share resolves and carries a share ID | Wrong name, invitation not accepted, or not a share at all |
| 3 | Files directly inside it download | Basic share access is broken |
| 4 | **Subfolders download** | **The known bug — the top level works and nothing below it does** |
| 5 | The share ID reached a file two levels down | Downloads may work today, but the requests are unscoped and will break |
| 6 | Downloads verify offline against the manifest | The integrity record was not written for shared files |
| 7 | A second run transfers nothing | Incremental skipping does not work on shared files |

If step 4 fails, please attach the log (`--log-file`) to
[iFetch issues](https://github.com/roshanlam/iFetch/issues). It is also directly
useful to the rclone thread linked above, which is still open.

## Doing it by hand

If you would rather run the steps yourself:

```sh
ifetch --list-shared --email accountB@example.com          # step 2
ifetch "SharedTest" ~/shared-test --email …                # step 3
ifetch "SharedTest/nested" ~/shared-nested --email …       # step 4
ifetch "SharedTest/nested/deeper" ~/shared-deeper --email …
ifetch-verify --offline ~/shared-test                      # step 6
ifetch "SharedTest" ~/shared-test --email …                # step 7 — expect 0 bytes
```

Step 3 should also leave `Deck.key` on disk as a **directory**, not a ZIP.

## Results

Paste the row `ifetch-sharecheck` prints, and update the README caveat in the
same commit. A validation nobody can see is a validation that did not happen.

| Date | iFetch | pyicloud | Step 2 | Step 3 | Step 4 | Step 5 | Step 6 | Verdict |
|------|--------|----------|--------|--------|--------|--------|--------|---------|
| _not yet run_ | | | | | | | | |

Until there is a row here, the README describes shared folders as fixed in
principle rather than proven.
