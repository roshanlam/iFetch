# Validating cross-account shared folders

iFetch's support for folders shared **by another Apple ID** is currently
*best-effort*. The code path is exercised by
[`tests/test_shared_folder_contract.py`](../tests/test_shared_folder_contract.py),
which replays recorded Apple responses, but recorded responses only prove that
iFetch branches correctly on the payloads we believe Apple sends. They cannot
prove Apple actually sends them.

Closing that gap needs two Apple IDs and about fifteen minutes. This document is
the procedure. Until someone runs it and records the result below, the README's
"best-effort" caveat stands.

## Why this is hard to automate

A cross-account share cannot be provisioned by a test suite: it requires a
second real Apple ID, a real share invitation, and a real acceptance. Apple
offers no sandbox. So this is a manual, human-in-the-loop validation.

## What is actually being tested

When you are a *participant* (not the owner) of a shared folder, the files
inside it belong to the owner's iCloud account. Their `docwsid` is not
addressable through the ordinary `download/by_id` endpoint using your
credentials. Apple requires a `shareID` parameter to be threaded through the
request.

pyicloud threads `shareID` through folder *traversal* but not through
`get_file()`, so the download endpoint still fails. iFetch adds the missing
piece in `DownloadManager._try_shared_open`.

The known failure mode in other clients is that the **share root works but any
subdirectory of it returns HTTP 400**
([rclone/rclone#9477](https://github.com/rclone/rclone/issues/9477)). Step 4
below is specifically designed to hit that case.

## Setup

You need:

- **Account A** — the owner. Creates and shares the folder.
- **Account B** — the participant. Runs iFetch.

On **Account A**, in iCloud Drive:

```
SharedTest/
├── root-file.txt          (any small text file)
├── Deck.key               (a real Keynote file - tests the package path too)
└── nested/
    ├── nested-file.txt
    └── deeper/
        └── deepest.txt
```

Share `SharedTest` with Account B, granting **edit** access. Accept the
invitation on Account B and confirm the folder appears in its iCloud Drive.

## Procedure

Run everything below signed in as **Account B**.

### 1. Confirm authentication is healthy first

A shared-folder failure and an auth failure look similar in logs. Rule the
second one out before starting.

```sh
ifetch auth doctor --email accountB@example.com --online
```

Expect exit code 0. If not, fix that first — nothing below is meaningful otherwise.

### 2. The share is visible

```sh
ifetch --list-shared --email accountB@example.com
```

**Expect:** `SharedTest` appears.
**If it does not:** the share was not accepted, or pyicloud cannot see the
shared root. Stop and record that.

### 3. The share root is readable

```sh
ifetch "SharedTest" ~/shared-test --email accountB@example.com
```

**Expect:** `root-file.txt` downloads, with matching content.
**Expect:** `Deck.key` arrives as a **directory**, not a ZIP.

### 4. Subdirectories of the share are readable — the critical case

This is the step that fails in other clients.

```sh
ifetch "SharedTest/nested" ~/shared-nested --email accountB@example.com
ifetch "SharedTest/nested/deeper" ~/shared-deeper --email accountB@example.com
```

**Expect:** both succeed.
**If either returns HTTP 400:** iFetch has the same limitation as rclone.
Capture the full log (`--log-file`) and record it below — that log is the
evidence needed to fix it.

### 5. Integrity of what came back

```sh
ifetch-verify "SharedTest" ~/shared-test --email accountB@example.com --level checksum
ifetch-verify --offline ~/shared-test
```

**Expect:** both pass. The second needs no credentials at all.

### 6. Re-runs skip correctly

```sh
ifetch "SharedTest" ~/shared-test --email accountB@example.com
```

**Expect:** everything reported as skipped, zero bytes transferred.

## Recording the result

Update the table below and the README caveat in the same commit. A validation
nobody can see is a validation that did not happen.

| Date | iFetch version | pyicloud version | Step 2 | Step 3 | Step 4 | Step 5 | Step 6 | Notes |
|------|----------------|------------------|--------|--------|--------|--------|--------|-------|
| _not yet run_ | | | | | | | | |

If step 4 fails, please also file the log against
[iFetch issues](https://github.com/roshanlam/iFetch/issues) — it is directly
useful to the rclone thread linked above, which is still open.
