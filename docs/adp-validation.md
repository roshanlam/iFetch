# Validating Advanced Data Protection

iFetch supports Apple IDs with Advanced Data Protection (ADP). It does this by
running Apple's PCS-cookie flow: `requestWebAccessState`, then device consent,
then bounded `requestPCS` polling. That path is covered by
[`tests/test_adp.py`](../tests/test_adp.py), which replays saved copies of
Apple's replies. So it proves iFetch reacts correctly to the replies we
*believe* Apple sends. It can't prove Apple sends them.

Closing that gap needs a real Apple ID with ADP turned on. That needs a physical
device and a recovery key, which no test suite can provide. This page is how to
close it ([#31](https://github.com/roshanlam/iFetch/issues/31)).

## The short version

```sh
ifetch auth renew --adp --email you@example.com      # approve the prompt on your device
ifetch-adpcheck ADPTest --email you@example.com --report adp-report.json
```

The second command runs the whole procedure: `auth doctor`, then list,
download, offline verify and re-run, then an unattended renew. It prints a
verdict and a row to paste into the [results table](#results) below. The
account is masked (`y***@example.com`) in both the printed report and
`--report`, so either is safe to attach to #31.

Exit codes:

| Code | Meaning |
|---|---|
| `0` | every step passed |
| `1` | a step failed: something is wrong |
| `2` | the run could not start (usually authentication) |
| `3` | steps were skipped, so this proves less than a pass |

It is read-only. It lists and downloads into a temporary directory and deletes
that directory afterwards (`--keep` keeps it). Nothing is written to your
iCloud.

## Setup

1. **A device.** An iPhone on iOS 16.2+ or a Mac on macOS 13.1+, signed in to
   the Apple ID. Every device on the account has to be up to date, or Apple
   won't let you turn ADP on.
2. **Turn ADP on.** Settings → [your name] → iCloud → **Advanced Data
   Protection** → Turn On. Apple makes you set up a recovery key or a recovery
   contact. Keep the key somewhere safe: with ADP on, Apple can't recover your
   data without it. UK accounts can no longer turn ADP on.
3. **Turn web access on.** Settings → [your name] → iCloud → **Access iCloud
   Data on the Web** → On.
4. **Test data.** Create an `ADPTest` folder in iCloud Drive with a few files,
   including one over 20 MB, and let it sync.
5. **Sign in.** Run `ifetch auth renew --adp`, enter the 2FA code, and tap
   **Allow** on the "Access iCloud Data on the Web" prompt on your device.

## What is actually being tested

With ADP on, Apple only serves Drive data to a session that carries a
per-service encryption cookie (PCS). Without one, sign-in succeeds and every
Drive request then fails with `HTTP 423 Missing PCS cookies`. That is the
confusing failure reported against other iCloud clients.

Step 1 is what keeps the run honest. On an account *without* ADP, every later
step passes without the PCS flow ever being exercised. A `validated` there
would be the exact false claim this command exists to prevent. So if Apple does
not report ADP as on, the run stops and says `incomplete`. `--assume-adp`
overrides that if you are certain.

Step 8 is the one that matters for scheduled backups. Once you have approved
the prompt, iFetch should reuse the stored PCS cookie, so an unattended renew
needs nobody to tap anything.

## The steps

| # | Step | What a failure means |
|---|---|---|
| 1 | The account is under ADP | Not an ADP account (skipped), or web access is off (fail) |
| 2 | `auth doctor --online --adp` is healthy | Sign-in, trust or Drive access is broken. The detail names which |
| 3 | A live PCS cookie is stored | The approval was never completed, or the cookie expired |
| 4 | The folder lists | Listing is refused under ADP |
| 5 | **The folder downloads** | **The 423 case: signed in, but Drive data is refused** |
| 6 | Downloads verify offline against the manifest | The integrity record was not written |
| 7 | A second run transfers nothing | Incremental skipping does not work on this account |
| 8 | **An unattended renew needs no approval** | **The stored cookie is not reused, so a scheduled run would stall** |

## Recording the failure modes

The acceptance criteria for #31 also ask for the two common failures to be
documented with real output. Both need you to flip a switch on the device, so
they are not part of `ifetch-adpcheck`:

```sh
# Web access OFF (Settings > [name] > iCloud > Access iCloud Data on the Web)
ifetch auth renew --reset --adp --json > webaccess-off.json
ifetch auth doctor --online --adp --json > webaccess-off-doctor.json

# Web access back ON, and ignore the prompt on the device
ifetch auth renew --reset --adp --adp-timeout 60 --json > not-approved.json

# Then sign in properly again before running ifetch-adpcheck
ifetch auth renew --reset --adp
```

Each should report a named cause with a fix, not a bare `HTTP 423`. Replace
your email address in these files before publishing them. Unlike
`ifetch-adpcheck`, `ifetch auth` does not mask it.

## Results

Paste the row `ifetch-adpcheck` prints, along with the device and OS. Update the
README and CHANGELOG caveats from "replay-proven" to "field-proven" in the same
commit. A validation nobody can see is a validation that did not happen.

| Date | iFetch | pyicloud | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | Verdict | Device / OS |
|------|--------|----------|---|---|---|---|---|---|---|---|---------|-------------|
| _not yet run_ | | | | | | | | | | | | |

Until there is a `validated` row here, the README describes ADP support as
replay-proven, not field-proven.
