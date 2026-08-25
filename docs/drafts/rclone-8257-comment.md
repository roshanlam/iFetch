# Draft comment for rclone/rclone#8257

**Not posted.** Review, edit to taste, and post under your own account if you
want to. Two notes before you do:

1. **Tone matters here.** #8257 is a thread of people waiting a long time for a
   feature, with two PRs already open ([#8818](https://github.com/rclone/rclone/pull/8818),
   [#9399](https://github.com/rclone/rclone/pull/9399)). A comment that reads as
   "use my tool instead" will land badly and is likely to be seen as
   self-promotion on someone else's issue tracker. The draft below leads with
   information useful to *rclone's* implementation and mentions iFetch once, at
   the end, as an interim option.
2. **Consider posting only if it is genuinely useful.** The most valuable part
   is arguably the pointer that pyicloud already solves this with a single flag
   and where the endpoint switch lives — that is directly actionable for whoever
   picks up the PR.

---

## Draft

> For anyone blocked on this: the endpoint switch is small enough that it may
> help to see it done elsewhere. pyicloud handles China Mainland with a single
> constructor flag, and derives all four endpoints from it in one place
> (`pyicloud/base.py`):
>
> ```python
> icloud_china = ".cn" if self._is_china_mainland else ""
> self._idmsa_endpoint = f"https://idmsa.apple.com{icloud_china}"
> self._home_endpoint  = f"https://www.icloud.com{icloud_china}"
> self._setup_endpoint = f"https://setup.icloud.com{icloud_china}/setup/ws/1"
> ```
>
> That matches what @ncw suggested above — making the four `const` values in
> `backend/iclouddrive/api/client.go` variables driven by a `region` option
> (`global` | `china`) rather than patching the URLs. It also matches what
> @wcumsjy confirmed works in the `fix-8257-iclouddrive-cn` branch.
>
> One thing worth flagging for whoever finishes the PR: the `.cn` swap has to
> include the **auth** endpoint (`idmsa.apple.com.cn`), not just the docws/setup
> ones. A China-registered Apple ID hitting the global `idmsa` gets a 302 with
> `{"domainToUse":"iCloud.com.cn"}` before authentication completes, which is the
> error reported at the top of this thread.
>
> In the meantime, if you only need to *download* from iCloud Drive, iFetch
> supports these accounts today via `--region china`
> (https://github.com/roshanlam/iFetch). It is download-only and iCloud-only, so
> it is not a replacement for rclone — just an option if you are stuck waiting.

---

## If you'd rather not mention iFetch at all

Drop the final paragraph. The rest stands on its own as a contribution to the
issue, and is more likely to be welcomed.

## Also worth considering

[#8404](https://github.com/rclone/rclone/issues/8404) (iWork files download with
the wrong size) is a better-matched thread: the package-token/ZIP behaviour iFetch
now implements is a direct answer to the problem being discussed there, and the
mechanism — `data_token` vs `package_token` in the `download/by_id` response,
with the listing reporting the logical size — is information that thread does not
yet have. A comment there explaining *why* the sizes differ would be useful to
rclone regardless of whether anyone uses iFetch.
