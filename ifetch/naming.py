"""Turning Apple's filenames into names this machine can actually use.

Two separate problems live here, and conflating them causes bugs.

Lookup: Apple speaks NFD
------------------------
Apple's Drive API returns filenames in Unicode NFD (decomposed): ``Café`` comes
back as ``C-a-f-e-U+0301``. Humans, shells and most filesystems use NFC
(``C-a-f-U+00E9``). The two are *different strings* that render identically, so
a plain ``==`` or even ``casefold()`` comparison fails:

    >>> 'Café'.casefold() == 'Café'.casefold()   # NFC vs NFD
    False

The visible symptom is ``Path not found`` for any folder with an accent in its
name, which is most non-English paths. :func:`fold` normalises both sides before
comparing so that stops happening.

Writing: not every name is a legal filename
-------------------------------------------
A remote name may contain characters this filesystem cannot store - a path
separator, a NUL, or (on Windows) any of ``<>:"|?*``. Writing them unchanged
either fails or, worse, escapes the destination directory.
:func:`sanitize_segment` maps such a name onto a safe one.

Why sanitising is conservative by default
-----------------------------------------
Changing a local filename changes where a file lives, which orphans the copy an
earlier run wrote and triggers a full re-download. So the default is to change
as little as possible: only what the *current* platform genuinely cannot
represent. ``portable=True`` opts into the strictest common denominator, which
is what you want when the destination is exFAT, a NAS share, or anything that
will later be read from Windows.

For the same reason the local spelling of a name is preserved as Apple sent it
unless normalisation is explicitly requested. On macOS this is invisible (APFS
compares normalisation-insensitively), but on Linux ``NFC`` and ``NFD`` are
genuinely different directory entries, so silently switching would strand every
existing mirror.
"""

from __future__ import annotations

import hashlib
import os
import re
import unicodedata
from typing import Dict, Iterable, Optional, Tuple

#: Characters no platform can hold in a single path segment. ``/`` and ``\`` are
#: included because a name containing one would otherwise create nested
#: directories - or escape the destination entirely.
_UNIVERSALLY_ILLEGAL = re.compile(r'[\x00-\x1f\x7f/\\]')

#: Additionally illegal on Windows.
_WINDOWS_ILLEGAL = re.compile(r'[<>:"|?*]')

#: Device names Windows reserves, with or without an extension.
_WINDOWS_RESERVED = frozenset(
    {"CON", "PRN", "AUX", "NUL"}
    | {f"COM{i}" for i in range(1, 10)}
    | {f"LPT{i}" for i in range(1, 10)}
)

REPLACEMENT = "_"

NORMALIZE_PRESERVE = "preserve"
NORMALIZE_NFC = "nfc"
NORMALIZE_CHOICES = (NORMALIZE_PRESERVE, NORMALIZE_NFC)


# ---------------------------------------------------------------------------
# Lookup
# ---------------------------------------------------------------------------

def nfc(name: str) -> str:
    """Return ``name`` in NFC (composed) form."""
    return unicodedata.normalize("NFC", str(name))


def fold(name: str) -> str:
    """The key used to decide whether two names refer to the same item.

    Normalising to NFC *before* case-folding is the whole point: Apple sends
    NFD, the user types NFC, and case-folding alone leaves them unequal.
    """
    return unicodedata.normalize("NFC", str(name)).casefold()


def names_equal(a: str, b: str) -> bool:
    """True when two names denote the same item, ignoring case and normalisation."""
    return fold(a) == fold(b)


def find_match(target: str, candidates: Iterable[str]) -> Optional[str]:
    """Find the candidate denoting ``target``, or ``None``.

    An exact match always wins over a folded one, so a directory legitimately
    holding both ``README`` and ``readme`` resolves the requested one rather
    than whichever happened to be listed first.
    """
    candidates = list(candidates)
    for candidate in candidates:
        if candidate == target:
            return candidate

    wanted = fold(target)
    for candidate in candidates:
        if fold(candidate) == wanted:
            return candidate
    return None


# ---------------------------------------------------------------------------
# Writing
# ---------------------------------------------------------------------------

def _needs_windows_rules(portable: bool) -> bool:
    return portable or os.name == "nt"


def is_safe_segment(name: str, portable: bool = False) -> bool:
    """True when ``name`` can be used as a path segment unchanged."""
    return sanitize_segment(name, portable=portable) == name


def sanitize_segment(
    name: str,
    portable: bool = False,
    normalize: str = NORMALIZE_PRESERVE,
) -> str:
    """Map a remote name onto a path segment this machine can store.

    Always removed: control characters, NUL, and path separators. Under Windows
    rules additionally: ``<>:"|?*``, trailing dots and spaces, and the reserved
    device names.

    The result is never empty and is never ``.`` or ``..``; those collapse to a
    placeholder rather than resolving to a directory the caller did not mean.
    """
    text = str(name or "")
    if normalize == NORMALIZE_NFC:
        text = nfc(text)

    cleaned = _UNIVERSALLY_ILLEGAL.sub(REPLACEMENT, text)

    if _needs_windows_rules(portable):
        cleaned = _WINDOWS_ILLEGAL.sub(REPLACEMENT, cleaned)
        # Windows silently strips these, so a name ending in one would come back
        # as a different file than the one we recorded.
        cleaned = cleaned.rstrip(" .")
        stem = cleaned.split(".", 1)[0].upper()
        if stem in _WINDOWS_RESERVED:
            cleaned = REPLACEMENT + cleaned

    if cleaned in ("", ".", ".."):
        return "_unnamed" if not cleaned else REPLACEMENT * len(cleaned)

    return cleaned


def _disambiguate(name: str, original: str) -> str:
    """Append a short stable digest so two sanitised names cannot collide.

    The digest is derived from the *original* remote name, so it is identical on
    every run. That matters more than brevity: a suffix that changed between
    runs would re-download the file every time.
    """
    digest = hashlib.sha256(original.encode("utf-8", "surrogatepass")).hexdigest()[:8]
    root, dot, extension = name.partition(".")
    return f"{root}~{digest}{dot}{extension}" if dot else f"{name}~{digest}"


class DirectorySanitizer:
    """Assigns safe, collision-free local names within a single directory.

    Sanitising is many-to-one: ``a:b`` and ``a?b`` both become ``a_b``. Left
    alone, the second file would silently overwrite the first - a backup tool
    losing data without saying so. This class detects that and gives the loser a
    deterministic suffix instead.
    """

    def __init__(self, portable: bool = False, normalize: str = NORMALIZE_PRESERVE):
        self.portable = portable
        self.normalize = normalize
        self._taken: Dict[str, str] = {}   # assigned name -> original remote name
        self.collisions: list = []

    def assign(self, remote_name: str) -> Tuple[str, bool]:
        """Return ``(local_name, was_changed)`` for one child of this directory."""
        candidate = sanitize_segment(
            remote_name, portable=self.portable, normalize=self.normalize
        )

        owner = self._taken.get(candidate)
        if owner is not None and owner != remote_name:
            resolved = _disambiguate(candidate, remote_name)
            self.collisions.append(
                {"remote": remote_name, "collides_with": owner, "local": resolved}
            )
            self._taken[resolved] = remote_name
            return resolved, True

        self._taken[candidate] = remote_name
        return candidate, candidate != remote_name
