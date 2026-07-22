"""Tests for filename lookup and sanitization.

Two independent contracts:

**Lookup.** Apple returns NFD, users type NFC. Those are different strings that
render identically, so any folder with an accent in its name must still resolve.
This is a regression suite for a real bug: before it, ``ifetch Café/Photos``
reported "Path not found" for a folder that plainly existed.

**Writing.** A remote name may not be a legal filename here. Sanitizing must
never escape the destination, never silently collapse two files onto one path,
and - crucially - never change a name that was already fine, because moving a
file orphans what an earlier run wrote and forces a full re-download.
"""

import os
import sys
import unicodedata as ud
from pathlib import Path

import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))

from ifetch.downloader import DownloadManager  # noqa: E402
from ifetch.naming import (  # noqa: E402
    NORMALIZE_NFC,
    NORMALIZE_PRESERVE,
    DirectorySanitizer,
    find_match,
    fold,
    is_safe_segment,
    names_equal,
    nfc,
    sanitize_segment,
)

# The two spellings Apple and users respectively produce.
NFC_CAFE = ud.normalize("NFC", "Café")
NFD_CAFE = ud.normalize("NFD", "Café")
NFC_RESUME = ud.normalize("NFC", "Résumé")
NFD_RESUME = ud.normalize("NFD", "Résumé")


def test_the_two_spellings_really_are_different_strings():
    """Guard the premise: if these were equal the whole suite proves nothing."""
    assert NFC_CAFE != NFD_CAFE
    assert NFC_CAFE.casefold() != NFD_CAFE.casefold()
    assert len(NFD_CAFE) == len(NFC_CAFE) + 1


class TestFolding:
    @pytest.mark.parametrize(
        "a,b",
        [
            (NFC_CAFE, NFD_CAFE),
            (NFC_RESUME, NFD_RESUME),
            ("café", "CAFÉ"),
            (NFD_CAFE, "CAFÉ"),
            ("Übungen", ud.normalize("NFD", "Übungen")),
            ("日本語", "日本語"),
            ("Ångström", ud.normalize("NFD", "ångström")),
        ],
    )
    def test_equivalent_names_fold_together(self, a, b):
        assert names_equal(a, b)

    @pytest.mark.parametrize(
        "a,b",
        [("Café", "Cafe"), ("Résumé", "Resume"), ("a", "b"), ("", "x")],
    )
    def test_genuinely_different_names_do_not_fold_together(self, a, b):
        assert not names_equal(a, b)

    def test_nfc_helper_composes(self):
        assert nfc(NFD_CAFE) == NFC_CAFE
        assert len(nfc(NFD_CAFE)) == 4

    def test_fold_is_idempotent(self):
        assert fold(fold(NFD_CAFE)) == fold(NFD_CAFE)


class TestFindMatch:
    def test_finds_the_nfd_entry_from_an_nfc_query(self):
        """The exact shape of the bug: Apple lists NFD, the user typed NFC."""
        assert find_match(NFC_CAFE, [NFD_CAFE, "Other"]) == NFD_CAFE

    def test_finds_the_nfc_entry_from_an_nfd_query(self):
        assert find_match(NFD_CAFE, [NFC_CAFE]) == NFC_CAFE

    def test_returns_none_when_absent(self):
        assert find_match("Missing", [NFD_CAFE, "Other"]) is None

    def test_empty_candidate_list(self):
        assert find_match("x", []) is None

    def test_exact_match_wins_over_a_folded_one(self):
        """A directory holding both README and readme must resolve the one asked for."""
        assert find_match("readme", ["README", "readme"]) == "readme"
        assert find_match("README", ["readme", "README"]) == "README"

    def test_case_insensitive_fallback_still_works(self):
        assert find_match("readme.txt", ["README.TXT"]) == "README.TXT"


class TestPathResolutionRegression:
    """Drive the real ``_resolve_child``, which is where the bug lived."""

    class Node(dict):
        def dir(self):
            return list(self.keys())

    def resolver(self):
        return DownloadManager.__new__(DownloadManager)

    def test_accented_folder_resolves_from_the_composed_spelling(self):
        remote = self.Node({NFD_CAFE: "the-folder"})
        assert self.resolver()._resolve_child(remote, NFC_CAFE) == "the-folder"

    def test_accented_folder_resolves_from_the_decomposed_spelling(self):
        remote = self.Node({NFC_CAFE: "the-folder"})
        assert self.resolver()._resolve_child(remote, NFD_CAFE) == "the-folder"

    def test_exact_match_still_takes_the_fast_path(self):
        remote = self.Node({"Documents": "d"})
        assert self.resolver()._resolve_child(remote, "Documents") == "d"

    def test_a_genuinely_missing_child_still_raises(self):
        """The fix must not turn "not found" into a wrong match."""
        remote = self.Node({NFD_CAFE: "x"})
        with pytest.raises(KeyError):
            self.resolver()._resolve_child(remote, "Nonexistent")

    def test_multi_segment_accented_path_walks_end_to_end(self):
        deepest = self.Node({ud.normalize("NFD", "Fotos"): "leaf"})
        middle = self.Node({ud.normalize("NFD", "Año"): deepest})
        root = self.Node({NFD_CAFE: middle})

        walked = self.resolver()._walk_path(
            root, [NFC_CAFE, ud.normalize("NFC", "Año"), "Fotos"]
        )
        assert walked == "leaf"


class TestSanitizeSegment:
    @pytest.mark.parametrize(
        "name",
        ["report.pdf", "My Folder", NFD_CAFE, "日本語", "a-b_c.tar.gz", "file(1)"],
    )
    def test_legal_names_are_returned_untouched(self, name):
        """Changing a safe name would move the file and force a re-download."""
        assert sanitize_segment(name) == name
        assert is_safe_segment(name)

    @pytest.mark.parametrize("name", ["a/b", "a\\b", "a\x00b", "a\x1fb", "a\x7fb"])
    def test_universally_illegal_characters_are_replaced_everywhere(self, name):
        cleaned = sanitize_segment(name)
        assert "/" not in cleaned and "\\" not in cleaned
        assert not any(ord(c) < 0x20 or ord(c) == 0x7F for c in cleaned)

    def test_a_separator_cannot_create_a_nested_path(self):
        """The security property: a remote name must stay one path segment."""
        assert "/" not in sanitize_segment("evil/../../etc/passwd")

    @pytest.mark.parametrize("name", ["", ".", ".."])
    def test_dangerous_or_empty_names_never_pass_through(self, name):
        cleaned = sanitize_segment(name)
        assert cleaned not in ("", ".", "..")

    @pytest.mark.parametrize("name", ['a<b', 'a>b', 'a:b', 'a"b', "a|b", "a?b", "a*b"])
    def test_windows_illegal_characters_under_portable_rules(self, name):
        assert sanitize_segment(name, portable=True) == "a_b"

    def test_windows_characters_are_left_alone_on_posix_by_default(self):
        """On macOS/Linux 'a:b' is a perfectly good filename; do not move it."""
        if os.name == "nt":
            pytest.skip("POSIX-specific behaviour")
        assert sanitize_segment("a:b") == "a:b"

    @pytest.mark.parametrize("name", ["CON", "con.txt", "NUL", "COM1", "LPT9"])
    def test_windows_reserved_device_names_are_escaped(self, name):
        assert sanitize_segment(name, portable=True) != name

    def test_trailing_dots_and_spaces_stripped_under_portable_rules(self):
        """Windows strips these silently, so the file would come back renamed."""
        assert sanitize_segment("name. ", portable=True) == "name"
        assert sanitize_segment("name...", portable=True) == "name"

    def test_normalize_nfc_is_opt_in(self):
        assert sanitize_segment(NFD_CAFE, normalize=NORMALIZE_PRESERVE) == NFD_CAFE
        assert sanitize_segment(NFD_CAFE, normalize=NORMALIZE_NFC) == NFC_CAFE

    def test_sanitizing_is_idempotent(self):
        once = sanitize_segment("a/b:c*d", portable=True)
        assert sanitize_segment(once, portable=True) == once


class TestDirectorySanitizer:
    def test_safe_names_pass_through_unreported(self):
        s = DirectorySanitizer()
        assert s.assign("report.pdf") == ("report.pdf", False)
        assert s.collisions == []

    def test_a_changed_name_is_reported_as_changed(self):
        s = DirectorySanitizer(portable=True)
        local, changed = s.assign("a:b")
        assert local == "a_b" and changed is True

    def test_two_names_collapsing_onto_one_do_not_overwrite_each_other(self):
        """Silent data loss otherwise: the second file would replace the first."""
        s = DirectorySanitizer(portable=True)
        first, _ = s.assign("a:b")
        second, changed = s.assign("a?b")

        assert first == "a_b"
        assert second != first
        assert changed is True
        assert len(s.collisions) == 1
        assert s.collisions[0]["remote"] == "a?b"

    def test_disambiguation_preserves_the_extension(self):
        s = DirectorySanitizer(portable=True)
        s.assign("q:a.pdf")
        second, _ = s.assign("q?a.pdf")
        assert second.endswith(".pdf")

    def test_disambiguation_is_stable_across_runs(self):
        """An unstable suffix would re-download the file on every single run."""
        def run():
            s = DirectorySanitizer(portable=True)
            s.assign("a:b")
            return s.assign("a?b")[0]

        assert run() == run()

    def test_the_same_remote_name_twice_is_not_a_collision(self):
        """Idempotence: re-assigning one name must give the same answer."""
        s = DirectorySanitizer(portable=True)
        first, _ = s.assign("a:b")
        again, _ = s.assign("a:b")
        assert first == again
        assert s.collisions == []

    def test_three_way_collision_yields_three_distinct_names(self):
        s = DirectorySanitizer(portable=True)
        names = {s.assign(n)[0] for n in ("x:y", "x?y", "x*y")}
        assert len(names) == 3

    def test_sanitizers_are_independent_per_directory(self):
        """Same name in two different folders is not a collision."""
        a, b = DirectorySanitizer(portable=True), DirectorySanitizer(portable=True)
        assert a.assign("a:b")[0] == b.assign("a:b")[0]
