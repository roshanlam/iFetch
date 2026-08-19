"""Tests for :func:`ifetch.render.say_once` and the reports that rely on it.

A refusal notice assembles its text from overlapping sources — a headline
reason, a list of alternatives, a list of underlying problems — and the same
sentence often reaches more than one of them. Printed three times it reads as
padding and buries the parts that differ, in reports whose whole job is to be
read carefully by somebody who has just been told something is wrong.

The subtlety worth pinning: the repeat is a *substring*, not an exact copy,
because the headline is built by appending the underlying reason to a lead-in.
Deduplicating on equality catches none of them, which is how this was missed
the first time.
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))

from ifetch.index import KIND_FILE, LocalItem, RemoteItem, open_index  # noqa: E402
from ifetch.render import say_once  # noqa: E402


class TestSayOnce:
    def test_first_use_of_a_sentence_passes(self):
        fresh = say_once()
        assert fresh("the scan recorded a listing failure") is True

    def test_an_exact_repeat_is_filtered(self):
        fresh = say_once()
        fresh("the scan recorded a listing failure")
        assert fresh("the scan recorded a listing failure") is False

    def test_a_sentence_already_contained_in_an_earlier_one_is_filtered(self):
        """The case that matters: headline first, then its own tail repeated."""
        fresh = say_once()
        fresh("this evidence is unusable. the scan recorded a listing failure.")
        assert fresh("the scan recorded a listing failure") is False

    def test_a_genuinely_different_sentence_still_passes(self):
        fresh = say_once()
        fresh("the scan recorded a listing failure")
        assert fresh("the files may all still be in iCloud") is True

    def test_whitespace_and_trailing_stops_do_not_defeat_it(self):
        fresh = say_once()
        fresh("the scan   recorded\na listing failure.")
        assert fresh("the scan recorded a listing failure") is False

    def test_case_differences_do_not_defeat_it(self):
        fresh = say_once()
        fresh("The Scan Recorded A Listing Failure")
        assert fresh("the scan recorded a listing failure") is False

    def test_empty_and_blank_text_never_passes(self):
        fresh = say_once()
        assert fresh("") is False
        assert fresh("   \n ") is False

    def test_two_filters_are_independent(self):
        a, b = say_once(), say_once()
        a("shared sentence")
        assert b("shared sentence") is True

    def test_a_longer_sentence_containing_an_earlier_one_still_passes(self):
        """Containment is one-directional: only repeats of what was said drop.

        A later sentence that adds detail to an earlier one is new information
        and must survive, or the report loses the specifics it exists to give.
        """
        fresh = say_once()
        fresh("the scan failed")
        assert fresh("the scan failed while listing Documents/Photos") is True


def _broken_scan_store(root: Path, missing: int = 40, total: int = 50):
    """A mirror whose latest scan errored, so everything looks missing."""
    store = open_index(root)
    scan = store.begin_scan("Docs")
    for i in range(total):
        name = f"f{i}.txt"
        (root / name).write_bytes(b"0123456789")
        store.record_local(LocalItem(path=name, kind=KIND_FILE, size=10,
                                     mtime=time.time()))
    for i in range(missing, total):
        store.record_remote(RemoteItem(path=f"f{i}.txt", kind=KIND_FILE, size=10), scan)
    store.finish_scan(scan, errors=[{"path": "Docs/Photos", "error": "HTTP 503"}])
    return store


def _repeated_lines(text: str) -> list:
    """Sentences that appear more than once, ignoring bullets and rule lines.

    Horizontal rules and table separators repeat by design; only prose counts.
    """
    seen, repeats = {}, []
    for line in text.splitlines():
        key = line.strip().lstrip("-! ").strip().rstrip(".").casefold()
        if len(key) < 40 or not any(c.isalpha() for c in key):
            continue
        seen[key] = seen.get(key, 0) + 1
        if seen[key] == 2:
            repeats.append(key[:60])
    return repeats


class TestRefusalNoticesDoNotRepeatThemselves:
    """Both refusal renderers draw on the same overlapping sources."""

    def test_the_vanished_refusal_says_each_thing_once(self, tmp_path):
        from ifetch.vanished import analyse, render_vanished

        store = _broken_scan_store(tmp_path)
        report = analyse(store, tmp_path, record=False)
        store.close()
        assert report.breaker.tripped
        assert _repeated_lines(render_vanished(report)) == []

    def test_the_upload_refusal_says_each_thing_once(self, tmp_path):
        from ifetch.uplink import plan_uploads, render_plan

        store = _broken_scan_store(tmp_path)
        plan = plan_uploads(store, tmp_path, icloud_path="Docs")
        store.close()
        assert plan.refused
        assert _repeated_lines(render_plan(plan)) == []

    def test_the_upload_refusal_still_names_a_distinct_alternative(self, tmp_path):
        """Deduplication must not swallow the alternatives that differ."""
        from ifetch.uplink import plan_uploads, render_plan

        store = _broken_scan_store(tmp_path)
        plan = plan_uploads(store, tmp_path, icloud_path="Docs")
        store.close()
        text = render_plan(plan)
        assert "still in iCloud" in text
