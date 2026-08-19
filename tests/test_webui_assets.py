"""Properties of the web UI's static assets that would otherwise rot silently.

No browser, no network, no server: this is a reader of the shipped files. The
things asserted here are the ones a careless edit breaks without anybody
noticing until a user is offline, or - worse - until the UI quietly presents an
incomplete check as a clean one.

Four groups:

* **it loads at all** - the page parses and has a title.
* **it works with no internet** - nothing on the page points off-origin, so a
  NAS on a LAN with no route out still renders it.
* **it never asks for the Apple ID password** - the server takes that from the
  OS keyring, and the browser must have nowhere to type one.
* **it cannot drift from the API contract or from the honesty rules** - every
  endpoint the JavaScript calls is one ``docs/webui-api.md`` documents, and the
  wording that keeps an unknown total unknown, a partial check partial and a
  refusal a refusal is still in the markup.
"""

from __future__ import annotations

import re
from html.parser import HTMLParser
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
STATIC = REPO / "ifetch" / "webui" / "static"
INDEX = STATIC / "index.html"
CONTRACT = REPO / "docs" / "webui-api.md"


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------

class _Page(HTMLParser):
    """Everything the assertions below need, in one pass."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.tags: list[tuple[str, dict[str, str]]] = []
        self.title = ""
        self.scripts: list[str] = []
        self.styles: list[str] = []
        self.label_text: list[str] = []
        self._capture: str | None = None
        self._buffer: list[str] = []

    def handle_starttag(self, tag, attrs):
        attributes = {name.lower(): (value or "") for name, value in attrs}
        self.tags.append((tag.lower(), attributes))
        if tag.lower() in {"title", "script", "style", "label", "legend"}:
            self._capture = tag.lower()
            self._buffer = []

    def handle_endtag(self, tag):
        tag = tag.lower()
        if self._capture != tag:
            return
        body = "".join(self._buffer)
        if tag == "title":
            self.title = body.strip()
        elif tag == "script":
            self.scripts.append(body)
        elif tag == "style":
            self.styles.append(body)
        else:
            self.label_text.append(body)
        self._capture = None
        self._buffer = []

    def handle_data(self, data):
        if self._capture is not None:
            self._buffer.append(data)


@pytest.fixture(scope="module")
def source() -> str:
    assert INDEX.is_file(), f"the web UI page is missing at {INDEX}"
    return INDEX.read_text(encoding="utf-8")


@pytest.fixture(scope="module")
def page(source: str) -> _Page:
    parser = _Page()
    parser.feed(source)
    parser.close()
    return parser


@pytest.fixture(scope="module")
def script(page: _Page) -> str:
    assert page.scripts, "the page ships no JavaScript, so nothing can talk to /api"
    return "\n".join(page.scripts)


@pytest.fixture(scope="module")
def css(page: _Page) -> str:
    assert page.styles, "the page ships no CSS"
    return "\n".join(page.styles)


def _inputs(page: _Page) -> list[dict[str, str]]:
    return [attrs for tag, attrs in page.tags if tag in {"input", "textarea", "select"}]


# ---------------------------------------------------------------------------
# It loads at all
# ---------------------------------------------------------------------------

def test_index_exists_and_parses(page: _Page) -> None:
    assert page.title, "the page has no <title>, so browser tabs and history are unlabelled"
    tags = {tag for tag, _ in page.tags}
    assert "html" in tags and "body" in tags and "main" in tags


def test_declares_a_viewport_so_a_phone_can_read_it(page: _Page) -> None:
    metas = [attrs for tag, attrs in page.tags if tag == "meta"]
    assert any(m.get("name") == "viewport" and "width=device-width" in m.get("content", "")
               for m in metas), "no responsive viewport: unusable at 380px on a phone"


# ---------------------------------------------------------------------------
# It works with no internet
# ---------------------------------------------------------------------------

REMOTE = re.compile(r"(?i)\b(?:https?:)?//")


def test_no_absolute_or_protocol_relative_urls_anywhere(source: str) -> None:
    """The page is opened from a NAS on a LAN that may have no route out."""
    for scheme in ("http://", "https://"):
        assert scheme not in source.lower(), (
            f"{scheme!r} appears in index.html; the page must load with no internet"
        )


def test_no_off_origin_src_or_href(page: _Page) -> None:
    for tag, attrs in page.tags:
        for attribute in ("src", "href", "action", "formaction", "poster", "data", "srcset"):
            value = attrs.get(attribute, "").strip()
            if value and REMOTE.match(value):
                pytest.fail(f"<{tag} {attribute}={value!r}> points off-origin")


def test_no_external_scripts_or_stylesheets(page: _Page) -> None:
    for tag, attrs in page.tags:
        if tag == "script" and attrs.get("src"):
            pytest.fail(f"<script src={attrs['src']!r}> - the page must be self-contained")
        if tag == "link" and "stylesheet" in attrs.get("rel", "").lower():
            pytest.fail(f"<link rel=stylesheet href={attrs.get('href')!r}> - inline the CSS")


def test_css_pulls_in_nothing(css: str) -> None:
    assert "@import" not in css, "@import can reach off-origin; inline the rules instead"
    for match in re.findall(r"url\(([^)]*)\)", css):
        assert not REMOTE.match(match.strip().strip("'\"")), f"url({match}) is off-origin"
    assert "@font-face" not in css, "no web fonts: the page must render with no network"


def test_the_only_network_client_is_fetch(script: str) -> None:
    for forbidden in ("XMLHttpRequest", "EventSource", "WebSocket", "navigator.sendBeacon",
                      "importScripts", "document.write"):
        assert forbidden not in script, f"{forbidden} is not used by this page"


# ---------------------------------------------------------------------------
# It never asks for the Apple ID password
# ---------------------------------------------------------------------------

def test_no_password_input_of_any_kind(page: _Page) -> None:
    """The server resolves the password from the OS keyring. The browser must
    have nowhere to type one, so that no amount of phishing a user can be
    talked through produces a password field on this page."""
    for attrs in _inputs(page):
        assert attrs.get("type", "text").lower() != "password", "a password input exists"
        haystack = " ".join([
            attrs.get("name", ""), attrs.get("id", ""), attrs.get("placeholder", ""),
            attrs.get("aria-label", ""), attrs.get("autocomplete", ""),
        ]).lower()
        assert "password" not in haystack, f"an input is named or labelled for a password: {attrs!r}"
        assert "passwd" not in haystack and "passphrase" not in haystack


def test_no_label_asks_for_a_password(page: _Page) -> None:
    for text in page.label_text:
        assert "password" not in text.lower(), f"a form label asks for a password: {text.strip()!r}"


def test_the_page_says_it_will_not_ask(source: str) -> None:
    assert "never asks for your Apple ID password" in source


# ---------------------------------------------------------------------------
# Theme and accessibility
# ---------------------------------------------------------------------------

def test_dark_mode_block_exists(css: str) -> None:
    assert "prefers-color-scheme: dark" in css.replace("prefers-color-scheme:dark",
                                                       "prefers-color-scheme: dark")


def test_reduced_motion_is_respected(css: str) -> None:
    assert "prefers-reduced-motion" in css, (
        "the indeterminate bar animates; it must stop for prefers-reduced-motion"
    )


def test_focus_is_visible(css: str) -> None:
    assert ":focus-visible" in css and "outline" in css


def test_aria_live_regions_for_status_and_progress(page: _Page) -> None:
    live = [attrs for _, attrs in page.tags if attrs.get("aria-live")]
    assert len(live) >= 2, "status and progress must both be announced"
    ids = {attrs.get("id", "") for attrs in live}
    assert "banner" in ids, "the connection/session banner is not an aria-live region"
    assert "progress-region" in ids, "the progress region is not an aria-live region"


def test_controls_are_real_buttons_and_labels(page: _Page) -> None:
    tags = [tag for tag, _ in page.tags]
    assert tags.count("button") >= 5, "actions must be real <button> elements"
    labelled = {attrs.get("for") for tag, attrs in page.tags if tag == "label"}
    for name in ("email", "code", "dest", "guard-path", "vanish-path"):
        assert name in labelled, f"the {name!r} field has no <label for=...>"


# ---------------------------------------------------------------------------
# It cannot drift from the API contract
# ---------------------------------------------------------------------------

_DOC_ENDPOINT = re.compile(r"\b(?:GET|POST)\s+(/api/[A-Za-z0-9_\-/]*)")
_JS_ENDPOINT = re.compile(r"""['"](/api/[A-Za-z0-9_\-/]*)['"]""")
_BARE_FETCH = re.compile(r"(?<![A-Za-z0-9_$.])fetch\s*\(")


@pytest.fixture(scope="module")
def documented() -> set[str]:
    assert CONTRACT.is_file(), f"the API contract is missing at {CONTRACT}"
    found = {path.rstrip("/") for path in _DOC_ENDPOINT.findall(CONTRACT.read_text(encoding="utf-8"))}
    assert found, "no endpoints could be parsed out of the contract"
    return found


def test_every_endpoint_the_js_calls_is_documented(script: str, documented: set[str]) -> None:
    called = {path.rstrip("/") for path in _JS_ENDPOINT.findall(script)}
    assert called, "the JavaScript names no /api path at all"
    undocumented = sorted(called - documented)
    assert not undocumented, (
        f"the page calls {undocumented}, which docs/webui-api.md does not document"
    )


def test_every_documented_endpoint_is_reachable_from_the_page(
    script: str, documented: set[str]
) -> None:
    called = {path.rstrip("/") for path in _JS_ENDPOINT.findall(script)}
    unused = sorted(documented - called)
    assert not unused, (
        f"docs/webui-api.md documents {unused}, which the page never calls - either the "
        "UI lost a feature or the contract grew one"
    )


def test_there_is_exactly_one_fetch_call_and_it_takes_a_path(script: str) -> None:
    """Every request funnels through one helper, so the literal check above is
    exhaustive rather than a sample."""
    calls = _BARE_FETCH.findall(script)
    assert len(calls) == 1, f"expected one fetch() call site, found {len(calls)}"
    assert re.search(r"(?<![A-Za-z0-9_$.])fetch\(\s*path\s*,", script), (
        "the single fetch() must take the path its caller passed in"
    )


# ---------------------------------------------------------------------------
# The honesty rules
# ---------------------------------------------------------------------------

def test_an_unknown_total_stays_unknown(source: str, script: str) -> None:
    assert "indeterminate" in source, "there is no indeterminate progress bar"
    assert "the total is not yet known" in source, (
        "a null files_total/bytes_total must be shown as unknown, never as 0 or 100%"
    )
    assert "var UNKNOWN = 'unknown';" in script
    # The percentage may only be computed when a real denominator exists.
    assert "knowsFiles" in script and "knowsBytes" in script
    assert re.search(r"isNum\(p\.bytes_total\)\s*&&\s*p\.bytes_total\s*>\s*0", script)


def test_an_incomplete_guard_check_is_labelled_incomplete(source: str, script: str) -> None:
    assert "This check was partial" in source
    assert "not a clean bill of health" in source
    assert "a floor, not a total" in source
    # The gap notice is emitted before the headline, from report.complete.
    assert "report.complete === true" in script
    assert "signals_unavailable" in script and "unreadable" in script


def test_a_tripped_breaker_is_shown_as_a_refusal(source: str, script: str) -> None:
    assert "Refused: these are not being reported as deletions" in source
    assert "cannot_rule_out" in script, "the alternatives the breaker names must be shown"
    assert "report.refused === true" in script and "breaker.tripped === true" in script
    # The refusal returns before any findings are rendered.
    refusal = script.index("Refused: these are not being reported as deletions")
    findings = script.index("CLASS_ORDER.forEach")
    assert refusal < findings, "findings must not be rendered when the breaker tripped"


def test_purge_deadlines_are_upper_bounds(source: str) -> None:
    assert "latest possible date, not an exact one" in source
    assert "at or before" in source
    assert "It is a ceiling, not an appointment." in source


def test_an_absent_baseline_is_not_a_pass(source: str) -> None:
    assert "absence of evidence" in source


def test_an_expired_token_says_so(source: str) -> None:
    assert "This link has expired" in source
    assert "ifetch serve" in source


def test_a_missing_server_is_survivable(source: str, script: str) -> None:
    assert "Lost contact with iFetch" in source
    assert "alert(" not in script and "confirm(" not in script, "no alert()/confirm()"


def test_polling_backs_off_and_pauses_when_hidden(script: str) -> None:
    assert "document.hidden" in script
    assert "visibilitychange" in script
    assert "RUNNING_MS = 1000" in script
    assert "IDLE_MAX_MS" in script
