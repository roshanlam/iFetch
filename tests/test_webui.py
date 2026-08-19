"""Tests for the local web UI server.

Two things are being checked here and they matter in different ways.

The first is the contract. ``docs/webui-api.md`` is being implemented by a
frontend at the same time, so every response shape below is asserted key by key
rather than by "it has the fields I happened to look for".

The second is access control, and it is the reason most of this file exists.
This server holds an authenticated iCloud session, so a mistake here is not a
broken page - it is somebody else's browser listing a stranger's files. Every
one of those properties is exercised through a real socket on an ephemeral
port, because a check that only holds when you call the handler directly is a
check that does not hold. The refusals tested are: no token, wrong token, a
``Host`` header that is not loopback, a CORS header appearing anywhere, a path
climbing out of the Drive root, a destination outside the folders the server
was given, an oversized body, and a password arriving from the browser.

Nothing here touches a network or a credential. A fake DownloadManager stands
in for Apple, the password is a string this file invents, and the guard and
vanish reports are built locally from the real report classes so the
pass-through is tested against the real shapes.
"""

import http.client
import json
import logging
import socket
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))

from ifetch import serve_cli  # noqa: E402
from ifetch.guard import ByteAccount, GuardReport  # noqa: E402
from ifetch.plugin import PluginManager  # noqa: E402
from ifetch.vanished import BreakerVerdict, ScanEvidence, VanishedReport  # noqa: E402
from ifetch.webui.server import (  # noqa: E402
    COOKIE_NAME,
    WebUIApp,
    clean_local_path,
    clean_remote_path,
    create_server,
    host_is_allowed,
)

TOKEN = "test-token-0123456789abcdef"
PASSWORD = "correct-horse-battery-staple-SECRET"

#: Every route in the contract, with the method it accepts.
ROUTES = [
    ("GET", "/api/state"),
    ("GET", "/api/browse"),
    ("POST", "/api/auth/start"),
    ("POST", "/api/auth/2fa"),
    ("POST", "/api/auth/signout"),
    ("POST", "/api/download"),
    ("POST", "/api/guard"),
    ("POST", "/api/vanish"),
    ("POST", "/api/cancel"),
]


# ---------------------------------------------------------------------------
# Fakes: enough of pyicloud and of DownloadManager to drive every endpoint
# ---------------------------------------------------------------------------

class FakeFile:
    type = "file"

    def __init__(self, name, size):
        self.name = name
        self.size = size

    def open(self, **kwargs):
        raise AssertionError("the tests must never open a stream")


class FakeFolder:
    type = "folder"

    def __init__(self, name, children=()):
        self.name = name
        self._children = {child.name: child for child in children}

    def dir(self):
        return list(self._children)

    def __getitem__(self, name):
        return self._children[name]


class Script:
    """What the fake downloader has been told to do, shared across instances."""

    def __init__(self, tree=None, files=(), requires_2fa=False, valid_code="123456",
                 auth_error=None):
        self.tree = tree if tree is not None else FakeFolder("")
        self.files = list(files)
        self.requires_2fa = requires_2fa
        self.valid_code = valid_code
        self.auth_error = auth_error
        self.gate = threading.Event()
        self.gate.set()
        self.file_started = threading.Event()
        self.completed = []
        self.attempts = []
        self.managers = []


class FakeManager:
    """The parts of DownloadManager the web UI actually uses."""

    def __init__(self, email, password, script):
        self.email = email
        self.password = password
        self.script = script
        self.plugin_manager = PluginManager(enabled=False)
        self.transferred = 0
        self.downloads = []

    def authenticate(self, two_factor=None):
        self.script.attempts.append(self.email)
        if self.script.requires_2fa:
            code = two_factor.resolve()
            if code != self.script.valid_code:
                raise Exception("Failed to verify 2FA code")
        if self.script.auth_error:
            raise Exception(self.script.auth_error)

    def get_drive_item(self, path):
        node = self.script.tree
        for part in [p for p in path.split("/") if p]:
            try:
                node = node[part]
            except (KeyError, TypeError):
                raise Exception(f"Path not found: {path}") from None
        return node

    def process_item_parallel(self, item, local_path, remote_path=None):
        dispatch = self.plugin_manager.dispatch
        dispatch("before_download", remote_item=item, local_path=local_path)
        dispatch("on_event", name="download_progress", remote_item=item,
                 local_path=local_path, downloaded=item.size // 2, total_size=None)
        self.script.file_started.set()
        self.script.gate.wait(10.0)
        dispatch("on_event", name="download_progress", remote_item=item,
                 local_path=local_path, downloaded=item.size, total_size=None)
        dispatch("after_download", remote_item=item, local_path=local_path, success=True)
        self.transferred += item.size
        self.script.completed.append(item.name)

    def download(self, icloud_path, local_path, log_file=None):
        self.downloads.append((icloud_path, local_path))
        for name, size in self.script.files:
            self.process_item_parallel(FakeFile(name, size), Path(local_path) / name)
        self.plugin_manager.dispatch(
            "on_event", name="download_session_completed",
            summary=self.generate_summary_report(),
        )

    def generate_summary_report(self):
        done = len(self.script.completed)
        return {
            "summary": {
                "total_files": done,
                "successful": done,
                "failed": 0,
                "skipped": 0,
                "total_bytes_transferred": self.transferred,
                "total_changed_chunks": 0,
                "timestamp": "2026-07-28 12:00:00",
            },
            "details": [],
        }


# ---------------------------------------------------------------------------
# A client that drives the server over a real socket
# ---------------------------------------------------------------------------

class _NoRedirect(urllib.request.HTTPRedirectHandler):
    """Redirects are part of what is being tested, so they are not followed."""

    def redirect_request(self, *args, **kwargs):
        return None


class Response:
    def __init__(self, status, headers, body):
        self.status = status
        self.headers = headers
        self.body = body

    @property
    def json(self):
        return json.loads(self.body.decode("utf-8"))


class Client:
    def __init__(self, server):
        self.server = server
        self.base = f"http://127.0.0.1:{server.port}"
        self.token = server.token
        self.cookie = None
        self._opener = urllib.request.build_opener(_NoRedirect)

    def request(self, method, path, body=None, token=None, use_cookie=True,
                host=None, raw_body=None, content_length=None):
        url = self.base + path
        if token:
            url += ("&" if "?" in path else "?") + f"t={token}"
        headers = {}
        if host is not None:
            headers["Host"] = host
        if use_cookie and self.cookie:
            headers["Cookie"] = self.cookie
        data = None
        if raw_body is not None:
            data = raw_body
            headers["Content-Type"] = "application/json"
        elif body is not None:
            data = json.dumps(body).encode("utf-8")
            headers["Content-Type"] = "application/json"
        if content_length is not None:
            headers["Content-Length"] = str(content_length)
        req = urllib.request.Request(url, data=data, headers=headers, method=method)
        try:
            with self._opener.open(req, timeout=10) as raw:
                return Response(raw.status, dict(raw.headers), raw.read())
        except urllib.error.HTTPError as exc:
            return Response(exc.code, dict(exc.headers), exc.read())

    def get(self, path, **kwargs):
        return self.request("GET", path, **kwargs)

    def post(self, path, body=None, **kwargs):
        return self.request("POST", path, body=body, **kwargs)

    def authorise(self):
        """First load: token in the URL, cookie back, token out of the URL."""
        response = self.request("GET", "/", token=self.token, use_cookie=False)
        assert response.status == 302
        self.cookie = response.headers["Set-Cookie"].split(";", 1)[0]
        return response


class LogSink(logging.Handler):
    def __init__(self):
        super().__init__(level=logging.DEBUG)
        self.records = []

    def emit(self, record):
        self.records.append(record)

    @property
    def text(self):
        return "\n".join(self.format(record) for record in self.records)


@pytest.fixture
def logs():
    sink = LogSink()
    logger = logging.getLogger("ifetch")
    previous = logger.level
    logger.setLevel(logging.DEBUG)
    logger.addHandler(sink)
    try:
        yield sink
    finally:
        logger.removeHandler(sink)
        logger.setLevel(previous)


def build(tmp_path, script=None, **overrides):
    """A bound server plus a client, with every outside dependency faked."""
    script = script if script is not None else Script()

    def factory(email, password):
        manager = FakeManager(email, password, script)
        script.managers.append(manager)
        return manager

    options = dict(
        token=TOKEN,
        manager_factory=factory,
        password_provider=lambda: PASSWORD,
        expiry_probe=lambda email: 12.0,
        allowed_roots=[tmp_path],
        default_local=tmp_path / "backup",
        # Never the real one: the frontend owns that folder and may fill it in
        # at any time, which would silently change what these tests assert.
        static_dir=tmp_path / "no-such-static",
        auth_timeout=10.0,
        code_timeout=10.0,
    )
    options.update(overrides)
    server = create_server(host="127.0.0.1", port=0, **options)
    server.start()
    return server, Client(server), script


@pytest.fixture
def server(tmp_path):
    made = []

    def make(script=None, **overrides):
        srv, client, scr = build(tmp_path, script=script, **overrides)
        made.append(srv)
        return srv, client, scr

    try:
        yield make
    finally:
        for srv in made:
            srv.stop()


def wait_for(predicate, timeout=10.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(0.01)
    raise AssertionError("condition was not met in time")


def sign_in(client, script, email="you@example.com"):
    client.authorise()
    response = client.post("/api/auth/start", {"email": email})
    assert response.status == 200, response.body
    return response


# ---------------------------------------------------------------------------
# Access control
# ---------------------------------------------------------------------------

def test_no_token_is_404_on_the_page_and_every_api_route(server):
    _, client, _ = server()
    assert client.get("/").status == 404
    for method, path in ROUTES:
        response = client.request(method, path, body={} if method == "POST" else None)
        assert response.status == 404, f"{method} {path} answered {response.status}"
        # A 401 would confirm there is something here worth coming back for.
        assert response.status != 401


def test_wrong_token_is_404(server):
    _, client, _ = server()
    assert client.get("/", token="not-the-token").status == 404
    assert client.get("/api/state", token="not-the-token").status == 404
    # A prefix of the real token must not pass either.
    assert client.get("/api/state", token=TOKEN[:-1]).status == 404


def test_valid_token_sets_the_cookie_and_the_cookie_then_authorises(server):
    _, client, _ = server()
    response = client.authorise()
    cookie = response.headers["Set-Cookie"]
    assert cookie.startswith(f"{COOKIE_NAME}=")
    assert "HttpOnly" in cookie
    assert "SameSite=Strict" in cookie
    assert response.headers["Location"] == "/"

    page = client.get("/")
    assert page.status == 200
    assert client.get("/api/state").status == 200


def test_a_non_loopback_host_header_is_refused_even_with_a_valid_token(server):
    _, client, _ = server()
    client.authorise()
    for hostile in ("evil.example.com", "attacker.test:8765", "192.0.2.1"):
        response = client.get("/api/state", host=hostile)
        assert response.status == 404, hostile
    # And with the token in the URL rather than the cookie.
    assert client.get("/", token=TOKEN, use_cookie=False,
                      host="evil.example.com").status == 404


def test_loopback_host_spellings_are_accepted(server):
    srv, client, _ = server()
    client.authorise()
    for good in ("127.0.0.1", f"127.0.0.1:{srv.port}", "localhost",
                 f"localhost:{srv.port}", "[::1]", f"[::1]:{srv.port}"):
        assert client.get("/api/state", host=good).status == 200, good


def test_host_is_allowed_rejects_a_missing_header():
    assert host_is_allowed(None, {"localhost"}) is False
    assert host_is_allowed("", {"localhost"}) is False
    assert host_is_allowed("localhost", {"localhost"}) is True
    assert host_is_allowed("[::1]:80", {"::1"}) is True
    assert host_is_allowed("evil.test", {"localhost", "127.0.0.1"}) is False


def test_no_cors_header_is_ever_sent(server, tmp_path):
    _, client, script = server()
    responses = [
        client.get("/"),
        client.get("/api/state"),
        client.authorise(),
        client.get("/"),
        client.get("/api/state"),
        client.get("/api/nope"),
        client.post("/api/state"),
        client.post("/api/download", {"icloud_path": "x", "local_path": "/etc"}),
        client.get("/api/browse"),
        client.request("PUT", "/api/state"),
    ]
    for response in responses:
        assert "Access-Control-Allow-Origin" not in response.headers
        assert not any(key.lower().startswith("access-control")
                       for key in response.headers)


def test_the_password_never_reaches_the_browser_or_the_log(server, logs):
    _, client, script = server(Script(files=[("a.bin", 4)]))
    sign_in(client, script)
    bodies = [
        client.get("/api/state").body,
        client.get("/api/browse").body,
        client.post("/api/auth/start", {"email": "you@example.com"}).body,
        client.post("/api/auth/2fa", {"code": "123456"}).body,
        client.post("/api/download",
                    {"icloud_path": "", "local_path": str(client.server.app.default_local)}).body,
        client.post("/api/guard", {"local_path": "/nope"}).body,
        client.get("/").body,
    ]
    for body in bodies:
        assert PASSWORD.encode() not in body
    assert PASSWORD not in logs.text
    # It did reach the downloader, which is the only place it belongs.
    assert script.managers[0].password == PASSWORD


def test_a_password_sent_by_the_browser_is_refused(server):
    _, client, _ = server()
    client.authorise()
    response = client.post("/api/auth/start",
                           {"email": "you@example.com", "password": "hunter2"})
    assert response.status == 400
    assert response.json["ok"] is False
    assert "never accepted from the browser" in response.json["error"]


def test_the_token_never_appears_in_a_body_or_a_log(server, logs):
    _, client, script = server()
    sign_in(client, script)
    bodies = [
        client.get("/").body,
        client.get("/api/state").body,
        client.get("/api/nope").body,
        client.get("/api/browse", token=TOKEN, use_cookie=False).body,
        client.request("GET", "/", token="wrong").body,
    ]
    for body in bodies:
        assert TOKEN.encode() not in body
    # The first load carries the token in the query string; the access log must
    # keep the path and drop the query.
    assert TOKEN not in logs.text
    assert any("GET /api/state" in record.getMessage() for record in logs.records)


# ---------------------------------------------------------------------------
# The page
# ---------------------------------------------------------------------------

def test_the_page_says_so_when_the_assets_are_not_installed(server):
    _, client, _ = server()
    client.authorise()
    response = client.get("/")
    assert response.status == 200
    assert response.headers["Content-Type"].startswith("text/plain")
    assert b"page assets are not installed" in response.body


def test_the_page_is_read_from_disk_at_request_time(server, tmp_path):
    static = tmp_path / "static"
    static.mkdir()
    _, client, _ = server(static_dir=static)
    client.authorise()
    assert client.get("/").headers["Content-Type"].startswith("text/plain")

    (static / "index.html").write_text("<h1>hello</h1>", encoding="utf-8")
    response = client.get("/")
    assert response.status == 200
    assert response.headers["Content-Type"].startswith("text/html")
    assert response.body == b"<h1>hello</h1>"


def test_assets_cannot_escape_the_static_folder(server, tmp_path):
    static = tmp_path / "static"
    static.mkdir()
    (static / "app.js").write_text("console.log(1)", encoding="utf-8")
    (tmp_path / "secret.txt").write_text("no", encoding="utf-8")
    _, client, _ = server(static_dir=static)
    client.authorise()

    assert client.get("/static/app.js").status == 200
    assert client.get("/static/../secret.txt").status == 404
    assert client.get("/static/missing.js").status == 404


# ---------------------------------------------------------------------------
# /api/state
# ---------------------------------------------------------------------------

def test_state_matches_the_contract_when_signed_out(server, tmp_path):
    _, client, _ = server()
    client.authorise()
    payload = client.get("/api/state").json

    assert set(payload) == {"version", "auth", "job", "last", "paths"}
    assert isinstance(payload["version"], str)
    assert set(payload["auth"]) == {"state", "email", "message", "expires_in_days"}
    assert payload["auth"]["state"] == "signed_out"
    assert payload["auth"]["email"] is None
    assert payload["auth"]["expires_in_days"] is None
    assert payload["job"] is None
    assert payload["last"] is None
    assert set(payload["paths"]) == {"default_local", "icloud_drive"}
    assert payload["paths"]["default_local"] == str(tmp_path / "backup")
    assert payload["paths"]["icloud_drive"] is None or isinstance(
        payload["paths"]["icloud_drive"], str
    )


def test_state_reports_the_session_expiry_once_signed_in(server):
    _, client, script = server()
    sign_in(client, script)
    auth = client.get("/api/state").json["auth"]
    assert auth["state"] == "signed_in"
    assert auth["email"] == "you@example.com"
    assert auth["expires_in_days"] == 12.0


# ---------------------------------------------------------------------------
# Sign in
# ---------------------------------------------------------------------------

def test_sign_in_without_two_factor(server):
    _, client, script = server()
    client.authorise()
    response = client.post("/api/auth/start", {"email": "you@example.com"})
    assert response.status == 200
    assert response.json == {
        "ok": True, "state": "signed_in", "message": "Signed in as you@example.com.",
    }


def test_two_factor_wrong_code_then_right_code(server):
    _, client, script = server(Script(requires_2fa=True, valid_code="123456"))
    client.authorise()

    started = client.post("/api/auth/start", {"email": "you@example.com"})
    assert started.status == 200
    assert started.json["ok"] is True
    assert started.json["state"] == "needs_2fa"
    assert client.get("/api/state").json["auth"]["state"] == "needs_2fa"

    wrong = client.post("/api/auth/2fa", {"code": "000000"})
    assert wrong.status == 400
    assert wrong.json["ok"] is False
    assert "reject" in wrong.json["error"].lower() or "verify" in wrong.json["error"].lower()
    # Still ready for another attempt rather than back at the start.
    assert client.get("/api/state").json["auth"]["state"] == "needs_2fa"

    right = client.post("/api/auth/2fa", {"code": "123456"})
    assert right.status == 200
    assert right.json["ok"] is True
    assert right.json["state"] == "signed_in"
    assert client.get("/api/state").json["auth"]["state"] == "signed_in"


def test_a_code_that_is_not_six_digits_is_refused_before_apple_sees_it(server):
    _, client, script = server(Script(requires_2fa=True))
    client.authorise()
    client.post("/api/auth/start", {"email": "you@example.com"})
    response = client.post("/api/auth/2fa", {"code": "abc"})
    assert response.status == 400
    assert "six-digit" in response.json["error"]
    assert len(script.attempts) == 1  # no retry was launched


def test_a_failed_sign_in_is_reported_as_an_error_state(server):
    _, client, _ = server(Script(auth_error="Invalid credentials"))
    client.authorise()
    response = client.post("/api/auth/start", {"email": "you@example.com"})
    assert response.status == 400
    assert response.json == {"ok": False, "error": "Invalid credentials"}
    assert client.get("/api/state").json["auth"]["state"] == "error"


def test_sign_in_requires_an_email(server):
    _, client, _ = server()
    client.authorise()
    response = client.post("/api/auth/start", {})
    assert response.status == 400
    assert "email" in response.json["error"]


def test_sign_out(server):
    _, client, script = server()
    sign_in(client, script)
    assert client.post("/api/auth/signout").json == {"ok": True}
    assert client.get("/api/state").json["auth"]["state"] == "signed_out"
    assert client.get("/api/browse").status == 409


# ---------------------------------------------------------------------------
# Browse
# ---------------------------------------------------------------------------

def drive():
    return FakeFolder("", [
        FakeFolder("Photos", [FakeFile("IMG_1.heic", 10)]),
        FakeFile("zebra.txt", 12),
        FakeFile("Deck.key", 4820213),
        FakeFolder("apples"),
        FakeFile("Alpha.txt", 3),
    ])


def test_browse_root_shape_and_sorting(server):
    _, client, script = server(Script(tree=drive()))
    sign_in(client, script)
    payload = client.get("/api/browse").json

    assert set(payload) == {"path", "parent", "entries"}
    assert payload["path"] == ""
    assert payload["parent"] is None  # the root has no "up"
    assert [e["name"] for e in payload["entries"]] == [
        "apples", "Photos", "Alpha.txt", "Deck.key", "zebra.txt",
    ]
    for entry in payload["entries"]:
        assert set(entry) == {"name", "kind", "size"}
    by_name = {e["name"]: e for e in payload["entries"]}
    assert by_name["Photos"] == {"name": "Photos", "kind": "dir", "size": None}
    assert by_name["Deck.key"] == {"name": "Deck.key", "kind": "package", "size": 4820213}
    assert by_name["zebra.txt"] == {"name": "zebra.txt", "kind": "file", "size": 12}


def test_browse_a_subfolder_reports_its_parent(server):
    _, client, script = server(Script(tree=drive()))
    sign_in(client, script)
    payload = client.get("/api/browse?path=Photos").json
    assert payload["path"] == "Photos"
    assert payload["parent"] == ""
    assert [e["name"] for e in payload["entries"]] == ["IMG_1.heic"]


def test_browse_refuses_to_climb_out_of_the_drive_root(server):
    _, client, script = server(Script(tree=drive()))
    sign_in(client, script)
    hostile = [
        "../etc",
        "Photos/../../etc",
        "..",
        "/etc/passwd",
        "/",
        "C:\\Windows",
        "Photos\\..\\..\\etc",
        # Fullwidth full stops normalise to '..', which is why the check runs
        # on an NFKC copy rather than the raw text.
        "\uff0e\uff0e/etc",
        "Photos/\uff0e\uff0e/\uff0e\uff0e/etc",
    ]
    for path in hostile:
        response = client.get(f"/api/browse?path={urllib.parse.quote(path)}")
        if path == "/":
            assert response.status == 200  # a bare slash is just the root
            continue
        assert response.status == 400, path
        assert response.json["ok"] is False


def test_browse_needs_a_signed_in_session(server):
    _, client, _ = server()
    client.authorise()
    response = client.get("/api/browse")
    assert response.status == 409
    assert "Sign in" in response.json["error"]


def test_browse_a_file_is_not_a_listing(server):
    _, client, script = server(Script(tree=drive()))
    sign_in(client, script)
    response = client.get("/api/browse?path=zebra.txt")
    assert response.status == 400
    assert "not a folder" in response.json["error"]


def test_browse_reports_a_missing_path(server):
    _, client, script = server(Script(tree=drive()))
    sign_in(client, script)
    response = client.get("/api/browse?path=Nowhere")
    assert response.status == 404
    assert response.json["ok"] is False


# ---------------------------------------------------------------------------
# Jobs
# ---------------------------------------------------------------------------

def test_download_reports_progress_without_inventing_a_denominator(server, tmp_path):
    script = Script(files=[("a.bin", 400), ("b.bin", 600)])
    script.gate.clear()
    _, client, script = server(script)
    sign_in(client, script)

    started = client.post("/api/download",
                          {"icloud_path": "Documents", "local_path": str(tmp_path)})
    assert started.status == 200
    assert started.json == {"ok": True, "job_id": "j1"}

    wait_for(script.file_started.is_set)
    job = wait_for(lambda: client.get("/api/state").json["job"])
    assert set(job) == {
        "id", "kind", "state", "label", "started_at", "finished_at",
        "message", "progress", "result",
    }
    assert job["id"] == "j1"
    assert job["kind"] == "download"
    assert job["state"] == "running"
    assert job["label"] == f"Documents -> {tmp_path}"
    assert isinstance(job["started_at"], float)
    assert job["finished_at"] is None
    assert job["result"] is None

    progress = job["progress"]
    assert set(progress) == {
        "files_done", "files_total", "bytes_done", "bytes_total",
        "skipped", "failed", "current",
    }
    assert progress["bytes_done"] == 200  # half of the first file, and nothing else
    assert progress["files_done"] == 0
    # The event stream reports bytes per file and never a run total, so these
    # stay null instead of becoming a denominator nobody computed.
    assert progress["files_total"] is None
    assert progress["bytes_total"] is None
    assert progress["current"] == "a.bin"

    script.gate.set()
    finished = wait_for(lambda: client.get("/api/state").json["last"])
    assert finished["state"] == "done"
    assert finished["finished_at"] is not None
    assert finished["progress"]["files_total"] == 2
    assert finished["progress"]["files_done"] == 2
    assert finished["progress"]["bytes_done"] == 1000
    # Even at the end: what was transferred is not what there was to transfer.
    assert finished["progress"]["bytes_total"] is None
    assert finished["result"]["summary"]["successful"] == 2
    assert client.get("/api/state").json["job"] is None


def test_only_one_job_runs_at_a_time(server, tmp_path):
    script = Script(files=[("a.bin", 4)])
    script.gate.clear()
    _, client, script = server(script)
    sign_in(client, script)

    first = client.post("/api/download", {"icloud_path": "", "local_path": str(tmp_path)})
    assert first.status == 200
    wait_for(script.file_started.is_set)

    second = client.post("/api/download", {"icloud_path": "", "local_path": str(tmp_path)})
    assert second.status == 409
    assert second.json["ok"] is False
    assert "already running" in second.json["error"]

    third = client.post("/api/guard", {"local_path": str(tmp_path)})
    assert third.status == 409

    script.gate.set()
    wait_for(lambda: client.get("/api/state").json["job"] is None)
    assert client.post("/api/download",
                       {"icloud_path": "", "local_path": str(tmp_path)}).status == 200


def test_cancel_stops_the_run_and_the_state_becomes_cancelled(server, tmp_path):
    script = Script(files=[(f"f{i}.bin", 100) for i in range(50)])
    script.gate.clear()
    _, client, script = server(script)
    sign_in(client, script)

    client.post("/api/download", {"icloud_path": "", "local_path": str(tmp_path)})
    wait_for(script.file_started.is_set)

    assert client.post("/api/cancel").json == {"ok": True}
    script.gate.set()  # let the file already in flight finish writing

    last = wait_for(lambda: client.get("/api/state").json["last"])
    assert last["state"] == "cancelled"
    assert "resumable" in last["message"]
    assert last["result"] is None
    # It really stopped: the remaining files were never touched.
    assert len(script.completed) < 50


def test_cancel_with_nothing_running_is_a_conflict(server):
    _, client, script = server()
    sign_in(client, script)
    response = client.post("/api/cancel")
    assert response.status == 409
    assert "nothing to cancel" in response.json["error"]


def test_a_failed_job_is_reported_rather_than_swallowed(server, tmp_path):
    _, client, script = server(guard_scan=_boom)
    sign_in(client, script)
    client.post("/api/guard", {"local_path": str(tmp_path)})
    last = wait_for(lambda: client.get("/api/state").json["last"])
    assert last["state"] == "failed"
    assert last["message"] == "the disk went away"
    assert last["result"] is None


def _boom(root):
    raise RuntimeError("the disk went away")


# ---------------------------------------------------------------------------
# Destination containment
# ---------------------------------------------------------------------------

def test_a_destination_outside_the_allowed_roots_is_refused(server, tmp_path):
    _, client, script = server()
    sign_in(client, script)
    for hostile in ["/etc", "/etc/cron.d", "relative/path", "", "~/../../etc"]:
        response = client.post("/api/download",
                               {"icloud_path": "x", "local_path": hostile})
        assert response.status == 400, hostile
        assert response.json["ok"] is False


def test_a_symlink_cannot_be_used_as_a_door_out(server, tmp_path):
    inside = tmp_path / "inside"
    inside.mkdir()
    (inside / "escape").symlink_to("/etc")
    _, client, script = server()
    sign_in(client, script)
    response = client.post("/api/download",
                           {"icloud_path": "x", "local_path": str(inside / "escape")})
    assert response.status == 400
    assert "outside the folders" in response.json["error"]


def test_clean_local_path_accepts_a_plausible_destination(tmp_path):
    target = clean_local_path(str(tmp_path / "backup"), [tmp_path])
    assert target == (tmp_path / "backup").resolve()

    (tmp_path / "afile").write_text("x", encoding="utf-8")
    with pytest.raises(Exception):
        clean_local_path(str(tmp_path / "afile"), [tmp_path])


def test_clean_remote_path_keeps_apple_spelling():
    assert clean_remote_path("Documents/Photos") == "Documents/Photos"
    assert clean_remote_path("Documents/") == "Documents"
    assert clean_remote_path(None) == ""
    assert clean_remote_path("") == ""
    assert clean_remote_path("/") == ""
    # NFD is what Apple returns; re-spelling it as NFC would 404.
    assert clean_remote_path("cafe\u0301") == "cafe\u0301"
    for bad in ["/Documents", "..", "a/../b", 12, "a\x00b"]:
        with pytest.raises(Exception):
            clean_remote_path(bad)


# ---------------------------------------------------------------------------
# Reports pass through unchanged
# ---------------------------------------------------------------------------

def partial_guard_report():
    report = GuardReport(root="/some/folder", platform_name="Linux")
    report.total = ByteAccount(label="(total)", logical_bytes=1000, resident_bytes=400)
    report.unreadable = [{"path": "/some/folder/locked", "error": "Permission denied"}]
    report.signals_unavailable = [
        {"signal": "dataless", "reason": "not macOS"},
    ]
    return report


def tripped_vanish_report():
    return VanishedReport(
        root="/some/mirror",
        baseline_count=900,
        has_baseline=True,
        scan=ScanEvidence(usable=True, item_count=10, scan_id=1),
        breaker=BreakerVerdict(
            tripped=True,
            reason="mass_disappearance",
            vanished_count=890,
            baseline_count=900,
            detail="890 of 900 baseline paths are absent from the latest listing.",
            cannot_rule_out=["a listing failure", "a partial scan"],
        ),
    )


def test_a_partial_guard_report_is_passed_through_unchanged(server, tmp_path):
    expected = partial_guard_report().to_dict()
    assert expected["complete"] is False
    _, client, script = server(guard_scan=lambda root: partial_guard_report().to_dict())
    sign_in(client, script)

    assert client.post("/api/guard", {"local_path": str(tmp_path)}).json == {
        "ok": True, "job_id": "j1",
    }
    last = wait_for(lambda: client.get("/api/state").json["last"])
    assert last["kind"] == "guard"
    assert last["state"] == "done"
    # The one thing this job cannot do is said out loud rather than left to be
    # discovered by pressing Cancel.
    assert "cannot be stopped once started" in last["message"]
    assert last["result"] == expected
    assert last["result"]["complete"] is False
    assert last["result"]["unreadable"]


def test_a_tripped_vanish_breaker_is_passed_through_unchanged(server, tmp_path):
    expected = tripped_vanish_report().to_dict()
    assert expected["refused"] is True
    _, client, script = server(vanish_check=lambda root: tripped_vanish_report().to_dict())
    sign_in(client, script)

    client.post("/api/vanish", {"local_path": str(tmp_path)})
    last = wait_for(lambda: client.get("/api/state").json["last"])
    assert last["kind"] == "vanish"
    assert last["result"] == expected
    assert last["result"]["refused"] is True
    assert last["result"]["breaker"]["cannot_rule_out"]


def test_guard_falls_back_to_the_icloud_folder_when_no_path_is_given(server, tmp_path):
    seen = []

    def scan(root):
        seen.append(root)
        return {"complete": True}

    _, client, script = server(guard_scan=scan, allowed_roots=[tmp_path, Path.home()])
    sign_in(client, script)
    response = client.post("/api/guard", {})
    # Either it ran against the machine's iCloud folder, or it said the folder
    # is not there. Both are answers; silence would not be.
    assert response.status in (200, 400)
    if response.status == 400:
        assert "does not exist" in response.json["error"]


# ---------------------------------------------------------------------------
# Malformed requests
# ---------------------------------------------------------------------------

def announce_body(client, length):
    """Send only the headers, then read the answer.

    An oversized body is refused on its Content-Length, before a byte of it is
    read, which is the property being checked - and it is also why the body is
    never sent here: a client that pushes megabytes at a server that has
    already stopped reading deadlocks on its own socket buffer.
    """
    connection = http.client.HTTPConnection("127.0.0.1", client.server.port, timeout=10)
    connection.putrequest("POST", "/api/auth/start")
    connection.putheader("Host", "127.0.0.1")
    connection.putheader("Cookie", client.cookie)
    connection.putheader("Content-Type", "application/json")
    connection.putheader("Content-Length", str(length))
    connection.endheaders()
    raw = connection.getresponse()
    try:
        return Response(raw.status, dict(raw.getheaders()), raw.read())
    finally:
        connection.close()


def test_an_oversized_body_is_refused_before_it_is_read(server):
    _, client, _ = server()
    client.authorise()
    response = announce_body(client, 200_000)
    assert response.status == 413
    assert response.json["ok"] is False
    assert "larger than" in response.json["error"]
    assert "Access-Control-Allow-Origin" not in response.headers


def test_a_body_far_beyond_the_cap_is_still_a_clean_refusal(server):
    _, client, _ = server()
    client.authorise()
    assert announce_body(client, 10 * 1024 * 1024).status == 413


def test_malformed_json_is_a_clean_400(server):
    _, client, _ = server()
    client.authorise()
    for raw in [b"{not json", b"", b"[]", b'"hello"', b"\xff\xfe"]:
        response = client.post("/api/auth/start", raw_body=raw)
        assert response.status == 400, raw
        assert response.json["ok"] is False
        assert "Traceback" not in response.json["error"]


def test_an_unknown_route_is_a_clean_404(server):
    _, client, _ = server()
    client.authorise()
    for path in ["/api/nope", "/api/auth/nope", "/nope", "/api/"]:
        response = client.get(path)
        assert response.status == 404, path
    assert client.get("/api/nope").json["ok"] is False


def test_the_wrong_method_is_a_clean_405(server):
    _, client, _ = server()
    client.authorise()
    response = client.post("/api/state")
    assert response.status == 405
    assert response.headers["Allow"] == "GET"
    assert "GET requests only" in response.json["error"]

    response = client.get("/api/cancel")
    assert response.status == 405
    assert response.headers["Allow"] == "POST"

    for method in ("PUT", "DELETE", "PATCH", "OPTIONS"):
        assert client.request(method, "/api/state").status in (404, 405)


def test_an_unexpected_error_becomes_a_500_without_a_traceback(server, monkeypatch):
    _, client, _ = server()
    client.authorise()
    app = client.server.app
    monkeypatch.setitem(app.routes, "/api/state", ("GET", _explode))
    response = client.get("/api/state")
    assert response.status == 500
    assert response.json["ok"] is False
    assert "Traceback" not in response.json["error"]
    assert "boom" not in response.json["error"]


def _explode(payload):
    raise ValueError("boom")


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------

def test_the_server_starts_and_stops_and_releases_the_port(tmp_path):
    server, client, _ = build(tmp_path)
    port = server.port
    client.authorise()
    assert client.get("/api/state").status == 200

    server.stop()

    with pytest.raises((urllib.error.URLError, ConnectionError, OSError)):
        client.get("/api/state")

    # The port is free: something else can listen on it now.
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        probe.bind(("127.0.0.1", port))
        probe.listen(1)


def test_stopping_cancels_a_job_still_running(tmp_path):
    script = Script(files=[(f"f{i}.bin", 10) for i in range(20)])
    script.gate.clear()
    server, client, script = build(tmp_path, script=script)
    try:
        sign_in(client, script)
        client.post("/api/download", {"icloud_path": "", "local_path": str(tmp_path)})
        wait_for(script.file_started.is_set)
    finally:
        server.stop()
    script.gate.set()
    assert server.app.jobs.wait(10.0)
    assert server.app.jobs.last.state == "cancelled"


def test_the_bind_defaults_to_loopback(tmp_path):
    server, _, _ = build(tmp_path)
    try:
        assert server._http.server_address[0] == "127.0.0.1"
        assert server.url.startswith(f"http://127.0.0.1:{server.port}/?t=")
    finally:
        server.stop()


def test_the_allowed_host_set_follows_the_bind_address(tmp_path):
    server = create_server(host="127.0.0.1", port=0, token=TOKEN,
                           static_dir=tmp_path / "none")
    try:
        assert "127.0.0.1" in server.app.allowed_hosts
        assert "localhost" in server.app.allowed_hosts
        assert "evil.test" not in server.app.allowed_hosts
    finally:
        server.stop()


# ---------------------------------------------------------------------------
# ifetch serve
# ---------------------------------------------------------------------------

class Sink:
    def __init__(self):
        self.text = ""

    def write(self, chunk):
        self.text += chunk

    def flush(self):
        pass


def test_serve_warns_and_names_what_a_non_loopback_bind_exposes():
    out = Sink()
    serve_cli._warn_about_host("0.0.0.0", 8765, [], out)
    assert "WARNING" in out.text
    assert "0.0.0.0:8765" in out.text
    assert "iCloud" in out.text
    assert "--allow-host" in out.text

    quiet = Sink()
    serve_cli._warn_about_host("127.0.0.1", 8765, [], quiet)
    assert quiet.text == ""


def test_the_bind_warning_reads_as_a_sentence():
    """A security warning nobody can parse is a warning nobody heeds.

    This one was assembled from a fragment substituted mid-sentence, and read
    "anyone who reaches every machine that can reach this one and has the
    token" — grammatical wreckage in the one paragraph that has to land.
    """
    for host, expected in (
        ("0.0.0.0", "Anyone who can reach this machine and has the"),
        ("10.0.0.5", "Anyone who can reach 10.0.0.5 and has the"),
    ):
        out = Sink()
        serve_cli._warn_about_host(host, 8765, [], out)
        flat = " ".join(out.text.split())
        assert expected in flat, flat
        assert "reaches every machine" not in flat

    # The wildcard note is specific to a wildcard bind; a named address has an
    # allowed Host of its own and does not need it.
    named = Sink()
    serve_cli._warn_about_host("10.0.0.5", 8765, [], named)
    assert "--allow-host" not in named.text


def test_serve_parser_defaults():
    args = serve_cli.build_parser().parse_args([])
    assert args.host == "127.0.0.1"
    assert args.port == 8765
    assert args.allow_paths == []
    assert args.open_browser is False


def test_serve_reports_a_port_it_cannot_bind(monkeypatch):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as taken:
        taken.bind(("127.0.0.1", 0))
        taken.listen(1)
        port = taken.getsockname()[1]
        code = serve_cli.main(["--port", str(port)], stdout=Sink())
    assert code == serve_cli.EXIT_ERROR


# ---------------------------------------------------------------------------
# Unit-level checks that do not need a socket
# ---------------------------------------------------------------------------

def test_token_is_random_and_long_enough():
    first = WebUIApp(static_dir=Path("/nowhere"))
    second = WebUIApp(static_dir=Path("/nowhere"))
    assert first.token != second.token
    assert len(first.token) >= 32
    assert first.matches_token(first.token) is True
    assert first.matches_token(second.token) is False
    assert first.matches_token(None) is False
    assert first.matches_token("") is False


def test_http_request_line_reaches_the_server_intact(tmp_path):
    """A raw HTTP/1.1 request, to prove nothing depends on urllib's helpfulness."""
    server, client, _ = build(tmp_path)
    try:
        connection = http.client.HTTPConnection("127.0.0.1", server.port, timeout=10)
        connection.request("GET", f"/api/state?t={TOKEN}",
                           headers={"Host": "evil.example.com"})
        assert connection.getresponse().status == 404
        connection.close()

        connection = http.client.HTTPConnection("127.0.0.1", server.port, timeout=10)
        connection.request("GET", f"/api/state?t={TOKEN}", headers={"Host": "localhost"})
        response = connection.getresponse()
        assert response.status == 200
        assert "Access-Control-Allow-Origin" not in dict(response.getheaders())
        connection.close()
    finally:
        server.stop()
