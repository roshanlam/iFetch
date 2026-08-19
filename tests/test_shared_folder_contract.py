"""Contract tests for folders shared by *another* Apple ID.

Why a fixture harness rather than a live test
---------------------------------------------
Validating this path for real needs two Apple IDs and a folder shared between
them, which no automated suite can provision. What *can* be pinned down is the
contract: given the exact HTTP responses Apple returns for a cross-account
share, does iFetch take the right branch?

The responses replayed here are the shapes reported against other iCloud
clients, most importantly the one where **the share root works and any
subdirectory of it returns HTTP 400** (rclone/rclone#9477). The owner's
``docwsid`` is not addressable by a participant through the ordinary
``download/by_id`` endpoint; Apple requires the ``shareID`` to be threaded
through, and a client that does not do so gets ``WSObjectNotFound`` on files
and ``400 Bad Request`` on nested listings.

These tests therefore assert two things:

1. iFetch *attempts* the shareID-bearing request rather than giving up at the
   first ``WSObjectNotFound`` - the fallback is wired, in order, with the right
   parameters;
2. when Apple answers, the bytes are returned; when Apple refuses, the failure
   is surfaced honestly rather than being recorded as a successful empty file.

What they do NOT prove is that Apple's live behaviour matches these fixtures.
``docs/shared-folder-validation.md`` holds the manual procedure for that, and
the caveat in the README stands until someone runs it.
"""

import io
import json
import sys
from pathlib import Path

import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))

from ifetch.downloader import DownloadManager, SyncState  # noqa: E402

# ---------------------------------------------------------------------------
# Recorded Apple responses
# ---------------------------------------------------------------------------

#: Apple's answer when a participant asks for an owner's document by its
#: docwsid without a shareID. This is what makes a naive client give up.
WS_OBJECT_NOT_FOUND = {
    "errorCode": 0,
    "reason": "WSObjectNotFound",
    "serverErrorCode": "WSObjectNotFound",
}

#: Apple's answer for any operation one level below a share root
#: (rclone/rclone#9477). Auth is fine; the request shape is what it rejects.
SHARED_SUBDIR_400 = {
    "requestUUID": "00000000-0000-0000-0000-000000000000",
    "errorReason": "Bad Request",
    "errorCode": 400,
}

#: A successful download/by_id response for a package-free shared file.
DATA_TOKEN_RESPONSE = {
    "document_id": "DOC-OWNED-BY-SOMEONE-ELSE",
    "item_id": "ITEM-1",
    "owner_dsid": 987654321,
    "data_token": {
        "url": "https://cvws.icloud-content.invalid/S/shared-file?token=abc",
        "token": "abc",
        "signature": "sig",
        "wrapping_key": "wk",
        "reference_signature": "rs",
    },
    "double_etag": "etag::1",
}

#: The same, for a shared *package* bundle: Apple returns a package_token
#: instead of a data_token, and the URL serves a ZIP.
PACKAGE_TOKEN_RESPONSE = {
    "document_id": "DOC-SHARED-KEYNOTE",
    "item_id": "ITEM-2",
    "owner_dsid": 987654321,
    "package_token": {
        "url": "https://cvws.icloud-content.invalid/S/shared-package?token=xyz",
        "token": "xyz",
        "signature": "sig",
        "wrapping_key": "wk",
        "reference_signature": "rs",
    },
    "double_etag": "etag::2",
}


class RecordedResponse:
    """A requests-like response replaying a recorded payload."""

    def __init__(self, payload=None, body=b"", status_code=200):
        self._payload = payload
        self.content = body
        self.status_code = status_code
        self.headers = {"content-length": str(len(body))} if body else {}
        self.url = "https://cvws.icloud-content.invalid/S/replayed"
        self.raw = io.BytesIO(body)

    @property
    def ok(self):
        """requests exposes this, and the fallback branches on it."""
        return self.status_code < 400

    def json(self):
        if self._payload is None:
            raise ValueError("no JSON body")
        return self._payload

    def iter_content(self, chunk_size=8192):
        stream = io.BytesIO(self.content)
        while True:
            chunk = stream.read(chunk_size)
            if not chunk:
                return
            yield chunk

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class RecordingDriveSession:
    """Captures the requests iFetch makes so their shape can be asserted."""

    def __init__(self, responses):
        self.responses = responses
        self.requests = []

    def get(self, url, params=None, **kwargs):
        self.requests.append({"url": url, "params": dict(params or {})})
        handler = self.responses.get("get")
        if callable(handler):
            return handler(url, params or {})
        return handler


class SharedDriveService:
    """Stand-in for pyicloud's DriveService for a cross-account share."""

    def __init__(self, session, params=None):
        self.session = session
        self.params = params or {"dsid": "111", "clientId": "cid"}
        # Attribute name matters: _try_shared_open builds the URL from it.
        self._document_root = "https://p99-docws.icloud.com.invalid"

    def get_file(self, file_id, zone=None, stream=True, **kwargs):
        raise Exception(json.dumps(WS_OBJECT_NOT_FOUND))

    def get_node_data(self, node_id, share_id=None):
        raise Exception(json.dumps(SHARED_SUBDIR_400))


class SharedNode:
    """A DriveNode for a file inside a folder shared by another Apple ID."""

    type = "file"

    def __init__(self, name="shared.pdf", share_id="SHARE-ABC",
                 docwsid="DOC-OWNED-BY-SOMEONE-ELSE", drivewsid=None, size=42):
        self.name = name
        self.size = size
        self.date_modified = None
        self.date_changed = None
        self.data = {
            "docwsid": docwsid,
            "drivewsid": drivewsid or f"FILE::com.apple.CloudDocs::{docwsid}",
            "zone": "com.apple.CloudDocs",
            "shareID": share_id,  # capital ID: the key pyicloud itself reads
            "type": "FILE",
        }
        self.connection = None

    def open(self, stream=True):
        # Exactly what pyicloud does: get_file(docwsid, zone=...) -> 404.
        raise Exception(json.dumps(WS_OBJECT_NOT_FOUND))


@pytest.fixture
def manager(tmp_path):
    def build(**kwargs):
        mgr = DownloadManager(email="you@example.com", max_retries=1, **kwargs)
        root = tmp_path / "dest"
        root.mkdir(exist_ok=True)
        mgr.root_path = root
        mgr.sync_state = SyncState(root)
        return mgr, root

    return build


def attach_drive(manager_obj, service):
    """Wire a fake pyicloud API exposing ``api.drive`` onto the manager."""

    class FakeAPI:
        drive = service

    manager_obj.api = FakeAPI()
    return manager_obj


# ---------------------------------------------------------------------------
# The fallback is wired, and shaped correctly
# ---------------------------------------------------------------------------

class TestSharedFallbackIsAttempted:
    def test_a_wsobjectnotfound_triggers_the_shareid_fallback(self, manager):
        """Without this, a shared file is simply reported as missing."""
        calls = []

        def handler(url, params):
            calls.append((url, params))
            return RecordedResponse(payload=DATA_TOKEN_RESPONSE)

        service = SharedDriveService(RecordingDriveSession({"get": handler}))
        mgr, root = manager()
        attach_drive(mgr, service)

        node = SharedNode()
        mgr._try_shared_open(node)

        assert calls, "no shareID request was attempted"

    def test_the_fallback_request_carries_the_share_id_and_document_id(self, manager):
        """Apple needs both; omitting shareID is exactly what 404s."""
        def handler(url, params):
            return RecordedResponse(payload=DATA_TOKEN_RESPONSE)

        session = RecordingDriveSession({"get": handler})
        service = SharedDriveService(session)
        mgr, root = manager()
        attach_drive(mgr, service)

        mgr._try_shared_open(SharedNode(share_id="SHARE-ABC"))

        # The *first* request is the by_id lookup; the second follows the signed
        # URL it returns, so assert against the first specifically.
        first = session.requests[0]
        assert "download/by_id" in first["url"]
        assert first["params"]["shareID"] == "SHARE-ABC"
        assert first["params"]["document_id"] == "DOC-OWNED-BY-SOMEONE-ELSE"
        # The zone must be in the path, not invented.
        assert "com.apple.CloudDocs" in first["url"]

    def test_a_node_with_no_share_id_anywhere_does_not_attempt_the_fallback(self, manager):
        """An ordinary owned file that 404s is a different problem.

        "No shareID" here means none on the node *and* none inherited from an
        ancestor - this node was never reached through a share root. That
        distinction is the whole of iFetch #15: a file inside somebody else's
        shared folder also arrives with no shareID of its own, and it must
        reach the fallback. See :class:`TestShareContextReachesDescendants`.
        """
        calls = []

        def handler(url, params):
            calls.append(url)
            return RecordedResponse(payload=DATA_TOKEN_RESPONSE)

        service = SharedDriveService(RecordingDriveSession({"get": handler}))
        mgr, root = manager()
        attach_drive(mgr, service)

        node = SharedNode(share_id=None)
        node.data.pop("shareID")
        mgr._try_shared_open(node)

        assert calls == []


# ---------------------------------------------------------------------------
# Apple answers: the bytes come back
# ---------------------------------------------------------------------------

class TestSharedDownloadSucceeds:
    def test_a_data_token_response_yields_the_file_body(self, manager):
        body = b"%PDF-1.4 shared document body"

        def handler(url, params):
            if "download/by_id" in url:
                return RecordedResponse(payload=DATA_TOKEN_RESPONSE)
            return RecordedResponse(body=body)

        service = SharedDriveService(RecordingDriveSession({"get": handler}))
        mgr, root = manager()
        attach_drive(mgr, service)

        response = mgr._try_shared_open(SharedNode())

        assert response is not None
        assert response.content == body

    def test_a_shared_package_returns_its_archive(self, manager):
        """A shared .key hits the package_token branch, not data_token."""
        import zipfile

        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w") as zf:
            zf.writestr("Deck.key/Index.zip", b"shared-index")
        archive = buffer.getvalue()

        def handler(url, params):
            if "download/by_id" in url:
                return RecordedResponse(payload=PACKAGE_TOKEN_RESPONSE)
            return RecordedResponse(body=archive)

        service = SharedDriveService(RecordingDriveSession({"get": handler}))
        mgr, root = manager()
        attach_drive(mgr, service)

        response = mgr._try_shared_open(SharedNode(name="Deck.key"))

        assert response is not None
        assert response.content.startswith(b"PK\x03\x04")


# ---------------------------------------------------------------------------
# Apple refuses: fail honestly
# ---------------------------------------------------------------------------

class TestSharedDownloadFailsHonestly:
    def test_the_nested_subdirectory_400_is_not_reported_as_success(self, manager):
        """rclone/rclone#9477's shape. A 400 must never become an empty file."""

        def handler(url, params):
            return RecordedResponse(payload=SHARED_SUBDIR_400, status_code=400)

        service = SharedDriveService(RecordingDriveSession({"get": handler}))
        mgr, root = manager()
        attach_drive(mgr, service)

        destination = root / "nested" / "shared.pdf"
        result = mgr.download_drive_item(SharedNode(), destination)

        assert result is False
        assert not destination.exists(), "a refused download left a file behind"
        assert [r.status for r in mgr.download_results] == ["failed"]

    def test_a_failed_shared_download_records_no_sync_state(self, manager):
        """A false 'completed' entry would make the next run skip it forever."""

        def handler(url, params):
            return RecordedResponse(payload=SHARED_SUBDIR_400, status_code=400)

        service = SharedDriveService(RecordingDriveSession({"get": handler}))
        mgr, root = manager()
        attach_drive(mgr, service)

        destination = root / "shared.pdf"
        mgr.download_drive_item(SharedNode(), destination)

        assert mgr.sync_state.key_for(destination) not in mgr.sync_state._data

    def test_the_failure_is_logged_with_an_actionable_hint(self, manager, caplog):
        def handler(url, params):
            return RecordedResponse(payload=SHARED_SUBDIR_400, status_code=400)

        service = SharedDriveService(RecordingDriveSession({"get": handler}))
        mgr, root = manager()
        attach_drive(mgr, service)

        with caplog.at_level("WARNING"):
            mgr.download_drive_item(SharedNode(), root / "shared.pdf")

        messages = " ".join(r.message for r in caplog.records)
        assert "shared" in messages.lower()

    def test_a_transport_error_during_the_fallback_is_not_fatal_to_the_run(self, manager):
        """One unreachable shared file must not abort a whole backup."""

        def handler(url, params):
            raise ConnectionError("connection reset by peer")

        service = SharedDriveService(RecordingDriveSession({"get": handler}))
        mgr, root = manager()
        attach_drive(mgr, service)

        assert mgr._try_shared_open(SharedNode()) is None

    def test_a_malformed_token_response_is_refused(self, manager):
        """Apple answered 200 but with no usable URL; that is not a success."""

        def handler(url, params):
            return RecordedResponse(payload={"document_id": "X", "double_etag": "e"})

        service = SharedDriveService(RecordingDriveSession({"get": handler}))
        mgr, root = manager()
        attach_drive(mgr, service)

        assert mgr._try_shared_open(SharedNode()) is None


# ---------------------------------------------------------------------------
# Fixture integrity
# ---------------------------------------------------------------------------

class TestRecordedFixtures:
    """Guard the fixtures themselves: a drifted recording proves nothing."""

    def test_the_404_fixture_is_the_shape_pyicloud_raises_on(self):
        assert WS_OBJECT_NOT_FOUND["reason"] == "WSObjectNotFound"

    def test_the_400_fixture_matches_the_reported_subdirectory_failure(self):
        assert SHARED_SUBDIR_400["errorCode"] == 400
        assert SHARED_SUBDIR_400["errorReason"] == "Bad Request"

    def test_token_fixtures_carry_a_url_because_that_is_what_is_consumed(self):
        assert DATA_TOKEN_RESPONSE["data_token"]["url"].startswith("https://")
        assert PACKAGE_TOKEN_RESPONSE["package_token"]["url"].startswith("https://")

    def test_data_and_package_responses_are_mutually_exclusive(self):
        """Apple returns one or the other; code that assumes both would be wrong."""
        assert "package_token" not in DATA_TOKEN_RESPONSE
        assert "data_token" not in PACKAGE_TOKEN_RESPONSE


# ---------------------------------------------------------------------------
# iFetch #15 / rclone #9477: the share context has to survive the descent
# ---------------------------------------------------------------------------

class SharedFolderNode:
    """A shared folder whose children arrive exactly as pyicloud builds them.

    ``DriveNode.get_children()`` constructs each child from Apple's raw item
    payload and injects nothing::

        DriveNode(self.connection, item_data)

    Apple does not repeat ``shareID`` on those items, so every descendant of a
    share root is born without one. This fake reproduces that faithfully - a
    helpful fake that copied the identifier down would make the tests below
    pass while the real defect survived.
    """

    type = "folder"

    def __init__(self, name="Oeuvres", share_id="SHARE-ABC", children=None):
        self.name = name
        self.data = {
            "drivewsid": f"FOLDER::com.apple.CloudDocs::{name}",
            "docwsid": f"DOC-{name}",
            "zone": "com.apple.CloudDocs",
            "type": "FOLDER",
        }
        if share_id is not None:
            self.data["shareID"] = share_id
        self._children = {c.name: c for c in (children or [])}

    def dir(self):
        return list(self._children)

    def __getitem__(self, name):
        return self._children[name]


def bare_child(name="JAB-007.jxl", docwsid="DOC-JAB"):
    """A file inside a share: no shareID of its own, because Apple sends none."""
    node = SharedNode(name=name, docwsid=docwsid)
    node.data.pop("shareID")
    return node


class TestShareContextReachesDescendants:
    """The regression for issue #15, at the level the user actually hit it.

    The unit-level behaviour of the inheritance rule lives in
    ``tests/test_sharing.py``. What these assert is that iFetch's *traversal*
    applies it, on both paths that resolve a child - because a rule that is
    never invoked fixes nothing.
    """

    @staticmethod
    def _share_tree():
        leaf = bare_child()
        subfolder = SharedFolderNode("Photoshoot", share_id=None, children=[leaf])
        root = SharedFolderNode("Oeuvres", share_id="SHARE-ABC", children=[subfolder])
        return root, subfolder, leaf

    def test_the_fake_reproduces_the_defect_it_is_testing(self):
        """Pins the premise: children really do arrive without a shareID."""
        root, subfolder, leaf = self._share_tree()
        assert root.data["shareID"] == "SHARE-ABC"
        assert "shareID" not in subfolder.data
        assert "shareID" not in leaf.data

    def test_walking_to_a_file_carries_the_share_id_down(self, manager):
        mgr, _ = manager()
        root, _, _ = self._share_tree()

        subfolder = mgr._resolve_child(root, "Photoshoot")
        leaf = mgr._resolve_child(subfolder, "JAB-007.jxl")

        assert subfolder.data["shareID"] == "SHARE-ABC"
        assert leaf.data["shareID"] == "SHARE-ABC"

    def test_the_seeded_key_is_the_one_pyicloud_reads_for_listings(self, manager):
        """Seeding fixes the HTTP 400 on the next listing, not just the download.

        pyicloud does ``get_node_data(drivewsid, self.data.get("shareID"))``
        when it fetches children, so a subfolder that carries the identifier
        can be listed. That is rclone #9477's symptom, repaired in passing.
        """
        mgr, _ = manager()
        root, _, _ = self._share_tree()
        subfolder = mgr._resolve_child(root, "Photoshoot")
        assert subfolder.data.get("shareID") == "SHARE-ABC"

    def test_an_inherited_file_now_reaches_the_fallback(self, manager):
        """Before the fix this file's shareID was None and Strategy 1 was skipped."""
        calls = []

        def handler(url, params):
            calls.append(params)
            return RecordedResponse(payload=DATA_TOKEN_RESPONSE)

        service = SharedDriveService(RecordingDriveSession({"get": handler}))
        mgr, _ = manager()
        attach_drive(mgr, service)

        root, _, _ = self._share_tree()
        subfolder = mgr._resolve_child(root, "Photoshoot")
        leaf = mgr._resolve_child(subfolder, "JAB-007.jxl")

        mgr._try_shared_open(leaf)

        assert calls, "the inherited shareID never reached the download request"
        assert calls[0]["shareID"] == "SHARE-ABC"
        assert calls[0]["document_id"] == "DOC-JAB"

    def test_the_recursive_download_walk_inherits_too(self, manager):
        """process_item_parallel resolves children with ``item[name]`` directly.

        That bypasses ``_resolve_child`` entirely, so the descent needs its own
        inheritance step - and this is the path a real ``ifetch <folder>`` run
        takes, which is to say the one issue #15 was filed against.
        """
        mgr, root_dir = manager()
        attach_drive(mgr, SharedDriveService(RecordingDriveSession({})))
        share_root, _, leaf = self._share_tree()

        mgr.process_item_parallel(share_root, root_dir / "Oeuvres", "Oeuvres")

        assert leaf.data.get("shareID") == "SHARE-ABC"

    def test_a_nested_share_keeps_its_own_identifier(self, manager):
        """A share inside a share overrides; it must not be masked by the outer one."""
        mgr, _ = manager()
        inner_leaf = bare_child("inner.txt", docwsid="DOC-INNER")
        inner = SharedFolderNode("Inner", share_id="SHARE-INNER", children=[inner_leaf])
        outer = SharedFolderNode("Outer", share_id="SHARE-OUTER", children=[inner])

        resolved_inner = mgr._resolve_child(outer, "Inner")
        resolved_leaf = mgr._resolve_child(resolved_inner, "inner.txt")

        assert resolved_inner.data["shareID"] == "SHARE-INNER"
        assert resolved_leaf.data["shareID"] == "SHARE-INNER"

    def test_owned_content_gains_no_share_keys(self, manager):
        """The unshared path must be untouched by any of this."""
        mgr, _ = manager()
        leaf = bare_child("report.pdf", docwsid="DOC-OWNED")
        owned = SharedFolderNode("Documents", share_id=None, children=[leaf])

        resolved = mgr._resolve_child(owned, "report.pdf")

        assert "shareID" not in resolved.data

    def test_nfd_named_children_inherit_as_well(self, manager):
        """Apple returns NFD; the user types NFC. Share context must survive both."""
        mgr, _ = manager()
        leaf = bare_child("Café.pdf", docwsid="DOC-NFD")
        share_root = SharedFolderNode("Shared", share_id="SHARE-ABC", children=[leaf])

        resolved = mgr._resolve_child(share_root, "Café.pdf")

        assert resolved.data["shareID"] == "SHARE-ABC"


class TestExhaustedFallbackIsDiagnosable:
    """Every strategy used to fail into ``except Exception: pass``.

    A user reporting "still 404" then gave us nothing: no way to tell which
    strategies ran, which were skipped for want of a precondition, or how each
    one failed. That silence is also what this codebase's own rule forbids -
    anything that could not be examined is named, not dropped.
    """

    def test_exhausting_every_strategy_reports_what_each_one_did(self, manager, caplog):
        service = SharedDriveService(RecordingDriveSession({
            "get": RecordedResponse(payload={}, status_code=404),
        }))
        mgr, _ = manager()
        attach_drive(mgr, service)

        with caplog.at_level("WARNING"):
            assert mgr._try_shared_open(SharedNode()) is None

        record = _exhausted_record(caplog)
        assert record is not None, "no shared_open_exhausted record was logged"
        strategies = {a["strategy"] for a in record["attempts"]}
        assert strategies == {
            "by_id_with_share",
            "drivewsid_as_document_id",
            "refresh_node_metadata",
        }

    def test_a_rejected_request_records_the_status_code(self, manager, caplog):
        service = SharedDriveService(RecordingDriveSession({
            "get": RecordedResponse(payload={}, status_code=423),
        }))
        mgr, _ = manager()
        attach_drive(mgr, service)

        with caplog.at_level("WARNING"):
            mgr._try_shared_open(SharedNode())

        attempts = _exhausted_record(caplog)["attempts"]
        by_id = next(a for a in attempts if a["strategy"] == "by_id_with_share")
        assert by_id["outcome"] == "rejected"
        assert "423" in by_id["detail"]

    def test_a_skipped_strategy_names_the_missing_precondition(self, manager, caplog):
        mgr, _ = manager()
        attach_drive(mgr, SharedDriveService(RecordingDriveSession({})))

        node = SharedNode()
        node.data.pop("shareID")
        with caplog.at_level("WARNING"):
            mgr._try_shared_open(node)

        attempts = _exhausted_record(caplog)["attempts"]
        by_id = next(a for a in attempts if a["strategy"] == "by_id_with_share")
        assert by_id["outcome"] == "skipped"
        assert "shareID" in by_id["detail"]

    def test_a_missing_share_id_states_the_likely_cause(self, manager, caplog):
        mgr, _ = manager()
        attach_drive(mgr, SharedDriveService(RecordingDriveSession({})))

        node = SharedNode()
        node.data.pop("shareID")
        with caplog.at_level("WARNING"):
            mgr._try_shared_open(node)

        record = _exhausted_record(caplog)
        assert record["share_id_present"] is False
        assert "inherited during traversal" in record["likely_cause"]

    def test_an_inherited_identifier_is_reported_as_inherited(self, manager, caplog):
        """Owned and inherited fail differently and must not read the same."""
        service = SharedDriveService(RecordingDriveSession({
            "get": RecordedResponse(payload={}, status_code=404),
        }))
        mgr, _ = manager()
        attach_drive(mgr, service)

        leaf = bare_child()
        share_root = SharedFolderNode("Shared", share_id="SHARE-ABC", children=[leaf])
        resolved = mgr._resolve_child(share_root, "JAB-007.jxl")

        with caplog.at_level("WARNING"):
            mgr._try_shared_open(resolved)

        record = _exhausted_record(caplog)
        assert record["share_id_present"] is True
        assert "inherited" in record["share_evidence"]

    def test_a_raised_error_is_recorded_rather_than_swallowed(self, manager, caplog):
        def boom(url, params):
            raise RuntimeError("connection reset by peer")

        service = SharedDriveService(RecordingDriveSession({"get": boom}))
        mgr, _ = manager()
        attach_drive(mgr, service)

        with caplog.at_level("WARNING"):
            mgr._try_shared_open(SharedNode())

        attempts = _exhausted_record(caplog)["attempts"]
        by_id = next(a for a in attempts if a["strategy"] == "by_id_with_share")
        assert by_id["outcome"] == "error"
        assert "connection reset by peer" in by_id["detail"]

    def test_a_success_logs_no_exhaustion_record(self, manager, caplog):
        service = SharedDriveService(RecordingDriveSession({
            "get": RecordedResponse(payload=DATA_TOKEN_RESPONSE),
        }))
        mgr, _ = manager()
        attach_drive(mgr, service)

        with caplog.at_level("WARNING"):
            assert mgr._try_shared_open(SharedNode()) is not None

        assert _exhausted_record(caplog) is None

    def test_a_multiline_apple_error_stays_on_one_line(self, manager, caplog):
        """Apple's bodies are multi-line JSON; a run's worth must stay readable."""
        def boom(url, params):
            raise RuntimeError(json.dumps(WS_OBJECT_NOT_FOUND, indent=2))

        service = SharedDriveService(RecordingDriveSession({"get": boom}))
        mgr, _ = manager()
        attach_drive(mgr, service)

        with caplog.at_level("WARNING"):
            mgr._try_shared_open(SharedNode())

        attempts = _exhausted_record(caplog)["attempts"]
        by_id = next(a for a in attempts if a["strategy"] == "by_id_with_share")
        assert "\n" not in by_id["detail"]
        assert len(by_id["detail"]) <= 200


def _exhausted_record(caplog):
    """Return the parsed ``shared_open_exhausted`` log payload, if one was emitted."""
    for record in caplog.records:
        try:
            payload = json.loads(record.getMessage())
        except (ValueError, TypeError):
            continue
        if payload.get("event") == "shared_open_exhausted":
            return payload
    return None
