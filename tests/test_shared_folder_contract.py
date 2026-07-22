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

    def test_a_node_with_no_share_id_does_not_attempt_the_fallback(self, manager):
        """An ordinary owned file that 404s is a different problem."""
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
