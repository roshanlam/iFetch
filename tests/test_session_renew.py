"""Mid-run single-flight session renewal under concurrency (#44)."""

import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))

from ifetch.auth import classify_session_death  # noqa: E402
from ifetch.downloader import DownloadManager, SessionRenewalError  # noqa: E402


class ApiError(Exception):
    """Shaped like pyicloud's PyiCloudAPIResponseException."""

    def __init__(self, reason, code=None):
        self.code = code
        self.response = None
        super().__init__(f"{reason} ({code})" if code else reason)


class Session:
    """The shared session every node depends on; dead until renewed."""

    def __init__(self):
        self.alive = False
        self.lock = threading.Lock()


class Node:
    def __init__(self, name, session, death=None, barrier=None):
        self.name = name
        self.session = session
        self.death = death or ApiError("Invalid Session Token", 401)
        self.barrier = barrier
        self.opens = 0

    def open(self, stream=True):
        self.opens += 1
        if not self.session.alive:
            if self.barrier is not None:
                # Every worker sees the dead session before anyone renews it.
                self.barrier.wait(timeout=5)
            raise self.death
        return f"stream:{self.name}"


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    sleeps = []
    monkeypatch.setattr("ifetch.downloader.time.sleep", sleeps.append)
    return sleeps


def _manager(renewer, **kwargs):
    kwargs.setdefault("max_retries", 3)
    dm = DownloadManager(email="user@example.com", **kwargs)
    dm.session_renewer = renewer
    return dm


# -- classification -----------------------------------------------------------

@pytest.mark.parametrize("error, code", [
    (ApiError("Invalid Session Token", 401), "invalid_session_token"),
    (ApiError("Unauthorized", 401), "session_expired"),
    (ApiError("Missing PCS cookies from the request", 423), "adp_pcs_cookies"),
    (Exception("HTTP 423 Locked"), "adp_pcs_cookies"),
    (Exception("Missing X-APPLE-WEBAUTH-TOKEN cookie"), "adp_webauth_token_missing"),
    (Exception("401 Client Error: Unauthorized for url: https://x"), "session_expired"),
])
def test_session_death_is_classified(error, code):
    failure = classify_session_death(error)
    assert failure is not None and failure.code == code
    assert "ifetch auth renew" in failure.remedy


@pytest.mark.parametrize("error", [
    ApiError("Not Found", 404),
    ApiError("Service Unavailable", 503),
    Exception("Connection reset by peer"),
    Exception("Invalid credentials"),
    Exception("Failed to open IMG_4012.jpg"),
    Exception("Document 423 Final.pdf: not found"),
])
def test_other_errors_are_not_session_death(error):
    assert classify_session_death(error) is None


# -- single flight ------------------------------------------------------------

def test_one_renew_for_n_concurrent_failures():
    workers = 8
    session = Session()
    barrier = threading.Barrier(workers)
    calls = []

    def renewer(cause):
        calls.append(cause.code)
        time.sleep(0.05)  # long enough that the others are queued behind us
        session.alive = True

    dm = _manager(renewer)
    nodes = [Node(f"f{i}", session, barrier=barrier) for i in range(workers)]
    with ThreadPoolExecutor(max_workers=workers) as pool:
        results = list(pool.map(dm._open_with_retry, nodes))

    assert calls == ["invalid_session_token"]
    assert results == [f"stream:f{i}" for i in range(workers)]
    assert all(n.opens == 2 for n in nodes)
    assert dm._auth_generation == 1


def test_renew_then_retry_does_not_spend_an_attempt(no_sleep):
    session = Session()
    dm = _manager(lambda cause: setattr(session, "alive", True), max_retries=1)
    node = Node("a", session)
    assert dm._open_with_retry(node) == "stream:a"
    assert node.opens == 2
    assert no_sleep == []  # renew is not throttle backoff


def test_failed_renew_propagates_once_to_every_worker():
    workers = 6
    session = Session()
    barrier = threading.Barrier(workers)
    calls = []

    def renewer(cause):
        calls.append(cause)
        raise ApiError("Invalid Session Token", 401)

    dm = _manager(renewer)
    nodes = [Node(f"f{i}", session, barrier=barrier) for i in range(workers)]

    def attempt(node):
        try:
            dm._open_with_retry(node)
        except SessionRenewalError as exc:
            return str(exc)
        return "opened"

    with ThreadPoolExecutor(max_workers=workers) as pool:
        outcomes = list(pool.map(attempt, nodes))

    assert len(calls) == 1
    assert all("ifetch auth renew" in o for o in outcomes)
    assert all("mid-run session renewal failed" in o for o in outcomes)
    # Later failures short-circuit instead of signing in again.
    with pytest.raises(SessionRenewalError):
        dm._open_with_retry(Node("late", session))
    assert len(calls) == 1


def test_second_death_after_renew_is_final():
    session = Session()
    calls = []
    dm = _manager(lambda cause: calls.append(cause))  # "succeeds", session stays dead
    node = Node("a", session)
    with pytest.raises(ApiError, match="Invalid Session Token"):
        dm._open_with_retry(node)
    assert len(calls) == 1
    assert node.opens == 2


def test_renewals_are_capped_per_run():
    session = Session()
    calls = []
    dm = _manager(lambda cause: calls.append(cause), max_session_renewals=2)
    for i in range(2):
        with pytest.raises(ApiError):
            dm._open_with_retry(Node(f"n{i}", session))
    with pytest.raises(SessionRenewalError, match="gave up after 2"):
        dm._open_with_retry(Node("n3", session))
    assert len(calls) == 2


def test_non_auth_errors_keep_the_existing_retry_path(no_sleep):
    dm = _manager(lambda cause: pytest.fail("no renew for a connection error"))

    class Flaky:
        name = "f"
        opens = 0

        def open(self, stream=True):
            self.opens += 1
            if self.opens < 3:
                raise Exception("Connection reset by peer")
            return "ok"

    node = Flaky()
    assert dm._open_with_retry(node) == "ok"
    assert len(no_sleep) == 2


def test_pcs_423_uses_the_adp_cause():
    session = Session()
    causes = []

    def renewer(cause):
        causes.append(cause.code)
        session.alive = True

    dm = _manager(renewer)
    node = Node("a", session, death=ApiError("Missing PCS cookies from the request", 423))
    assert dm._open_with_retry(node) == "stream:a"
    assert causes == ["adp_pcs_cookies"]


# -- the default renewer ------------------------------------------------------

class FakeApi:
    def __init__(self, requires_2fa=False):
        self.refreshes = []
        self.requires_2fa = requires_2fa
        self.requires_2sa = False

    def authenticate(self, force_refresh=False, service=None):
        self.refreshes.append(force_refresh)


def test_default_renewer_refreshes_the_shared_session_in_place():
    dm = DownloadManager(email="user@example.com")
    dm.api = FakeApi()
    dm.renew_session(0, classify_session_death(ApiError("Invalid Session Token", 401)))
    assert dm.api.refreshes == [True]
    assert dm._auth_generation == 1


def test_default_renewer_fails_clearly_when_2fa_is_required():
    dm = DownloadManager(email="user@example.com")
    dm.api = FakeApi(requires_2fa=True)
    with pytest.raises(SessionRenewalError) as info:
        dm.renew_session(0, classify_session_death(ApiError("Invalid Session Token", 401)))
    assert "ifetch auth renew" in str(info.value)
    assert dm._auth_generation == 0


# -- end to end through download_drive_item ------------------------------------

def test_download_survives_session_death_mid_tree(tmp_path, monkeypatch):
    session = Session()
    content = b"x" * 1000

    class FileNode(Node):
        size = len(content)
        date_modified = None
        date_changed = None
        type = "file"

        def open(self, stream=True):
            super().open(stream)

            class Ctx:
                headers = {"content-length": str(len(content))}
                url = "https://example.invalid/f"

                def __enter__(self):
                    return self

                def __exit__(self, *exc):
                    return False

            return Ctx()

    calls = []

    def renewer(cause):
        calls.append(cause)
        session.alive = True

    dm = _manager(renewer)
    monkeypatch.setattr(dm, "download_chunk", lambda url, s, e, item=None: content[s:e + 1])
    nodes = [FileNode(f"f{i}.bin", session) for i in range(4)]
    with ThreadPoolExecutor(max_workers=4) as pool:
        ok = list(pool.map(lambda n: dm.download_drive_item(n, tmp_path / n.name), nodes))

    assert ok == [True] * 4
    assert len(calls) == 1
    assert all((tmp_path / n.name).read_bytes() == content for n in nodes)


def test_download_reports_failed_renew_with_remedy(tmp_path):
    session = Session()

    def renewer(cause):
        raise ApiError("Invalid Session Token", 401)

    dm = _manager(renewer)
    node = Node("f.bin", session)
    node.size = 10
    node.date_modified = None
    assert dm.download_drive_item(node, tmp_path / "f.bin") is False
    error = dm.download_results[-1].error
    assert "ifetch auth renew" in error
