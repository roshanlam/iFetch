"""2FA codes must be *requested* before they are validated (pyicloud 2.6+).

pyicloud starts Apple's trusted-device bridge - the HSA2 flow current
accounts use - in ``request_2fa_code()``. Skip it and ``validate_2fa_code``
falls back to the legacy endpoint, which refuses even a correct code. That is
what a live run hit: a fresh, correctly typed code came back "not accepted".
"""

import io
import sys
from pathlib import Path

import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))

from ifetch.auth import (  # noqa: E402
    TwoFactorRouteUnsupported,
    describe_delivery,
    request_two_factor_code,
)
from ifetch.auth_cli import build_parser, cmd_renew  # noqa: E402
from ifetch.downloader import DownloadManager  # noqa: E402

ACCOUNT = "user@example.com"


class BridgeService:
    """Validates a code only if the bridge was started first, like Apple."""

    def __init__(self, method="trusted_device", delivered=True, raises=None):
        self.requires_2fa = True
        self.requires_2sa = False
        self.is_trusted_session = False
        self.events = []
        self._method = method
        self._delivered = delivered
        self._raises = raises
        self.two_factor_delivery_method = "unknown"

    def request_2fa_code(self):
        self.events.append("request")
        if self._raises:
            raise self._raises
        self.two_factor_delivery_method = self._method
        return self._delivered

    def validate_2fa_code(self, code):
        self.events.append(f"validate:{code}")
        ok = "request" in self.events
        if ok:
            self.is_trusted_session = True
        return ok

    def trust_session(self):
        self.is_trusted_session = True
        return True


class LegacyService(BridgeService):
    """An older pyicloud with no request_2fa_code at all."""

    request_2fa_code = None

    def validate_2fa_code(self, code):
        self.events.append(f"validate:{code}")
        self.is_trusted_session = True
        return True


# -- the helper -----------------------------------------------------------------

def test_request_reports_the_route():
    service = BridgeService("trusted_device")
    assert request_two_factor_code(service) == "trusted_device"
    assert service.events == ["request"]


def test_sms_route():
    assert request_two_factor_code(BridgeService("sms")) == "sms"


def test_older_pyicloud_is_a_no_op():
    assert request_two_factor_code(LegacyService()) == ""


def test_security_key_is_named_not_attempted():
    with pytest.raises(TwoFactorRouteUnsupported) as info:
        request_two_factor_code(BridgeService("security_key", delivered=False))
    assert info.value.failure.code == "security_key_required"


def test_delivery_lines():
    assert "trusted Apple devices" in describe_delivery("trusted_device")
    assert "SMS" in describe_delivery("sms")
    assert describe_delivery("") == ""


# -- auth renew -------------------------------------------------------------------

def _renew(tmp_path, service, *extra):
    args = build_parser().parse_args(
        ["renew", "--email", ACCOUNT, "--cookie-directory", str(tmp_path),
         "--no-adp", *extra])
    out = io.StringIO()
    code = cmd_renew(args, out, service_factory=lambda **kw: service)
    return code, out.getvalue()


def test_renew_requests_before_validating(tmp_path):
    service = BridgeService()
    code, out = _renew(tmp_path, service, "--2fa-code", "123456", "--no-stdin")
    assert code == 0, out
    assert service.events == ["request", "validate:123456"]
    assert "trusted Apple devices" in out


def test_renew_requests_before_prompting(tmp_path, monkeypatch):
    """The prompt must come after the request, or the code is not sent yet."""
    service = BridgeService()

    class Terminal:
        def isatty(self):
            return True

        def readline(self):
            service.events.append("prompted")
            return "654321\n"

    monkeypatch.setattr(sys, "stdin", Terminal())
    code, _ = _renew(tmp_path, service)
    assert code == 0
    assert service.events == ["request", "prompted", "validate:654321"]


def test_renew_security_key_fails_with_its_own_cause(tmp_path):
    code, out = _renew(tmp_path, BridgeService("security_key", delivered=False),
                       "--2fa-code", "123456", "--no-stdin")
    assert code == 2
    assert "security key" in out


def test_renew_still_works_on_older_pyicloud(tmp_path):
    service = LegacyService()
    code, _ = _renew(tmp_path, service, "--2fa-code", "123456", "--no-stdin")
    assert code == 0
    assert service.events == ["validate:123456"]


# -- the download path (DownloadManager.authenticate) ----------------------------

def test_download_authenticate_requests_before_validating(monkeypatch):
    service = BridgeService()
    monkeypatch.setattr("ifetch.downloader.PyiCloudService", lambda **kw: service)

    class Resolver:
        def resolve(self):
            service.events.append("resolved")
            return "111222"

        def describe_sources(self):
            return ["test"]

    dm = DownloadManager(email=ACCOUNT)
    monkeypatch.setattr(dm, "check_session_expiry", lambda: None)
    dm.authenticate(Resolver())
    assert service.events == ["request", "resolved", "validate:111222"]


def test_download_authenticate_security_key_is_named(monkeypatch):
    service = BridgeService("security_key", delivered=False)
    monkeypatch.setattr("ifetch.downloader.PyiCloudService", lambda **kw: service)
    dm = DownloadManager(email=ACCOUNT)
    monkeypatch.setattr(dm, "check_session_expiry", lambda: None)
    with pytest.raises(TwoFactorRouteUnsupported):
        dm.authenticate()
