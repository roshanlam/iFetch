"""Contract tests for Advanced Data Protection (PCS cookie) support.

Why a replay harness rather than a live test
--------------------------------------------
Advanced Data Protection cannot be exercised by an automated suite. Turning it
on requires a physical Apple device, a recovery contact or recovery key, and an
approval tap on a trusted device for every acquisition - none of which CI can
provision, and no shared test account can safely hold. **Nothing below has ever
run against a live ADP-enabled Apple ID.**

What *can* be pinned down is the contract: given the exact payloads Apple
returns, does iFetch take the right branch? The fixtures here are the response
shapes used by two independent working implementations - pyicloud's
``_request_pcs_for_service`` and rclone's ``acquirePCSCookiesFor`` (its
``backend/iclouddrive/api/session.go``) - cross-checked against each other.
Where the two disagree, or where a field name is not corroborated, the code
under test reports "could not determine" and these tests assert that it does.

So these tests prove:

1. the request/approve/poll exchange is wired in the right order with the right
   parameters, and terminates on every path;
2. each distinct failure Apple can return becomes a distinct, actionable
   diagnosis rather than a relayed status code;
3. the PCS cookie - a bearer credential for end-to-end-encrypted data - never
   reaches a log, a report or an exception message;
4. an account *without* ADP issues no extra request and follows the identical
   path it did before this code existed.

They do NOT prove that Apple's live behaviour matches these fixtures. Until
someone runs the procedure against a real ADP account, that caveat stands and
the README says so.
"""

import io
import json
import logging
import sys
import time
from http.cookiejar import Cookie, LWPCookieJar
from pathlib import Path

import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))

from ifetch.auth import (  # noqa: E402
    ADP_OFF,
    ADP_ON,
    ADP_UNDETERMINED,
    CHECK_FAIL,
    CHECK_INFO,
    CHECK_OK,
    CHECK_SKIP,
    CHECK_WARN,
    DEFAULT_PCS_MAX_ATTEMPTS,
    PCS_ACQUIRED,
    PCS_ALREADY_PRESENT,
    PCS_NOT_REQUIRED,
    PCS_REQUEST_PATH,
    PCS_UNDETERMINED,
    PCS_WEB_ACCESS_STATE_PATH,
    REDACTED,
    WEB_ACCESS_DISABLED,
    WEB_ACCESS_ENABLED,
    ADPError,
    AuthDoctor,
    PyiCloudPCSTransport,
    adp_status_from_webservices,
    classify_auth_error,
    ensure_pcs_cookies,
    interpret_web_access_state,
    pcs_cookie_state,
    pcs_service,
    read_session_snapshot,
    redact_secrets,
    session_slug,
)
from ifetch.auth_cli import build_parser, cmd_renew, cmd_status  # noqa: E402

ACCOUNT = "you@example.com"
DAY = 86400.0
NOW = 1_800_000_000.0

DRIVE_COOKIE = "X-APPLE-WEBAUTH-PCS-Documents"

#: The value of a real PCS cookie is an opaque base64 blob. Any test that can
#: find this string in output has found a leaked credential.
SECRET = "PCSDocs-AAAABBBBCCCCDDDDEEEEFFFF-thisIsTheSecret"


# ---------------------------------------------------------------------------
# Recorded Apple responses
# ---------------------------------------------------------------------------

#: ``requestWebAccessState`` on an account with ADP on, web access enabled and a
#: device that has already consented. ``isICDRSDisabled`` is the ADP signal:
#: enabling ADP is what turns the iCloud Data Recovery Service off.
WEB_ACCESS_ADP_READY = {
    "isICDRSDisabled": True,
    "isWebAccessAllowed": True,
    "isDeviceConsentedForPCS": True,
}

#: The same account before any device has consented.
WEB_ACCESS_NEEDS_CONSENT = {
    "isICDRSDisabled": True,
    "isWebAccessAllowed": True,
    "isDeviceConsentedForPCS": False,
}

#: ADP on, but "Access iCloud Data on the Web" switched off. This is the state
#: that no amount of re-authentication can fix, and the one users misdiagnose.
WEB_ACCESS_DISABLED_PAYLOAD = {
    "isICDRSDisabled": True,
    "isWebAccessAllowed": False,
    "isDeviceConsentedForPCS": True,
}

#: An ordinary, non-ADP account.
WEB_ACCESS_NO_ADP = {
    "isICDRSDisabled": False,
    "isWebAccessAllowed": True,
    "isDeviceConsentedForPCS": True,
}

#: A payload with none of the fields we know: Apple changed something, or this
#: is a region we have no recording for. Must yield "undetermined".
WEB_ACCESS_UNRECOGNISED = {"requestUUID": "0000-0000", "status": "ok"}

#: ``requestPCS`` while the trusted device has not yet uploaded the keys. Both
#: strings are the ones pyicloud treats as retryable.
PCS_PENDING_UPLOAD = {
    "status": "failure",
    "message": "Requested the device to upload cookies.",
}
PCS_PENDING_SERVER = {
    "status": "failure",
    "message": "Cookies not available yet on server.",
}

#: ``requestPCS`` succeeding.
PCS_SUCCESS = {"status": "success", "message": "PCS cookies available."}

#: ``requestPCS`` refusing for a reason we have no recording for.
PCS_UNKNOWN_STATE = {"status": "failure", "message": "ICDRS is not disabled."}

#: ``enableDeviceConsentForPCS`` accepting and refusing.
CONSENT_SENT = {"isDeviceConsentNotificationSent": True}
CONSENT_NOT_SENT = {"isDeviceConsentNotificationSent": False}

#: Apple's 423 on a Drive request made without PCS cookies.
LOCKED_423 = (
    "HTTP error 423 (423 Locked) returned body: "
    '"{\\"errorReason\\":\\"Missing PCS cookies from the request\\"}"'
)

#: pyicloud's own refusal when the session lost its web-auth cookie.
MISSING_WEBAUTH = "Missing X-APPLE-WEBAUTH-TOKEN cookie"

#: rclone's shape for a failed PCS exchange.
REQUEST_PCS_FAILED = (
    "requestPCS(iclouddrive): server returned success but cookies still missing: "
    "[X-APPLE-WEBAUTH-PCS-Documents]"
)


# ---------------------------------------------------------------------------
# Replay harness
# ---------------------------------------------------------------------------

class RecordedTransport:
    """Replays recorded Apple responses and records the request sequence.

    Implements exactly the four methods :func:`ensure_pcs_cookies` uses, which
    is the whole reason that function takes a transport rather than a
    ``PyiCloudService``: the protocol is small enough to replay honestly.
    """

    def __init__(self, responses=None, webservices=None, cookies=(), grants=None):
        #: path -> a payload, a list of payloads consumed in order, or a callable.
        self.responses = dict(responses or {})
        self._webservices = webservices
        self._cookies = dict(cookies)
        #: After this many ``requestPCS`` calls, Apple sets the cookie.
        self.grants = grants
        self.calls = []
        self.persisted = 0

    # -- protocol ------------------------------------------------------
    def cookie_names(self):
        return list(self._cookies)

    def webservices(self):
        return self._webservices

    def post(self, path, payload=None):
        self.calls.append((path, payload))
        if path not in self.responses:
            raise AssertionError(f"unexpected request to {path}")
        recorded = self.responses[path]
        if isinstance(recorded, list):
            recorded = recorded.pop(0) if len(recorded) > 1 else recorded[0]
        if callable(recorded):
            return recorded(self)
        if (
            path == PCS_REQUEST_PATH
            and self.grants is not None
            and self.pcs_calls >= self.grants
        ):
            self.set_cookie()
            return dict(PCS_SUCCESS)
        return recorded

    def persist(self):
        self.persisted += 1

    # -- helpers -------------------------------------------------------
    @property
    def paths(self):
        return [path for path, _ in self.calls]

    @property
    def pcs_calls(self):
        return self.paths.count(PCS_REQUEST_PATH)

    def set_cookie(self, name=DRIVE_COOKIE, value=SECRET):
        self._cookies[name] = value


class FakeClock:
    """A clock that only advances when something sleeps on it.

    A test that waits five minutes for a real approval would be a test nobody
    runs, so the wait is simulated exactly - and any code that blocks without
    going through ``sleep`` shows up as a test that hangs.
    """

    def __init__(self, start=NOW):
        self.now = start
        self.slept = []

    def __call__(self):
        return self.now

    def sleep(self, seconds):
        self.slept.append(seconds)
        self.now += seconds

    @property
    def total_slept(self):
        return sum(self.slept)


def adp_transport(**kwargs):
    """A transport for an ADP account whose device approves immediately."""
    responses = {
        PCS_WEB_ACCESS_STATE_PATH: dict(WEB_ACCESS_ADP_READY),
        PCS_REQUEST_PATH: dict(PCS_PENDING_UPLOAD),
    }
    responses.update(kwargs.pop("responses", {}))
    return RecordedTransport(
        responses=responses,
        webservices=kwargs.pop("webservices", {"drivews": {"pcsRequired": True}}),
        **kwargs,
    )


def write_pcs_cookiejar(path, expires, name=DRIVE_COOKIE, value=SECRET):
    """Write an LWP cookie jar holding a PCS cookie, as pyicloud would."""
    jar = LWPCookieJar(str(path))
    for cookie_name, cookie_value, cookie_expires in (
        ("X-APPLE-WEBAUTH-TOKEN", "session-token-value", NOW + 20 * DAY),
        (name, value, expires),
    ):
        jar.set_cookie(
            Cookie(
                version=0, name=cookie_name, value=cookie_value, port=None,
                port_specified=False, domain="icloud.com", domain_specified=True,
                domain_initial_dot=False, path="/", path_specified=True,
                secure=True, expires=cookie_expires, discard=False, comment=None,
                comment_url=None, rest={},
            )
        )
    jar.save(ignore_discard=True, ignore_expires=True)


def write_session_files(directory, pcs_expires=None, account=ACCOUNT):
    slug = session_slug(account)
    (directory / f"{slug}.session").write_text(
        json.dumps({"session_token": "abc", "trust_token": "xyz"}), encoding="utf-8"
    )
    jar_path = directory / f"{slug}.cookiejar"
    if pcs_expires is None:
        jar = LWPCookieJar(str(jar_path))
        jar.set_cookie(
            Cookie(
                version=0, name="X-APPLE-WEBAUTH-TOKEN", value="session-token-value",
                port=None, port_specified=False, domain="icloud.com",
                domain_specified=True, domain_initial_dot=False, path="/",
                path_specified=True, secure=True, expires=NOW + 20 * DAY,
                discard=False, comment=None, comment_url=None, rest={},
            )
        )
        jar.save(ignore_discard=True, ignore_expires=True)
    else:
        write_pcs_cookiejar(jar_path, pcs_expires)
    return jar_path


# ---------------------------------------------------------------------------
# The acquisition flow
# ---------------------------------------------------------------------------

class TestPCSAcquisition:
    """request -> not ready -> not ready -> ready, against recorded payloads."""

    def test_the_full_flow_acquires_the_cookie(self):
        clock = FakeClock()
        transport = adp_transport(grants=3)

        result = ensure_pcs_cookies(
            transport, "drive", now=clock, sleep=clock.sleep
        )

        assert result.status == PCS_ACQUIRED
        assert result.attempts == 3
        assert result.cookies_present == (DRIVE_COOKIE,)

    def test_web_access_state_is_asked_before_any_pcs_request(self):
        """The one condition retrying cannot fix must be checked first."""
        clock = FakeClock()
        transport = adp_transport(grants=1)

        ensure_pcs_cookies(transport, now=clock, sleep=clock.sleep)

        assert transport.paths[0] == PCS_WEB_ACCESS_STATE_PATH

    def test_the_request_is_scoped_to_the_service(self):
        """appName is per-service; the wrong one succeeds and sets nothing."""
        clock = FakeClock()
        transport = adp_transport(grants=1)

        ensure_pcs_cookies(transport, "drive", now=clock, sleep=clock.sleep)

        body = dict(transport.calls[-1][1])
        assert body["appName"] == "iclouddrive"

    def test_photos_scopes_to_its_own_app_name_and_cookie_pair(self):
        clock = FakeClock()
        transport = RecordedTransport(
            responses={
                PCS_WEB_ACCESS_STATE_PATH: dict(WEB_ACCESS_ADP_READY),
                PCS_REQUEST_PATH: lambda t: (
                    t.set_cookie("X-APPLE-WEBAUTH-PCS-Photos"),
                    t.set_cookie("X-APPLE-WEBAUTH-PCS-Sharing"),
                    dict(PCS_SUCCESS),
                )[-1],
            },
            webservices={"ckdatabasews": {"pcsRequired": True}},
        )

        result = ensure_pcs_cookies(
            transport, "photos", now=clock, sleep=clock.sleep
        )

        assert transport.calls[-1][1]["appName"] == "photos"
        assert set(result.cookies_present) == {
            "X-APPLE-WEBAUTH-PCS-Photos",
            "X-APPLE-WEBAUTH-PCS-Sharing",
        }

    def test_only_the_first_request_claims_a_user_action(self):
        """derivedFromUserAction is what pushes the prompt; repeating it re-notifies."""
        clock = FakeClock()
        transport = adp_transport(grants=3)

        ensure_pcs_cookies(transport, now=clock, sleep=clock.sleep)

        flags = [
            body["derivedFromUserAction"]
            for path, body in transport.calls
            if path == PCS_REQUEST_PATH
        ]
        assert flags == [True, False, False]

    def test_polling_backs_off_rather_than_busy_waiting(self):
        clock = FakeClock()
        transport = adp_transport(grants=4)

        ensure_pcs_cookies(transport, now=clock, sleep=clock.sleep)

        assert clock.slept, "polled without sleeping at all"
        assert clock.slept == sorted(clock.slept), "intervals did not grow"
        assert min(clock.slept) > 0

    def test_a_success_that_sets_no_cookie_is_not_reported_as_success(self):
        """rclone hit exactly this: status success, cookie jar unchanged."""
        transport = adp_transport(
            responses={PCS_REQUEST_PATH: dict(PCS_SUCCESS)}
        )
        clock = FakeClock()

        with pytest.raises(ADPError) as excinfo:
            ensure_pcs_cookies(transport, now=clock, sleep=clock.sleep)

        assert excinfo.value.failure.code == "adp_pcs_cookies_not_set"

    def test_consent_is_requested_when_no_device_has_given_it(self):
        clock = FakeClock()
        transport = adp_transport(
            responses={
                PCS_WEB_ACCESS_STATE_PATH: dict(WEB_ACCESS_NEEDS_CONSENT),
                "enableDeviceConsentForPCS": dict(CONSENT_SENT),
            },
            grants=1,
        )

        ensure_pcs_cookies(transport, now=clock, sleep=clock.sleep)

        assert "enableDeviceConsentForPCS" in transport.paths

    def test_consent_is_not_requested_when_a_device_already_consented(self):
        clock = FakeClock()
        transport = adp_transport(grants=1)

        ensure_pcs_cookies(transport, now=clock, sleep=clock.sleep)

        assert "enableDeviceConsentForPCS" not in transport.paths

    def test_a_consent_request_apple_refuses_to_send_fails_immediately(self):
        """Waiting for an approval that was never delivered is a hang, not a wait."""
        clock = FakeClock()
        transport = adp_transport(
            responses={
                PCS_WEB_ACCESS_STATE_PATH: dict(WEB_ACCESS_NEEDS_CONSENT),
                "enableDeviceConsentForPCS": dict(CONSENT_NOT_SENT),
            },
        )

        with pytest.raises(ADPError) as excinfo:
            ensure_pcs_cookies(transport, now=clock, sleep=clock.sleep)

        assert excinfo.value.failure.code == "adp_consent_not_sent"
        assert transport.pcs_calls == 0
        assert clock.slept == []


# ---------------------------------------------------------------------------
# Bounded polling
# ---------------------------------------------------------------------------

class TestPollingIsBounded:
    """A backup job that hangs forever is worse than one that fails."""

    def test_a_never_ready_server_terminates(self):
        clock = FakeClock()
        transport = adp_transport()

        with pytest.raises(ADPError) as excinfo:
            ensure_pcs_cookies(transport, now=clock, sleep=clock.sleep)

        assert excinfo.value.failure.code == "adp_approval_timeout"

    def test_the_deadline_is_not_overshot(self):
        clock = FakeClock()
        transport = adp_transport()

        with pytest.raises(ADPError):
            ensure_pcs_cookies(transport, timeout=60.0, now=clock, sleep=clock.sleep)

        assert clock.total_slept <= 60.0

    def test_a_clock_that_never_advances_still_stops_on_the_attempt_cap(self):
        """Both bounds are real: a frozen clock must not mean an infinite loop."""
        frozen = FakeClock()
        transport = adp_transport()

        with pytest.raises(ADPError):
            ensure_pcs_cookies(
                transport,
                timeout=1e9,
                max_attempts=5,
                now=frozen,
                sleep=lambda seconds: None,
            )

        assert transport.pcs_calls == 5

    def test_the_timeout_names_the_likely_cause_not_just_the_elapsed_time(self):
        clock = FakeClock()

        with pytest.raises(ADPError) as excinfo:
            ensure_pcs_cookies(adp_transport(), now=clock, sleep=clock.sleep)

        summary = excinfo.value.failure.summary
        remedy = excinfo.value.failure.remedy
        assert "trusted device" in summary or "trusted device" in remedy
        assert "Access iCloud Data on the Web" in summary + remedy
        assert "--adp-timeout" in remedy

    def test_the_timeout_does_not_blame_two_factor_authentication(self):
        """Conflating the two gates sends people to burn 2FA attempts for nothing."""
        clock = FakeClock()

        with pytest.raises(ADPError) as excinfo:
            ensure_pcs_cookies(adp_transport(), now=clock, sleep=clock.sleep)

        remedy = excinfo.value.failure.remedy
        assert "--2fa-code" not in remedy
        assert "separate approval" in remedy

    def test_a_zero_timeout_still_makes_exactly_one_attempt(self):
        """Do-while, not while-do: an already-approved account must not be skipped."""
        clock = FakeClock()
        transport = adp_transport()

        with pytest.raises(ADPError):
            ensure_pcs_cookies(transport, timeout=0.0, now=clock, sleep=clock.sleep)

        assert transport.pcs_calls == 1
        assert clock.slept == []

    def test_the_default_attempt_cap_is_finite(self):
        assert 0 < DEFAULT_PCS_MAX_ATTEMPTS < 1000


# ---------------------------------------------------------------------------
# The three error shapes, each with its own diagnosis
# ---------------------------------------------------------------------------

class TestDistinctDiagnostics:
    """Each opaque Apple error becomes one specific, accurate sentence."""

    def test_423_missing_pcs_cookies(self):
        failure = classify_auth_error(LOCKED_423)
        assert failure.code == "adp_pcs_cookies"
        assert "Advanced Data Protection" in failure.summary
        assert "Access iCloud Data on the Web" in failure.remedy
        assert "423" not in failure.summary

    def test_missing_webauth_token_is_not_confused_with_a_pcs_problem(self):
        """A signed-out session and an unapproved one need opposite actions."""
        failure = classify_auth_error(MISSING_WEBAUTH)
        assert failure.code == "adp_webauth_token_missing"
        assert "signed out" in failure.summary
        assert "auth renew --reset" in failure.remedy
        # It must not send the user to approve a prompt that will never appear.
        assert "trusted device" not in failure.remedy

    def test_request_pcs_failure_is_named_as_the_approval_step(self):
        failure = classify_auth_error(REQUEST_PCS_FAILED)
        assert failure.code == "adp_request_pcs_failed"
        assert "not two-factor sign-in" in failure.summary
        assert "trusted Apple device" in failure.remedy

    def test_the_three_shapes_produce_three_different_codes(self):
        codes = {
            classify_auth_error(text).code
            for text in (LOCKED_423, MISSING_WEBAUTH, REQUEST_PCS_FAILED)
        }
        assert len(codes) == 3

    def test_every_adp_diagnosis_carries_a_remedy(self):
        for text in (LOCKED_423, MISSING_WEBAUTH, REQUEST_PCS_FAILED):
            failure = classify_auth_error(text)
            assert failure.remedy.strip()
            assert failure.summary.strip()

    def test_an_unrecognised_pcs_state_is_a_failure_not_an_infinite_retry(self):
        clock = FakeClock()
        transport = adp_transport(
            responses={PCS_REQUEST_PATH: dict(PCS_UNKNOWN_STATE)}
        )

        with pytest.raises(ADPError) as excinfo:
            ensure_pcs_cookies(transport, now=clock, sleep=clock.sleep)

        assert excinfo.value.failure.code == "adp_request_pcs_failed"
        assert "ICDRS is not disabled." in excinfo.value.failure.summary
        assert transport.pcs_calls == 1

    def test_a_transport_error_is_classified_not_relayed(self):
        clock = FakeClock()

        def explode(transport):
            raise RuntimeError("Connection reset by peer")

        transport = adp_transport(responses={PCS_REQUEST_PATH: explode})

        with pytest.raises(ADPError) as excinfo:
            ensure_pcs_cookies(transport, now=clock, sleep=clock.sleep)

        assert excinfo.value.failure.code == "adp_request_pcs_failed"

    def test_an_apple_423_during_the_flow_keeps_its_own_classification(self):
        clock = FakeClock()

        def locked(transport):
            raise RuntimeError(LOCKED_423)

        transport = adp_transport(responses={PCS_REQUEST_PATH: locked})

        with pytest.raises(ADPError) as excinfo:
            ensure_pcs_cookies(transport, now=clock, sleep=clock.sleep)

        assert excinfo.value.failure.code == "adp_pcs_cookies"


# ---------------------------------------------------------------------------
# "Access iCloud Data on the Web" disabled
# ---------------------------------------------------------------------------

class TestWebAccessDisabled:
    """The setting people cannot find, named exactly, with the path to it."""

    def test_it_fails_with_the_specific_remediation_not_a_generic_auth_error(self):
        clock = FakeClock()
        transport = adp_transport(
            responses={PCS_WEB_ACCESS_STATE_PATH: dict(WEB_ACCESS_DISABLED_PAYLOAD)},
        )

        with pytest.raises(ADPError) as excinfo:
            ensure_pcs_cookies(transport, now=clock, sleep=clock.sleep)

        failure = excinfo.value.failure
        assert failure.code == "adp_web_access_disabled"
        assert "Access iCloud Data on the Web" in failure.summary
        assert "Settings > [your name] > iCloud" in failure.remedy
        assert "System Settings" in failure.remedy  # macOS path too

    def test_it_does_not_waste_time_polling_for_an_approval_that_cannot_come(self):
        clock = FakeClock()
        transport = adp_transport(
            responses={PCS_WEB_ACCESS_STATE_PATH: dict(WEB_ACCESS_DISABLED_PAYLOAD)},
        )

        with pytest.raises(ADPError):
            ensure_pcs_cookies(transport, now=clock, sleep=clock.sleep)

        assert transport.pcs_calls == 0
        assert clock.slept == []

    def test_it_is_not_reported_as_a_credentials_problem(self):
        clock = FakeClock()
        transport = adp_transport(
            responses={PCS_WEB_ACCESS_STATE_PATH: dict(WEB_ACCESS_DISABLED_PAYLOAD)},
        )

        with pytest.raises(ADPError) as excinfo:
            ensure_pcs_cookies(transport, now=clock, sleep=clock.sleep)

        text = (excinfo.value.failure.summary + excinfo.value.failure.remedy).lower()
        assert "password" not in text
        assert "2fa" not in text and "two-factor" not in text

    def test_the_doctor_reports_it_as_a_failed_check(self, tmp_path):
        write_session_files(tmp_path)
        transport = RecordedTransport(
            responses={PCS_WEB_ACCESS_STATE_PATH: dict(WEB_ACCESS_DISABLED_PAYLOAD)},
            webservices={"drivews": {"pcsRequired": True}},
        )

        diagnosis = AuthDoctor(
            ACCOUNT, cookie_directory=tmp_path, online=True, now=NOW,
            service_factory=lambda **kw: object(),
            drive_probe=lambda svc: [],
            adp_transport=lambda service: transport,
        ).run()

        check = next(
            c for c in diagnosis.checks if c.name == "advanced_data_protection"
        )
        assert check.status == CHECK_FAIL
        assert "Access iCloud Data on the Web" in check.detail
        assert diagnosis.exit_code == 2


# ---------------------------------------------------------------------------
# Credential redaction
# ---------------------------------------------------------------------------

class TestTheCookieIsNeverDisclosed:
    """A PCS cookie reads end-to-end-encrypted data. It is not debug output."""

    def test_it_is_absent_from_the_json_summary(self):
        clock = FakeClock()
        transport = adp_transport(grants=1)

        result = ensure_pcs_cookies(transport, now=clock, sleep=clock.sleep)

        blob = json.dumps(result.to_dict())
        assert SECRET not in blob
        assert DRIVE_COOKIE in blob, "the cookie *name* is useful and must remain"

    def test_it_is_absent_from_every_log_record(self, caplog):
        clock = FakeClock()
        transport = adp_transport(grants=3)
        logger = logging.getLogger("ifetch.test.adp")

        with caplog.at_level(logging.DEBUG):
            ensure_pcs_cookies(
                transport, now=clock, sleep=clock.sleep, log=logger.info
            )

        assert caplog.records, "nothing was logged, so this proves nothing"
        for record in caplog.records:
            assert SECRET not in record.getMessage()

    def test_it_is_absent_from_an_exception_message(self):
        """Including the one case where Apple echoes it back at us."""
        clock = FakeClock()

        def leaky(transport):
            raise RuntimeError(
                f"upstream refused: Cookie: {DRIVE_COOKIE}={SECRET}; other=1"
            )

        transport = adp_transport(responses={PCS_REQUEST_PATH: leaky})

        with pytest.raises(ADPError) as excinfo:
            ensure_pcs_cookies(transport, now=clock, sleep=clock.sleep)

        assert SECRET not in str(excinfo.value)
        assert SECRET not in json.dumps(excinfo.value.failure.to_dict())

    def test_a_leaked_value_is_scrubbed_from_a_classified_apple_error(self):
        failure = classify_auth_error(
            f"HTTP 500 with Set-Cookie: {DRIVE_COOKIE}={SECRET}; Path=/"
        )
        assert SECRET not in failure.raw
        assert REDACTED in failure.raw

    def test_redaction_leaves_the_rest_of_the_message_readable(self):
        text = f"before {DRIVE_COOKIE}={SECRET}; after"
        scrubbed = redact_secrets(text)
        assert scrubbed.startswith("before ")
        assert scrubbed.endswith("; after")
        assert SECRET not in scrubbed

    def test_a_named_value_is_scrubbed_even_without_its_cookie_name(self):
        assert SECRET not in redact_secrets(f"raw body: {SECRET}", values=[SECRET])

    def test_redaction_does_not_blank_short_status_words(self):
        """Over-eager redaction would corrupt the message it is protecting."""
        assert redact_secrets("status: ok", values=["ok"]) == "status: ok"

    def test_the_session_snapshot_records_names_and_expiry_only(self, tmp_path):
        write_session_files(tmp_path, pcs_expires=NOW + DAY)

        snapshot = read_session_snapshot(ACCOUNT, tmp_path)

        assert SECRET not in json.dumps(snapshot.to_dict())
        assert DRIVE_COOKIE in snapshot.pcs_expiries

    def test_the_doctor_report_never_carries_the_cookie(self, tmp_path):
        write_session_files(tmp_path, pcs_expires=NOW + DAY)

        diagnosis = AuthDoctor(ACCOUNT, cookie_directory=tmp_path, now=NOW).run()

        assert SECRET not in json.dumps(diagnosis.to_dict())


# ---------------------------------------------------------------------------
# Persistence and reuse
# ---------------------------------------------------------------------------

class TestPersistence:
    """An unattended re-run must not need somebody to tap a phone."""

    def test_a_stored_live_cookie_is_reused_without_any_request(self):
        transport = adp_transport(cookies={DRIVE_COOKIE: SECRET})

        result = ensure_pcs_cookies(transport)

        assert result.status == PCS_ALREADY_PRESENT
        assert transport.calls == []

    def test_the_cookie_lands_in_the_shared_jar_rather_than_a_second_store(self):
        """iFetch adds no storage of its own; pyicloud's jar is the one store."""
        clock = FakeClock()
        transport = adp_transport(grants=1)

        ensure_pcs_cookies(transport, now=clock, sleep=clock.sleep)

        assert DRIVE_COOKIE in transport.cookie_names()
        assert transport.persisted >= 1

    def test_a_live_stored_cookie_reads_back_as_usable(self, tmp_path):
        write_session_files(tmp_path, pcs_expires=NOW + 5 * DAY)

        state = pcs_cookie_state(read_session_snapshot(ACCOUNT, tmp_path), now=NOW)

        assert state.usable
        assert state.present == (DRIVE_COOKIE,)
        assert not state.expired

    def test_an_expired_stored_cookie_is_reported_expired(self, tmp_path):
        write_session_files(tmp_path, pcs_expires=NOW - DAY)

        state = pcs_cookie_state(read_session_snapshot(ACCOUNT, tmp_path), now=NOW)

        assert state.expired
        assert not state.usable

    def test_an_expired_stored_cookie_triggers_re_acquisition(self):
        """Expiry is what makes the difference; the name alone is not enough."""
        clock = FakeClock()
        # The live session no longer holds it: an expired cookie is dropped by
        # the jar, which is exactly what makes the next run re-request it.
        transport = adp_transport(grants=1)

        result = ensure_pcs_cookies(transport, now=clock, sleep=clock.sleep)

        assert result.status == PCS_ACQUIRED
        assert transport.pcs_calls == 1

    def test_a_corrupt_stored_cookie_is_discarded_not_crashed_on(self, tmp_path):
        """A truncated write leaves a valueless cookie; that must not be reused."""
        jar_path = write_session_files(tmp_path, pcs_expires=NOW + DAY)
        write_pcs_cookiejar(jar_path, NOW + DAY, value="")

        snapshot = read_session_snapshot(ACCOUNT, tmp_path)
        state = pcs_cookie_state(snapshot, now=NOW)

        assert state.present == ()
        assert DRIVE_COOKIE in state.missing
        assert any(DRIVE_COOKIE in error for error in state.read_errors)

    def test_a_corrupt_cookie_jar_does_not_crash_the_reader(self, tmp_path):
        slug = session_slug(ACCOUNT)
        (tmp_path / f"{slug}.session").write_text("{}", encoding="utf-8")
        (tmp_path / f"{slug}.cookiejar").write_text("not a cookie jar", encoding="utf-8")

        state = pcs_cookie_state(read_session_snapshot(ACCOUNT, tmp_path), now=NOW)

        assert state.present == ()
        assert not state.usable

    def test_a_partial_cookie_set_is_not_treated_as_complete(self):
        """Photos needs two; holding one and calling it done fails later, remotely."""
        transport = RecordedTransport(
            responses={
                PCS_WEB_ACCESS_STATE_PATH: dict(WEB_ACCESS_ADP_READY),
                PCS_REQUEST_PATH: dict(PCS_SUCCESS),
            },
            webservices={"ckdatabasews": {"pcsRequired": True}},
            cookies={"X-APPLE-WEBAUTH-PCS-Photos": SECRET},
        )

        with pytest.raises(ADPError) as excinfo:
            ensure_pcs_cookies(transport, "photos", now=FakeClock(), sleep=lambda s: None)

        assert excinfo.value.failure.code == "adp_pcs_cookies_not_set"
        assert "X-APPLE-WEBAUTH-PCS-Sharing" in excinfo.value.failure.summary


# ---------------------------------------------------------------------------
# Non-ADP accounts are untouched
# ---------------------------------------------------------------------------

class TestNonADPAccountsAreUnaffected:
    """The regression that matters most: everybody else must notice nothing."""

    def test_no_request_is_made_when_apple_says_no_pcs_cookie_is_needed(self):
        transport = RecordedTransport(webservices={"drivews": {"pcsRequired": False}})

        result = ensure_pcs_cookies(transport)

        assert result.status == PCS_NOT_REQUIRED
        assert transport.calls == [], "a non-ADP account paid for an extra request"
        assert result.requests_made == 0

    def test_no_request_is_made_when_nothing_is_known(self):
        """No evidence is not evidence: do nothing, and say that nothing was done."""
        transport = RecordedTransport(webservices=None)

        result = ensure_pcs_cookies(transport)

        assert result.status == PCS_UNDETERMINED
        assert transport.calls == []
        assert result.adp.state == ADP_UNDETERMINED

    def test_the_doctor_makes_no_adp_request_for_an_ordinary_account(self, tmp_path):
        write_session_files(tmp_path)
        transport = RecordedTransport(webservices={"drivews": {"pcsRequired": False}})

        diagnosis = AuthDoctor(
            ACCOUNT, cookie_directory=tmp_path, online=True, now=NOW,
            service_factory=lambda **kw: FakeService(),
            drive_probe=lambda svc: ["Documents"],
            adp_transport=lambda service: transport,
        ).run()

        assert transport.calls == []
        check = next(c for c in diagnosis.checks if c.name == "advanced_data_protection")
        assert check.status == CHECK_OK
        assert diagnosis.exit_code == 0

    def test_an_ordinary_account_still_exits_zero_offline(self, tmp_path):
        write_session_files(tmp_path)

        diagnosis = AuthDoctor(ACCOUNT, cookie_directory=tmp_path, now=NOW).run()

        assert diagnosis.exit_code == 0
        assert diagnosis.status == CHECK_OK

    def test_the_offline_check_sequence_is_unchanged_except_for_additions(self, tmp_path):
        write_session_files(tmp_path)

        diagnosis = AuthDoctor(ACCOUNT, cookie_directory=tmp_path, now=NOW).run()
        names = [c.name for c in diagnosis.checks]

        assert names[:3] == ["region", "stored_session", "session_expiry"]
        assert "live_authentication" in names

    def test_renew_on_an_ordinary_account_prints_no_adp_noise(self, tmp_path):
        write_session_files(tmp_path)
        transport = RecordedTransport(webservices={"drivews": {"pcsRequired": False}})

        code, out = run_renew(
            ["--email", ACCOUNT, "--cookie-directory", str(tmp_path)],
            service_factory=lambda **kw: FakeService(),
            adp_transport=lambda service: transport,
        )

        assert code == 0
        assert "Advanced Data Protection" not in out
        assert transport.calls == []

    def test_forcing_the_flow_is_the_only_way_to_get_requests_without_evidence(self):
        clock = FakeClock()
        transport = RecordedTransport(
            responses={
                PCS_WEB_ACCESS_STATE_PATH: dict(WEB_ACCESS_ADP_READY),
                PCS_REQUEST_PATH: dict(PCS_PENDING_UPLOAD),
            },
            webservices=None,
            grants=1,
        )

        result = ensure_pcs_cookies(
            transport, force=True, now=clock, sleep=clock.sleep
        )

        assert result.status == PCS_ACQUIRED
        assert transport.calls


# ---------------------------------------------------------------------------
# Determining ADP status honestly
# ---------------------------------------------------------------------------

class TestStatusIsNeverGuessed:
    def test_icdrs_disabled_means_adp_is_on(self):
        assert interpret_web_access_state(WEB_ACCESS_ADP_READY).state == ADP_ON

    def test_icdrs_enabled_means_adp_is_off(self):
        assert interpret_web_access_state(WEB_ACCESS_NO_ADP).state == ADP_OFF

    def test_an_unrecognised_payload_is_undetermined_not_off(self):
        """The damaging lie would be 'off'; it sends people hunting elsewhere."""
        status = interpret_web_access_state(WEB_ACCESS_UNRECOGNISED)
        assert status.state == ADP_UNDETERMINED
        assert status.web_access == "undetermined"

    def test_what_could_not_be_checked_is_named(self):
        status = interpret_web_access_state(WEB_ACCESS_UNRECOGNISED)
        assert status.unchecked
        assert any("isICDRSDisabled" in item for item in status.unchecked)

    @pytest.mark.parametrize("payload", [None, "", [], 42, "not json"])
    def test_a_junk_payload_never_raises(self, payload):
        assert interpret_web_access_state(payload).state == ADP_UNDETERMINED

    def test_web_access_is_read_when_present(self):
        assert (
            interpret_web_access_state(WEB_ACCESS_DISABLED_PAYLOAD).web_access
            == WEB_ACCESS_DISABLED
        )
        assert (
            interpret_web_access_state(WEB_ACCESS_ADP_READY).web_access
            == WEB_ACCESS_ENABLED
        )

    def test_pcs_required_true_is_evidence_of_adp(self):
        status = adp_status_from_webservices({"drivews": {"pcsRequired": True}})
        assert status.state == ADP_ON
        assert status.evidence

    def test_pcs_required_false_is_evidence_against(self):
        assert adp_status_from_webservices({"drivews": {"pcsRequired": False}}).state == ADP_OFF

    def test_a_missing_webservices_payload_is_undetermined_and_says_why(self):
        status = adp_status_from_webservices(None)
        assert status.state == ADP_UNDETERMINED
        assert status.unchecked

    def test_a_missing_flag_is_undetermined_rather_than_false(self):
        status = adp_status_from_webservices({"drivews": {"url": "https://x"}})
        assert status.state == ADP_UNDETERMINED
        assert any("pcsRequired" in item for item in status.unchecked)

    def test_the_doctor_says_undetermined_when_it_cannot_tell(self, tmp_path):
        write_session_files(tmp_path)
        transport = RecordedTransport(webservices=None)

        diagnosis = AuthDoctor(
            ACCOUNT, cookie_directory=tmp_path, online=True, now=NOW,
            service_factory=lambda **kw: object(),
            drive_probe=lambda svc: [],
            adp_transport=lambda service: transport,
        ).run()

        check = next(c for c in diagnosis.checks if c.name == "advanced_data_protection")
        assert "could not be determined" in check.detail
        assert check.data["state"] == ADP_UNDETERMINED
        assert check.data["unchecked"]

    def test_an_offline_doctor_does_not_claim_adp_is_off(self, tmp_path):
        write_session_files(tmp_path)

        diagnosis = AuthDoctor(ACCOUNT, cookie_directory=tmp_path, now=NOW).run()

        check = next(c for c in diagnosis.checks if c.name == "advanced_data_protection")
        assert check.data["state"] == ADP_UNDETERMINED
        assert "not a report that it is off" in check.detail
        assert check.status == CHECK_INFO

    def test_no_adp_records_that_the_check_was_skipped(self, tmp_path):
        """'Not checked' must never be able to read as 'checked and fine'."""
        write_session_files(tmp_path)

        diagnosis = AuthDoctor(
            ACCOUNT, cookie_directory=tmp_path, online=True, now=NOW, adp=False,
            service_factory=lambda **kw: object(),
            drive_probe=lambda svc: [],
        ).run()

        check = next(c for c in diagnosis.checks if c.name == "advanced_data_protection")
        assert check.status == CHECK_SKIP
        assert "unknown, not off" in check.detail

    def test_a_probe_that_explodes_is_reported_as_undetermined(self, tmp_path):
        write_session_files(tmp_path)

        class Broken:
            def webservices(self):
                raise RuntimeError("no session")

        diagnosis = AuthDoctor(
            ACCOUNT, cookie_directory=tmp_path, online=True, now=NOW,
            service_factory=lambda **kw: object(),
            drive_probe=lambda svc: [],
            adp_transport=lambda service: Broken(),
        ).run()

        check = next(c for c in diagnosis.checks if c.name == "advanced_data_protection")
        assert check.status == CHECK_WARN
        assert "could not be determined" in check.detail


# ---------------------------------------------------------------------------
# Doctor and status reporting
# ---------------------------------------------------------------------------

class TestDoctorAndStatusReporting:
    def test_a_stored_pcs_cookie_is_reported_by_the_doctor(self, tmp_path):
        write_session_files(tmp_path, pcs_expires=NOW + 5 * DAY)

        diagnosis = AuthDoctor(ACCOUNT, cookie_directory=tmp_path, now=NOW).run()

        check = next(c for c in diagnosis.checks if c.name == "pcs_cookies")
        assert check.status == CHECK_OK
        assert "reused" in check.detail

    def test_an_expired_pcs_cookie_warns_before_the_next_run_fails(self, tmp_path):
        write_session_files(tmp_path, pcs_expires=NOW - DAY)

        diagnosis = AuthDoctor(ACCOUNT, cookie_directory=tmp_path, now=NOW).run()

        check = next(c for c in diagnosis.checks if c.name == "pcs_cookies")
        assert check.status == CHECK_WARN
        assert "--adp" in check.remedy
        assert diagnosis.exit_code == 1

    def test_no_pcs_cookie_is_information_not_a_fault(self, tmp_path):
        """Most accounts have none, and a red cross for that is noise."""
        write_session_files(tmp_path)

        diagnosis = AuthDoctor(ACCOUNT, cookie_directory=tmp_path, now=NOW).run()

        check = next(c for c in diagnosis.checks if c.name == "pcs_cookies")
        assert check.status == CHECK_INFO

    def test_status_stays_one_line_for_an_ordinary_account(self, tmp_path):
        write_session_files(tmp_path)
        args = build_parser().parse_args(
            ["status", "--email", ACCOUNT, "--cookie-directory", str(tmp_path)]
        )
        out = io.StringIO()

        cmd_status(args, out)

        assert len(out.getvalue().strip().splitlines()) == 1

    def test_status_reports_an_expired_pcs_cookie_and_raises_the_exit_code(self, tmp_path):
        write_session_files(tmp_path, pcs_expires=time.time() - DAY)
        args = build_parser().parse_args(
            ["status", "--email", ACCOUNT, "--cookie-directory", str(tmp_path)]
        )
        out = io.StringIO()

        code = cmd_status(args, out)

        assert code >= 1
        assert "ADP/PCS" in out.getvalue()

    def test_status_json_always_carries_the_pcs_state(self, tmp_path):
        write_session_files(tmp_path)
        args = build_parser().parse_args(
            ["status", "--email", ACCOUNT, "--cookie-directory", str(tmp_path), "--json"]
        )
        out = io.StringIO()

        cmd_status(args, out)
        payload = json.loads(out.getvalue())

        assert payload["pcs_cookies"]["service"] == "drive"
        assert payload["pcs_cookies"]["present"] == []

    def test_status_with_adp_shows_the_detail_even_when_quiet(self, tmp_path):
        write_session_files(tmp_path)
        args = build_parser().parse_args(
            ["status", "--email", ACCOUNT, "--cookie-directory", str(tmp_path), "--adp"]
        )
        out = io.StringIO()

        cmd_status(args, out)

        assert "ADP/PCS" in out.getvalue()

    def test_a_drive_423_is_still_named_as_adp_by_the_doctor(self, tmp_path):
        write_session_files(tmp_path)

        def probe(service):
            raise RuntimeError(LOCKED_423)

        diagnosis = AuthDoctor(
            ACCOUNT, cookie_directory=tmp_path, online=True, now=NOW,
            service_factory=lambda **kw: object(),
            drive_probe=probe,
            adp_transport=lambda service: RecordedTransport(webservices=None),
        ).run()

        drive = next(c for c in diagnosis.checks if c.name == "drive_access")
        assert "Advanced Data Protection" in drive.detail
        assert "--adp" in drive.remedy


# ---------------------------------------------------------------------------
# Interaction with 2FA
# ---------------------------------------------------------------------------

class FakeService:
    """Stand-in for PyiCloudService, with the 2FA surface ``renew`` uses."""

    def __init__(self, requires_2fa=False, trusted=True, validate=True):
        self.requires_2fa = requires_2fa
        self.is_trusted_session = trusted
        self._validate = validate
        self.validated_with = None
        self.trusted_calls = 0

    def validate_2fa_code(self, code):
        self.validated_with = code
        if self._validate:
            self.is_trusted_session = True
        return self._validate

    def trust_session(self):
        self.trusted_calls += 1
        self.is_trusted_session = True
        return True


def run_renew(argv, service_factory, adp_transport=None):
    args = build_parser().parse_args(["renew"] + argv)
    out = io.StringIO()
    code = cmd_renew(
        args, out, service_factory=service_factory, adp_transport=adp_transport
    )
    return code, out.getvalue()


class TestTwoFactorAndADPAreSeparateGates:
    """Two different approvals, two different failures, two different fixes."""

    def test_the_pcs_flow_runs_only_after_2fa_completes(self, tmp_path):
        write_session_files(tmp_path)
        service = FakeService(requires_2fa=True)
        transport = adp_transport(grants=1)

        code, out = run_renew(
            ["--email", ACCOUNT, "--cookie-directory", str(tmp_path),
             "--2fa-code", "123456", "--no-stdin", "--adp"],
            service_factory=lambda **kw: service,
            adp_transport=lambda svc: transport,
        )

        assert code == 0
        assert service.validated_with == "123456"
        assert transport.pcs_calls == 1
        assert "per-service encryption cookie obtained" in out

    def test_a_2fa_code_source_is_never_consumed_by_the_pcs_approval(self, tmp_path):
        """The device tap is not a code; asking for one would be a dead end."""
        write_session_files(tmp_path)
        code_file = tmp_path / "code.txt"
        code_file.write_text("999111")
        service = FakeService(requires_2fa=False)
        transport = adp_transport(grants=1)

        run_renew(
            ["--email", ACCOUNT, "--cookie-directory", str(tmp_path),
             "--2fa-file", str(code_file), "--no-stdin", "--adp"],
            service_factory=lambda **kw: service,
            adp_transport=lambda svc: transport,
        )

        assert service.validated_with is None
        for _, body in transport.calls:
            assert "securityCode" not in json.dumps(body or {})

    def test_an_environment_2fa_code_does_not_change_the_pcs_flow(self, tmp_path, monkeypatch):
        write_session_files(tmp_path)
        monkeypatch.setenv("IFETCH_2FA_CODE", "222333")
        transport = adp_transport(grants=1)

        code, _ = run_renew(
            ["--email", ACCOUNT, "--cookie-directory", str(tmp_path), "--adp"],
            service_factory=lambda **kw: FakeService(requires_2fa=True),
            adp_transport=lambda svc: transport,
        )

        assert code == 0
        assert transport.pcs_calls == 1

    def test_a_pcs_failure_is_reported_as_adp_not_as_a_2fa_problem(self, tmp_path):
        write_session_files(tmp_path)
        transport = adp_transport(
            responses={PCS_WEB_ACCESS_STATE_PATH: dict(WEB_ACCESS_DISABLED_PAYLOAD)},
        )

        code, out = run_renew(
            ["--email", ACCOUNT, "--cookie-directory", str(tmp_path), "--adp"],
            service_factory=lambda **kw: FakeService(),
            adp_transport=lambda svc: transport,
        )

        assert code == 2
        assert "Access iCloud Data on the Web" in out
        assert "--2fa-code" not in out

    def test_a_2fa_failure_is_not_reported_as_an_adp_problem(self, tmp_path):
        write_session_files(tmp_path)

        code, out = run_renew(
            ["--email", ACCOUNT, "--cookie-directory", str(tmp_path),
             "--2fa-code", "000000", "--no-stdin", "--adp"],
            service_factory=lambda **kw: FakeService(requires_2fa=True, validate=False),
            adp_transport=lambda svc: adp_transport(grants=1),
        )

        assert code == 2
        assert "Advanced Data Protection" not in out

    def test_no_adp_skips_the_flow_and_says_the_state_is_unknown(self, tmp_path):
        write_session_files(tmp_path)
        transport = adp_transport(grants=1)

        code, out = run_renew(
            ["--email", ACCOUNT, "--cookie-directory", str(tmp_path), "--no-adp"],
            service_factory=lambda **kw: FakeService(),
            adp_transport=lambda svc: transport,
        )

        assert code == 0
        assert transport.calls == []
        assert "unknown, not off" in out

    def test_the_renew_json_report_carries_the_pcs_outcome_without_the_cookie(self, tmp_path):
        write_session_files(tmp_path)
        transport = adp_transport(grants=1)

        code, out = run_renew(
            ["--email", ACCOUNT, "--cookie-directory", str(tmp_path), "--adp", "--json"],
            service_factory=lambda **kw: FakeService(),
            adp_transport=lambda svc: transport,
        )

        payload = json.loads(out)
        assert payload["adp"]["status"] == PCS_ACQUIRED
        assert SECRET not in out

    def test_a_bounded_wait_is_configurable_from_the_command_line(self, tmp_path):
        """--adp-timeout 0 proves the flag reaches the loop without a real wait."""
        write_session_files(tmp_path)
        transport = adp_transport()

        code, out = run_renew(
            ["--email", ACCOUNT, "--cookie-directory", str(tmp_path),
             "--adp", "--adp-timeout", "0"],
            service_factory=lambda **kw: FakeService(),
            adp_transport=lambda svc: transport,
        )

        assert code == 2
        assert transport.pcs_calls == 1
        assert "trusted device" in out


# ---------------------------------------------------------------------------
# Transport adapter
# ---------------------------------------------------------------------------

class TestPyiCloudTransport:
    """The thin adapter over pyicloud, which owns the session and the jar."""

    class Jar(list):
        def __init__(self, names):
            super().__init__(_NamedCookie(name) for name in names)
            self.saved = 0

        def save(self):
            self.saved += 1

    class Session:
        def __init__(self, jar, payload=None):
            self.cookies = jar
            self.payload = payload or {}
            self.posts = []

        def post(self, url, json=None, params=None):
            self.posts.append((url, json, params))
            return _JSONResponse(self.payload)

    def _service(self, names=(), payload=None, data=None):
        session = self.Session(self.Jar(names), payload)
        service = type(
            "Service",
            (),
            {
                "session": session,
                "params": {"dsid": "1"},
                "_setup_endpoint": "https://setup.icloud.invalid/setup/ws/1",
                "data": data,
            },
        )()
        return service, session

    def test_cookie_names_are_read_without_touching_values(self):
        service, _ = self._service(names=[DRIVE_COOKIE, "other"])
        assert PyiCloudPCSTransport(service).cookie_names() == [DRIVE_COOKIE, "other"]

    def test_webservices_come_from_the_payload_pyicloud_already_fetched(self):
        service, _ = self._service(data={"webservices": {"drivews": {"pcsRequired": True}}})
        assert PyiCloudPCSTransport(service).webservices() == {
            "drivews": {"pcsRequired": True}
        }

    def test_webservices_is_none_when_the_account_payload_is_absent(self):
        """None must mean 'unknown', never 'nothing required'."""
        service, _ = self._service()
        assert PyiCloudPCSTransport(service).webservices() is None

    def test_the_request_goes_to_the_accounts_own_setup_endpoint(self):
        service, session = self._service(payload=dict(PCS_SUCCESS))

        PyiCloudPCSTransport(service).post(PCS_REQUEST_PATH, {"appName": "iclouddrive"})

        url, body, params = session.posts[0]
        assert url.endswith("/setup/ws/1/requestPCS")
        assert body == {"appName": "iclouddrive"}
        assert params == {"dsid": "1"}

    def test_a_session_that_cannot_request_fails_with_a_sentence_not_an_attribute_error(self):
        service = type("Bare", (), {})()
        with pytest.raises(RuntimeError) as excinfo:
            PyiCloudPCSTransport(service).post(PCS_REQUEST_PATH, None)
        assert "Advanced Data Protection" in str(excinfo.value)

    def test_persist_never_turns_a_success_into_a_failure(self):
        service, session = self._service(names=[DRIVE_COOKIE])
        PyiCloudPCSTransport(service).persist()
        assert session.cookies.saved == 1

    def test_persist_tolerates_a_session_with_no_jar(self):
        PyiCloudPCSTransport(type("Bare", (), {})()).persist()  # must not raise


class _NamedCookie:
    def __init__(self, name):
        self.name = name


class _JSONResponse:
    def __init__(self, payload):
        self._payload = payload

    def json(self):
        return self._payload


# ---------------------------------------------------------------------------
# Fixture integrity
# ---------------------------------------------------------------------------

class TestRecordedFixtures:
    """Guard the recordings themselves: a drifted fixture proves nothing."""

    def test_the_pending_messages_are_the_ones_the_code_retries_on(self):
        from ifetch.auth import _is_pending

        assert _is_pending(PCS_PENDING_UPLOAD["message"])
        assert _is_pending(PCS_PENDING_SERVER["message"])
        assert not _is_pending(PCS_UNKNOWN_STATE["message"])
        assert not _is_pending(PCS_SUCCESS["message"])

    def test_adp_and_non_adp_fixtures_differ_only_in_the_signal_field(self):
        differing = {
            key
            for key in set(WEB_ACCESS_ADP_READY) | set(WEB_ACCESS_NO_ADP)
            if WEB_ACCESS_ADP_READY.get(key) != WEB_ACCESS_NO_ADP.get(key)
        }
        assert differing == {"isICDRSDisabled"}

    def test_the_service_registry_matches_apples_app_names(self):
        assert pcs_service("drive").app_name == "iclouddrive"
        assert pcs_service("photos").app_name == "photos"

    def test_an_unknown_service_is_rejected_loudly(self):
        with pytest.raises(ValueError) as excinfo:
            pcs_service("calendar")
        assert "drive" in str(excinfo.value)

    def test_the_drive_cookie_name_is_the_one_apple_sets(self):
        assert pcs_service("drive").cookies == (DRIVE_COOKIE,)

    def test_the_423_fixture_is_the_body_apple_actually_returns(self):
        assert "Missing PCS cookies from the request" in LOCKED_423
        assert "423" in LOCKED_423
