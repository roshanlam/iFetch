"""Tests for authentication reliability: diagnosis, expiry, non-interactive 2FA.

The contract under test:

* a failure is reported as a *named cause with a remedy*, never as a raw HTTP
  status;
* expiry is computed from the real cookie on disk, and an estimate is always
  labelled as one;
* a 2FA code can be obtained with no terminal attached, and the resolver never
  blocks a daemon on a TTY read;
* no diagnostic ever raises on malformed input - a doctor that dies on a
  corrupt session file is useless exactly when it is needed.
"""

import json
import os
import sys
import time
from http.cookiejar import Cookie, LWPCookieJar
from pathlib import Path

import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))

from ifetch.auth import (  # noqa: E402
    CHECK_FAIL,
    CHECK_OK,
    CHECK_SKIP,
    CHECK_WARN,
    REGION_CHINA,
    REGION_GLOBAL,
    STATUS_EXPIRED,
    STATUS_OK,
    STATUS_UNKNOWN,
    STATUS_WARN,
    TRUST_TOKEN_LIFETIME_DAYS,
    AuthDoctor,
    TwoFactorResolver,
    TwoFactorUnavailable,
    classify_auth_error,
    evaluate_expiry,
    extract_code,
    read_session_snapshot,
    region_service_kwargs,
    render_diagnosis,
    render_expiry_warning,
    resolve_region,
    session_slug,
)

DAY = 86400.0
NOW = 1_800_000_000.0


# ---------------------------------------------------------------------------
# Region resolution
# ---------------------------------------------------------------------------

class TestRegionResolution:
    """China Mainland Apple IDs are served by entirely different endpoints."""

    def test_defaults_to_global(self):
        assert resolve_region(None, env={}) == REGION_GLOBAL

    def test_explicit_argument_wins_over_environment(self):
        env = {"ICLOUD_REGION": "china", "ICLOUD_CHINA": "true"}
        assert resolve_region("global", env=env) == REGION_GLOBAL

    def test_named_environment_variable(self):
        assert resolve_region(None, env={"ICLOUD_REGION": "china"}) == REGION_CHINA

    @pytest.mark.parametrize("value", ["CHINA", " china ", "China"])
    def test_region_is_case_and_space_insensitive(self, value):
        assert resolve_region(value, env={}) == REGION_CHINA

    def test_legacy_icloud_china_flag_still_honoured(self):
        """The env var predates --region; existing deployments must not break."""
        assert resolve_region(None, env={"ICLOUD_CHINA": "true"}) == REGION_CHINA

    def test_legacy_flag_ignored_when_not_true(self):
        assert resolve_region(None, env={"ICLOUD_CHINA": "false"}) == REGION_GLOBAL

    def test_named_variable_beats_legacy_flag(self):
        env = {"ICLOUD_REGION": "global", "ICLOUD_CHINA": "true"}
        assert resolve_region(None, env=env) == REGION_GLOBAL

    @pytest.mark.parametrize("bad", ["usa", "cn", "prc", "eu"])
    def test_unknown_region_is_rejected_loudly(self, bad):
        """A typo must fail at startup, not silently sync against the wrong cloud."""
        with pytest.raises(ValueError) as excinfo:
            resolve_region(bad, env={})
        assert bad in str(excinfo.value)

    def test_unknown_region_in_environment_is_rejected(self):
        with pytest.raises(ValueError):
            resolve_region(None, env={"ICLOUD_REGION": "atlantis"})

    def test_service_kwargs_only_set_flag_for_china(self):
        assert region_service_kwargs(REGION_GLOBAL) == {}
        assert region_service_kwargs(REGION_CHINA) == {"china_mainland": True}


# ---------------------------------------------------------------------------
# Error classification
# ---------------------------------------------------------------------------

class TestErrorClassification:
    """Every message a user actually sees must name a cause and a remedy."""

    def test_pcs_cookie_error_names_advanced_data_protection(self):
        failure = classify_auth_error(
            'HTTP error 423 (423 Locked) returned body: '
            '"{\\"errorReason\\":\\"Missing PCS cookies from the request\\"}"'
        )
        assert failure.code == "adp_pcs_cookies"
        assert "Advanced Data Protection" in failure.summary
        assert "Access iCloud Data on the Web" in failure.remedy

    def test_invalid_session_token_suggests_reset(self):
        failure = classify_auth_error(
            'HTTP error 400: {"success":false,"error":"Invalid Session Token"}'
        )
        assert failure.code == "invalid_session_token"
        assert "--reset" in failure.remedy

    def test_china_redirect_is_recognised_as_a_region_problem(self):
        failure = classify_auth_error(
            'HTTP error 302 (302 Found) returned body: "{\\"domainToUse\\":\\"iCloud.com.cn\\"}"'
        )
        assert failure.code == "wrong_region"
        assert "--region china" in failure.remedy

    def test_409_with_valid_code_is_distinguished_from_a_wrong_code(self):
        """Apple says the code is valid and rejects it anyway; that is not user error."""
        failure = classify_auth_error(
            'validate2FACode failed: HTTP 409, X-Apple-Edp: true, body {"valid": true}'
        )
        assert failure.code == "code_accepted_but_rejected"
        assert "offline code" in failure.remedy.lower()

    def test_app_specific_password_is_called_out(self):
        failure = classify_auth_error("app-specific passwords are not supported")
        assert failure.code == "app_specific_password"

    def test_missing_keyring_password_points_at_the_login_command(self):
        failure = classify_auth_error("PyiCloudNoStoredPasswordAvailableException")
        assert failure.code == "no_stored_password"
        assert "icloud auth login" in failure.remedy

    def test_rate_limiting_advises_waiting_rather_than_retrying(self):
        failure = classify_auth_error("HTTP 429 too many requests")
        assert failure.code == "rate_limited"
        assert "wait" in failure.remedy.lower()

    def test_unrecognised_error_is_reported_honestly_not_guessed(self):
        failure = classify_auth_error("something entirely novel went wrong")
        assert failure.code == "unknown"
        assert "something entirely novel" in failure.raw

    def test_accepts_an_exception_object_not_just_a_string(self):
        failure = classify_auth_error(RuntimeError("Invalid Session Token"))
        assert failure.code == "invalid_session_token"

    @pytest.mark.parametrize("value", [None, "", 0])
    def test_empty_input_never_raises(self, value):
        assert classify_auth_error(value).code == "unknown"

    def test_every_failure_carries_a_remedy(self):
        """A named cause with no remedy is no better than an HTTP status."""
        samples = [
            "Missing PCS cookies", "Invalid Session Token", "domainToUse",
            "409 valid", "app-specific", "no stored password", "429", "???",
        ]
        for sample in samples:
            failure = classify_auth_error(sample)
            assert failure.remedy.strip(), f"no remedy for {sample!r}"
            assert failure.summary.strip()


# ---------------------------------------------------------------------------
# Session snapshot (filesystem integration)
# ---------------------------------------------------------------------------

def write_cookiejar(path: Path, expires: float, name: str = "X-APPLE-WEBAUTH-TOKEN"):
    """Write a real LWP-format cookie jar, as pyicloud does."""
    jar = LWPCookieJar(str(path))
    jar.set_cookie(
        Cookie(
            version=0, name=name, value="token-value", port=None, port_specified=False,
            domain="icloud.com", domain_specified=True, domain_initial_dot=False,
            path="/", path_specified=True, secure=True, expires=expires,
            discard=False, comment=None, comment_url=None, rest={},
        )
    )
    jar.save(ignore_discard=True, ignore_expires=True)


def write_session(path: Path, session_token="abc", trust_token="xyz"):
    payload = {}
    if session_token is not None:
        payload["session_token"] = session_token
    if trust_token is not None:
        payload["trust_token"] = trust_token
    path.write_text(json.dumps(payload), encoding="utf-8")


class TestSessionSnapshot:
    def test_slug_matches_pyicloud_word_character_rule(self):
        assert session_slug("you@example.com") == "youexamplecom"
        assert session_slug("a.b-c_d@e.com") == "abc_decom"

    def test_missing_session_is_reported_not_raised(self, tmp_path):
        snapshot = read_session_snapshot("you@example.com", tmp_path)
        assert snapshot.exists is False
        assert snapshot.has_session_token is False
        assert snapshot.read_errors == []

    def test_reads_tokens_and_cookie_expiry(self, tmp_path):
        slug = session_slug("you@example.com")
        write_session(tmp_path / f"{slug}.session")
        write_cookiejar(tmp_path / f"{slug}.cookiejar", NOW + 10 * DAY)

        snapshot = read_session_snapshot("you@example.com", tmp_path)
        assert snapshot.exists
        assert snapshot.has_session_token
        assert snapshot.has_trust_token
        assert snapshot.webauth_expires_at == pytest.approx(NOW + 10 * DAY, abs=1)

    def test_already_expired_cookie_is_still_read(self, tmp_path):
        """Reporting how long ago a session died is the point of the diagnostic."""
        slug = session_slug("you@example.com")
        write_session(tmp_path / f"{slug}.session")
        write_cookiejar(tmp_path / f"{slug}.cookiejar", NOW - 5 * DAY)

        snapshot = read_session_snapshot("you@example.com", tmp_path)
        assert snapshot.webauth_expires_at == pytest.approx(NOW - 5 * DAY, abs=1)

    def test_corrupt_session_file_is_captured_as_an_error(self, tmp_path):
        slug = session_slug("you@example.com")
        (tmp_path / f"{slug}.session").write_text("{not json", encoding="utf-8")

        snapshot = read_session_snapshot("you@example.com", tmp_path)
        assert snapshot.exists
        assert snapshot.has_session_token is False
        assert any("parse session file" in e for e in snapshot.read_errors)

    def test_corrupt_cookiejar_is_captured_as_an_error(self, tmp_path):
        slug = session_slug("you@example.com")
        write_session(tmp_path / f"{slug}.session")
        (tmp_path / f"{slug}.cookiejar").write_text("garbage", encoding="utf-8")

        snapshot = read_session_snapshot("you@example.com", tmp_path)
        assert snapshot.webauth_expires_at is None
        assert any("cookie jar" in e for e in snapshot.read_errors)

    def test_session_without_trust_token_is_detected(self, tmp_path):
        slug = session_slug("you@example.com")
        write_session(tmp_path / f"{slug}.session", trust_token=None)
        snapshot = read_session_snapshot("you@example.com", tmp_path)
        assert snapshot.has_session_token is True
        assert snapshot.has_trust_token is False

    def test_snapshot_is_json_serialisable(self, tmp_path):
        snapshot = read_session_snapshot("you@example.com", tmp_path)
        json.dumps(snapshot.to_dict())  # must not raise


# ---------------------------------------------------------------------------
# Expiry arithmetic
# ---------------------------------------------------------------------------

def snapshot_expiring_in(tmp_path, days, account="you@example.com"):
    slug = session_slug(account)
    write_session(tmp_path / f"{slug}.session")
    write_cookiejar(tmp_path / f"{slug}.cookiejar", NOW + days * DAY)
    return read_session_snapshot(account, tmp_path)


class TestExpiry:
    def test_healthy_session_is_ok(self, tmp_path):
        verdict = evaluate_expiry(snapshot_expiring_in(tmp_path, 20), now=NOW)
        assert verdict.status == STATUS_OK
        assert verdict.days_remaining == pytest.approx(20, abs=0.01)
        assert verdict.estimated is False
        assert verdict.needs_attention is False

    def test_warns_before_expiry_not_after(self, tmp_path):
        """The whole point: act while there is still time."""
        verdict = evaluate_expiry(snapshot_expiring_in(tmp_path, 3), now=NOW)
        assert verdict.status == STATUS_WARN
        assert verdict.needs_attention is True

    def test_expired_session_reports_how_long_ago(self, tmp_path):
        verdict = evaluate_expiry(snapshot_expiring_in(tmp_path, -4), now=NOW)
        assert verdict.status == STATUS_EXPIRED
        assert "4.0 days ago" in verdict.detail

    @pytest.mark.parametrize(
        "days,warn,expected",
        [
            (8, 7, STATUS_OK),
            (7, 7, STATUS_WARN),      # boundary is inclusive
            (6.9, 7, STATUS_WARN),
            (0.01, 7, STATUS_WARN),
            (0, 7, STATUS_EXPIRED),   # exactly at expiry counts as expired
            (-0.01, 7, STATUS_EXPIRED),
            (14, 30, STATUS_WARN),    # a longer warning window catches it earlier
        ],
    )
    def test_boundaries(self, tmp_path, days, warn, expected):
        snapshot = snapshot_expiring_in(tmp_path, days)
        assert evaluate_expiry(snapshot, warn_days=warn, now=NOW).status == expected

    def test_no_session_at_all_is_unknown_not_ok(self, tmp_path):
        """Absence of evidence must never be reported as a healthy session."""
        verdict = evaluate_expiry(read_session_snapshot("nobody@example.com", tmp_path), now=NOW)
        assert verdict.status == STATUS_UNKNOWN
        assert verdict.days_remaining is None

    def test_falls_back_to_file_mtime_and_says_it_is_an_estimate(self, tmp_path):
        """Without a readable cookie we guess - and must label the guess."""
        slug = session_slug("you@example.com")
        session = tmp_path / f"{slug}.session"
        write_session(session)
        os.utime(session, (NOW - 25 * DAY, NOW - 25 * DAY))

        verdict = evaluate_expiry(read_session_snapshot("you@example.com", tmp_path), now=NOW)
        assert verdict.estimated is True
        assert verdict.days_remaining == pytest.approx(TRUST_TOKEN_LIFETIME_DAYS - 25, abs=0.1)
        assert "estimated" in verdict.detail

    def test_real_cookie_expiry_beats_the_mtime_estimate(self, tmp_path):
        """When both signals exist the authoritative one must win."""
        slug = session_slug("you@example.com")
        session = tmp_path / f"{slug}.session"
        write_session(session)
        os.utime(session, (NOW - 29 * DAY, NOW - 29 * DAY))  # estimate: 1 day left
        write_cookiejar(tmp_path / f"{slug}.cookiejar", NOW + 20 * DAY)

        verdict = evaluate_expiry(read_session_snapshot("you@example.com", tmp_path), now=NOW)
        assert verdict.estimated is False
        assert verdict.days_remaining == pytest.approx(20, abs=0.1)

    def test_verdict_is_json_serialisable(self, tmp_path):
        verdict = evaluate_expiry(snapshot_expiring_in(tmp_path, 5), now=NOW)
        payload = json.dumps(verdict.to_dict())
        assert "expires_at" in payload

    def test_warning_text_is_none_when_healthy(self, tmp_path):
        verdict = evaluate_expiry(snapshot_expiring_in(tmp_path, 20), now=NOW)
        assert render_expiry_warning(verdict, "you@example.com") is None

    def test_warning_text_names_the_account(self, tmp_path):
        verdict = evaluate_expiry(snapshot_expiring_in(tmp_path, 2), now=NOW)
        text = render_expiry_warning(verdict, "you@example.com")
        assert "you@example.com" in text and text.startswith("WARNING")


# ---------------------------------------------------------------------------
# Code extraction
# ---------------------------------------------------------------------------

class TestExtractCode:
    @pytest.mark.parametrize(
        "raw,expected",
        [
            ("123456", "123456"),
            ("  123456\n", "123456"),
            (b"123456", "123456"),
            ("Your Apple ID code is 482913. Do not share it.", "482913"),
            ('{"code": "998877"}', "998877"),
            ("482913\n482913\n", "482913"),  # same code twice is unambiguous
        ],
    )
    def test_accepts_messy_real_world_input(self, raw, expected):
        assert extract_code(raw) == expected

    @pytest.mark.parametrize(
        "raw",
        [
            None, "", "no digits here", "12345", "1234567",
            "code 111111 or maybe 222222",  # ambiguous: refuse rather than guess
        ],
    )
    def test_rejects_absent_short_long_and_ambiguous(self, raw):
        assert extract_code(raw) is None

    def test_ambiguity_is_refused_because_a_wrong_guess_burns_an_attempt(self):
        assert extract_code("111111 then 222222") is None


# ---------------------------------------------------------------------------
# Non-interactive 2FA
# ---------------------------------------------------------------------------

class FakeClock:
    """A controllable clock so timeout behaviour is tested without waiting."""

    def __init__(self, start=0.0):
        self.t = start
        self.slept = []

    def now(self):
        return self.t

    def sleep(self, seconds):
        self.slept.append(seconds)
        self.t += seconds


class TestTwoFactorResolver:
    def test_explicit_code_wins(self):
        resolver = TwoFactorResolver(code="123456", env={}, allow_stdin=False)
        assert resolver.resolve() == "123456"

    def test_environment_variable(self):
        resolver = TwoFactorResolver(env={"IFETCH_2FA_CODE": "654321"}, allow_stdin=False)
        assert resolver.resolve() == "654321"

    def test_custom_environment_variable_name(self):
        resolver = TwoFactorResolver(
            env_var="MY_CODE", env={"MY_CODE": "111222"}, allow_stdin=False
        )
        assert resolver.resolve() == "111222"

    def test_explicit_code_beats_environment(self):
        resolver = TwoFactorResolver(
            code="123456", env={"IFETCH_2FA_CODE": "654321"}, allow_stdin=False
        )
        assert resolver.resolve() == "123456"

    def test_reads_a_piped_stdin(self):
        import io

        resolver = TwoFactorResolver(env={}, stdin=io.StringIO("246813\n"))
        assert resolver.resolve() == "246813"

    def test_never_reads_a_tty_because_that_would_hang_a_daemon(self):
        class TTY:
            def isatty(self):
                return True

            def readline(self):  # pragma: no cover - must never be called
                raise AssertionError("readline() called on a TTY")

        resolver = TwoFactorResolver(env={}, stdin=TTY())
        with pytest.raises(TwoFactorUnavailable):
            resolver.resolve()

    def test_file_that_already_exists_is_picked_up_immediately(self, tmp_path):
        code_file = tmp_path / "code.txt"
        code_file.write_text("135791")
        clock = FakeClock()
        resolver = TwoFactorResolver(
            env={}, file=code_file, allow_stdin=False,
            now=clock.now, sleep=clock.sleep, timeout=0,
        )
        assert resolver.resolve() == "135791"
        assert clock.slept == []  # no waiting needed

    def test_file_appearing_later_is_picked_up(self, tmp_path):
        code_file = tmp_path / "code.txt"
        clock = FakeClock()
        writes = {"n": 0}

        def sleep(seconds):
            clock.sleep(seconds)
            writes["n"] += 1
            if writes["n"] == 2:
                code_file.write_text("Apple code: 864209")

        resolver = TwoFactorResolver(
            env={}, file=code_file, allow_stdin=False,
            now=clock.now, sleep=sleep, timeout=60, poll_interval=5,
        )
        assert resolver.resolve() == "864209"
        assert len(clock.slept) == 2

    def test_partially_written_file_does_not_abort_the_wait(self, tmp_path):
        """A half-written file yields no code; polling must continue, not fail."""
        code_file = tmp_path / "code.txt"
        code_file.write_text("Apple")  # no code yet
        clock = FakeClock()

        def sleep(seconds):
            clock.sleep(seconds)
            code_file.write_text("Apple code 314159")

        resolver = TwoFactorResolver(
            env={}, file=code_file, allow_stdin=False,
            now=clock.now, sleep=sleep, timeout=60, poll_interval=5,
        )
        assert resolver.resolve() == "314159"

    def test_webhook_source(self):
        resolver = TwoFactorResolver(
            env={}, webhook="https://example.invalid/code", allow_stdin=False,
            http_get=lambda url: '{"code":"778899"}',
        )
        assert resolver.resolve() == "778899"

    def test_transient_webhook_failures_are_retried_not_fatal(self):
        clock = FakeClock()
        attempts = {"n": 0}

        def flaky(url):
            attempts["n"] += 1
            if attempts["n"] < 3:
                raise ConnectionError("webhook down")
            return "556677"

        resolver = TwoFactorResolver(
            env={}, webhook="https://example.invalid/code", allow_stdin=False,
            http_get=flaky, now=clock.now, sleep=clock.sleep,
            timeout=60, poll_interval=1,
        )
        assert resolver.resolve() == "556677"
        assert attempts["n"] == 3

    def test_timeout_raises_with_the_sources_it_tried(self, tmp_path):
        clock = FakeClock()
        resolver = TwoFactorResolver(
            env={}, file=tmp_path / "never.txt", allow_stdin=False,
            now=clock.now, sleep=clock.sleep, timeout=10, poll_interval=5,
        )
        with pytest.raises(TwoFactorUnavailable) as excinfo:
            resolver.resolve()
        message = str(excinfo.value)
        assert "never.txt" in message and "Timed out" in message

    def test_no_source_configured_fails_immediately_without_polling(self):
        clock = FakeClock()
        resolver = TwoFactorResolver(
            env={}, allow_stdin=False, now=clock.now, sleep=clock.sleep, timeout=999,
        )
        with pytest.raises(TwoFactorUnavailable) as excinfo:
            resolver.resolve()
        assert clock.slept == []  # did not wait 999s for a source that cannot arrive
        assert "--2fa-code" in str(excinfo.value)

    def test_describe_sources_lists_what_is_configured(self, tmp_path):
        resolver = TwoFactorResolver(
            code="123456", file=tmp_path / "c.txt",
            webhook="https://example.invalid", env={}, allow_stdin=True,
        )
        described = " ".join(resolver.describe_sources())
        assert "--2fa-code" in described
        assert "c.txt" in described
        assert "webhook" in described
        assert "stdin" in described

    def test_file_takes_precedence_over_webhook_when_both_have_codes(self, tmp_path):
        code_file = tmp_path / "code.txt"
        code_file.write_text("111111")
        resolver = TwoFactorResolver(
            env={}, file=code_file, webhook="https://example.invalid",
            allow_stdin=False, http_get=lambda url: "222222", timeout=0,
            now=FakeClock().now, sleep=lambda s: None,
        )
        assert resolver.resolve() == "111111"


# ---------------------------------------------------------------------------
# AuthDoctor
# ---------------------------------------------------------------------------

class FakeService:
    """Stand-in for PyiCloudService with controllable auth state."""

    def __init__(self, requires_2fa=False, trusted=True, drive_items=None):
        self.requires_2fa = requires_2fa
        self.is_trusted_session = trusted
        self._drive_items = drive_items if drive_items is not None else ["Documents"]


class TestAuthDoctor:
    def test_offline_run_makes_no_network_call(self, tmp_path):
        def exploding_factory(**kwargs):  # pragma: no cover
            raise AssertionError("network used in offline mode")

        doctor = AuthDoctor(
            "you@example.com", cookie_directory=tmp_path,
            online=False, service_factory=exploding_factory,
        )
        diagnosis = doctor.run()
        live = [c for c in diagnosis.checks if c.name == "live_authentication"]
        assert live[0].status == CHECK_SKIP

    def test_missing_session_fails_with_a_remedy(self, tmp_path):
        diagnosis = AuthDoctor("you@example.com", cookie_directory=tmp_path).run()
        stored = next(c for c in diagnosis.checks if c.name == "stored_session")
        assert stored.status == CHECK_FAIL
        assert "ifetch auth renew" in stored.remedy
        assert diagnosis.exit_code == 2

    def test_healthy_offline_session_passes(self, tmp_path):
        snapshot_expiring_in(tmp_path, 20)
        diagnosis = AuthDoctor(
            "you@example.com", cookie_directory=tmp_path, now=NOW
        ).run()
        assert diagnosis.exit_code == 0
        assert diagnosis.status == CHECK_OK

    def test_expiring_session_exits_1_not_0_or_2(self, tmp_path):
        """A cron job must be able to tell 'renew soon' from 'broken now'."""
        snapshot_expiring_in(tmp_path, 3)
        diagnosis = AuthDoctor(
            "you@example.com", cookie_directory=tmp_path, now=NOW
        ).run()
        assert diagnosis.exit_code == 1

    def test_expired_session_exits_2(self, tmp_path):
        snapshot_expiring_in(tmp_path, -1)
        diagnosis = AuthDoctor(
            "you@example.com", cookie_directory=tmp_path, now=NOW
        ).run()
        assert diagnosis.exit_code == 2

    def test_region_is_always_reported(self, tmp_path):
        diagnosis = AuthDoctor(
            "you@example.com", region=REGION_CHINA, cookie_directory=tmp_path
        ).run()
        region = next(c for c in diagnosis.checks if c.name == "region")
        assert "iCloud.com.cn" in region.detail

    def test_global_region_hints_at_china_when_relevant(self, tmp_path):
        diagnosis = AuthDoctor("you@example.com", cookie_directory=tmp_path).run()
        region = next(c for c in diagnosis.checks if c.name == "region")
        assert "--region china" in region.remedy

    def test_online_pcs_failure_is_reported_as_adp_not_as_http_423(self, tmp_path):
        """The headline behaviour: name the cause, never relay the status code."""
        snapshot_expiring_in(tmp_path, 20)

        def drive_probe(service):
            raise RuntimeError(
                'HTTP error 423 (423 Locked) returned body: '
                '"{\\"errorReason\\":\\"Missing PCS cookies from the request\\"}"'
            )

        diagnosis = AuthDoctor(
            "you@example.com", cookie_directory=tmp_path, online=True, now=NOW,
            service_factory=lambda **kw: FakeService(),
            drive_probe=drive_probe,
        ).run()

        drive = next(c for c in diagnosis.checks if c.name == "drive_access")
        assert drive.status == CHECK_FAIL
        assert "Advanced Data Protection" in drive.detail
        assert "423" not in drive.detail
        assert "Access iCloud Data on the Web" in drive.remedy

    def test_online_healthy_account_passes_every_check(self, tmp_path):
        snapshot_expiring_in(tmp_path, 20)
        diagnosis = AuthDoctor(
            "you@example.com", cookie_directory=tmp_path, online=True, now=NOW,
            service_factory=lambda **kw: FakeService(),
            drive_probe=lambda svc: ["Documents", "Photos"],
        ).run()
        assert diagnosis.exit_code == 0
        drive = next(c for c in diagnosis.checks if c.name == "drive_access")
        assert "2 top-level items" in drive.detail

    def test_untrusted_session_is_flagged(self, tmp_path):
        snapshot_expiring_in(tmp_path, 20)
        diagnosis = AuthDoctor(
            "you@example.com", cookie_directory=tmp_path, online=True, now=NOW,
            service_factory=lambda **kw: FakeService(requires_2fa=True),
            drive_probe=lambda svc: [],
        ).run()
        trusted = next(c for c in diagnosis.checks if c.name == "trusted_session")
        assert trusted.status == CHECK_FAIL
        assert "--2fa-code" in trusted.remedy

    def test_region_is_threaded_through_to_the_service(self, tmp_path):
        captured = {}

        def factory(**kwargs):
            captured.update(kwargs)
            return FakeService()

        AuthDoctor(
            "you@example.com", region=REGION_CHINA, cookie_directory=tmp_path,
            online=True, service_factory=factory, drive_probe=lambda s: [],
        ).run()
        assert captured["china_mainland"] is True
        assert captured["apple_id"] == "you@example.com"

    def test_authentication_failure_is_classified(self, tmp_path):
        def factory(**kwargs):
            raise RuntimeError('{"error":"Invalid Session Token"}')

        diagnosis = AuthDoctor(
            "you@example.com", cookie_directory=tmp_path, online=True,
            service_factory=factory,
        ).run()
        live = next(c for c in diagnosis.checks if c.name == "live_authentication")
        assert live.status == CHECK_FAIL
        assert "expired" in live.detail or "invalidated" in live.detail

    def test_diagnosis_is_json_serialisable(self, tmp_path):
        diagnosis = AuthDoctor("you@example.com", cookie_directory=tmp_path).run()
        json.dumps(diagnosis.to_dict())

    def test_rendered_output_shows_every_check(self, tmp_path):
        diagnosis = AuthDoctor("you@example.com", cookie_directory=tmp_path).run()
        text = render_diagnosis(diagnosis)
        for check in diagnosis.checks:
            assert check.name in text
        assert "Overall:" in text
