"""Tests for ifetch.notify.

No test here touches the network. Every backend goes through
``HttpTransport``, whose ``session`` and ``sleep`` are injected, so the whole
suite runs against recording doubles and a fake clock.

The load-bearing assertions are the negative ones: that nothing raises out of
``Notifier.notify`` no matter what the endpoint does, and that no ping URL or
token ever reaches a log record, a report, or an exception message.
"""

import json
import logging
import sys
from pathlib import Path

import pytest
import requests

sys.path.append(str(Path(__file__).resolve().parents[1]))

from ifetch.notify import (  # noqa: E402
    EVENT_ANOMALY,
    EVENT_FAILURE,
    EVENT_START,
    EVENT_SUCCESS,
    NOTIFY_ENV_VARS,
    REDACTED,
    DeliveryResult,
    HealthchecksBackend,
    HttpTransport,
    NotificationBackend,
    Notifier,
    NotifyConfig,
    NotifyConfigError,
    NotifyError,
    NtfyBackend,
    NullNotifier,
    Redactor,
    RunEvent,
    WebhookBackend,
    add_notification_arguments,
    anomalies_from_report,
    build_notifier,
    format_run_summary,
    safe_url,
)

REPO_ROOT = Path(__file__).resolve().parents[1]

HC_UUID = "0f9d1b2e-4c3a-4f1d-9a7b-2c8e5d6f0a11"
HC_URL = f"https://hc-ping.com/{HC_UUID}"
SELF_HOSTED_HC = f"https://hc.lan.example.org/ping/{HC_UUID}"
NTFY_URL = "https://ntfy.sh/ifetch-secret-topic-9f3a"
NTFY_TOKEN = "tk_AbCdEf0123456789wxyz"
WEBHOOK_URL = "https://hooks.example.com/services/T000/B111/xoxb-super-secret-value"


# ---------------------------------------------------------------------------
# Doubles
# ---------------------------------------------------------------------------

class FakeResponse:
    def __init__(self, status_code=200, text="OK"):
        self.status_code = status_code
        self.text = text


class MalformedResponse:
    """Something that came back from a proxy and is not an HTTP response."""

    status_code = "not-a-number"


class RecordingSession:
    """Stands in for ``requests``; records calls and replays scripted outcomes."""

    def __init__(self, outcomes=None):
        self.calls = []
        self._outcomes = list(outcomes or [])

    def request(self, method, url, **kwargs):
        call = dict(kwargs)
        call["method"] = method
        call["url"] = url
        self.calls.append(call)
        outcome = self._outcomes.pop(0) if self._outcomes else FakeResponse(200)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome

    @property
    def urls(self):
        return [c["url"] for c in self.calls]

    def body(self, index=0):
        raw = self.calls[index].get("data")
        return raw.decode("utf-8") if isinstance(raw, bytes) else raw


class FakeClock:
    def __init__(self):
        self.slept = []
        self.now = 1_000.0

    def sleep(self, seconds):
        self.slept.append(seconds)
        self.now += seconds

    def time(self):
        return self.now


def transport(session=None, clock=None, **kwargs):
    clock = clock or FakeClock()
    return HttpTransport(session=session or RecordingSession(), sleep=clock.sleep, **kwargs)


def hc_backend(session=None, clock=None, url=HC_URL, transport_kwargs=None, **kwargs):
    return HealthchecksBackend(
        url=url,
        transport=transport(session, clock, **(transport_kwargs or {})),
        **kwargs,
    )


def event(kind=EVENT_SUCCESS, **kwargs):
    kwargs.setdefault("title", f"iFetch {kind}")
    kwargs.setdefault("run_id", HC_UUID)
    return RunEvent(event=kind, **kwargs)


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    """No ambient notification configuration leaks into a test."""
    for name in NOTIFY_ENV_VARS:
        monkeypatch.delenv(name, raising=False)


# ---------------------------------------------------------------------------
# Redaction primitives
# ---------------------------------------------------------------------------

def test_redactor_replaces_registered_secret():
    r = Redactor([HC_URL])
    assert HC_URL not in r.redact(f"failed to POST {HC_URL}/start")
    assert REDACTED in r.redact(f"failed to POST {HC_URL}/start")


def test_redactor_ignores_short_values():
    r = Redactor(["abc", "", None])
    assert len(r) == 0
    assert r.redact("abc stays") == "abc stays"


def test_redactor_prefers_longest_secret_first():
    r = Redactor([HC_UUID, HC_URL])
    scrubbed = r.redact(f"url={HC_URL} uuid={HC_UUID}")
    assert HC_URL not in scrubbed and HC_UUID not in scrubbed


def test_redactor_merge_absorbs_other_secrets():
    a, b = Redactor([HC_URL]), Redactor([NTFY_TOKEN])
    a.merge(b)
    assert NTFY_TOKEN not in a.redact(f"token {NTFY_TOKEN}")


def test_redactor_never_exposes_values():
    r = Redactor([HC_URL])
    assert HC_URL not in repr(r)
    assert len(r) == 1


def test_safe_url_keeps_host_and_destroys_path():
    rendered = safe_url(SELF_HOSTED_HC)
    assert rendered == f"https://hc.lan.example.org/{REDACTED}"
    assert HC_UUID not in rendered


def test_safe_url_drops_userinfo_and_handles_garbage():
    assert "hunter2" not in safe_url("https://bob:hunter2@example.com/x")
    assert safe_url("not a url") == REDACTED
    assert safe_url(None) == REDACTED
    assert safe_url("https://ntfy.lan:8080") == "https://ntfy.lan:8080"


# ---------------------------------------------------------------------------
# Healthchecks backend
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "kind,suffix",
    [
        (EVENT_START, "/start"),
        (EVENT_SUCCESS, ""),
        (EVENT_FAILURE, "/fail"),
        (EVENT_ANOMALY, "/log"),
    ],
)
def test_healthchecks_endpoint_per_event(kind, suffix):
    session = RecordingSession()
    backend = hc_backend(session)
    result = backend.deliver(event(kind))
    assert session.calls[0]["method"] == "POST"
    assert session.urls == [HC_URL + suffix]
    assert result.delivered and result.status_code == 200


def test_healthchecks_anomaly_can_be_promoted_to_failure():
    session = RecordingSession()
    backend = hc_backend(session, anomaly_is_failure=True)
    backend.deliver(event(EVENT_ANOMALY))
    assert session.urls == [HC_URL + "/fail"]


def test_healthchecks_posts_run_summary_in_the_body():
    session = RecordingSession()
    backend = hc_backend(session)
    backend.deliver(
        event(EVENT_SUCCESS, message="12 files seen, 3 downloaded", details={"failed": 0})
    )
    body = session.body()
    assert "12 files seen, 3 downloaded" in body
    assert "failed: 0" in body
    assert session.calls[0]["headers"]["Content-Type"].startswith("text/plain")


def test_healthchecks_sends_rid_only_for_uuid_run_ids():
    session = RecordingSession()
    backend = hc_backend(session)
    backend.deliver(event(EVENT_START))
    assert session.calls[0]["params"] == {"rid": HC_UUID}

    session2 = RecordingSession()
    hc_backend(session2).deliver(event(EVENT_START, run_id="run-7"))
    assert session2.calls[0]["params"] is None


def test_healthchecks_self_hosted_url_is_not_rewritten_to_hc_ping():
    session = RecordingSession()
    backend = HealthchecksBackend(url=SELF_HOSTED_HC, transport=transport(session))
    backend.deliver(event(EVENT_SUCCESS))
    assert session.urls == [SELF_HOSTED_HC]


def test_healthchecks_uuid_plus_self_hosted_base_url():
    session = RecordingSession()
    backend = HealthchecksBackend(
        uuid_=HC_UUID,
        base_url="https://hc.lan.example.org/ping/",
        transport=transport(session),
    )
    backend.deliver(event(EVENT_FAILURE))
    assert session.urls == [f"https://hc.lan.example.org/ping/{HC_UUID}/fail"]


def test_healthchecks_uuid_defaults_to_hc_ping():
    backend = HealthchecksBackend(uuid_=HC_UUID, transport=transport())
    assert backend.ping_url == HC_URL


def test_healthchecks_normalises_a_pasted_endpoint_suffix():
    for pasted in (f"{HC_URL}/fail", f"{HC_URL}/start", f"{HC_URL}/log", f"{HC_URL}/"):
        assert HealthchecksBackend(url=pasted, transport=transport()).ping_url == HC_URL


def test_healthchecks_rejects_a_non_http_url_without_quoting_it():
    with pytest.raises(NotifyConfigError) as excinfo:
        HealthchecksBackend(url=f"ftp://hc.example.org/{HC_UUID}")
    assert HC_UUID not in str(excinfo.value)


def test_healthchecks_requires_some_configuration():
    with pytest.raises(NotifyConfigError):
        HealthchecksBackend()


def test_healthchecks_describe_is_redaction_safe():
    described = hc_backend().describe()
    assert HC_UUID not in described
    assert "hc-ping.com" in described


# ---------------------------------------------------------------------------
# ntfy backend
# ---------------------------------------------------------------------------

def test_ntfy_posts_to_topic_url_with_title_and_body():
    session = RecordingSession()
    backend = NtfyBackend(url=NTFY_URL, transport=transport(session))
    backend.deliver(event(EVENT_SUCCESS, title="iFetch run finished", message="all good"))
    call = session.calls[0]
    assert call["method"] == "POST" and call["url"] == NTFY_URL
    assert call["headers"]["Title"] == "iFetch run finished"
    assert session.body() == "all good"


@pytest.mark.parametrize(
    "kind,priority,tag",
    [
        (EVENT_START, "min", "hourglass_flowing_sand"),
        (EVENT_SUCCESS, "default", "white_check_mark"),
        (EVENT_ANOMALY, "high", "warning"),
        (EVENT_FAILURE, "urgent", "rotating_light"),
    ],
)
def test_ntfy_priority_and_tags_follow_the_event(kind, priority, tag):
    session = RecordingSession()
    NtfyBackend(url=NTFY_URL, transport=transport(session)).deliver(event(kind))
    headers = session.calls[0]["headers"]
    assert headers["Priority"] == priority
    assert tag in headers["Tags"]


def test_ntfy_pinned_priority_and_extra_tags_apply():
    session = RecordingSession()
    NtfyBackend(
        url=NTFY_URL, priority="low", tags=("nas", "backup"), transport=transport(session)
    ).deliver(event(EVENT_FAILURE))
    headers = session.calls[0]["headers"]
    assert headers["Priority"] == "low"
    assert "nas" in headers["Tags"] and "backup" in headers["Tags"]


def test_ntfy_self_hosted_server_and_topic():
    session = RecordingSession()
    backend = NtfyBackend(
        topic="icloud", server="http://ntfy.lan:8080/", transport=transport(session)
    )
    backend.deliver(event(EVENT_SUCCESS))
    assert session.urls == ["http://ntfy.lan:8080/icloud"]


def test_ntfy_token_becomes_a_bearer_header():
    session = RecordingSession()
    NtfyBackend(url=NTFY_URL, token=NTFY_TOKEN, transport=transport(session)).deliver(
        event(EVENT_SUCCESS)
    )
    assert session.calls[0]["headers"]["Authorization"] == f"Bearer {NTFY_TOKEN}"


def test_ntfy_header_values_are_ascii_sanitised():
    session = RecordingSession()
    NtfyBackend(url=NTFY_URL, transport=transport(session)).deliver(
        event(EVENT_SUCCESS, title="Reunión — café.pages")
    )
    title = session.calls[0]["headers"]["Title"]
    title.encode("ascii")  # would raise if sanitisation failed
    assert "caf" in title


def test_ntfy_rejects_a_server_url_with_no_topic():
    with pytest.raises(NotifyConfigError):
        NtfyBackend(url="https://ntfy.sh")
    with pytest.raises(NotifyConfigError):
        NtfyBackend()


def test_ntfy_describe_hides_the_topic():
    described = NtfyBackend(url=NTFY_URL, transport=transport()).describe()
    assert "ifetch-secret-topic-9f3a" not in described
    assert "ntfy.sh" in described


# ---------------------------------------------------------------------------
# Webhook backend
# ---------------------------------------------------------------------------

def test_webhook_posts_json_payload():
    session = RecordingSession()
    backend = WebhookBackend(url=WEBHOOK_URL, transport=transport(session))
    backend.deliver(event(EVENT_ANOMALY, message="2 files failed", details={"failed": 2}))
    call = session.calls[0]
    assert call["method"] == "POST" and call["url"] == WEBHOOK_URL
    payload = call["json"]
    assert payload["source"] == "ifetch"
    assert payload["event"] == EVENT_ANOMALY
    assert payload["message"] == "2 files failed"
    assert payload["details"] == {"failed": 2}
    assert payload["run_id"] == HC_UUID
    assert payload["timestamp_iso"].endswith("Z")
    json.dumps(payload)  # must be serialisable as-is


def test_webhook_custom_headers_and_method():
    session = RecordingSession()
    backend = WebhookBackend(
        url=WEBHOOK_URL,
        headers={"X-Api-Key": "k-1234567890abcdef"},
        method="put",
        transport=transport(session),
    )
    backend.deliver(event(EVENT_SUCCESS))
    call = session.calls[0]
    assert call["method"] == "PUT"
    assert call["headers"]["X-Api-Key"] == "k-1234567890abcdef"
    assert call["headers"]["Content-Type"] == "application/json"


def test_webhook_rejects_a_relative_url():
    with pytest.raises(NotifyConfigError):
        WebhookBackend(url="/hooks/run")


def test_webhook_payload_survives_unserialisable_details():
    session = RecordingSession()
    WebhookBackend(url=WEBHOOK_URL, transport=transport(session)).deliver(
        event(EVENT_SUCCESS, details={"path": Path("/mnt/backup")})
    )
    json.dumps(session.calls[0]["json"])


# ---------------------------------------------------------------------------
# Transport: timeouts, retries, backoff
# ---------------------------------------------------------------------------

def test_timeout_is_always_passed_to_requests():
    session = RecordingSession()
    transport(session, timeout=3.5).request("POST", HC_URL)
    assert session.calls[0]["timeout"] == 3.5


def test_timeout_is_forced_even_when_caller_passes_none():
    session = RecordingSession()
    transport(session, timeout=2.0).request("POST", HC_URL, timeout=None)
    assert session.calls[0]["timeout"] == 2.0


def test_backends_pass_the_configured_timeout_through():
    session = RecordingSession()
    hc_backend(session, transport_kwargs={"timeout": 1.25}).deliver(event(EVENT_START))
    assert session.calls[0]["timeout"] == 1.25


def test_retries_on_server_error_then_succeeds():
    clock = FakeClock()
    session = RecordingSession([FakeResponse(500), FakeResponse(503), FakeResponse(200)])
    result = transport(session, clock, retries=2, backoff=1.0).request("POST", HC_URL)
    assert result.attempts == 3 and result.status_code == 200
    assert clock.slept == [1.0, 2.0]  # exponential, and no real sleeping


def test_retries_are_bounded_and_then_give_up():
    clock = FakeClock()
    session = RecordingSession([FakeResponse(500)] * 10)
    with pytest.raises(NotifyError) as excinfo:
        transport(session, clock, retries=2).request("POST", HC_URL)
    assert len(session.calls) == 3
    assert excinfo.value.attempts == 3
    assert excinfo.value.status_code == 500


def test_backoff_is_capped():
    clock = FakeClock()
    session = RecordingSession([FakeResponse(500)] * 20)
    with pytest.raises(NotifyError):
        transport(session, clock, retries=6, backoff=4.0, max_backoff=10.0).request(
            "POST", HC_URL
        )
    assert max(clock.slept) == 10.0


def test_rate_limit_is_retried():
    clock = FakeClock()
    session = RecordingSession([FakeResponse(429), FakeResponse(200)])
    assert transport(session, clock).request("POST", HC_URL).attempts == 2


def test_client_error_is_not_retried():
    clock = FakeClock()
    session = RecordingSession([FakeResponse(404)] * 5)
    with pytest.raises(NotifyError) as excinfo:
        transport(session, clock, retries=3).request("POST", HC_URL)
    assert len(session.calls) == 1
    assert clock.slept == []
    assert "HTTP 404" in str(excinfo.value)


@pytest.mark.parametrize(
    "exc",
    [
        requests.exceptions.ConnectionError("refused"),
        requests.exceptions.ReadTimeout("timed out"),
        requests.exceptions.ConnectTimeout("timed out connecting"),
    ],
)
def test_network_errors_are_retried(exc):
    clock = FakeClock()
    session = RecordingSession([exc, FakeResponse(200)])
    assert transport(session, clock).request("POST", HC_URL).attempts == 2


def test_unexpected_exception_is_not_retried():
    clock = FakeClock()
    session = RecordingSession([ValueError("bug in a backend")] * 5)
    with pytest.raises(NotifyError):
        transport(session, clock, retries=3).request("POST", HC_URL)
    assert len(session.calls) == 1


def test_malformed_response_is_reported_not_retried():
    clock = FakeClock()
    session = RecordingSession([MalformedResponse(), MalformedResponse()])
    with pytest.raises(NotifyError) as excinfo:
        transport(session, clock, retries=2).request("POST", HC_URL)
    assert "no usable status code" in str(excinfo.value)
    assert len(session.calls) == 1


def test_transport_redacts_exception_text():
    session = RecordingSession(
        [requests.exceptions.ConnectionError(f"Max retries exceeded with url: {HC_URL}")]
    )
    t = transport(session, retries=0)
    t.redactor.add(HC_URL)
    with pytest.raises(NotifyError) as excinfo:
        t.request("POST", HC_URL)
    assert HC_URL not in str(excinfo.value)
    assert REDACTED in str(excinfo.value)


# ---------------------------------------------------------------------------
# Notifier: failure isolation
# ---------------------------------------------------------------------------

class ExplodingBackend(NotificationBackend):
    name = "exploding"

    def __init__(self, exc=None):
        super().__init__()
        self.exc = exc or RuntimeError("unexpected bug in a notification backend")
        self.calls = 0

    def deliver(self, event):
        self.calls += 1
        raise self.exc


@pytest.mark.parametrize(
    "outcome",
    [
        requests.exceptions.ConnectionError("connection refused"),
        requests.exceptions.ReadTimeout("read timed out"),
        FakeResponse(500),
        FakeResponse(404),
        MalformedResponse(),
        ValueError("garbage from a proxy"),
        AttributeError("bug"),
    ],
)
def test_no_exception_escapes_notify(outcome, caplog):
    session = RecordingSession([outcome] * 10)
    notifier = Notifier([hc_backend(session, transport_kwargs={"retries": 0})])
    with caplog.at_level(logging.WARNING, logger="ifetch.notify"):
        results = notifier.notify(event(EVENT_SUCCESS))
    assert len(results) == 1 and results[0].delivered is False
    assert results[0].error
    assert any("Notification not delivered" in r.message for r in caplog.records)


def test_backend_raising_an_unexpected_exception_is_contained(caplog):
    notifier = Notifier([ExplodingBackend()])
    with caplog.at_level(logging.WARNING, logger="ifetch.notify"):
        results = notifier.notify(event(EVENT_FAILURE))
    assert results[0].delivered is False
    assert "unexpected bug" in results[0].error


def test_every_lifecycle_call_is_exception_free():
    notifier = Notifier([ExplodingBackend()])
    assert notifier.start() and notifier.start()[0].delivered is False
    assert notifier.success(report={"summary": {"failed": 0}})[0].delivered is False
    assert notifier.failure(RuntimeError("boom"))[0].delivered is False
    assert notifier.anomaly(findings=["x"])[0].delivered is False
    assert notifier.run_finished({"summary": {"failed": 1}})


def test_keyboard_interrupt_is_not_swallowed():
    notifier = Notifier([ExplodingBackend(KeyboardInterrupt())])
    with pytest.raises(KeyboardInterrupt):
        notifier.notify(event(EVENT_SUCCESS))


def test_one_failing_backend_does_not_stop_the_others():
    hc_session = RecordingSession([FakeResponse(500)] * 5)
    ntfy_session = RecordingSession()
    hook_session = RecordingSession()
    notifier = Notifier(
        [
            hc_backend(hc_session, transport_kwargs={"retries": 0}),
            NtfyBackend(url=NTFY_URL, transport=transport(ntfy_session)),
            WebhookBackend(url=WEBHOOK_URL, transport=transport(hook_session)),
        ]
    )
    results = notifier.notify(event(EVENT_FAILURE))
    assert [r.delivered for r in results] == [False, True, True]
    assert len(ntfy_session.calls) == 1 and len(hook_session.calls) == 1


def test_exploding_backend_does_not_stop_later_backends():
    session = RecordingSession()
    notifier = Notifier(
        [ExplodingBackend(), NtfyBackend(url=NTFY_URL, transport=transport(session))]
    )
    results = notifier.notify(event(EVENT_SUCCESS))
    assert [r.delivered for r in results] == [False, True]


# ---------------------------------------------------------------------------
# Notifier: secret redaction end to end
# ---------------------------------------------------------------------------

def test_ping_url_never_reaches_the_log(caplog, monkeypatch):
    monkeypatch.setenv("IFETCH_HEALTHCHECKS_URL", HC_URL)
    session = RecordingSession(
        [requests.exceptions.ConnectionError(f"Max retries exceeded with url: {HC_URL}/start")]
    )
    notifier = Notifier.from_config(
        NotifyConfig.from_env(), transport=transport(session, retries=0)
    )
    with caplog.at_level(logging.WARNING, logger="ifetch.notify"):
        notifier.start()
    logged = "\n".join(r.getMessage() for r in caplog.records)
    assert HC_UUID not in logged
    assert HC_URL not in logged
    assert REDACTED in logged


def test_ntfy_token_never_reaches_the_log(caplog):
    session = RecordingSession([requests.exceptions.ConnectionError(f"auth {NTFY_TOKEN}")] * 3)
    config = NotifyConfig(ntfy_url=NTFY_URL, ntfy_token=NTFY_TOKEN, retries=0)
    notifier = Notifier.from_config(config, transport=transport(session, retries=0))
    with caplog.at_level(logging.WARNING, logger="ifetch.notify"):
        notifier.failure(RuntimeError("run died"))
    logged = "\n".join(r.getMessage() for r in caplog.records)
    assert NTFY_TOKEN not in logged
    assert NTFY_URL not in logged


def test_report_snapshot_contains_no_secrets():
    session = RecordingSession([FakeResponse(500)] * 10)
    config = NotifyConfig(
        healthchecks_url=HC_URL,
        ntfy_url=NTFY_URL,
        ntfy_token=NTFY_TOKEN,
        webhook_url=WEBHOOK_URL,
        webhook_headers={"X-Token": "secret-header-value-123"},
        retries=0,
    )
    notifier = Notifier.from_config(config, transport=transport(session, retries=0))
    notifier.start()
    notifier.run_finished({"summary": {"failed": 3, "total_files": 10}})
    blob = json.dumps(notifier.report_snapshot())
    for secret in (HC_URL, HC_UUID, NTFY_URL, NTFY_TOKEN, WEBHOOK_URL, "secret-header-value-123"):
        assert secret not in blob
    assert notifier.report_snapshot()["undelivered"] > 0


def test_describe_is_safe_but_still_diagnostic():
    config = NotifyConfig(
        healthchecks_url=SELF_HOSTED_HC, ntfy_url=NTFY_URL, webhook_url=WEBHOOK_URL
    )
    described = " ".join(Notifier.from_config(config).describe())
    assert HC_UUID not in described and "xoxb-super-secret-value" not in described
    assert "hc.lan.example.org" in described  # the host is the useful half


def test_redaction_covers_an_error_message_carrying_the_url():
    config = NotifyConfig(healthchecks_url=HC_URL)
    notifier = Notifier.from_config(config)
    assert HC_URL not in notifier.redact(f"POST {HC_URL}/fail failed")


def test_failure_event_message_is_redacted():
    session = RecordingSession()
    config = NotifyConfig(webhook_url=WEBHOOK_URL)
    notifier = Notifier.from_config(config, transport=transport(session))
    notifier.failure(RuntimeError(f"could not reach {WEBHOOK_URL}"))
    assert WEBHOOK_URL not in json.dumps(session.calls[0]["json"])


def test_misconfigured_backend_is_reported_without_quoting_the_secret(caplog):
    config = NotifyConfig(healthchecks_url=f"ftp://hc.example.org/{HC_UUID}", ntfy_url=NTFY_URL)
    with caplog.at_level(logging.WARNING, logger="ifetch.notify"):
        notifier = Notifier.from_config(config)
    logged = "\n".join(r.getMessage() for r in caplog.records)
    assert "could not be configured" in logged
    assert HC_UUID not in logged
    # The healthy backend still runs: one bad setting must not silence the rest.
    assert [b.name for b in notifier.backends] == ["ntfy"]


# ---------------------------------------------------------------------------
# Configuration: flags, environment, precedence
# ---------------------------------------------------------------------------

def test_config_reads_every_backend_from_the_environment(monkeypatch):
    monkeypatch.setenv("IFETCH_HEALTHCHECKS_UUID", HC_UUID)
    monkeypatch.setenv("IFETCH_HEALTHCHECKS_BASE_URL", "https://hc.lan/ping")
    monkeypatch.setenv("IFETCH_HEALTHCHECKS_ANOMALY_FAILS", "yes")
    monkeypatch.setenv("IFETCH_NTFY_TOPIC", "icloud")
    monkeypatch.setenv("IFETCH_NTFY_SERVER", "http://ntfy.lan")
    monkeypatch.setenv("IFETCH_NTFY_TOKEN", NTFY_TOKEN)
    monkeypatch.setenv("IFETCH_NTFY_TAGS", "nas, backup")
    monkeypatch.setenv("IFETCH_WEBHOOK_URL", WEBHOOK_URL)
    monkeypatch.setenv("IFETCH_NOTIFY_TIMEOUT", "2.5")
    monkeypatch.setenv("IFETCH_NOTIFY_RETRIES", "4")

    config = NotifyConfig.from_env()
    assert config.healthchecks_uuid == HC_UUID
    assert config.healthchecks_base_url == "https://hc.lan/ping"
    assert config.healthchecks_anomaly_is_failure is True
    assert config.ntfy_topic == "icloud" and config.ntfy_server == "http://ntfy.lan"
    assert config.ntfy_token == NTFY_TOKEN
    assert config.ntfy_tags == ("nas", "backup")
    assert config.webhook_url == WEBHOOK_URL
    assert config.timeout == 2.5 and config.retries == 4
    assert config.is_configured()


def test_flags_take_precedence_over_environment(monkeypatch):
    monkeypatch.setenv("IFETCH_HEALTHCHECKS_URL", HC_URL)
    monkeypatch.setenv("IFETCH_NOTIFY_TIMEOUT", "9")
    args = _parse_flags(["--healthchecks-url", SELF_HOSTED_HC, "--notify-timeout", "1.5"])
    config = NotifyConfig.from_args(args)
    assert config.healthchecks_url == SELF_HOSTED_HC
    assert config.timeout == 1.5


def test_environment_is_used_when_no_flag_is_given(monkeypatch):
    monkeypatch.setenv("IFETCH_NTFY_URL", NTFY_URL)
    config = NotifyConfig.from_args(_parse_flags([]))
    assert config.ntfy_url == NTFY_URL


def test_no_notify_flag_beats_a_configured_environment(monkeypatch):
    monkeypatch.setenv("IFETCH_HEALTHCHECKS_URL", HC_URL)
    config = NotifyConfig.from_args(_parse_flags(["--no-notify"]))
    assert config.enabled is False and config.is_configured() is False


def test_disable_environment_variable(monkeypatch):
    monkeypatch.setenv("IFETCH_HEALTHCHECKS_URL", HC_URL)
    monkeypatch.setenv("IFETCH_NOTIFY_DISABLED", "1")
    assert NotifyConfig.from_env().is_configured() is False


def test_unconfigured_is_a_silent_no_op(caplog):
    with caplog.at_level(logging.DEBUG, logger="ifetch.notify"):
        notifier = build_notifier(_parse_flags([]), env={})
        assert isinstance(notifier, NullNotifier)
        assert notifier.enabled is False
        assert notifier.start() == []
        assert notifier.run_finished({"summary": {"failed": 2}}) == []
    assert [r for r in caplog.records if r.levelno >= logging.WARNING] == []


def test_build_notifier_from_flags_only():
    notifier = build_notifier(_parse_flags(["--ntfy-url", NTFY_URL]), env={})
    assert [b.name for b in notifier.backends] == ["ntfy"]


def test_webhook_headers_from_json_environment(monkeypatch):
    monkeypatch.setenv("IFETCH_WEBHOOK_URL", WEBHOOK_URL)
    monkeypatch.setenv("IFETCH_WEBHOOK_HEADERS", '{"X-Api-Key": "abc1234567"}')
    assert NotifyConfig.from_env().webhook_headers == {"X-Api-Key": "abc1234567"}


def test_webhook_headers_from_repeated_flags():
    args = _parse_flags(
        [
            "--webhook-url",
            WEBHOOK_URL,
            "--webhook-header",
            "X-Api-Key: abc1234567",
            "--webhook-header",
            "X-Env: prod",
        ]
    )
    assert NotifyConfig.from_args(args, env={}).webhook_headers == {
        "X-Api-Key": "abc1234567",
        "X-Env": "prod",
    }


def test_malformed_webhook_headers_are_dropped_not_guessed(caplog, monkeypatch):
    monkeypatch.setenv("IFETCH_WEBHOOK_HEADERS", "not-json-and-no-colon")
    with caplog.at_level(logging.WARNING, logger="ifetch.notify"):
        assert NotifyConfig.from_env().webhook_headers == {}
    monkeypatch.setenv("IFETCH_WEBHOOK_HEADERS", "{oops")
    assert NotifyConfig.from_env().webhook_headers == {}


def test_unparseable_numeric_setting_falls_back_to_the_default(monkeypatch, caplog):
    monkeypatch.setenv("IFETCH_NOTIFY_TIMEOUT", "soon")
    with caplog.at_level(logging.WARNING, logger="ifetch.notify"):
        config = NotifyConfig.from_env()
    assert config.timeout == 5.0


def test_config_from_env_ignores_blank_values(monkeypatch):
    monkeypatch.setenv("IFETCH_HEALTHCHECKS_URL", "   ")
    assert NotifyConfig.from_env().is_configured() is False


def _parse_flags(argv):
    import argparse

    parser = argparse.ArgumentParser()
    add_notification_arguments(parser)
    return parser.parse_args(argv)


def test_add_notification_arguments_mentions_env_vars():
    import argparse

    parser = argparse.ArgumentParser()
    add_notification_arguments(parser)
    help_text = parser.format_help()
    for name in ("IFETCH_HEALTHCHECKS_URL", "IFETCH_NTFY_URL", "IFETCH_WEBHOOK_URL"):
        assert name in help_text


# ---------------------------------------------------------------------------
# Run lifecycle helpers
# ---------------------------------------------------------------------------

def test_run_finished_sends_success_only_for_a_clean_run():
    session = RecordingSession()
    notifier = Notifier.from_config(
        NotifyConfig(healthchecks_url=HC_URL), transport=transport(session)
    )
    notifier.run_finished({"summary": {"total_files": 5, "successful": 5, "failed": 0}})
    assert session.urls == [HC_URL]


def test_run_finished_adds_an_anomaly_when_files_failed():
    session = RecordingSession()
    notifier = Notifier.from_config(
        NotifyConfig(healthchecks_url=HC_URL), transport=transport(session)
    )
    notifier.run_finished({"summary": {"total_files": 5, "successful": 3, "failed": 2}})
    assert session.urls == [HC_URL, HC_URL + "/log"]
    assert "2 file(s) failed to download" in session.body(1)


def test_run_finished_reports_a_crash_as_failure_not_anomaly():
    session = RecordingSession()
    notifier = Notifier.from_config(
        NotifyConfig(healthchecks_url=HC_URL), transport=transport(session)
    )
    notifier.run_finished(None, error=RuntimeError("iCloud Drive service not available"))
    assert session.urls == [HC_URL + "/fail"]
    assert "iCloud Drive service not available" in session.body(0)


def test_run_finished_carries_external_findings():
    session = RecordingSession()
    notifier = Notifier.from_config(
        NotifyConfig(healthchecks_url=HC_URL), transport=transport(session)
    )
    notifier.run_finished(
        {"summary": {"failed": 0}}, findings=["4 placeholder files found by ifetch-recover"]
    )
    assert session.urls == [HC_URL, HC_URL + "/log"]
    assert "placeholder files" in session.body(1)


def test_anomaly_is_distinguishable_from_failure_on_every_backend():
    hc, ntfy, hook = RecordingSession(), RecordingSession(), RecordingSession()
    notifier = Notifier(
        [
            hc_backend(hc),
            NtfyBackend(url=NTFY_URL, transport=transport(ntfy)),
            WebhookBackend(url=WEBHOOK_URL, transport=transport(hook)),
        ]
    )
    notifier.anomaly(findings=["1 file failed"])
    notifier.failure(RuntimeError("died"))
    assert hc.urls == [HC_URL + "/log", HC_URL + "/fail"]
    assert ntfy.calls[0]["headers"]["Priority"] != ntfy.calls[1]["headers"]["Priority"]
    assert hook.calls[0]["json"]["event"] == EVENT_ANOMALY
    assert hook.calls[1]["json"]["event"] == EVENT_FAILURE


def test_start_then_finish_share_a_run_id():
    session = RecordingSession()
    notifier = Notifier.from_config(
        NotifyConfig(webhook_url=WEBHOOK_URL), transport=transport(session)
    )
    notifier.start()
    notifier.success(report={"summary": {"failed": 0}})
    assert session.calls[0]["json"]["run_id"] == session.calls[1]["json"]["run_id"]


def test_duration_is_reported_after_start():
    clock = FakeClock()
    session = RecordingSession()
    notifier = Notifier.from_config(
        NotifyConfig(webhook_url=WEBHOOK_URL),
        transport=transport(session, clock),
        clock=clock.time,
    )
    notifier.start()
    clock.now += 125
    notifier.success(report={"summary": {"failed": 0}})
    assert session.calls[1]["json"]["details"]["duration"] == "2m 5s"


def test_format_run_summary_is_actionable():
    text = format_run_summary(
        {
            "summary": {
                "total_files": 120,
                "successful": 4,
                "failed": 1,
                "skipped": 115,
                "total_bytes_transferred": 5 * 1024 * 1024,
            }
        }
    )
    assert "120 files seen" in text and "1 failed" in text and "5.0 MB transferred" in text


def test_format_run_summary_tolerates_rubbish():
    assert format_run_summary(None) == ""
    assert format_run_summary({}) == ""
    assert format_run_summary({"summary": "nope"}) == ""


def test_anomalies_from_report_only_claims_what_it_can_prove():
    assert anomalies_from_report({"summary": {"failed": 0}}) == []
    assert anomalies_from_report({"summary": {"failed": "3"}}) == [
        "3 file(s) failed to download"
    ]
    assert anomalies_from_report({"summary": {"failed": "many"}}) == []
    assert anomalies_from_report(None) == []


def test_null_notifier_is_a_working_no_op():
    notifier = NullNotifier()
    assert notifier.enabled is False
    assert notifier.notify(event(EVENT_FAILURE)) == []
    assert notifier.describe() == []
    assert notifier.report_snapshot()["deliveries"] == []


def test_delivery_result_round_trips_to_json():
    result = DeliveryResult(backend="ntfy", event=EVENT_SUCCESS, delivered=True, attempts=1)
    json.dumps(result.to_dict())


def test_run_event_body_text_includes_host_and_run():
    text = event(EVENT_SUCCESS, message="done", details={"total_files": 3}).body_text()
    assert "done" in text and "total files: 3" in text and "host:" in text


# ---------------------------------------------------------------------------
# Packaging files
# ---------------------------------------------------------------------------

def test_dockerfile_is_multistage_non_root_and_has_no_build_toolchain():
    text = (REPO_ROOT / "Dockerfile").read_text(encoding="utf-8")
    froms = [line for line in text.splitlines() if line.strip().upper().startswith("FROM ")]
    assert len(froms) >= 2, "expected a multi-stage build"
    assert "USER ifetch" in text
    assert "WORKDIR" in text
    runtime = text.split(froms[-1])[-1]
    assert "build-essential" not in runtime and "gcc" not in runtime


def test_dockerignore_excludes_the_heavy_directories():
    text = (REPO_ROOT / ".dockerignore").read_text(encoding="utf-8")
    for pattern in (".git", "__pycache__", "tests", "learn"):
        assert pattern in text


def test_compose_and_workflow_are_valid_yaml():
    yaml = pytest.importorskip(
        "yaml", reason="PyYAML is not a dependency of iFetch; skipping YAML validation"
    )
    compose = yaml.safe_load((REPO_ROOT / "docker-compose.yml").read_text(encoding="utf-8"))
    service = compose["services"]["ifetch"]
    assert any("/data" in str(v) for v in service["volumes"])
    assert any("/config" in str(v) for v in service["volumes"])
    assert service["environment"]["TMPDIR"] == "/config"

    workflow_text = (REPO_ROOT / ".github/workflows/docker.yml").read_text(encoding="utf-8")
    workflow = yaml.safe_load(workflow_text)
    # PyYAML parses a bare `on:` key as the boolean True (YAML 1.1).
    triggers = workflow.get("on", workflow.get(True))
    assert "tags" in triggers["push"]
    assert "ghcr.io" in workflow_text
    steps = workflow["jobs"]["build"]["steps"]
    assert any("cache-from" in str(step.get("with", {})) for step in steps)


def test_compose_mentions_the_notification_env_vars():
    text = (REPO_ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    assert "IFETCH_HEALTHCHECKS_URL" in text
    assert "IFETCH_NTFY_URL" in text


def test_monitoring_doc_documents_every_environment_variable():
    text = (REPO_ROOT / "docs/monitoring.md").read_text(encoding="utf-8")
    missing = [name for name in NOTIFY_ENV_VARS if name not in text]
    assert missing == []
