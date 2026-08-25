"""CLI-level tests for ``ifetch auth`` and the offline ``ifetch-verify`` mode.

These exercise the commands the way an operator and a cron job do: through
argv, checking stdout and - critically - **exit codes**, because a scheduled
job's only real interface is the exit status.

The contract:

* ``doctor`` exits 0 healthy / 1 renew-soon / 2 broken-now, so a cron job can
  tell "act eventually" from "act now";
* every command has a ``--json`` form that is machine-readable;
* ``renew`` never blocks on a terminal when a code source is configured;
* ``ifetch-verify --offline`` audits a mirror with no credentials at all.
"""

import io
import json
import os
import sys
from pathlib import Path

import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))

from ifetch.auth_cli import main as auth_main  # noqa: E402
from ifetch.manifest import MANIFEST_FILENAME, Manifest  # noqa: E402
from ifetch.verify import main as verify_main  # noqa: E402

from test_auth import write_cookiejar, write_session  # noqa: E402
from ifetch.auth import session_slug  # noqa: E402

ACCOUNT = "you@example.com"
DAY = 86400.0


def snapshot_expiring_in(directory, days, account=ACCOUNT):
    """Write a session that expires ``days`` from *now*.

    The CLI reads the real clock, so these fixtures must be anchored to it -
    a fixed epoch would drift into meaninglessness as time passes.
    """
    import time

    slug = session_slug(account)
    write_session(directory / f"{slug}.session")
    write_cookiejar(directory / f"{slug}.cookiejar", time.time() + days * DAY)


def run_auth(argv, capture=True):
    """Run the auth CLI, returning ``(exit_code, stdout_text)``."""
    buffer = io.StringIO()
    code = auth_main(argv, stdout=buffer if capture else None)
    return code, buffer.getvalue()


class FakeService:
    def __init__(self, requires_2fa=False, trusted=True, validate=True, trust=True):
        self.requires_2fa = requires_2fa
        self.is_trusted_session = trusted
        self._validate = validate
        self._trust = trust
        self.validated_with = None

    def validate_2fa_code(self, code):
        self.validated_with = code
        if self._validate:
            self.is_trusted_session = self._trust
        return self._validate

    def trust_session(self):
        self.is_trusted_session = self._trust
        return self._trust


# ---------------------------------------------------------------------------
# doctor
# ---------------------------------------------------------------------------

class TestDoctorCommand:
    def test_healthy_session_exits_zero(self, tmp_path):
        snapshot_expiring_in(tmp_path, 20)
        code, out = run_auth(
            ["doctor", "--email", ACCOUNT, "--cookie-directory", str(tmp_path)]
        )
        assert code == 0
        assert "Overall: ok" in out

    def test_missing_session_exits_two_and_says_what_to_run(self, tmp_path):
        code, out = run_auth(
            ["doctor", "--email", ACCOUNT, "--cookie-directory", str(tmp_path)]
        )
        assert code == 2
        assert "ifetch auth renew" in out

    def test_expiring_session_exits_one(self, tmp_path):
        """Distinct from 2: a cron job should renew, not alert a human."""
        snapshot_expiring_in(tmp_path, 2)
        code, _ = run_auth(
            ["doctor", "--email", ACCOUNT, "--cookie-directory", str(tmp_path)]
        )
        assert code == 1

    def test_warn_days_widens_the_window(self, tmp_path):
        snapshot_expiring_in(tmp_path, 12)
        healthy, _ = run_auth(
            ["doctor", "--email", ACCOUNT, "--cookie-directory", str(tmp_path)]
        )
        warned, _ = run_auth(
            ["doctor", "--email", ACCOUNT, "--cookie-directory", str(tmp_path),
             "--warn-days", "20"]
        )
        assert healthy == 0 and warned == 1

    def test_json_output_is_parseable_and_complete(self, tmp_path):
        snapshot_expiring_in(tmp_path, 20)
        code, out = run_auth(
            ["doctor", "--email", ACCOUNT, "--cookie-directory", str(tmp_path), "--json"]
        )
        payload = json.loads(out)
        assert payload["account"] == ACCOUNT
        assert payload["region"] == "global"
        names = {c["name"] for c in payload["checks"]}
        assert {"region", "stored_session", "session_expiry"} <= names

    def test_email_falls_back_to_the_environment(self, tmp_path, monkeypatch):
        monkeypatch.setenv("ICLOUD_EMAIL", ACCOUNT)
        snapshot_expiring_in(tmp_path, 20)
        code, out = run_auth(["doctor", "--cookie-directory", str(tmp_path)])
        assert code == 0
        assert ACCOUNT in out

    def test_missing_email_is_a_usage_error(self, tmp_path, monkeypatch, capsys):
        monkeypatch.delenv("ICLOUD_EMAIL", raising=False)
        code, _ = run_auth(["doctor", "--cookie-directory", str(tmp_path)])
        assert code == 64
        assert "ICLOUD_EMAIL" in capsys.readouterr().err

    def test_region_china_is_reported(self, tmp_path):
        snapshot_expiring_in(tmp_path, 20)
        _, out = run_auth(
            ["doctor", "--email", ACCOUNT, "--cookie-directory", str(tmp_path),
             "--region", "china"]
        )
        assert "iCloud.com.cn" in out

    def test_invalid_region_is_rejected_by_argparse(self, tmp_path):
        with pytest.raises(SystemExit):
            run_auth(
                ["doctor", "--email", ACCOUNT, "--cookie-directory", str(tmp_path),
                 "--region", "atlantis"]
            )

    def test_bad_region_from_the_environment_is_a_usage_error(self, tmp_path, monkeypatch, capsys):
        monkeypatch.setenv("ICLOUD_REGION", "atlantis")
        code, _ = run_auth(
            ["doctor", "--email", ACCOUNT, "--cookie-directory", str(tmp_path)]
        )
        assert code == 64
        assert "atlantis" in capsys.readouterr().err

    def test_corrupt_session_does_not_crash_the_doctor(self, tmp_path):
        """A diagnostic that dies on bad input is useless when it is needed."""
        slug = session_slug(ACCOUNT)
        (tmp_path / f"{slug}.session").write_text("{{{corrupt")
        (tmp_path / f"{slug}.cookiejar").write_text("also corrupt")

        code, out = run_auth(
            ["doctor", "--email", ACCOUNT, "--cookie-directory", str(tmp_path)]
        )
        assert code in (1, 2)
        assert "renew" in out


# ---------------------------------------------------------------------------
# status
# ---------------------------------------------------------------------------

class TestStatusCommand:
    def test_healthy_session_prints_one_line_and_exits_zero(self, tmp_path):
        snapshot_expiring_in(tmp_path, 20)
        code, out = run_auth(
            ["status", "--email", ACCOUNT, "--cookie-directory", str(tmp_path)]
        )
        assert code == 0
        assert len(out.strip().splitlines()) == 1
        assert ACCOUNT in out

    def test_expiring_session_exits_one(self, tmp_path):
        snapshot_expiring_in(tmp_path, 1)
        code, _ = run_auth(
            ["status", "--email", ACCOUNT, "--cookie-directory", str(tmp_path)]
        )
        assert code == 1

    def test_expired_session_exits_two(self, tmp_path):
        snapshot_expiring_in(tmp_path, -3)
        code, out = run_auth(
            ["status", "--email", ACCOUNT, "--cookie-directory", str(tmp_path)]
        )
        assert code == 2
        assert "expired" in out.lower()

    def test_no_session_exits_one_not_zero(self, tmp_path):
        """Unknown must never be reported as healthy."""
        code, _ = run_auth(
            ["status", "--email", ACCOUNT, "--cookie-directory", str(tmp_path)]
        )
        assert code == 1

    def test_json_output(self, tmp_path):
        snapshot_expiring_in(tmp_path, 20)
        _, out = run_auth(
            ["status", "--email", ACCOUNT, "--cookie-directory", str(tmp_path), "--json"]
        )
        payload = json.loads(out)
        assert payload["status"] == "ok"
        assert payload["days_remaining"] > 19


# ---------------------------------------------------------------------------
# renew
# ---------------------------------------------------------------------------

def run_renew(argv, factory, stdout=None):
    import argparse

    from ifetch.auth_cli import build_parser, cmd_renew

    args = build_parser().parse_args(["renew"] + argv)
    buffer = stdout if stdout is not None else io.StringIO()
    code = cmd_renew(args, buffer, service_factory=factory)
    return code, buffer.getvalue()


class TestRenewCommand:
    def test_already_trusted_session_renews_without_a_code(self, tmp_path):
        snapshot_expiring_in(tmp_path, 20)
        code, out = run_renew(
            ["--email", ACCOUNT, "--cookie-directory", str(tmp_path)],
            factory=lambda **kw: FakeService(),
        )
        assert code == 0
        assert "renewed" in out

    def test_a_code_from_the_command_line_is_submitted(self, tmp_path):
        snapshot_expiring_in(tmp_path, 20)
        service = FakeService(requires_2fa=True)
        code, out = run_renew(
            ["--email", ACCOUNT, "--cookie-directory", str(tmp_path),
             "--2fa-code", "123456", "--no-stdin"],
            factory=lambda **kw: service,
        )
        assert code == 0
        assert service.validated_with == "123456"

    def test_a_code_from_a_file_is_submitted(self, tmp_path):
        snapshot_expiring_in(tmp_path, 20)
        code_file = tmp_path / "code.txt"
        code_file.write_text("Your code is 424242")
        service = FakeService(requires_2fa=True)

        code, _ = run_renew(
            ["--email", ACCOUNT, "--cookie-directory", str(tmp_path),
             "--2fa-file", str(code_file), "--no-stdin"],
            factory=lambda **kw: service,
        )
        assert code == 0
        assert service.validated_with == "424242"

    def test_no_code_available_fails_cleanly_rather_than_hanging(self, tmp_path):
        """The headless failure mode: report it, do not block forever."""
        snapshot_expiring_in(tmp_path, 20)
        code, out = run_renew(
            ["--email", ACCOUNT, "--cookie-directory", str(tmp_path), "--no-stdin"],
            factory=lambda **kw: FakeService(requires_2fa=True),
        )
        assert code == 2
        assert "--2fa-code" in out

    def test_a_rejected_code_is_reported(self, tmp_path):
        snapshot_expiring_in(tmp_path, 20)
        code, out = run_renew(
            ["--email", ACCOUNT, "--cookie-directory", str(tmp_path),
             "--2fa-code", "000000", "--no-stdin"],
            factory=lambda **kw: FakeService(requires_2fa=True, validate=False),
        )
        assert code == 2
        assert "rejected" in out.lower() or "409" in out

    def test_an_apple_error_is_classified_not_relayed_raw(self, tmp_path):
        snapshot_expiring_in(tmp_path, 20)

        def factory(**kwargs):
            raise RuntimeError('{"success":false,"error":"Invalid Session Token"}')

        code, out = run_renew(
            ["--email", ACCOUNT, "--cookie-directory", str(tmp_path)], factory=factory
        )
        assert code == 2
        assert "--reset" in out

    def test_reset_removes_the_stored_session_files(self, tmp_path):
        snapshot_expiring_in(tmp_path, 20)
        slug = session_slug(ACCOUNT)
        assert (tmp_path / f"{slug}.session").exists()

        run_renew(
            ["--email", ACCOUNT, "--cookie-directory", str(tmp_path), "--reset"],
            factory=lambda **kw: FakeService(),
        )

        assert not (tmp_path / f"{slug}.session").exists()
        assert not (tmp_path / f"{slug}.cookiejar").exists()

    def test_if_expiring_within_skips_a_healthy_session(self, tmp_path):
        """The cron shape: renew only when it is actually needed."""
        snapshot_expiring_in(tmp_path, 25)
        called = {"n": 0}

        def factory(**kwargs):
            called["n"] += 1
            return FakeService()

        code, out = run_renew(
            ["--email", ACCOUNT, "--cookie-directory", str(tmp_path),
             "--if-expiring-within", "7"],
            factory=factory,
        )
        assert code == 0
        assert called["n"] == 0, "authenticated despite a healthy session"
        assert "Nothing to do" in out

    def test_if_expiring_within_acts_when_the_window_is_reached(self, tmp_path):
        snapshot_expiring_in(tmp_path, 3)
        called = {"n": 0}

        def factory(**kwargs):
            called["n"] += 1
            return FakeService()

        code, _ = run_renew(
            ["--email", ACCOUNT, "--cookie-directory", str(tmp_path),
             "--if-expiring-within", "7"],
            factory=factory,
        )
        assert code == 0
        assert called["n"] == 1

    def test_region_is_threaded_into_the_service(self, tmp_path):
        snapshot_expiring_in(tmp_path, 20)
        captured = {}

        def factory(**kwargs):
            captured.update(kwargs)
            return FakeService()

        run_renew(
            ["--email", ACCOUNT, "--cookie-directory", str(tmp_path), "--region", "china"],
            factory=factory,
        )
        assert captured["china_mainland"] is True

    def test_failure_to_trust_is_reported_not_swallowed(self, tmp_path):
        """A renewal that cannot trust would need a fresh code every single run."""
        snapshot_expiring_in(tmp_path, 20)
        code, out = run_renew(
            ["--email", ACCOUNT, "--cookie-directory", str(tmp_path),
             "--2fa-code", "123456", "--no-stdin"],
            factory=lambda **kw: FakeService(requires_2fa=True, trusted=False, trust=False),
        )
        assert code == 2
        assert "trust" in out.lower()


# ---------------------------------------------------------------------------
# ifetch auth dispatch from the main CLI
# ---------------------------------------------------------------------------

class TestMainCliDispatch:
    def test_ifetch_auth_dispatches_to_the_auth_command_tree(self, tmp_path, capsys):
        from ifetch.cli import main as ifetch_main

        snapshot_expiring_in(tmp_path, 20)
        with pytest.raises(SystemExit) as excinfo:
            ifetch_main(["auth", "status", "--email", ACCOUNT,
                         "--cookie-directory", str(tmp_path)])
        assert excinfo.value.code == 0
        assert ACCOUNT in capsys.readouterr().out


# ---------------------------------------------------------------------------
# ifetch-verify --offline
# ---------------------------------------------------------------------------

KEY = "test-signing-key"


@pytest.fixture
def signed_mirror(tmp_path, monkeypatch):
    root = tmp_path / "mirror"
    root.mkdir()
    (root / "a.txt").write_bytes(b"alpha")
    (root / "b.txt").write_bytes(b"bravo")
    manifest = Manifest(root, key=KEY.encode())
    manifest.record_file(root / "a.txt")
    manifest.record_file(root / "b.txt")
    manifest.save()
    monkeypatch.delenv("IFETCH_MANIFEST_KEY", raising=False)
    return root


class TestOfflineVerify:
    def test_intact_mirror_passes_with_no_credentials(self, signed_mirror, capsys):
        code = verify_main(
            ["--offline", "--sign-key", KEY, "--quiet", "dummy", str(signed_mirror)]
        )
        assert code == 0

    def test_no_icloud_path_is_needed_offline(self, signed_mirror):
        """The whole point: this works years later with no account."""
        code = verify_main(["--offline", "--sign-key", KEY, "--quiet"] + [str(signed_mirror)])
        assert code == 0

    def test_bit_rot_fails_the_audit(self, signed_mirror, capsys):
        (signed_mirror / "a.txt").write_bytes(b"ALPHA")  # same length, different bytes

        code = verify_main(["--offline", "--sign-key", KEY, str(signed_mirror)])

        assert code == 1
        assert "a.txt" in capsys.readouterr().out

    def test_missing_manifest_is_an_operational_error(self, tmp_path, capsys):
        empty = tmp_path / "empty"
        empty.mkdir()
        code = verify_main(["--offline", str(empty)])
        assert code == 2
        assert MANIFEST_FILENAME in capsys.readouterr().err

    def test_require_signature_fails_when_the_signature_was_stripped(self, signed_mirror, capsys):
        """The forged-unsigned-manifest case, caught by policy."""
        payload = json.loads((signed_mirror / MANIFEST_FILENAME).read_text())
        payload.pop("signature")
        (signed_mirror / MANIFEST_FILENAME).write_text(json.dumps(payload))

        lenient = verify_main(["--offline", "--sign-key", KEY, "--quiet", str(signed_mirror)])
        strict = verify_main(
            ["--offline", "--sign-key", KEY, "--require-signature", "--quiet",
             str(signed_mirror)]
        )
        assert lenient == 0
        assert strict == 1
        assert "unsigned" in capsys.readouterr().err

    def test_a_wrong_key_fails_even_when_files_are_intact(self, signed_mirror):
        code = verify_main(
            ["--offline", "--sign-key", "the-wrong-key", "--quiet", str(signed_mirror)]
        )
        assert code == 1

    def test_key_can_come_from_the_environment(self, signed_mirror, monkeypatch):
        monkeypatch.setenv("IFETCH_MANIFEST_KEY", KEY)
        assert verify_main(["--offline", "--quiet", str(signed_mirror)]) == 0

    def test_key_can_come_from_a_file(self, signed_mirror, tmp_path):
        key_file = tmp_path / "key"
        key_file.write_text(KEY)
        code = verify_main(
            ["--offline", "--sign-key-file", str(key_file), "--quiet", str(signed_mirror)]
        )
        assert code == 0

    def test_an_empty_key_file_is_an_operational_error(self, signed_mirror, tmp_path, capsys):
        key_file = tmp_path / "key"
        key_file.write_text("")
        code = verify_main(
            ["--offline", "--sign-key-file", str(key_file), "--quiet", str(signed_mirror)]
        )
        assert code == 2
        assert "empty" in capsys.readouterr().err

    def test_report_is_written_as_json(self, signed_mirror, tmp_path):
        report = tmp_path / "audit.json"
        verify_main(
            ["--offline", "--sign-key", KEY, "--quiet", "--report", str(report),
             str(signed_mirror)]
        )
        payload = json.loads(report.read_text())
        assert payload["signature"] == "valid"
        assert payload["ok"] is True

    def test_online_mode_still_requires_an_icloud_path(self, signed_mirror):
        with pytest.raises(SystemExit):
            verify_main([])
