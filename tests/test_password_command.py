"""Tests for sourcing the Apple ID password from a command.

Why this exists: a system keyring is exactly what Docker containers, systemd
units and NAS boxes do not have. Without this, the password stays the one part
of authentication that still needs a human, which undoes the rest of the
headless work.

The contract:

* not configuring it returns ``None`` so the keyring remains the default;
* a quoted path containing spaces works (the failure mode reported against
  other tools that split on whitespace);
* the command runs **without a shell**, so it is not an injection vector;
* every failure is reported with a cause, and the password never appears in an
  error message or a log line.
"""

import os
import stat
import sys
from pathlib import Path

import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))

from ifetch.auth import PasswordCommandError, resolve_password  # noqa: E402

SECRET = "hunter2-correct-horse"


@pytest.fixture
def script(tmp_path):
    """Build an executable helper script, optionally at a path with spaces."""

    def build(body, name="get-password.sh", directory=None):
        target = (directory or tmp_path) / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text("#!/bin/sh\n" + body + "\n")
        target.chmod(target.stat().st_mode | stat.S_IXUSR)
        return target

    return build


class TestNotConfigured:
    def test_no_command_returns_none_so_the_keyring_is_used(self):
        assert resolve_password(None, env={}) is None

    def test_empty_and_whitespace_commands_are_treated_as_unset(self):
        assert resolve_password("", env={}) is None
        assert resolve_password("   ", env={}) is None

    def test_environment_variable_is_honoured(self, script):
        path = script(f'echo "{SECRET}"')
        assert resolve_password(
            None, env={"IFETCH_PASSWORD_COMMAND": str(path)}
        ) == SECRET

    def test_explicit_argument_beats_the_environment(self, script):
        chosen = script('echo "from-argument"', name="a.sh")
        other = script('echo "from-env"', name="b.sh")
        result = resolve_password(
            str(chosen), env={"IFETCH_PASSWORD_COMMAND": str(other)}
        )
        assert result == "from-argument"


class TestSuccess:
    def test_reads_the_password_from_stdout(self, script):
        assert resolve_password(str(script(f'echo "{SECRET}"')), env={}) == SECRET

    def test_trailing_newline_is_stripped(self, script):
        assert resolve_password(str(script(f'printf "{SECRET}\\n"')), env={}) == SECRET

    def test_no_trailing_newline_still_works(self, script):
        assert resolve_password(str(script(f'printf "{SECRET}"')), env={}) == SECRET

    def test_only_the_first_line_is_used(self, script):
        """A helper may print diagnostics after the secret."""
        path = script(f'echo "{SECRET}"\necho "note: unlocked keychain"')
        assert resolve_password(str(path), env={}) == SECRET

    def test_surrounding_whitespace_is_stripped(self, script):
        assert resolve_password(str(script(f'echo "  {SECRET}  "')), env={}) == SECRET

    def test_arguments_are_passed_through(self, script):
        path = script('echo "$1-$2"')
        assert resolve_password(f'{path} alpha beta', env={}) == "alpha-beta"

    def test_a_password_containing_spaces_survives(self, script):
        path = script('echo "two words here"')
        assert resolve_password(str(path), env={}) == "two words here"


class TestPathsWithSpaces:
    """The exact bug reported against rclone's equivalent flag (#9596)."""

    def test_a_quoted_path_containing_spaces_works(self, script, tmp_path):
        directory = tmp_path / "my secrets dir"
        path = script(f'echo "{SECRET}"', directory=directory)
        assert " " in str(path)

        assert resolve_password(f'"{path}"', env={}) == SECRET

    def test_a_quoted_path_with_spaces_plus_arguments(self, script, tmp_path):
        directory = tmp_path / "my secrets dir"
        path = script('echo "$1"', directory=directory)
        assert resolve_password(f'"{path}" {SECRET}', env={}) == SECRET

    def test_an_unquoted_path_with_spaces_fails_with_a_useful_message(
        self, script, tmp_path
    ):
        """It cannot work, but the error must say how to fix it."""
        directory = tmp_path / "my secrets dir"
        path = script(f'echo "{SECRET}"', directory=directory)

        with pytest.raises(PasswordCommandError) as excinfo:
            resolve_password(str(path), env={})
        assert "not found" in str(excinfo.value)


class TestFailures:
    def test_a_missing_command_names_the_binary(self):
        with pytest.raises(PasswordCommandError) as excinfo:
            resolve_password("/nonexistent/definitely-not-here", env={})
        assert "not found" in str(excinfo.value)

    def test_a_nonzero_exit_is_reported_with_stderr(self, script):
        path = script('echo "keychain is locked" >&2\nexit 3')
        with pytest.raises(PasswordCommandError) as excinfo:
            resolve_password(str(path), env={})
        message = str(excinfo.value)
        assert "exited 3" in message
        assert "keychain is locked" in message

    def test_empty_output_is_an_error_not_an_empty_password(self, script):
        """An empty password would produce a confusing auth failure later."""
        with pytest.raises(PasswordCommandError) as excinfo:
            resolve_password(str(script("exit 0")), env={})
        assert "no output" in str(excinfo.value)

    def test_whitespace_only_output_is_an_error(self, script):
        with pytest.raises(PasswordCommandError):
            resolve_password(str(script('echo "   "')), env={})

    def test_unbalanced_quotes_explain_how_to_quote(self, script):
        with pytest.raises(PasswordCommandError) as excinfo:
            resolve_password('"/unterminated/path', env={})
        assert "Quote paths" in str(excinfo.value)

    def test_a_hanging_command_times_out_with_an_explanation(self, script):
        path = script("sleep 30")
        with pytest.raises(PasswordCommandError) as excinfo:
            resolve_password(str(path), env={}, timeout=0.5)
        assert "timed out" in str(excinfo.value)
        assert "print the password and exit" in str(excinfo.value)


class TestSecrecyAndSafety:
    def test_the_password_never_appears_in_a_failure_message(self, script):
        """stdout may hold the secret; a nonzero exit must not echo it."""
        path = script(f'echo "{SECRET}"\necho "failed" >&2\nexit 1')
        with pytest.raises(PasswordCommandError) as excinfo:
            resolve_password(str(path), env={})
        assert SECRET not in str(excinfo.value)

    def test_no_shell_is_used_so_metacharacters_are_not_interpreted(self, tmp_path):
        """If a shell ran this, the `touch` would execute. It must not.

        Without a shell the whole tail is passed to `echo` as literal argv, so
        the call *succeeds* and returns nonsense - the point is only that the
        second command never runs.
        """
        canary = tmp_path / "pwned"
        result = resolve_password(f"/bin/echo hi; touch {canary}", env={})

        assert not canary.exists(), "a shell interpreted the command separator"
        assert result == f"hi; touch {canary}"

    def test_a_semicolon_is_passed_as_a_literal_argument(self, script):
        path = script('echo "$1"')
        assert resolve_password(f'{path} ";rm -rf /"', env={}) == ";rm -rf /"

    def test_injected_runner_is_used_when_supplied(self):
        """The seam that keeps the rest of the suite from spawning processes."""
        seen = {}

        def runner(argv):
            seen["argv"] = argv
            return SECRET

        assert resolve_password("my-helper --flag", env={}, runner=runner) == SECRET
        assert seen["argv"] == ["my-helper", "--flag"]


class TestDownloadManagerIntegration:
    def test_the_password_reaches_pyicloud(self, monkeypatch):
        from ifetch.downloader import DownloadManager

        captured = {}

        class FakeService:
            requires_2fa = False
            requires_2sa = False
            is_trusted_session = True

            def __init__(self, **kwargs):
                captured.update(kwargs)

        monkeypatch.setattr("ifetch.downloader.PyiCloudService", FakeService)

        mgr = DownloadManager(email="you@example.com", password=SECRET)
        mgr.authenticate()

        assert captured["password"] == SECRET
        assert captured["apple_id"] == "you@example.com"

    def test_without_a_password_none_is_passed_so_the_keyring_is_used(self, monkeypatch):
        from ifetch.downloader import DownloadManager

        captured = {}

        class FakeService:
            requires_2fa = False
            requires_2sa = False
            is_trusted_session = True

            def __init__(self, **kwargs):
                captured.update(kwargs)

        monkeypatch.setattr("ifetch.downloader.PyiCloudService", FakeService)

        DownloadManager(email="you@example.com").authenticate()

        assert captured["password"] is None
