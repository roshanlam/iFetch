import sys
from pathlib import Path

import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))

from ifetch import mirror_cli  # noqa: E402
from ifetch.mirror import MirrorResult  # noqa: E402


class _FakePipeline:
    instances = []

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.run_calls = []
        self.results = [MirrorResult(cycle=1)]
        type(self).instances.append(self)

    def run_once(self, cycle=1):
        self.run_calls.append(cycle)
        outcome = self.results[min(len(self.run_calls), len(self.results)) - 1]
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome


@pytest.fixture(autouse=True)
def _patch_pipeline(monkeypatch):
    _FakePipeline.instances = []
    monkeypatch.setattr(mirror_cli, "MirrorPipeline", _FakePipeline)


def test_help_exits_zero(capsys):
    with pytest.raises(SystemExit) as exc:
        mirror_cli.main(["--help"])

    assert exc.value.code == 0
    out = capsys.readouterr().out
    assert "--gdrive-folder" in out
    assert "--watch" in out


def test_missing_positional_args_exit_nonzero():
    with pytest.raises(SystemExit) as exc:
        mirror_cli.main([])

    assert exc.value.code == 2


def test_single_shot_success_returns_zero():
    code = mirror_cli.main(["Documents", "/nas/mirror", "--email", "user@example.com"])

    assert code == 0
    pipeline = _FakePipeline.instances[0]
    assert pipeline.run_calls == [1]
    assert pipeline.kwargs["icloud_path"] == "Documents"
    assert pipeline.kwargs["local_path"] == "/nas/mirror"
    assert pipeline.kwargs["email"] == "user@example.com"
    assert pipeline.kwargs["gdrive_folder"] is None
    assert pipeline.kwargs["dry_run"] is False


def test_args_passed_through_to_pipeline():
    code = mirror_cli.main([
        "Documents", "/nas/mirror",
        "--gdrive-folder", "iCloud Mirror",
        "--email", "user@example.com",
        "--max-workers", "8",
        "--max-retries", "6",
        "--chunk-size", "2048",
        "--log-file", "/tmp/mirror.log",
        "--credentials", "/creds.json",
        "--token", "/token.pickle",
        "--dry-run",
    ])

    assert code == 0
    kwargs = _FakePipeline.instances[0].kwargs
    assert kwargs["gdrive_folder"] == "iCloud Mirror"
    assert kwargs["max_workers"] == 8
    assert kwargs["max_retries"] == 6
    assert kwargs["chunk_size"] == 2048
    assert kwargs["log_file"] == "/tmp/mirror.log"
    assert kwargs["credentials_file"] == "/creds.json"
    assert kwargs["token_file"] == "/token.pickle"
    assert kwargs["dry_run"] is True


def test_profile_patterns_passed_to_pipeline(tmp_path):
    profile_file = tmp_path / "profiles.json"
    profile_file.write_text('{"docs": {"include": ["*.pdf"], "exclude": ["Archive/*"]}}')

    code = mirror_cli.main([
        "Documents", "/nas/mirror",
        "--profile", "docs",
        "--profile-file", str(profile_file),
    ])

    assert code == 0
    kwargs = _FakePipeline.instances[0].kwargs
    assert kwargs["include_patterns"] == ["*.pdf"]
    assert kwargs["exclude_patterns"] == ["Archive/*"]


def test_missing_profile_returns_one(tmp_path, capsys):
    code = mirror_cli.main([
        "Documents", "/nas/mirror",
        "--profile", "docs",
        "--profile-file", str(tmp_path / "missing.json"),
    ])

    assert code == 1
    assert "Error" in capsys.readouterr().err


def test_single_shot_failure_returns_one(monkeypatch):
    class _FailingPipeline(_FakePipeline):
        def __init__(self, **kwargs):
            super().__init__(**kwargs)
            self.results = [MirrorResult(cycle=1, download_failed=2)]

    monkeypatch.setattr(mirror_cli, "MirrorPipeline", _FailingPipeline)

    code = mirror_cli.main(["Documents", "/nas/mirror"])

    assert code == 1


def test_single_shot_errors_printed_to_stderr(monkeypatch, capsys):
    failure = MirrorResult(cycle=1, errors=["stage 1 (iCloud -> local): boom"])

    class _FailingPipeline(_FakePipeline):
        def __init__(self, **kwargs):
            super().__init__(**kwargs)
            self.results = [failure]

    monkeypatch.setattr(mirror_cli, "MirrorPipeline", _FailingPipeline)

    code = mirror_cli.main(["Documents", "/nas/mirror"])

    assert code == 1
    assert "boom" in capsys.readouterr().err


def test_watch_loop_runs_cycles_until_interrupt(monkeypatch, capsys):
    class _WatchPipeline(_FakePipeline):
        def __init__(self, **kwargs):
            super().__init__(**kwargs)
            self.results = [
                MirrorResult(cycle=1, downloaded=3),
                MirrorResult(cycle=2, downloaded=1),
                KeyboardInterrupt(),
            ]

    monkeypatch.setattr(mirror_cli, "MirrorPipeline", _WatchPipeline)
    sleeps = []
    monkeypatch.setattr(mirror_cli.time, "sleep", lambda s: sleeps.append(s))

    code = mirror_cli.main(["Documents", "/nas/mirror", "--watch", "30"])

    assert code == 0
    pipeline = _WatchPipeline.instances[0]
    assert pipeline.run_calls == [1, 2, 3]
    assert sleeps == [30.0, 30.0]
    out = capsys.readouterr().out
    assert "[cycle 1]" in out
    assert "[cycle 2]" in out
    assert "stopped by user" in out


def test_watch_loop_contains_per_cycle_exceptions(monkeypatch, capsys):
    class _FlakyPipeline(_FakePipeline):
        def __init__(self, **kwargs):
            super().__init__(**kwargs)
            self.results = [
                MirrorResult(cycle=1, downloaded=3),
                RuntimeError("transient network blip"),
                MirrorResult(cycle=3, downloaded=2),
                KeyboardInterrupt(),
            ]

    monkeypatch.setattr(mirror_cli, "MirrorPipeline", _FlakyPipeline)
    monkeypatch.setattr(mirror_cli.time, "sleep", lambda s: None)

    code = mirror_cli.main(["Documents", "/nas/mirror", "--watch", "5"])

    assert code == 0
    pipeline = _FlakyPipeline.instances[0]
    # The RuntimeError in cycle 2 did not kill the loop; cycles 3 and 4 ran.
    assert pipeline.run_calls == [1, 2, 3, 4]
    captured = capsys.readouterr()
    assert "transient network blip" in captured.err
    assert "[cycle 3]" in captured.out


def test_watch_interrupt_during_sleep_stops_cleanly(monkeypatch, capsys):
    monkeypatch.setattr(
        mirror_cli.time, "sleep",
        lambda s: (_ for _ in ()).throw(KeyboardInterrupt()),
    )

    code = mirror_cli.main(["Documents", "/nas/mirror", "--watch", "60"])

    assert code == 0
    assert _FakePipeline.instances[0].run_calls == [1]
    assert "stopped by user" in capsys.readouterr().out


def test_single_shot_keyboard_interrupt_returns_130(monkeypatch):
    class _InterruptedPipeline(_FakePipeline):
        def __init__(self, **kwargs):
            super().__init__(**kwargs)
            self.results = [KeyboardInterrupt()]

    monkeypatch.setattr(mirror_cli, "MirrorPipeline", _InterruptedPipeline)

    code = mirror_cli.main(["Documents", "/nas/mirror"])

    assert code == 130
