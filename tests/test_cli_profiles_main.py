import runpy
import sys
import types
from pathlib import Path

import pytest

from ifetch import cli
from ifetch.profiles import ProfileManager


def test_profile_manager_defaults_when_no_profile():
    manager = ProfileManager()

    assert manager.get_patterns() == ([], [])


def test_profile_manager_loads_profile(tmp_path):
    config = tmp_path / "profiles.json"
    config.write_text('{"docs": {"include": ["*.pdf"], "exclude": ["Archive/*"]}}')

    manager = ProfileManager("docs", config_path=config)

    assert manager.get_patterns() == (["*.pdf"], ["Archive/*"])


def test_profile_manager_missing_profile_file(tmp_path):
    with pytest.raises(FileNotFoundError):
        ProfileManager("docs", config_path=tmp_path / "missing.json")


def test_profile_manager_invalid_json(tmp_path):
    config = tmp_path / "profiles.json"
    config.write_text("{invalid")

    with pytest.raises(ValueError):
        ProfileManager("docs", config_path=config)


def test_profile_manager_unknown_profile(tmp_path):
    config = tmp_path / "profiles.json"
    config.write_text('{"docs": {"include": []}}')

    with pytest.raises(KeyError):
        ProfileManager("images", config_path=config)


def test_cli_requires_icloud_path_for_download(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["cli.py", "--email", "user@example.com"])
    monkeypatch.setattr(cli.DownloadManager, "authenticate", lambda self: None)

    with pytest.raises(SystemExit) as exc:
        cli.main()

    assert exc.value.code == 1


def test_cli_handles_keyboard_interrupt(monkeypatch):
    class _FakeDM:
        def __init__(self, *args, **kwargs):
            pass

        def authenticate(self):
            raise KeyboardInterrupt

    monkeypatch.setattr(sys, "argv", ["cli.py", "Documents", "--email", "user@example.com", "--list"])
    monkeypatch.setattr(cli, "DownloadManager", _FakeDM)

    with pytest.raises(SystemExit) as exc:
        cli.main()

    assert exc.value.code == 130


def test_cli_loads_profile_patterns(monkeypatch, tmp_path):
    config = tmp_path / "profiles.json"
    config.write_text('{"docs": {"include": ["*.pdf"], "exclude": ["tmp/*"]}}')
    captured = {}

    class _FakeDM:
        def __init__(self, *args, **kwargs):
            captured["include"] = kwargs["include_patterns"]
            captured["exclude"] = kwargs["exclude_patterns"]

        def authenticate(self):
            pass

        def list_contents(self, path):
            captured["path"] = path

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "cli.py",
            "Documents",
            "--email",
            "user@example.com",
            "--list",
            "--profile",
            "docs",
            "--profile-file",
            str(config),
        ],
    )
    monkeypatch.setattr(cli, "DownloadManager", _FakeDM)

    cli.main()

    assert captured["include"] == ["*.pdf"]
    assert captured["exclude"] == ["tmp/*"]
    assert captured["path"] == "Documents"


def test_cli_passes_skip_existing_flag(monkeypatch, tmp_path):
    captured = {}

    class _FakeDM:
        def __init__(self, *args, **kwargs):
            captured["skip_existing"] = kwargs.get("skip_existing")

        def authenticate(self):
            pass

        def download(self, *args, **kwargs):
            captured["downloaded"] = True

        def generate_summary_report(self):
            return {"summary": {
                "total_files": 0, "successful": 0, "skipped": 0, "failed": 0,
                "total_bytes_transferred": 0, "total_changed_chunks": 0,
            }}

    monkeypatch.setattr(
        sys,
        "argv",
        ["cli.py", "/", str(tmp_path), "--email", "user@example.com", "--skip-existing"],
    )
    monkeypatch.setattr(cli, "DownloadManager", _FakeDM)

    cli.main()

    assert captured["skip_existing"] is True
    assert captured.get("downloaded") is True


def test_cli_skip_existing_defaults_to_false(monkeypatch, tmp_path):
    captured = {}

    class _FakeDM:
        def __init__(self, *args, **kwargs):
            captured["skip_existing"] = kwargs.get("skip_existing")

        def authenticate(self):
            pass

        def list_contents(self, path):
            pass

    monkeypatch.setattr(
        sys,
        "argv",
        ["cli.py", "/", "--email", "user@example.com", "--list"],
    )
    monkeypatch.setattr(cli, "DownloadManager", _FakeDM)

    cli.main()

    assert captured["skip_existing"] is False


def test_main_module_routes_to_cli(monkeypatch):
    fake_cli = types.ModuleType("ifetch.cli")
    fake_cli.main = lambda: 7
    monkeypatch.setitem(sys.modules, "ifetch.cli", fake_cli)
    monkeypatch.setattr(sys, "argv", ["ifetch"])

    with pytest.raises(SystemExit) as exc:
        runpy.run_module("ifetch", run_name="__main__")

    assert exc.value.code == 7


def test_main_module_routes_to_export(monkeypatch):
    fake_export = types.ModuleType("ifetch.export_cli")
    fake_export.main = lambda: 9
    monkeypatch.setitem(sys.modules, "ifetch.export_cli", fake_export)
    monkeypatch.setattr(sys, "argv", ["ifetch", "export"])

    with pytest.raises(SystemExit) as exc:
        runpy.run_module("ifetch", run_name="__main__")

    assert exc.value.code == 9
