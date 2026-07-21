import importlib
import io
import json
import os
import pickle
import stat
import sys
import types
from pathlib import Path

import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))


def _load_google_modules(monkeypatch):
    class FakeHttpError(Exception):
        pass

    class FakeCredentials:
        def __init__(self, valid=True, expired=False, refresh_token=False, token="tok"):
            self.valid = valid
            self.expired = expired
            self.refresh_token = refresh_token
            self.refreshed = False
            self.token = token

        def refresh(self, request):
            self.refreshed = True
            self.valid = True

        def to_json(self):
            return json.dumps({
                "token": self.token,
                "valid": self.valid,
                "expired": self.expired,
                "refresh_token": self.refresh_token,
            })

        @classmethod
        def from_authorized_user_info(cls, info, scopes=None):
            return cls(
                valid=info.get("valid", True),
                expired=info.get("expired", False),
                refresh_token=info.get("refresh_token", False),
                token=info.get("token"),
            )

        @classmethod
        def from_authorized_user_file(cls, filename, scopes=None):
            with open(filename, "r") as handle:
                return cls.from_authorized_user_info(json.load(handle), scopes)

    class FakeFlow:
        creds = FakeCredentials(valid=True)

        @classmethod
        def from_client_secrets_file(cls, credentials_file, scopes):
            cls.last_call = (credentials_file, tuple(scopes))
            return cls()

        def run_local_server(self, port=0):
            return self.creds

    class FakeMediaFileUpload:
        def __init__(self, filename, resumable=True, chunksize=None):
            self.filename = filename
            self.resumable = resumable
            self.chunksize = chunksize

    google_pkg = types.ModuleType("google")
    google_auth_pkg = types.ModuleType("google.auth")
    google_auth_transport_pkg = types.ModuleType("google.auth.transport")
    google_auth_transport_requests = types.ModuleType("google.auth.transport.requests")
    google_auth_transport_requests.Request = object

    google_oauth2_pkg = types.ModuleType("google.oauth2")
    google_oauth2_credentials = types.ModuleType("google.oauth2.credentials")
    google_oauth2_credentials.Credentials = FakeCredentials

    google_auth_oauthlib_pkg = types.ModuleType("google_auth_oauthlib")
    google_auth_oauthlib_flow = types.ModuleType("google_auth_oauthlib.flow")
    google_auth_oauthlib_flow.InstalledAppFlow = FakeFlow

    googleapiclient_pkg = types.ModuleType("googleapiclient")
    googleapiclient_discovery = types.ModuleType("googleapiclient.discovery")
    googleapiclient_http = types.ModuleType("googleapiclient.http")
    googleapiclient_errors = types.ModuleType("googleapiclient.errors")

    build_calls = []

    def fake_build(*args, **kwargs):
        build_calls.append((args, kwargs))
        return types.SimpleNamespace()

    googleapiclient_discovery.build = fake_build
    googleapiclient_http.MediaFileUpload = FakeMediaFileUpload
    googleapiclient_errors.HttpError = FakeHttpError

    for name, module in {
        "google": google_pkg,
        "google.auth": google_auth_pkg,
        "google.auth.transport": google_auth_transport_pkg,
        "google.auth.transport.requests": google_auth_transport_requests,
        "google.oauth2": google_oauth2_pkg,
        "google.oauth2.credentials": google_oauth2_credentials,
        "google_auth_oauthlib": google_auth_oauthlib_pkg,
        "google_auth_oauthlib.flow": google_auth_oauthlib_flow,
        "googleapiclient": googleapiclient_pkg,
        "googleapiclient.discovery": googleapiclient_discovery,
        "googleapiclient.http": googleapiclient_http,
        "googleapiclient.errors": googleapiclient_errors,
    }.items():
        monkeypatch.setitem(sys.modules, name, module)

    for mod in ["ifetch.exporters.googledrive", "ifetch.export_cli"]:
        sys.modules.pop(mod, None)

    googledrive = importlib.import_module("ifetch.exporters.googledrive")
    export_cli = importlib.import_module("ifetch.export_cli")
    return googledrive, export_cli, FakeCredentials, FakeFlow, FakeHttpError, build_calls


class _FakeExecute:
    def __init__(self, payload):
        self.payload = payload

    def execute(self):
        if isinstance(self.payload, Exception):
            raise self.payload
        return self.payload


class _FakeUploadStatus:
    def __init__(self, fraction):
        self._fraction = fraction

    def progress(self):
        return self._fraction


class _FakeUploadRequest:
    def __init__(self, responses):
        self.responses = list(responses)

    def next_chunk(self):
        return self.responses.pop(0)


class _FakeFilesAPI:
    def __init__(self):
        self.list_payload = {"files": []}
        self.create_payload = {"id": "new-id"}
        self.upload_request = _FakeUploadRequest([(_FakeUploadStatus(1.0), {"id": "upload-id"})])
        self.last_list_kwargs = None
        self.last_create_kwargs = None

    def list(self, **kwargs):
        self.last_list_kwargs = kwargs
        return _FakeExecute(self.list_payload)

    def create(self, **kwargs):
        self.last_create_kwargs = kwargs
        media_body = kwargs.get("media_body")
        if media_body is not None:
            return self.upload_request
        return _FakeExecute(self.create_payload)


class _FakeService:
    def __init__(self):
        self._files = _FakeFilesAPI()

    def files(self):
        return self._files


def test_export_cli_show_index_stats(monkeypatch, capsys):
    _, export_cli, *_ = _load_google_modules(monkeypatch)

    class FakeIndexManager:
        def __init__(self, path):
            self.path = path

        def print_stats(self):
            print(f"stats:{self.path}")

    monkeypatch.setattr("ifetch.exporters.index_manager.UploadIndexManager", FakeIndexManager)
    monkeypatch.setattr(sys, "argv", ["export_cli.py", "--show-index-stats", "--index-file", "idx.json"])

    assert export_cli.main() == 0
    assert "stats:idx.json" in capsys.readouterr().out


def test_export_cli_rebuild_and_list_defaults(monkeypatch, capsys, tmp_path):
    _, export_cli, *_ = _load_google_modules(monkeypatch)

    rebuilt = []

    class FakeIndexManager:
        def __init__(self, path):
            self.path = path

        def rebuild_index(self):
            rebuilt.append(self.path)

    monkeypatch.setattr("ifetch.exporters.index_manager.UploadIndexManager", FakeIndexManager)
    monkeypatch.setattr(sys, "argv", ["export_cli.py", "--rebuild-index", "--index-file", "idx.json"])
    assert export_cli.main() == 0
    assert rebuilt == ["idx.json"]

    docs = tmp_path / "Documents"
    docs.mkdir()
    monkeypatch.setattr(export_cli.Path, "home", lambda: tmp_path)
    monkeypatch.setattr(sys, "argv", ["export_cli.py", "--list-defaults"])
    assert export_cli.main() == 0
    assert str(docs) in capsys.readouterr().out


def test_export_cli_cancel_no_folders_and_success(monkeypatch, capsys, tmp_path):
    _, export_cli, *_ = _load_google_modules(monkeypatch)

    monkeypatch.setattr(export_cli, "_stdin_is_interactive", lambda: True)
    monkeypatch.setattr(export_cli, "get_default_folders", lambda: [])
    monkeypatch.setattr(sys, "argv", ["export_cli.py"])
    assert export_cli.main() == 1

    calls = {}

    class FakeExporter:
        def __init__(self, **kwargs):
            calls["init"] = kwargs

        def authenticate(self):
            calls["auth"] = True

        def export_local_folders(self, folders, force, include_patterns, exclude_patterns):
            calls["export"] = (folders, force, include_patterns, exclude_patterns)
            return {"total_files": 1, "uploaded": 1, "skipped": 0, "failed": 0, "ignored": 0, "total_bytes": 1}

        def print_summary(self, stats):
            calls["summary"] = stats

    monkeypatch.setattr(export_cli, "GoogleDriveExporter", FakeExporter)
    monkeypatch.setattr(export_cli, "get_default_folders", lambda: [str(tmp_path)])
    monkeypatch.setattr("builtins.input", lambda prompt="": "n")
    monkeypatch.setattr(sys, "argv", ["export_cli.py"])
    assert export_cli.main() == 0
    assert "Export cancelled." in capsys.readouterr().out

    monkeypatch.setattr("builtins.input", lambda prompt="": "y")
    monkeypatch.setattr(
        sys,
        "argv",
        ["export_cli.py", "--folders", str(tmp_path), "--include", "*.pdf", "--exclude", "*.tmp", "--force"],
    )
    assert export_cli.main() == 0
    assert calls["auth"] is True
    assert calls["export"][1:] == (True, ["*.pdf"], ["*.tmp"])
    assert calls["summary"]["uploaded"] == 1


def test_export_cli_handles_export_errors(monkeypatch, capsys, tmp_path):
    _, export_cli, *_ = _load_google_modules(monkeypatch)

    class FakeExporter:
        def __init__(self, **kwargs):
            pass

        def authenticate(self):
            raise RuntimeError("boom")

    monkeypatch.setattr(export_cli, "GoogleDriveExporter", FakeExporter)
    monkeypatch.setattr(export_cli, "_stdin_is_interactive", lambda: True)
    monkeypatch.setattr("builtins.input", lambda prompt="": "y")
    monkeypatch.setattr(sys, "argv", ["export_cli.py", "--folders", str(tmp_path)])

    assert export_cli.main() == 1
    assert "Unexpected error: boom" in capsys.readouterr().out


def test_get_default_folders_has_no_personal_paths(monkeypatch, tmp_path):
    _, export_cli, *_ = _load_google_modules(monkeypatch)

    for name in ("Documents", "Downloads", "Desktop", "Pictures", "LocalDoc"):
        (tmp_path / name).mkdir()

    monkeypatch.setattr(export_cli.Path, "home", lambda: tmp_path)
    defaults = export_cli.get_default_folders()

    assert defaults == [
        str(tmp_path / "Documents"),
        str(tmp_path / "Downloads"),
        str(tmp_path / "Desktop"),
        str(tmp_path / "Pictures"),
    ]
    assert not any("LocalDoc" in folder for folder in defaults)


def test_get_default_folders_filters_missing(monkeypatch, tmp_path):
    _, export_cli, *_ = _load_google_modules(monkeypatch)

    (tmp_path / "Documents").mkdir()
    (tmp_path / "Desktop").mkdir()
    monkeypatch.setattr(export_cli.Path, "home", lambda: tmp_path)

    assert export_cli.get_default_folders() == [
        str(tmp_path / "Documents"),
        str(tmp_path / "Desktop"),
    ]


def test_export_cli_yes_flag_skips_prompt(monkeypatch, tmp_path):
    _, export_cli, *_ = _load_google_modules(monkeypatch)

    calls = {}

    class FakeExporter:
        def __init__(self, **kwargs):
            calls["init"] = kwargs

        def authenticate(self):
            calls["auth"] = True

        def export_local_folders(self, folders, force, include_patterns, exclude_patterns):
            calls["export"] = folders
            return {"total_files": 0, "uploaded": 0, "skipped": 0, "failed": 0, "ignored": 0, "total_bytes": 0}

        def print_summary(self, stats):
            calls["summary"] = stats

    def _boom(prompt=""):
        raise AssertionError("prompt must not be shown when --yes is passed")

    monkeypatch.setattr(export_cli, "GoogleDriveExporter", FakeExporter)
    monkeypatch.setattr(export_cli, "_stdin_is_interactive", lambda: False)
    monkeypatch.setattr("builtins.input", _boom)
    monkeypatch.setattr(sys, "argv", ["export_cli.py", "--yes", "--folders", str(tmp_path)])

    assert export_cli.main() == 0
    assert calls["export"] == [str(tmp_path)]

    # Short form behaves identically
    calls.clear()
    monkeypatch.setattr(sys, "argv", ["export_cli.py", "-y", "--folders", str(tmp_path)])
    assert export_cli.main() == 0
    assert calls["auth"] is True


def test_export_cli_non_tty_without_yes_fails_cleanly(monkeypatch, capsys, tmp_path):
    _, export_cli, *_ = _load_google_modules(monkeypatch)

    class FakeExporter:
        def __init__(self, **kwargs):
            raise AssertionError("exporter must not run without confirmation")

    def _boom(prompt=""):
        raise AssertionError("must not block on input() without a TTY")

    monkeypatch.setattr(export_cli, "GoogleDriveExporter", FakeExporter)
    monkeypatch.setattr(export_cli, "_stdin_is_interactive", lambda: False)
    monkeypatch.setattr("builtins.input", _boom)
    monkeypatch.setattr(sys, "argv", ["export_cli.py", "--folders", str(tmp_path)])

    assert export_cli.main() == 1
    assert "--yes" in capsys.readouterr().err


def test_export_cli_eof_on_prompt_fails_cleanly(monkeypatch, capsys, tmp_path):
    _, export_cli, *_ = _load_google_modules(monkeypatch)

    def _eof(prompt=""):
        raise EOFError

    monkeypatch.setattr(export_cli, "GoogleDriveExporter", lambda **kwargs: None)
    monkeypatch.setattr(export_cli, "_stdin_is_interactive", lambda: True)
    monkeypatch.setattr("builtins.input", _eof)
    monkeypatch.setattr(sys, "argv", ["export_cli.py", "--folders", str(tmp_path)])

    assert export_cli.main() == 1
    assert "--yes" in capsys.readouterr().err


def test_stdin_is_interactive_handles_broken_stdin(monkeypatch):
    _, export_cli, *_ = _load_google_modules(monkeypatch)

    monkeypatch.setattr(export_cli.sys, "stdin", None)
    assert export_cli._stdin_is_interactive() is False

    class ClosedStdin:
        def isatty(self):
            raise ValueError("I/O operation on closed file")

    monkeypatch.setattr(export_cli.sys, "stdin", ClosedStdin())
    assert export_cli._stdin_is_interactive() is False

    monkeypatch.setattr(export_cli.sys, "stdin", types.SimpleNamespace(isatty=lambda: True))
    assert export_cli._stdin_is_interactive() is True


def test_export_cli_token_default_is_json(monkeypatch):
    googledrive, export_cli, *_ = _load_google_modules(monkeypatch)

    monkeypatch.setattr(sys, "argv", ["export_cli.py"])
    args = export_cli.parse_args()
    assert args.token == googledrive.DEFAULT_TOKEN_FILE
    assert args.token.endswith(".json")


def test_googledrive_authenticate_refresh_and_flow(monkeypatch, tmp_path):
    googledrive, _, FakeCredentials, FakeFlow, _, build_calls = _load_google_modules(monkeypatch)
    token_file = tmp_path / "token.json"
    token_file.write_text(json.dumps({"token": "cached", "valid": False, "expired": True, "refresh_token": True}))

    exporter = googledrive.GoogleDriveExporter(
        credentials_file=str(tmp_path / "creds.json"),
        token_file=str(token_file),
        ignore_file=None,
        use_index=False,
    )
    service = _FakeService()
    monkeypatch.setattr(googledrive, "build", lambda *args, **kwargs: service)
    monkeypatch.setattr(exporter, "_get_or_create_folder", lambda name, parent_id=None: "root-id")

    exporter.authenticate()

    assert exporter._credentials.refreshed is True
    assert exporter.root_folder_id == "root-id"

    token_file.unlink()
    creds_file = tmp_path / "creds.json"
    creds_file.write_text("{}")
    FakeFlow.creds = FakeCredentials(valid=True, token="fresh")
    exporter = googledrive.GoogleDriveExporter(
        credentials_file=str(creds_file),
        token_file=str(token_file),
        ignore_file=None,
        use_index=False,
    )
    monkeypatch.setattr(googledrive, "build", lambda *args, **kwargs: service)
    monkeypatch.setattr(exporter, "_get_or_create_folder", lambda name, parent_id=None: "root-id")

    exporter.authenticate()

    assert token_file.exists()
    assert FakeFlow.last_call[0] == str(creds_file)
    assert json.loads(token_file.read_text())["token"] == "fresh"


def test_googledrive_token_is_json_with_0600_permissions(monkeypatch, tmp_path):
    googledrive, _, FakeCredentials, FakeFlow, *_ = _load_google_modules(monkeypatch)
    token_file = tmp_path / "nested" / ".gdrive_token.json"
    creds_file = tmp_path / "creds.json"
    creds_file.write_text("{}")
    FakeFlow.creds = FakeCredentials(valid=True, token="secret")

    exporter = googledrive.GoogleDriveExporter(
        credentials_file=str(creds_file),
        token_file=str(token_file),
        ignore_file=None,
        use_index=False,
    )
    monkeypatch.setattr(googledrive, "build", lambda *args, **kwargs: _FakeService())
    monkeypatch.setattr(exporter, "_get_or_create_folder", lambda name, parent_id=None: "root-id")

    exporter.authenticate()

    assert token_file.exists()
    assert stat.S_IMODE(token_file.stat().st_mode) == 0o600
    assert json.loads(token_file.read_text())["token"] == "secret"

    # A pre-existing world-readable token gets tightened on the next write
    token_file.chmod(0o644)
    exporter._save_token(FakeCredentials(valid=True, token="rotated"))
    assert stat.S_IMODE(token_file.stat().st_mode) == 0o600
    assert json.loads(token_file.read_text())["token"] == "rotated"

    # Round-trip through the loader
    loaded = exporter._load_token()
    assert loaded.token == "rotated"


def test_googledrive_legacy_pickle_token_is_migrated(monkeypatch, tmp_path):
    googledrive, _, FakeCredentials, *_ = _load_google_modules(monkeypatch)
    legacy = tmp_path / ".gdrive_token.pickle"
    legacy.write_bytes(b"legacy-token")

    exporter = googledrive.GoogleDriveExporter(
        token_file=str(tmp_path / ".gdrive_token.json"),
        ignore_file=None,
        use_index=False,
    )
    assert exporter.legacy_token_file == str(legacy)

    creds = FakeCredentials(valid=True, token="from-pickle")
    monkeypatch.setattr(googledrive.pickle, "load", lambda handle: creds)

    loaded = exporter._load_token()

    assert loaded is creds
    migrated = tmp_path / ".gdrive_token.json"
    assert json.loads(migrated.read_text())["token"] == "from-pickle"
    assert stat.S_IMODE(migrated.stat().st_mode) == 0o600
    # Legacy file is left in place for rollback
    assert legacy.exists()

    # Once migrated, the JSON token wins and the pickle is never read again
    def _explode(handle):
        raise AssertionError("pickle must not be read once a JSON token exists")

    monkeypatch.setattr(googledrive.pickle, "load", _explode)
    assert exporter._load_token().token == "from-pickle"


def test_googledrive_explicit_pickle_token_path_is_honoured(monkeypatch, tmp_path):
    googledrive, _, FakeCredentials, *_ = _load_google_modules(monkeypatch)
    legacy = tmp_path / "custom.pickle"
    legacy.write_bytes(b"legacy-token")

    exporter = googledrive.GoogleDriveExporter(
        token_file=str(legacy),
        ignore_file=None,
        use_index=False,
    )
    assert exporter.legacy_token_file == str(legacy)
    assert exporter.json_token_file == str(tmp_path / "custom.json")

    monkeypatch.setattr(
        googledrive.pickle, "load", lambda handle: FakeCredentials(valid=True, token="legacy")
    )
    assert exporter._load_token().token == "legacy"
    assert json.loads((tmp_path / "custom.json").read_text())["token"] == "legacy"


def test_googledrive_corrupt_tokens_do_not_crash(monkeypatch, tmp_path, capsys):
    googledrive, *_ = _load_google_modules(monkeypatch)
    token_file = tmp_path / ".gdrive_token.json"
    token_file.write_text("not json at all")
    legacy = tmp_path / ".gdrive_token.pickle"
    legacy.write_bytes(b"\x80\x04 not a pickle")

    exporter = googledrive.GoogleDriveExporter(
        token_file=str(token_file), ignore_file=None, use_index=False
    )

    assert exporter._load_token() is None
    out = capsys.readouterr().out
    assert "could not read token" in out
    assert "could not read legacy token" in out


def test_googledrive_thread_local_and_ignore_patterns(monkeypatch, tmp_path, capsys):
    googledrive, *_ = _load_google_modules(monkeypatch)
    ignore_file = tmp_path / ".gdriveexportignore"
    ignore_file.write_text("# comment\n*.tmp\nnode_modules/\n**/*.log\n")
    builds = []
    monkeypatch.setattr(googledrive, "build", lambda *args, **kwargs: builds.append((args, kwargs)) or _FakeService())

    exporter = googledrive.GoogleDriveExporter(ignore_file=str(ignore_file), use_index=False)
    exporter._credentials = object()

    assert exporter._should_ignore(tmp_path / "test.tmp") is True
    assert exporter._should_ignore(tmp_path / "node_modules", tmp_path) is True
    assert exporter._should_ignore(tmp_path / "logs" / "a.log", tmp_path) is True
    assert exporter._should_ignore(tmp_path / "keep.txt") is False
    assert exporter._get_thread_local_service() is exporter._get_thread_local_service()
    assert builds
    assert "Loaded 3 ignore patterns" in capsys.readouterr().out


def test_googledrive_folder_and_file_lookup(monkeypatch):
    googledrive, _, _, _, FakeHttpError, _ = _load_google_modules(monkeypatch)
    exporter = googledrive.GoogleDriveExporter(ignore_file=None, use_index=False)
    service = _FakeService()
    exporter._thread_local.service = service

    service._files.list_payload = {"files": [{"id": "folder-1", "name": "Docs"}]}
    assert exporter._get_or_create_folder("Docs") == "folder-1"
    assert exporter._get_or_create_folder("Docs") == "folder-1"

    exporter._folder_cache.clear()
    service._files.list_payload = {"files": []}
    service._files.create_payload = {"id": "folder-2"}
    assert exporter._get_or_create_folder("Docs") == "folder-2"

    service._files.list_payload = FakeHttpError("bad list")
    service._files.create_payload = {"id": "folder-3"}
    assert exporter._get_or_create_folder("Docs2") == "folder-3"

    service._files.list_payload = {"files": [{"id": "file-1", "md5Checksum": "abc"}]}
    assert exporter._file_exists_in_drive("a.txt", "parent", "abc") == "file-1"
    service._files.list_payload = {"files": [{"id": "file-1", "md5Checksum": "def"}]}
    assert exporter._file_exists_in_drive("a.txt", "parent", "abc") is None
    service._files.list_payload = FakeHttpError("bad file lookup")
    assert exporter._file_exists_in_drive("a.txt", "parent", "abc") is None


def test_escape_query_value(monkeypatch):
    googledrive, *_ = _load_google_modules(monkeypatch)
    escape = googledrive.escape_query_value

    assert escape("plain") == "plain"
    assert escape("Roshan's Files") == "Roshan\\'s Files"
    assert escape("back\\slash") == "back\\\\slash"
    # Backslashes are escaped before quotes, so the quote escape survives
    assert escape("a\\'b") == "a\\\\\\'b"


def test_googledrive_queries_escape_quotes_and_backslashes(monkeypatch):
    googledrive, *_ = _load_google_modules(monkeypatch)
    exporter = googledrive.GoogleDriveExporter(ignore_file=None, use_index=False)
    service = _FakeService()
    exporter._thread_local.service = service

    service._files.list_payload = {"files": [{"id": "folder-1"}]}
    exporter._get_or_create_folder("Roshan's Files", "par'ent")
    query = service._files.last_list_kwargs["q"]
    assert "name='Roshan\\'s Files'" in query
    assert "'par\\'ent' in parents" in query

    exporter._folder_cache.clear()
    exporter._get_or_create_folder("back\\slash")
    query = service._files.last_list_kwargs["q"]
    assert "name='back\\\\slash'" in query
    assert "'root' in parents" in query

    service._files.list_payload = {"files": []}
    exporter._file_exists_in_drive("Roshan's note\\.txt", "par'ent", "abc")
    query = service._files.last_list_kwargs["q"]
    assert "name='Roshan\\'s note\\\\.txt'" in query
    assert "'par\\'ent' in parents" in query


def test_googledrive_upload_file_branches(monkeypatch, tmp_path, capsys):
    googledrive, *_ = _load_google_modules(monkeypatch)
    file_path = tmp_path / "file.txt"
    file_path.write_text("hello")
    exporter = googledrive.GoogleDriveExporter(ignore_file=None, use_index=False)
    exporter._thread_local.service = _FakeService()

    assert exporter.upload_file(tmp_path / "missing.txt", "parent") is None

    monkeypatch.setattr(exporter, "_should_ignore", lambda *args, **kwargs: True)
    assert exporter.upload_file(file_path, "parent") is None

    monkeypatch.setattr(exporter, "_should_ignore", lambda *args, **kwargs: False)
    monkeypatch.setattr(exporter, "_get_file_info", lambda path: {"size": 5, "md5": "abc", "modified": "now"})
    monkeypatch.setattr(exporter, "_file_exists_in_drive", lambda *args, **kwargs: "existing-id")
    assert exporter.upload_file(file_path, "parent", force=False, base_path=tmp_path) == "existing-id"

    exporter.index_manager = types.SimpleNamespace(
        file_needs_upload=lambda *args, **kwargs: False,
        get_gdrive_file_id=lambda path: "indexed-id",
        add_file_entry=lambda *args, **kwargs: None,
    )
    assert exporter.upload_file(file_path, "parent", force=False, base_path=tmp_path) == "indexed-id"

    exporter.index_manager = None
    service = _FakeService()
    exporter._thread_local.service = service
    monkeypatch.setattr(exporter, "_file_exists_in_drive", lambda *args, **kwargs: None)
    uploaded = exporter.upload_file(file_path, "parent", force=True, base_path=tmp_path)
    assert uploaded == "upload-id"
    assert exporter._total_uploaded_bytes == 5

    class BadRequest:
        def next_chunk(self):
            raise googledrive.HttpError("upload failed")

    service._files.upload_request = BadRequest()
    assert exporter.upload_file(file_path, "parent", force=True, base_path=tmp_path) is None
    assert exporter._upload_file_with_status(file_path, "parent", True, tmp_path) == (
        googledrive.UPLOAD_FAILED,
        None,
    )
    out = capsys.readouterr().out
    assert "Skipping:" in out
    assert "Ignored:" in out
    assert "Uploaded:" in out or "✓ Uploaded:" in out


def test_googledrive_upload_accounting_uses_reported_status(monkeypatch, tmp_path):
    """Counters come from what the worker did - no re-hashing, no extra API calls."""
    googledrive, *_ = _load_google_modules(monkeypatch)
    base = tmp_path / "root"
    base.mkdir()
    for name in ("uploaded.txt", "skipped.txt", "failed.txt", "ignored.txt"):
        (base / name).write_text(name)

    exporter = googledrive.GoogleDriveExporter(ignore_file=None, use_index=False, upload_workers=2)
    exporter.root_folder_id = "root-id"
    monkeypatch.setattr(exporter, "_get_or_create_folder", lambda name, parent_id=None: f"id-{name}")
    monkeypatch.setattr(exporter, "_should_ignore", lambda path, base_path=None: False)

    statuses = {
        "uploaded.txt": (googledrive.UPLOAD_UPLOADED, "id-1"),
        "skipped.txt": (googledrive.UPLOAD_SKIPPED, "id-2"),
        "failed.txt": (googledrive.UPLOAD_FAILED, None),
        "ignored.txt": (googledrive.UPLOAD_IGNORED, None),
    }

    def fake_upload(file_path, parent_id, force=False, base_path=None):
        return statuses[file_path.name]

    monkeypatch.setattr(exporter, "_upload_file_with_status", fake_upload)

    def _no_extra_calls(*args, **kwargs):
        raise AssertionError("upload accounting must not issue extra Drive API calls")

    monkeypatch.setattr(exporter, "_file_exists_in_drive", _no_extra_calls)
    monkeypatch.setattr(exporter, "_calculate_md5", staticmethod(_no_extra_calls))

    stats = exporter.upload_directory(base)

    assert stats["total_files"] == 4
    assert stats["uploaded"] == 1
    assert stats["skipped"] == 1
    assert stats["failed"] == 1
    assert stats["ignored"] == 1


def test_googledrive_upload_directory_counts_worker_exceptions(monkeypatch, tmp_path, capsys):
    googledrive, *_ = _load_google_modules(monkeypatch)
    base = tmp_path / "root"
    base.mkdir()
    (base / "boom.txt").write_text("x")

    exporter = googledrive.GoogleDriveExporter(ignore_file=None, use_index=False, upload_workers=1)
    exporter.root_folder_id = "root-id"
    monkeypatch.setattr(exporter, "_get_or_create_folder", lambda name, parent_id=None: "id")
    monkeypatch.setattr(exporter, "_should_ignore", lambda path, base_path=None: False)

    def _raise(file_path, parent_id, force=False, base_path=None):
        raise RuntimeError("kaboom")

    monkeypatch.setattr(exporter, "_upload_file_with_status", _raise)

    stats = exporter.upload_directory(base)
    assert stats["failed"] == 1
    assert "kaboom" in capsys.readouterr().out


def test_googledrive_upload_directory_export_and_summary(monkeypatch, tmp_path, capsys):
    googledrive, *_ = _load_google_modules(monkeypatch)
    base = tmp_path / "root"
    base.mkdir()
    (base / "a.txt").write_text("a")
    (base / "skip.tmp").write_text("x")
    sub = base / "sub"
    sub.mkdir()
    (sub / "b.txt").write_text("b")

    exporter = googledrive.GoogleDriveExporter(ignore_file=None, use_index=False, upload_workers=2)
    exporter.root_folder_id = "root-id"
    monkeypatch.setattr(exporter, "_get_or_create_folder", lambda name, parent_id=None: f"id-{name}")
    monkeypatch.setattr(exporter, "_should_ignore", lambda path, base_path=None: path.name == "skip.tmp")
    monkeypatch.setattr(
        exporter,
        "_upload_file_with_status",
        lambda file_path, parent_id, force=False, base_path=None: (
            (googledrive.UPLOAD_FAILED, None)
            if file_path.name == "b.txt"
            else (googledrive.UPLOAD_UPLOADED, f"id-{file_path.name}")
        ),
    )
    monkeypatch.setattr(exporter, "_get_file_info", lambda file_path: {"size": file_path.stat().st_size, "md5": "md5", "modified": "now"})

    stats = exporter.upload_directory(base, include_patterns=["*.txt"], exclude_patterns=["b*"])
    assert stats["total_files"] >= 1
    assert stats["ignored"] == 1

    exporter.index_manager = types.SimpleNamespace(_pending_saves=1, save_index=lambda force=False: capsys.readouterr())
    monkeypatch.setattr(exporter, "upload_directory", lambda *args, **kwargs: {"total_files": 1, "uploaded": 1, "skipped": 0, "failed": 0, "ignored": 0, "total_bytes": 1})
    exporter._total_uploaded_bytes = 10
    monkeypatch.setattr(googledrive.time, "time", lambda: 100.0)
    export_stats = exporter.export_local_folders([str(base), str(tmp_path / "missing")])
    assert str(base.resolve()) in export_stats["folders"]

    exporter.print_summary({
        "total_files": 2,
        "uploaded": 1,
        "skipped": 1,
        "failed": 0,
        "ignored": 0,
        "total_bytes": 10,
        "uploaded_bytes": 10,
        "upload_time": 2.0,
        "avg_speed_mbps": 1.0,
        "folders": {str(base): {"total_files": 2, "uploaded": 1, "skipped": 1, "ignored": 0, "failed": 0}},
    })
    assert "EXPORT SUMMARY" in capsys.readouterr().out
