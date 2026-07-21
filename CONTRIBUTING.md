# Contributing to iFetch

Thanks for your interest in contributing! This document explains how to set up
a development environment, run the tests, and submit changes.

## Development setup

1. **Fork and clone** the repository, then create a virtual environment:

   ```bash
   git clone https://github.com/<your-username>/iFetch.git
   cd iFetch
   python3 -m venv venv
   source venv/bin/activate   # Windows: venv\Scripts\activate
   ```

2. **Install iFetch in editable mode** with the development and Google Drive
   export extras:

   ```bash
   pip install --upgrade pip
   pip install -e ".[dev,gdrive]"
   ```

   If the editable install is not available in your checkout, install the
   pinned dependencies directly:

   ```bash
   pip install -r requirements.txt
   ```

3. **pyicloud.** iFetch depends on the actively maintained
   [timlaing/pyicloud](https://github.com/timlaing/pyicloud), which now
   publishes to PyPI as `pyicloud`. Version 2.5.0+ is required — it adds the
   shared-drive support (shareID propagation for folder traversal) that iFetch
   relies on. Both install paths above pull it automatically.

## Running the tests

The test suite uses pytest (configuration lives in `pytest.ini`):

```bash
pytest                  # run everything
pytest --cov=ifetch     # with coverage (what CI runs)
pytest tests/test_downloader.py -k retry   # a focused subset
```

Tests are fully mocked — they never contact iCloud — so no Apple ID is needed
to run them.

## Code style

- Follow PEP 8; keep functions small and focused.
- Use type hints and docstrings for public functions and classes (the existing
  modules use NumPy/reST-style docstrings — match the surrounding code).
- CI runs `ruff check ifetch/` in advisory mode. Please run it locally and
  avoid introducing new warnings:

  ```bash
  pip install ruff
  ruff check ifetch/
  ```

- Never log or commit credentials, session tokens, or personal data.

## How the plugin system works

iFetch ships a lightweight plugin architecture (`ifetch/plugin.py`) that lets
external code hook into high-level events inside the `DownloadManager` without
modifying core code — for example to notify Slack, feed a local indexer, or
bridge to another storage provider.

- **Writing a plugin:** drop a `.py` file into the `plugins/` directory that
  sits next to the `ifetch` package (or point the `IFETCH_PLUGIN_PATH`
  environment variable at another directory). In that file, subclass
  `ifetch.plugin.BasePlugin` and override any callbacks you need — all are
  optional no-ops by default:
  - `on_authenticated(downloader, **kwargs)` — after iCloud auth succeeds
  - `on_list_contents(path, contents, **kwargs)` — after a folder listing
  - `before_download(remote_item, local_path, **kwargs)` — before a file download
  - `after_download(remote_item, local_path, success, **kwargs)` — after a
    download completes or fails
  - `on_event(name, **payload)` — generic catch-all for other events
- **Discovery and dispatch:** at startup, `PluginManager` scans the search
  paths, imports each `*.py` file, instantiates the first `BasePlugin`
  subclass found in it, and later dispatches hooks to every loaded plugin.
  Exceptions raised inside plugins are swallowed so a faulty plugin cannot
  crash the core application.
- **Forward compatibility:** callbacks receive extra context via `**kwargs`,
  so always accept `**kwargs` in your overrides — future iFetch versions may
  pass additional fields.

## Pull request guidelines

1. Branch from `dev` (the active development branch); `main` is for releases.
2. Keep PRs focused — one feature or fix per PR.
3. Add or update tests for any behavior change, and make sure
   `pytest --cov=ifetch` passes locally.
4. Update documentation (ReadMe.md, docstrings) and add a line to
   `CHANGELOG.md` under **Unreleased** when the change is user-visible.
5. Fill in the pull request template, including the checklist.
6. Reference related issues (e.g. `Fixes #123`) in the PR description.

For larger changes, please open an issue or discussion first so we can agree
on the approach before you invest significant time.

## Reporting bugs and requesting features

Use the issue forms on GitHub. When attaching logs, **always sanitize them
first** — remove your Apple ID, session tokens, cookies, and any file names
you do not want public.
