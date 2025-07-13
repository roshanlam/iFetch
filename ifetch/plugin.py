"""Plugin system for iFetch.

This module defines a very lightweight plugin architecture that lets external
code to hook into high-level events inside the DownloadManager.  It therefore
becomes trivial to integrate iFetch with other cloud providers, send progress
information to a local indexer, or trigger custom business logic.

Usage
-----
1.  Drop a Python file inside the directory named ``plugins`` that sits next to
    the *ifetch* package (or pass an explicit path via the ``IFETCH_PLUGIN_PATH``
    environment variable).
2.  Inside that file declare a subclass of :class:`BasePlugin` and implement
    whichever callbacks you need.
3.  Run iFetch as usual – your plugin will be auto-discovered and its callbacks
    executed.

Example::

    from ifetch.plugin import BasePlugin
    import requests

    class NotifySlack(BasePlugin):
        def after_download(self, remote_item, local_path, success, **kwargs):
            if success:
                msg = f"Downloaded {remote_item.name} to {local_path}"
                requests.post("https://hooks.slack.com/...", json={"text": msg})

All callbacks receive *kwargs* with extra contextual fields so future versions
of iFetch can add more data without breaking compatibility.
"""

from __future__ import annotations

import importlib.util
import os
import sys
from pathlib import Path
from types import ModuleType
from typing import List, Sequence, Type

__all__ = [
    "BasePlugin",
    "PluginManager",
]


class BasePlugin:
    """Base class for all iFetch plugins.

    Sub-classes can override whichever callbacks they are interested in.  All
    callbacks are optional – the default implementation is a *no-op* – so that
    plugins can remain lean.
    """

    # --- Authentication --------------------------------------------------
    def on_authenticated(self, downloader, **kwargs):  # noqa: D401 – imperative mood
        """Called once iCloud authentication succeeds."""

    # --- Listing ---------------------------------------------------------
    def on_list_contents(self, path: str, contents, **kwargs):
        """Called when *path* has been listed.

        *contents* is a list of dicts in the same format produced by
        ``DownloadManager.list_contents``.
        """

    # --- Download lifecycle ---------------------------------------------
    def before_download(self, remote_item, local_path, **kwargs):
        """Called *before* a file starts downloading."""

    def after_download(
        self,
        remote_item,
        local_path,
        success: bool,
        **kwargs,
    ):
        """Called *after* a file finished downloading (or failed).

        *success* indicates whether the download has completed without errors.
        """

    # --- Generic hook ----------------------------------------------------
    def on_event(self, name: str, **payload):  # noqa: D401 – imperative mood
        """Receive a generic event that is not covered by the above helpers."""


class PluginManager:
    """Loads and dispatches events to plugins."""

    _ENV_PATH = "IFETCH_PLUGIN_PATH"

    def __init__(self, search_paths: Sequence[os.PathLike[str] | str] | None = None):
        # Determine plugin search paths – *plugins* dir next to project + env var
        default_path = Path(__file__).resolve().parent.parent / "plugins"
        env_path = os.getenv(self._ENV_PATH)
        paths: List[Path] = []

        if search_paths is not None:
            paths.extend(Path(p) for p in search_paths)
        else:
            paths.append(default_path)
            if env_path:
                paths.append(Path(env_path))

        # Ensure uniqueness while preserving order
        self._paths: List[Path] = []
        seen: set[str] = set()
        for p in paths:
            p = p.expanduser().resolve()
            if p.exists() and p.is_dir() and str(p) not in seen:
                self._paths.append(p)
                seen.add(str(p))

        self._plugins: List[BasePlugin] = []
        self._discover_plugins()

    # ------------------------------------------------------------------
    # Public helpers
    # ------------------------------------------------------------------
    @property
    def plugins(self) -> List[BasePlugin]:
        return list(self._plugins)

    # Dispatch helpers ----------------------------------------------------
    def dispatch(self, hook: str, *args, **kwargs):
        """Invoke *hook* on all registered plugins (if implemented)."""
        for plugin in self._plugins:
            cb = getattr(plugin, hook, None)
            if callable(cb):
                try:
                    cb(*args, **kwargs)
                except Exception:  # pragma: no cover – plugin errors shouldn’t crash core
                    # We deliberately swallow exceptions raised by plugins so that
                    # they cannot destabilise the core application.  Consider
                    # extending this with proper logging.
                    pass

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------
    def _discover_plugins(self):
        """Import *.py* files from the search paths and instantiate plugins."""
        for base in self._paths:
            for path in base.glob("*.py"):
                plugin_cls = self._load_plugin_from_file(path)
                if plugin_cls is not None:
                    try:
                        instance = plugin_cls()
                        self._plugins.append(instance)
                    except Exception:
                        # Plugin instantiation failed – ignore plugin but keep going
                        continue

    def _load_plugin_from_file(self, file_path: Path) -> Type[BasePlugin] | None:
        module_name = f"ifetch_plugin_{file_path.stem}"
        spec = importlib.util.spec_from_file_location(module_name, file_path)
        if spec and spec.loader:
            module = importlib.util.module_from_spec(spec)
            sys.modules[module_name] = module
            try:
                spec.loader.exec_module(module)  # type: ignore[arg-type]
            except Exception:
                return None  # Skip faulty module

            # A plugin file can declare multiple plugins – pick all subclasses
            candidates = [
                obj
                for obj in module.__dict__.values()
                if isinstance(obj, type) and issubclass(obj, BasePlugin) and obj is not BasePlugin
            ]
            # For simplicity we use the first candidate – advanced users can use
            # multiple files.
            return candidates[0] if candidates else None
        return None 