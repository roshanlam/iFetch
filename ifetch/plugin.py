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

Opting out
----------
Plugin discovery executes arbitrary Python found in the search paths.  Set the
environment variable ``IFETCH_NO_PLUGINS=1`` (or pass ``enabled=False`` to
:class:`PluginManager`) to skip discovery entirely.
"""

from __future__ import annotations

import importlib.util
import logging
import os
import sys
from pathlib import Path
from types import ModuleType
from typing import List, Optional, Sequence, Type

__all__ = [
    "BasePlugin",
    "PluginManager",
]

logger = logging.getLogger(__name__)

#: Values that count as "true" for the opt-out environment variable.
_TRUTHY = {"1", "true", "yes", "on"}


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
    _ENV_DISABLE = "IFETCH_NO_PLUGINS"

    def __init__(
        self,
        search_paths: Sequence[os.PathLike[str] | str] | None = None,
        enabled: Optional[bool] = None,
    ):
        """Create a plugin manager.

        Args:
            search_paths: Explicit directories to scan.  When omitted the
                *plugins* directory next to the project and
                ``IFETCH_PLUGIN_PATH`` are used.
            enabled: ``False`` disables discovery programmatically.  When
                ``None`` (default) the ``IFETCH_NO_PLUGINS`` environment
                variable decides.  The environment variable always wins when
                it disables plugins.
        """
        self.enabled = self._resolve_enabled(enabled)
        self._plugins: List[BasePlugin] = []
        self._paths: List[Path] = []

        if not self.enabled:
            logger.debug("Plugin discovery disabled; skipping plugin search paths")
            return

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
        seen: set[str] = set()
        for p in paths:
            p = p.expanduser().resolve()
            if p.exists() and p.is_dir() and str(p) not in seen:
                self._paths.append(p)
                seen.add(str(p))

        self._discover_plugins()

    # ------------------------------------------------------------------
    # Public helpers
    # ------------------------------------------------------------------
    @classmethod
    def _resolve_enabled(cls, enabled: Optional[bool]) -> bool:
        """Combine the explicit flag with the ``IFETCH_NO_PLUGINS`` env var."""
        disabled_by_env = os.getenv(cls._ENV_DISABLE, "").strip().lower() in _TRUTHY
        if enabled is None:
            return not disabled_by_env
        return bool(enabled) and not disabled_by_env

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
                except Exception as exc:  # plugin errors must not crash the core
                    # Contain the failure so one bad plugin cannot abort a run,
                    # but make it visible instead of silently discarding it.
                    logger.warning(
                        "Plugin %s failed in hook '%s': %s",
                        type(plugin).__name__,
                        hook,
                        exc,
                        exc_info=True,
                    )

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
                    except Exception as exc:
                        # Plugin instantiation failed – skip it but keep going
                        logger.warning(
                            "Failed to instantiate plugin %s from '%s': %s",
                            getattr(plugin_cls, "__name__", plugin_cls),
                            path,
                            exc,
                            exc_info=True,
                        )
                        continue

    def _load_plugin_from_file(self, file_path: Path) -> Type[BasePlugin] | None:
        module_name = f"ifetch_plugin_{file_path.stem}"
        spec = importlib.util.spec_from_file_location(module_name, file_path)
        if spec and spec.loader:
            module = importlib.util.module_from_spec(spec)
            sys.modules[module_name] = module
            try:
                spec.loader.exec_module(module)  # type: ignore[arg-type]
            except Exception as exc:
                logger.warning(
                    "Failed to load plugin module '%s': %s", file_path, exc, exc_info=True
                )
                sys.modules.pop(module_name, None)
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