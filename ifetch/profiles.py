"""Profile management for inclusion/exclusion patterns.

Profiles live in JSON (or YAML) at ``~/.ifetch_profiles.json`` by default:

{
  "pdfs": {
    "include": ["*.pdf"],
    "exclude": ["Archive/*"]
  },
  "photos": {
    "include": ["*.jpg", "*.png"],
    "exclude": []
  }
}

Users select one via ``--profile pdfs``. If no profile is provided, *include*
patterns default to empty (meaning «everything»), *exclude* default to empty.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import List, Dict, Any, Tuple

DEFAULT_PATH = Path.home() / ".ifetch_profiles.json"

class ProfileManager:
    def __init__(self, profile_name: str | None = None, config_path: Path | None = None):
        self.config_path = config_path or DEFAULT_PATH
        self.profile_name = profile_name
        self.include: List[str] = []
        self.exclude: List[str] = []
        self._load()

    def _load(self):
        if not self.profile_name:
            return  # defaults already set
        if not self.config_path.exists():
            raise FileNotFoundError(f"Profile config not found at {self.config_path}")
        try:
            data: Dict[str, Any] = json.loads(self.config_path.read_text())
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in {self.config_path}: {e}") from e

        prof = data.get(self.profile_name)
        if prof is None:
            raise KeyError(f"Profile '{self.profile_name}' not defined in {self.config_path}")
        self.include = list(prof.get("include", []))
        self.exclude = list(prof.get("exclude", []))

    def get_patterns(self) -> Tuple[List[str], List[str]]:
        return self.include, self.exclude 