"""Configuration file support for chops."""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any

if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib  # type: ignore[import-not-found,unused-ignore]

CONFIG_FILENAMES = ["chops.toml", ".chops.toml"]
CONFIG_HOME = (
    Path(os.getenv("XDG_CONFIG_HOME", str(Path.home() / ".config"))) / "chops" / "config.toml"
)


def find_config() -> Path | None:
    """Find config file: local dir first, then XDG config home."""
    for name in CONFIG_FILENAMES:
        path = Path.cwd() / name
        if path.exists():
            return path

    if CONFIG_HOME.exists():
        return CONFIG_HOME

    return None


def load_config(path: Path | None = None) -> dict[str, Any]:
    """Load config from TOML file."""
    if path is None:
        path = find_config()

    if path is None or not path.exists():
        return {}

    with path.open("rb") as f:
        result: dict[str, Any] = tomllib.load(f)
        return result


def get_profile(config: dict[str, Any], profile_name: str | None = None) -> dict[str, Any]:
    """Get connection settings from a named profile or defaults."""
    # Check env var for default profile
    if profile_name is None:
        profile_name = os.getenv("CHOPS_PROFILE")

    # If still no profile, use top-level connection settings
    if profile_name is None:
        return {
            k: config.get(k)
            for k in ("host", "port", "user", "password", "database", "secure")
            if config.get(k) is not None
        }

    profiles = config.get("profiles", {})
    if profile_name not in profiles:
        return {}

    return dict(profiles[profile_name])
