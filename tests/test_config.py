"""Tests for config file support."""

from __future__ import annotations

from pathlib import Path

from chops.config import get_profile, load_config


def test_load_config_nonexistent() -> None:
    """Loading a non-existent config returns empty dict."""
    result = load_config(Path("/tmp/does-not-exist.toml"))
    assert result == {}


def test_load_config_valid(tmp_path: Path) -> None:
    """Loading a valid TOML config returns parsed dict."""
    cfg = tmp_path / "chops.toml"
    cfg.write_text('host = "myhost"\nport = 9000\n')
    result = load_config(cfg)
    assert result["host"] == "myhost"
    assert result["port"] == 9000


def test_get_profile_no_profile() -> None:
    """Without a profile name, returns top-level connection keys."""
    config = {"host": "localhost", "port": 8123, "extra": "ignored"}
    result = get_profile(config, None)
    assert result == {"host": "localhost", "port": 8123}
    assert "extra" not in result


def test_get_profile_named() -> None:
    """Named profile returns that profile's settings."""
    config = {
        "host": "default-host",
        "profiles": {
            "prod": {"host": "prod-host", "port": 9000, "user": "admin"},
        },
    }
    result = get_profile(config, "prod")
    assert result == {"host": "prod-host", "port": 9000, "user": "admin"}


def test_get_profile_missing_name() -> None:
    """Missing profile name returns empty dict."""
    config = {"profiles": {"prod": {"host": "prod-host"}}}
    result = get_profile(config, "staging")
    assert result == {}
