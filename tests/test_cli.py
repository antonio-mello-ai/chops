"""Tests for chops CLI."""

from __future__ import annotations

from typer.testing import CliRunner

from chops.cli import app

runner = CliRunner()


def test_version() -> None:
    result = runner.invoke(app, ["version"])
    assert result.exit_code == 0
    assert "chops" in result.output


def test_help() -> None:
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    assert "ClickHouse Operations" in result.output


def test_health_help() -> None:
    result = runner.invoke(app, ["health", "--help"])
    assert result.exit_code == 0
    assert "summary" in result.output


def test_dq_help() -> None:
    result = runner.invoke(app, ["dq", "--help"])
    assert result.exit_code == 0
    assert "profile" in result.output
    assert "freshness" in result.output
    assert "check" in result.output
    assert "anomalies" in result.output
    assert "compare" in result.output


def test_query_help() -> None:
    result = runner.invoke(app, ["query", "--help"])
    assert result.exit_code == 0
    assert "SQL" in result.output or "sql" in result.output
    assert "format" in result.output.lower()


def test_migrate_help() -> None:
    result = runner.invoke(app, ["migrate", "--help"])
    assert result.exit_code == 0
    assert "validate" in result.output


def test_profile_option() -> None:
    """Profile option exists in callback signature."""
    import inspect

    from chops.cli import main

    sig = inspect.signature(main)
    assert "profile" in sig.parameters


def test_config_option() -> None:
    """Config option exists in callback signature."""
    import inspect

    from chops.cli import main

    sig = inspect.signature(main)
    assert "config_path" in sig.parameters
