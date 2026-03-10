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
