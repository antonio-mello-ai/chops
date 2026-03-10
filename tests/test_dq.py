"""Tests for data quality commands."""

from __future__ import annotations

from chops.commands.dq import _resolve_table


def test_resolve_table_with_db() -> None:
    db, tbl = _resolve_table("mydb.events", None)
    assert db == "mydb"
    assert tbl == "events"


def test_resolve_table_without_db_default() -> None:
    db, tbl = _resolve_table("events", None)
    assert db == "default"
    assert tbl == "events"


def test_resolve_table_without_db_context() -> None:
    db, tbl = _resolve_table("events", "analytics")
    assert db == "analytics"
    assert tbl == "events"


def test_resolve_table_dotted_overrides_context() -> None:
    db, tbl = _resolve_table("mydb.events", "analytics")
    assert db == "mydb"
    assert tbl == "events"
