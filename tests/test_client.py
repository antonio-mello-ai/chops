"""Tests for the client helpers, including SQL-identifier hardening."""

from __future__ import annotations

import pytest

from chops.client import quote_identifier


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        ("events", "`events`"),
        ("my_table", "`my_table`"),
        ("_chops_dq_snapshots", "`_chops_dq_snapshots`"),
        ("Col123", "`Col123`"),
        ("UPPER", "`UPPER`"),
    ],
)
def test_quote_identifier_valid(name: str, expected: str) -> None:
    """Valid identifiers are backtick-quoted unchanged."""
    assert quote_identifier(name) == expected


@pytest.mark.parametrize(
    "name",
    [
        "foo'; DROP TABLE users; --",  # classic injection
        "tbl`; SELECT 1",  # backtick break-out
        "db.table",  # dotted: must be split by caller, not passed whole
        "name with spaces",
        "1leading_digit",  # cannot start with a digit
        "has-dash",
        "weird$char",
        "",  # empty
        "tab\tname",
        "new\nline",
    ],
)
def test_quote_identifier_rejects_malicious(name: str) -> None:
    """Anything outside the plain-identifier rule raises, closing injection."""
    with pytest.raises(ValueError, match="Invalid SQL identifier"):
        quote_identifier(name)


def test_quote_identifier_does_not_interpolate_payload() -> None:
    """A malicious payload never reaches the SQL string — it raises instead."""
    payload = "events`; DROP TABLE system.tables; --"
    with pytest.raises(ValueError):
        quote_identifier(payload)
