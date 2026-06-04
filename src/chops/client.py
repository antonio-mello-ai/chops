"""ClickHouse connection client."""

from __future__ import annotations

import os
import re
from typing import Any

import clickhouse_connect
from clickhouse_connect.driver.client import Client

# ClickHouse unquoted identifier rule: starts with a letter or underscore,
# followed by letters, digits, or underscores. We deliberately stay strict
# (no dollar signs, no leading digits) so that anything outside this set is
# rejected rather than silently spliced into a query.
_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def quote_identifier(name: str) -> str:
    """Validate and backtick-quote a SQL identifier (database/table/column name).

    Identifiers cannot be passed as bound query parameters, so any user-supplied
    db/table/column name must be validated against ClickHouse's identifier rules
    and quoted before interpolation. Raises ``ValueError`` on anything that is not
    a plain identifier, closing the SQL-injection vector.
    """
    if not isinstance(name, str) or not _IDENTIFIER_RE.match(name):
        msg = f"Invalid SQL identifier: {name!r}"
        raise ValueError(msg)
    return f"`{name}`"


def get_client(
    host: str | None = None,
    port: int | None = None,
    user: str | None = None,
    password: str | None = None,
    database: str | None = None,
    secure: bool | None = None,
) -> Client:
    """Create a ClickHouse client from explicit args or environment variables."""
    return clickhouse_connect.get_client(
        host=host or os.getenv("CLICKHOUSE_HOST", "localhost"),
        port=port or int(os.getenv("CLICKHOUSE_PORT", "8123")),
        username=user or os.getenv("CLICKHOUSE_USER", "default"),
        password=password if password is not None else os.getenv("CLICKHOUSE_PASSWORD", ""),
        database=database if database is not None else os.getenv("CLICKHOUSE_DATABASE", "default"),
        secure=(
            secure
            if secure is not None
            else os.getenv("CLICKHOUSE_SECURE", "false").lower() == "true"
        ),
        connect_timeout=10,
    )


def query(client: Client, sql: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    """Execute a query and return results as list of dicts."""
    result = client.query(sql, parameters=params or {})
    columns = result.column_names
    return [dict(zip(columns, row, strict=False)) for row in result.result_rows]


def command(client: Client, sql: str, params: dict[str, Any] | None = None) -> None:
    """Execute a DDL/DML command (no result set)."""
    client.command(sql, parameters=params or {})
