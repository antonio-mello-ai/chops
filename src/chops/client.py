"""ClickHouse connection client."""

from __future__ import annotations

import os
from typing import Any

import clickhouse_connect
from clickhouse_connect.driver.client import Client


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


def command(client: Client, sql: str) -> None:
    """Execute a DDL/DML command (no result set)."""
    client.command(sql)
