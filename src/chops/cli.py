"""CLI entry point for chops."""

from __future__ import annotations

import csv
import io
import json
import sys

import typer
from rich.console import Console
from rich.table import Table

from chops import __version__
from chops.client import get_client, query
from chops.commands import dq, health, migrate
from chops.config import get_profile, load_config

app = typer.Typer(
    name="chops",
    help="ClickHouse Operations CLI — health checks, data quality, and observability.",
    no_args_is_help=True,
)

app.add_typer(health.app, name="health", help="Cluster health and observability commands.")
app.add_typer(dq.app, name="dq", help="Data quality profiling and checks.")
app.add_typer(migrate.app, name="migrate", help="Schema migration management.")

console = Console()


@app.command()
def version() -> None:
    """Show chops version."""
    typer.echo(f"chops {__version__}")


@app.command(name="query")
def query_cmd(
    ctx: typer.Context,
    sql: str = typer.Argument(..., help="SQL query to execute"),
    output_format: str = typer.Option(
        "table", "--format", "-f", help="Output format: table, json, csv"
    ),
) -> None:
    """Run an ad-hoc SQL query and display results."""
    obj = ctx.obj or {}
    client = get_client(
        host=obj.get("host"),
        port=obj.get("port"),
        user=obj.get("user"),
        password=obj.get("password"),
        database=obj.get("database"),
        secure=obj.get("secure"),
    )

    rows = query(client, sql)

    if not rows:
        console.print("[dim]No results.[/dim]")
        return

    if output_format == "json":
        typer.echo(json.dumps(rows, indent=2, default=str))
    elif output_format == "csv":
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
        sys.stdout.write(output.getvalue())
    else:
        table = Table()
        for col in rows[0]:
            table.add_column(col)
        for r in rows:
            table.add_row(*[str(v) for v in r.values()])
        console.print(table)


@app.callback()
def main(
    ctx: typer.Context,
    host: str | None = typer.Option(
        None,
        "--host",
        "-h",
        envvar="CLICKHOUSE_HOST",
        help="ClickHouse host",
    ),
    port: int | None = typer.Option(
        None,
        "--port",
        "-p",
        envvar="CLICKHOUSE_PORT",
        help="ClickHouse HTTP port",
    ),
    user: str | None = typer.Option(
        None,
        "--user",
        "-u",
        envvar="CLICKHOUSE_USER",
        help="ClickHouse user",
    ),
    password: str | None = typer.Option(
        None,
        "--password",
        envvar="CLICKHOUSE_PASSWORD",
        help="ClickHouse password",
    ),
    database: str | None = typer.Option(
        None,
        "--database",
        "-d",
        envvar="CLICKHOUSE_DATABASE",
        help="Default database",
    ),
    profile: str | None = typer.Option(
        None,
        "--profile",
        envvar="CHOPS_PROFILE",
        help="Config profile name from chops.toml",
    ),
    config_path: str | None = typer.Option(
        None,
        "--config",
        help="Path to config file (default: auto-discover chops.toml)",
    ),
) -> None:
    """Global connection options."""
    ctx.ensure_object(dict)

    # Load config file and profile
    from pathlib import Path

    cfg_path = Path(config_path) if config_path else None
    config = load_config(cfg_path)
    profile_settings = get_profile(config, profile)

    # CLI flags > env vars (already resolved by typer) > config file values
    ctx.obj["host"] = host or profile_settings.get("host")
    ctx.obj["port"] = port or profile_settings.get("port")
    ctx.obj["user"] = user or profile_settings.get("user")
    ctx.obj["password"] = password if password is not None else profile_settings.get("password")
    ctx.obj["database"] = database if database is not None else profile_settings.get("database")
    ctx.obj["secure"] = profile_settings.get("secure")
