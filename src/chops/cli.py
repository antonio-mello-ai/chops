"""CLI entry point for chops."""

from __future__ import annotations

import typer

from chops import __version__
from chops.commands import dq, health

app = typer.Typer(
    name="chops",
    help="ClickHouse Operations CLI — health checks, data quality, and observability.",
    no_args_is_help=True,
)

app.add_typer(health.app, name="health", help="Cluster health and observability commands.")
app.add_typer(dq.app, name="dq", help="Data quality profiling and checks.")


@app.command()
def version() -> None:
    """Show chops version."""
    typer.echo(f"chops {__version__}")


@app.callback()
def main(
    ctx: typer.Context,
    host: str | None = typer.Option(
        None, "--host", "-h", envvar="CLICKHOUSE_HOST", help="ClickHouse host",
    ),
    port: int | None = typer.Option(
        None, "--port", "-p", envvar="CLICKHOUSE_PORT", help="ClickHouse HTTP port",
    ),
    user: str | None = typer.Option(
        None, "--user", "-u", envvar="CLICKHOUSE_USER", help="ClickHouse user",
    ),
    password: str | None = typer.Option(
        None, "--password", envvar="CLICKHOUSE_PASSWORD", help="ClickHouse password",
    ),
    database: str | None = typer.Option(
        None, "--database", "-d", envvar="CLICKHOUSE_DATABASE", help="Default database",
    ),
) -> None:
    """Global connection options."""
    ctx.ensure_object(dict)
    ctx.obj["host"] = host
    ctx.obj["port"] = port
    ctx.obj["user"] = user
    ctx.obj["password"] = password
    ctx.obj["database"] = database
