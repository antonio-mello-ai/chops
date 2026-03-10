"""Schema migration commands."""

from __future__ import annotations

import re
from datetime import datetime, timezone
from pathlib import Path

import typer
from clickhouse_connect.driver.client import Client
from rich.console import Console
from rich.table import Table

from chops.client import command, get_client, query

app = typer.Typer(no_args_is_help=True)
console = Console()

TRACKING_TABLE = "_chops_migrations"
DEFAULT_DIR = "migrations"

TEMPLATE = """-- migrate:up
-- Write your UP migration SQL here.
-- Each statement must end with a semicolon.


-- migrate:down
-- Write your DOWN migration SQL here (to revert the UP).

"""


def _get_conn(ctx: typer.Context) -> Client:
    """Build client from context."""
    obj = ctx.obj or {}
    return get_client(
        host=obj.get("host"),
        port=obj.get("port"),
        user=obj.get("user"),
        password=obj.get("password"),
        database=obj.get("database"),
    )


def _migrations_dir(directory: str) -> Path:
    """Resolve migrations directory path."""
    return Path(directory).resolve()


def _ensure_tracking_table(client: Client, database: str) -> None:
    """Create the migrations tracking table if it doesn't exist."""
    command(
        client,
        f"""
        CREATE TABLE IF NOT EXISTS {database}.{TRACKING_TABLE} (
            version String,
            name String,
            applied_at DateTime DEFAULT now()
        )
        ENGINE = MergeTree()
        ORDER BY version
        """,
    )


def _applied_versions(client: Client, database: str) -> list[dict[str, object]]:
    """Get list of applied migrations, ordered by version."""
    return query(
        client,
        f"""
        SELECT version, name, applied_at
        FROM {database}.{TRACKING_TABLE}
        ORDER BY version
        """,
    )


def _parse_migration(path: Path) -> tuple[list[str], list[str]]:
    """Parse a migration file into up and down statement lists."""
    content = path.read_text()

    up_match = re.search(r"--\s*migrate:up\s*\n(.*?)(?=--\s*migrate:down|\Z)", content, re.DOTALL)
    down_match = re.search(r"--\s*migrate:down\s*\n(.*)", content, re.DOTALL)

    up_sql = up_match.group(1).strip() if up_match else ""
    down_sql = down_match.group(1).strip() if down_match else ""

    up_statements = _split_statements(up_sql)
    down_statements = _split_statements(down_sql)

    return up_statements, down_statements


def _split_statements(sql: str) -> list[str]:
    """Split SQL text into individual statements by semicolon."""
    statements = []
    for stmt in sql.split(";"):
        cleaned = stmt.strip()
        # Skip empty statements and comment-only lines
        if cleaned and not all(line.strip().startswith("--") for line in cleaned.splitlines()):
            statements.append(cleaned)
    return statements


def _discover_migrations(directory: Path) -> list[Path]:
    """Find all migration files sorted by version."""
    if not directory.exists():
        return []
    return sorted(directory.glob("*.sql"))


def _version_from_path(path: Path) -> str:
    """Extract version (timestamp prefix) from migration filename."""
    return path.stem.split("_", 1)[0]


def _name_from_path(path: Path) -> str:
    """Extract human-readable name from migration filename."""
    parts = path.stem.split("_", 1)
    return parts[1] if len(parts) > 1 else path.stem


@app.command()
def init(
    directory: str = typer.Option(DEFAULT_DIR, "--dir", "-d", help="Migrations directory"),
) -> None:
    """Initialize migrations directory."""
    migrations_dir = _migrations_dir(directory)

    if migrations_dir.exists():
        console.print(f"Directory already exists: {migrations_dir}")
        return

    migrations_dir.mkdir(parents=True)
    console.print(f"[green]Created migrations directory:[/green] {migrations_dir}")


@app.command()
def new(
    name: str = typer.Argument(help="Migration name (e.g. 'create_users_table')"),
    directory: str = typer.Option(DEFAULT_DIR, "--dir", "-d", help="Migrations directory"),
) -> None:
    """Create a new migration file."""
    migrations_dir = _migrations_dir(directory)

    if not migrations_dir.exists():
        console.print(f"[red]Migrations directory not found: {migrations_dir}[/red]")
        console.print("Run [bold]chops migrate init[/bold] first.")
        raise typer.Exit(1)

    # Sanitize name
    safe_name = re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")
    timestamp = datetime.now(tz=timezone.utc).strftime("%Y%m%d%H%M%S")
    filename = f"{timestamp}_{safe_name}.sql"

    filepath = migrations_dir / filename
    filepath.write_text(TEMPLATE)

    console.print(f"[green]Created:[/green] {filepath}")


@app.command()
def status(
    ctx: typer.Context,
    directory: str = typer.Option(DEFAULT_DIR, "--dir", "-d", help="Migrations directory"),
) -> None:
    """Show migration status: applied vs pending."""
    client = _get_conn(ctx)
    obj = ctx.obj or {}
    database = obj.get("database") or "default"

    _ensure_tracking_table(client, database)

    applied = _applied_versions(client, database)
    applied_set = {r["version"] for r in applied}

    migrations_dir = _migrations_dir(directory)
    all_files = _discover_migrations(migrations_dir)

    if not all_files and not applied:
        console.print("No migrations found.")
        return

    table = Table(title="Migration Status")
    table.add_column("Version", style="dim")
    table.add_column("Name", style="bold")
    table.add_column("Status")
    table.add_column("Applied At")

    for f in all_files:
        version = _version_from_path(f)
        name = _name_from_path(f)

        if version in applied_set:
            applied_row = next(r for r in applied if r["version"] == version)
            table.add_row(
                version,
                name,
                "[green]applied[/green]",
                str(applied_row["applied_at"]),
            )
        else:
            table.add_row(version, name, "[yellow]pending[/yellow]", "")

    # Show applied migrations that no longer have files (orphaned)
    file_versions = {_version_from_path(f) for f in all_files}
    for row in applied:
        if row["version"] not in file_versions:
            table.add_row(
                str(row["version"]),
                str(row["name"]),
                "[red]orphaned[/red]",
                str(row["applied_at"]),
            )

    console.print(table)

    pending = [f for f in all_files if _version_from_path(f) not in applied_set]
    console.print(f"\n{len(applied)} applied, {len(pending)} pending")


@app.command()
def up(
    ctx: typer.Context,
    directory: str = typer.Option(DEFAULT_DIR, "--dir", "-d", help="Migrations directory"),
    steps: int = typer.Option(0, "--steps", "-n", help="Apply N migrations (0 = all pending)"),
) -> None:
    """Apply pending migrations."""
    client = _get_conn(ctx)
    obj = ctx.obj or {}
    database = obj.get("database") or "default"

    _ensure_tracking_table(client, database)

    applied = _applied_versions(client, database)
    applied_set = {r["version"] for r in applied}

    migrations_dir = _migrations_dir(directory)
    all_files = _discover_migrations(migrations_dir)
    pending = [f for f in all_files if _version_from_path(f) not in applied_set]

    if not pending:
        console.print("[green]No pending migrations.[/green]")
        return

    if steps > 0:
        pending = pending[:steps]

    for migration_file in pending:
        version = _version_from_path(migration_file)
        name = _name_from_path(migration_file)

        console.print(f"Applying [bold]{version}_{name}[/bold]...", end=" ")

        up_statements, _ = _parse_migration(migration_file)

        if not up_statements:
            console.print("[yellow]skipped (empty)[/yellow]")
            continue

        try:
            for stmt in up_statements:
                command(client, stmt)

            # Record migration
            command(
                client,
                f"INSERT INTO {database}.{TRACKING_TABLE} (version, name) "
                f"VALUES ('{version}', '{name}')",
            )
            console.print("[green]done[/green]")
        except Exception as e:
            console.print("[red]FAILED[/red]")
            console.print(f"  [red]{e}[/red]")
            console.print(
                "\n[yellow]Migration partially applied. "
                "ClickHouse does not support DDL transactions.[/yellow]"
            )
            console.print(
                "Review the migration and fix manually, "
                "then run [bold]chops migrate up[/bold] to continue."
            )
            raise typer.Exit(1) from None

    console.print(f"\n[green]{len(pending)} migration(s) applied.[/green]")


@app.command()
def down(
    ctx: typer.Context,
    directory: str = typer.Option(DEFAULT_DIR, "--dir", "-d", help="Migrations directory"),
    steps: int = typer.Option(1, "--steps", "-n", help="Revert N migrations (default: 1)"),
    confirm: bool = typer.Option(False, "--confirm", help="Required to execute"),
) -> None:
    """Revert applied migrations (most recent first)."""
    if not confirm:
        console.print("[red]Destructive operation.[/red] Pass --confirm to execute.")
        raise typer.Exit(1)

    client = _get_conn(ctx)
    obj = ctx.obj or {}
    database = obj.get("database") or "default"

    _ensure_tracking_table(client, database)

    applied = _applied_versions(client, database)

    if not applied:
        console.print("[green]No applied migrations to revert.[/green]")
        return

    # Revert most recent first
    to_revert = list(reversed(applied))[:steps]

    migrations_dir = _migrations_dir(directory)
    all_files = {_version_from_path(f): f for f in _discover_migrations(migrations_dir)}

    for row in to_revert:
        version = str(row["version"])
        name = str(row["name"])

        console.print(f"Reverting [bold]{version}_{name}[/bold]...", end=" ")

        migration_file = all_files.get(version)
        if not migration_file:
            console.print("[red]FAILED — migration file not found[/red]")
            raise typer.Exit(1)

        _, down_statements = _parse_migration(migration_file)

        if not down_statements:
            console.print("[yellow]skipped (no down section)[/yellow]")
            # Still remove from tracking
            command(
                client,
                f"ALTER TABLE {database}.{TRACKING_TABLE} DELETE WHERE version = '{version}'",
            )
            continue

        try:
            for stmt in down_statements:
                command(client, stmt)

            command(
                client,
                f"ALTER TABLE {database}.{TRACKING_TABLE} DELETE WHERE version = '{version}'",
            )
            console.print("[green]done[/green]")
        except Exception as e:
            console.print("[red]FAILED[/red]")
            console.print(f"  [red]{e}[/red]")
            console.print(
                "\n[yellow]Revert partially applied. "
                "ClickHouse does not support DDL transactions.[/yellow]"
            )
            raise typer.Exit(1) from None

    console.print(f"\n[green]{len(to_revert)} migration(s) reverted.[/green]")
