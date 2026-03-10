"""Health and observability commands."""

from __future__ import annotations

import typer
from clickhouse_connect.driver.client import Client
from rich.console import Console
from rich.table import Table

from chops.client import get_client, query

app = typer.Typer(no_args_is_help=True)
console = Console()


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


@app.command()
def summary(ctx: typer.Context) -> None:
    """Show cluster health summary: version, uptime, databases, tables, parts, merges, queries."""
    client = _get_conn(ctx)

    # Server info
    info = query(client, "SELECT version() AS version, uptime() AS uptime_seconds")
    row = info[0] if info else {}
    ver = row.get("version", "?")
    uptime_s = int(row.get("uptime_seconds", 0))
    days, rem = divmod(uptime_s, 86400)
    hours, rem = divmod(rem, 3600)
    mins, _ = divmod(rem, 60)
    uptime_str = f"{days}d {hours}h {mins}m"

    # Counts
    sys_dbs = "('system', 'INFORMATION_SCHEMA', 'information_schema')"
    db_count = query(
        client, f"SELECT count() AS c FROM system.databases WHERE name NOT IN {sys_dbs}",
    )
    table_count = query(
        client, f"SELECT count() AS c FROM system.tables WHERE database NOT IN {sys_dbs}",
    )
    parts_count = query(client, "SELECT count() AS c FROM system.parts WHERE active")
    merges_count = query(client, "SELECT count() AS c FROM system.merges")
    queries_count = query(
        client, "SELECT count() AS c FROM system.processes WHERE is_initial_query",
    )

    # Disk usage
    disk = query(client, """
        SELECT
            formatReadableSize(sum(bytes_on_disk)) AS total_size,
            sum(rows) AS total_rows
        FROM system.parts
        WHERE active
    """)

    table = Table(title="ClickHouse Health Summary", show_header=False, border_style="dim")
    table.add_column("Metric", style="bold")
    table.add_column("Value")

    table.add_row("Version", str(ver))
    table.add_row("Uptime", uptime_str)
    table.add_row("Databases", str(db_count[0]["c"] if db_count else 0))
    table.add_row("Tables", str(table_count[0]["c"] if table_count else 0))
    table.add_row("Active parts", str(parts_count[0]["c"] if parts_count else 0))
    table.add_row("Running merges", str(merges_count[0]["c"] if merges_count else 0))
    table.add_row("Active queries", str(queries_count[0]["c"] if queries_count else 0))
    if disk:
        table.add_row("Total data size", str(disk[0].get("total_size", "?")))
        table.add_row("Total rows", f"{int(disk[0].get('total_rows', 0)):,}")

    console.print(table)


@app.command(name="table-sizes")
def table_sizes(
    ctx: typer.Context,
    limit: int = typer.Option(20, "--limit", "-n", help="Number of tables to show"),
    database: str | None = typer.Option(None, "--database", "-d", help="Filter by database"),
) -> None:
    """Show disk usage by table, sorted by size."""
    client = _get_conn(ctx)

    where = "WHERE database NOT IN ('system', 'INFORMATION_SCHEMA', 'information_schema')"
    if database:
        where += f" AND database = '{database}'"

    rows = query(client, f"""
        SELECT
            database,
            name AS table_name,
            engine,
            formatReadableSize(total_bytes) AS size,
            total_bytes,
            formatReadableQuantity(total_rows) AS rows,
            total_rows AS raw_rows,
            partition_count
        FROM (
            SELECT
                database,
                table AS name,
                engine,
                sum(bytes_on_disk) AS total_bytes,
                sum(rows) AS total_rows,
                count(DISTINCT partition) AS partition_count
            FROM system.parts
            {where}
            AND active
            GROUP BY database, table, engine
        )
        ORDER BY total_bytes DESC
        LIMIT {limit}
    """)

    table = Table(title=f"Top {limit} Tables by Size")
    table.add_column("Database", style="cyan")
    table.add_column("Table", style="bold")
    table.add_column("Engine")
    table.add_column("Size", justify="right", style="green")
    table.add_column("Rows", justify="right")
    table.add_column("Partitions", justify="right")

    for r in rows:
        table.add_row(
            r["database"], r["table_name"], r["engine"],
            r["size"], r["rows"], str(r["partition_count"]),
        )

    console.print(table)


@app.command(name="slow-queries")
def slow_queries(
    ctx: typer.Context,
    limit: int = typer.Option(10, "--limit", "-n", help="Number of queries to show"),
    hours: int = typer.Option(24, "--hours", help="Look back N hours"),
) -> None:
    """Show slowest queries from the query log."""
    client = _get_conn(ctx)

    rows = query(client, f"""
        SELECT
            type,
            query_duration_ms / 1000 AS duration_s,
            formatReadableSize(read_bytes) AS read_size,
            read_rows,
            formatReadableSize(memory_usage) AS peak_memory,
            user,
            substring(query, 1, 120) AS query_preview
        FROM system.query_log
        WHERE event_time > now() - INTERVAL {hours} HOUR
            AND type IN ('QueryFinish', 'ExceptionWhileProcessing')
            AND query_kind = 'Select'
            AND is_initial_query
        ORDER BY query_duration_ms DESC
        LIMIT {limit}
    """)

    table = Table(title=f"Top {limit} Slow Queries (last {hours}h)")
    table.add_column("#", justify="right", style="dim")
    table.add_column("Duration", justify="right", style="bold red")
    table.add_column("Read", justify="right")
    table.add_column("Rows", justify="right")
    table.add_column("Memory", justify="right")
    table.add_column("User")
    table.add_column("Query", max_width=80)

    for i, r in enumerate(rows, 1):
        dur = f"{r['duration_s']:.1f}s"
        table.add_row(
            str(i), dur, r["read_size"], f"{int(r['read_rows']):,}",
            r["peak_memory"], r["user"], r["query_preview"],
        )

    console.print(table)


@app.command()
def merges(ctx: typer.Context) -> None:
    """Show currently running merges."""
    client = _get_conn(ctx)

    rows = query(client, """
        SELECT
            database,
            table,
            round(progress * 100, 1) AS progress_pct,
            round(elapsed, 1) AS elapsed_s,
            num_parts,
            formatReadableSize(total_size_bytes_compressed) AS size,
            formatReadableSize(bytes_read_uncompressed) AS bytes_read,
            formatReadableSize(bytes_written_uncompressed) AS bytes_written
        FROM system.merges
        ORDER BY elapsed DESC
    """)

    if not rows:
        console.print("[green]No active merges.[/green]")
        return

    table = Table(title=f"Active Merges ({len(rows)})")
    table.add_column("Database", style="cyan")
    table.add_column("Table", style="bold")
    table.add_column("Progress", justify="right")
    table.add_column("Elapsed", justify="right")
    table.add_column("Parts", justify="right")
    table.add_column("Size", justify="right")

    for r in rows:
        table.add_row(
            r["database"], r["table"], f"{r['progress_pct']}%",
            f"{r['elapsed_s']}s", str(r["num_parts"]), r["size"],
        )

    console.print(table)


@app.command(name="running-queries")
def running_queries(ctx: typer.Context) -> None:
    """Show currently running queries."""
    client = _get_conn(ctx)

    rows = query(client, """
        SELECT
            query_id,
            user,
            round(elapsed, 1) AS elapsed_s,
            formatReadableSize(read_bytes) AS read_size,
            read_rows,
            formatReadableSize(memory_usage) AS memory,
            substring(query, 1, 100) AS query_preview
        FROM system.processes
        WHERE is_initial_query
        ORDER BY elapsed DESC
    """)

    if not rows:
        console.print("[green]No running queries.[/green]")
        return

    table = Table(title=f"Running Queries ({len(rows)})")
    table.add_column("Elapsed", justify="right", style="bold")
    table.add_column("User")
    table.add_column("Read", justify="right")
    table.add_column("Memory", justify="right")
    table.add_column("Query", max_width=80)

    for r in rows:
        table.add_row(
            f"{r['elapsed_s']}s", r["user"], r["read_size"],
            r["memory"], r["query_preview"],
        )

    console.print(table)
