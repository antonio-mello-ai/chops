"""Data quality commands."""

from __future__ import annotations

import json as json_mod

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


def _resolve_table(table: str, ctx_database: str | None) -> tuple[str, str]:
    """Resolve 'db.table' or 'table' into (database, table)."""
    if "." in table:
        db, tbl = table.split(".", 1)
        return db, tbl
    return ctx_database or "default", table


@app.command()
def profile(
    ctx: typer.Context,
    table: str = typer.Argument(help="Table to profile (e.g. 'mydb.events' or 'events')"),
    output: str = typer.Option("table", "--output", "-o", help="Output format: table, json"),
    sample: int | None = typer.Option(
        None,
        "--sample",
        "-s",
        help="Sample N rows (faster for huge tables)",
    ),
) -> None:
    """Profile a table: row count, null rates, cardinality, min/max per column."""
    client = _get_conn(ctx)
    obj = ctx.obj or {}
    db, tbl = _resolve_table(table, obj.get("database"))

    # Get columns
    cols = query(
        client,
        f"""
        SELECT name, type
        FROM system.columns
        WHERE database = '{db}' AND table = '{tbl}'
        ORDER BY position
    """,
    )

    if not cols:
        console.print(f"[red]Table '{db}.{tbl}' not found or has no columns.[/red]")
        raise typer.Exit(1)

    # Row count
    count_result = query(client, f"SELECT count() AS c FROM {db}.{tbl}")
    total_rows = int(count_result[0]["c"]) if count_result else 0

    # Build profiling query
    select_parts: list[str] = []
    for col in cols:
        name = col["name"]
        col_type = col["type"]
        escaped = f"`{name}`"

        select_parts.append(f"countIf({escaped} IS NULL) AS `{name}__nulls`")
        select_parts.append(f"uniq({escaped}) AS `{name}__cardinality`")

        # Min/max only for numeric and date types
        if any(t in col_type for t in ("Int", "UInt", "Float", "Decimal", "Date", "DateTime")):
            select_parts.append(f"min({escaped}) AS `{name}__min`")
            select_parts.append(f"max({escaped}) AS `{name}__max`")

    source = f"(SELECT * FROM {db}.{tbl} LIMIT {sample})" if sample else f"{db}.{tbl}"

    profile_sql = f"SELECT {', '.join(select_parts)} FROM {source}"
    profile_result = query(client, profile_sql)
    stats = profile_result[0] if profile_result else {}

    row_base = sample if sample else total_rows

    # Build results
    results: list[dict[str, object]] = []
    for col in cols:
        name = col["name"]
        nulls = int(stats.get(f"{name}__nulls", 0))
        cardinality = int(stats.get(f"{name}__cardinality", 0))
        null_pct = round(nulls / row_base * 100, 1) if row_base > 0 else 0.0

        entry: dict[str, object] = {
            "column": name,
            "type": col["type"],
            "null_count": nulls,
            "null_pct": null_pct,
            "cardinality": cardinality,
        }

        min_val = stats.get(f"{name}__min")
        max_val = stats.get(f"{name}__max")
        if min_val is not None:
            entry["min"] = min_val
            entry["max"] = max_val

        results.append(entry)

    if output == "json":
        payload = {"table": f"{db}.{tbl}", "rows": total_rows, "columns": results}
        typer.echo(json_mod.dumps(payload, indent=2, default=str))
        return

    # Rich table output
    t = Table(title=f"Profile: {db}.{tbl} ({total_rows:,} rows)")
    t.add_column("Column", style="bold")
    t.add_column("Type", style="dim")
    t.add_column("Nulls", justify="right")
    t.add_column("Null %", justify="right")
    t.add_column("Cardinality", justify="right")
    t.add_column("Min", justify="right")
    t.add_column("Max", justify="right")

    for r in results:
        null_style = "red" if r["null_pct"] > 10 else ""  # type: ignore[operator]
        t.add_row(
            str(r["column"]),
            str(r["type"]),
            f"{r['null_count']:,}",
            f"[{null_style}]{r['null_pct']}%[/{null_style}]" if null_style else f"{r['null_pct']}%",
            f"{r['cardinality']:,}",
            str(r.get("min", "")),
            str(r.get("max", "")),
        )

    console.print(t)


@app.command()
def freshness(
    ctx: typer.Context,
    table: str = typer.Argument(help="Table to check (e.g. 'mydb.events' or 'events')"),
    column: str | None = typer.Option(
        None,
        "--column",
        "-c",
        help="DateTime column to check (auto-detected if not provided)",
    ),
    warn_minutes: int = typer.Option(60, "--warn", help="Warning threshold in minutes"),
    critical_minutes: int = typer.Option(
        1440,
        "--critical",
        help="Critical threshold in minutes (default 24h)",
    ),
) -> None:
    """Check data freshness — time since last row was inserted."""
    client = _get_conn(ctx)
    obj = ctx.obj or {}
    db, tbl = _resolve_table(table, obj.get("database"))

    # Auto-detect datetime column
    if not column:
        dt_cols = query(
            client,
            f"""
            SELECT name FROM system.columns
            WHERE database = '{db}' AND table = '{tbl}'
                AND type LIKE '%DateTime%'
            ORDER BY position
            LIMIT 1
        """,
        )
        if not dt_cols:
            msg = f"No DateTime column found in {db}.{tbl}. Use --column to specify."
            console.print(f"[red]{msg}[/red]")
            raise typer.Exit(1)
        column = dt_cols[0]["name"]

    result = query(
        client,
        f"""
        SELECT
            max(`{column}`) AS latest,
            dateDiff('minute', max(`{column}`), now()) AS minutes_ago
        FROM {db}.{tbl}
    """,
    )

    if not result or result[0].get("latest") is None:
        console.print(f"[red]Table {db}.{tbl} is empty or column '{column}' has no data.[/red]")
        raise typer.Exit(2)

    row = result[0]
    minutes = int(row["minutes_ago"])
    latest = row["latest"]

    if minutes >= critical_minutes:
        style = "bold red"
        status = "CRITICAL"
        exit_code = 2
    elif minutes >= warn_minutes:
        style = "bold yellow"
        status = "WARNING"
        exit_code = 1
    else:
        style = "bold green"
        status = "OK"
        exit_code = 0

    hours, mins = divmod(minutes, 60)
    age_str = f"{hours}h {mins}m" if hours > 0 else f"{mins}m"

    console.print(f"[{style}]{status}[/{style}] — {db}.{tbl}.{column}")
    console.print(f"  Latest: {latest}")
    console.print(f"  Age: {age_str} ago")

    if exit_code > 0:
        raise typer.Exit(exit_code)


@app.command()
def check(
    ctx: typer.Context,
    table: str = typer.Argument(help="Table to check (e.g. 'mydb.events' or 'events')"),
    max_null_pct: float = typer.Option(
        5.0,
        "--max-null-pct",
        help="Max null percentage before failing",
    ),
    min_rows: int | None = typer.Option(None, "--min-rows", help="Minimum expected row count"),
    output: str = typer.Option("table", "--output", "-o", help="Output format: table, json"),
) -> None:
    """Run data quality checks on a table. Returns non-zero exit code on failure."""
    client = _get_conn(ctx)
    obj = ctx.obj or {}
    db, tbl = _resolve_table(table, obj.get("database"))

    # Row count
    count_result = query(client, f"SELECT count() AS c FROM {db}.{tbl}")
    total_rows = int(count_result[0]["c"]) if count_result else 0

    # Columns
    cols = query(
        client,
        f"""
        SELECT name, type
        FROM system.columns
        WHERE database = '{db}' AND table = '{tbl}'
        ORDER BY position
    """,
    )

    if not cols:
        console.print(f"[red]Table '{db}.{tbl}' not found.[/red]")
        raise typer.Exit(2)

    # Null checks
    select_parts = [f"countIf(`{c['name']}` IS NULL) AS `{c['name']}__nulls`" for c in cols]
    null_result = query(client, f"SELECT {', '.join(select_parts)} FROM {db}.{tbl}")
    null_stats = null_result[0] if null_result else {}

    failures: list[dict[str, object]] = []
    passes: list[dict[str, object]] = []

    # Check min rows
    if min_rows is not None and total_rows < min_rows:
        failures.append(
            {
                "check": "min_rows",
                "column": "-",
                "expected": f">= {min_rows:,}",
                "actual": f"{total_rows:,}",
            }
        )
    else:
        passes.append(
            {
                "check": "row_count",
                "column": "-",
                "expected": f">= {min_rows:,}" if min_rows else "any",
                "actual": f"{total_rows:,}",
            }
        )

    # Check null rates
    for col in cols:
        name = col["name"]
        nulls = int(null_stats.get(f"{name}__nulls", 0))
        null_pct = round(nulls / total_rows * 100, 1) if total_rows > 0 else 0.0

        entry: dict[str, object] = {
            "check": "null_rate",
            "column": name,
            "expected": f"<= {max_null_pct}%",
            "actual": f"{null_pct}%",
        }

        if null_pct > max_null_pct:
            failures.append(entry)
        else:
            passes.append(entry)

    if output == "json":
        typer.echo(
            json_mod.dumps(
                {
                    "table": f"{db}.{tbl}",
                    "rows": total_rows,
                    "passed": len(passes),
                    "failed": len(failures),
                    "failures": failures,
                },
                indent=2,
                default=str,
            )
        )
    else:
        total_checks = len(passes) + len(failures)
        status = "[green]PASSED[/green]" if not failures else "[red]FAILED[/red]"
        console.print(f"\n{status} — {db}.{tbl}: {len(passes)}/{total_checks} checks passed\n")

        if failures:
            t = Table(title="Failed Checks", border_style="red")
            t.add_column("Check", style="bold")
            t.add_column("Column")
            t.add_column("Expected")
            t.add_column("Actual", style="red")
            for f in failures:
                t.add_row(str(f["check"]), str(f["column"]), str(f["expected"]), str(f["actual"]))
            console.print(t)

    if failures:
        raise typer.Exit(1)
