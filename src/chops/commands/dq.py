"""Data quality commands."""

from __future__ import annotations

import json as json_mod

import typer
from clickhouse_connect.driver.client import Client
from rich.console import Console
from rich.table import Table

from chops.client import command, get_client, query

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


SNAPSHOT_TABLE = "_chops_dq_snapshots"


def _build_profile(
    client: Client,
    db: str,
    tbl: str,
    sample: int | None = None,
) -> tuple[int, list[dict[str, object]]]:
    """Profile a table and return (total_rows, column_profiles)."""
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
        return 0, []

    count_result = query(client, f"SELECT count() AS c FROM {db}.{tbl}")
    total_rows = int(count_result[0]["c"]) if count_result else 0

    select_parts: list[str] = []
    for col in cols:
        name = col["name"]
        col_type = col["type"]
        escaped = f"`{name}`"

        select_parts.append(f"countIf({escaped} IS NULL) AS `{name}__nulls`")
        select_parts.append(f"uniq({escaped}) AS `{name}__cardinality`")

        if any(t in col_type for t in ("Int", "UInt", "Float", "Decimal", "Date", "DateTime")):
            select_parts.append(f"min({escaped}) AS `{name}__min`")
            select_parts.append(f"max({escaped}) AS `{name}__max`")

    source = f"(SELECT * FROM {db}.{tbl} LIMIT {sample})" if sample else f"{db}.{tbl}"

    profile_result = query(client, f"SELECT {', '.join(select_parts)} FROM {source}")
    stats = profile_result[0] if profile_result else {}

    row_base = sample if sample else total_rows

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

    return total_rows, results


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

    total_rows, results = _build_profile(client, db, tbl, sample)

    if not results:
        console.print(f"[red]Table '{db}.{tbl}' not found or has no columns.[/red]")
        raise typer.Exit(1)

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


def _ensure_snapshot_table(client: Client, database: str) -> None:
    """Create the DQ snapshots table if it doesn't exist."""
    command(
        client,
        f"""
        CREATE TABLE IF NOT EXISTS {database}.{SNAPSHOT_TABLE} (
            table_database String,
            table_name String,
            column_name String,
            column_type String,
            null_count UInt64,
            null_pct Float64,
            cardinality UInt64,
            total_rows UInt64,
            snapshot_at DateTime DEFAULT now()
        )
        ENGINE = MergeTree()
        ORDER BY (table_database, table_name, column_name, snapshot_at)
        """,
    )


def _save_snapshot(
    client: Client,
    database: str,
    db: str,
    tbl: str,
    total_rows: int,
    results: list[dict[str, object]],
) -> None:
    """Save current profile as a snapshot."""
    for r in results:
        command(
            client,
            f"INSERT INTO {database}.{SNAPSHOT_TABLE} "
            f"(table_database, table_name, column_name, column_type, "
            f"null_count, null_pct, cardinality, total_rows) VALUES "
            f"('{db}', '{tbl}', '{r['column']}', '{r['type']}', "
            f"{r['null_count']}, {r['null_pct']}, {r['cardinality']}, {total_rows})",
        )


def _get_last_snapshot(
    client: Client,
    database: str,
    db: str,
    tbl: str,
) -> list[dict[str, object]]:
    """Get the most recent snapshot for a table."""
    return query(
        client,
        f"""
        SELECT
            column_name, column_type, null_count, null_pct,
            cardinality, total_rows, snapshot_at
        FROM {database}.{SNAPSHOT_TABLE}
        WHERE table_database = '{db}' AND table_name = '{tbl}'
            AND snapshot_at = (
                SELECT max(snapshot_at)
                FROM {database}.{SNAPSHOT_TABLE}
                WHERE table_database = '{db}' AND table_name = '{tbl}'
            )
        ORDER BY column_name
        """,
    )


@app.command()
def drift(
    ctx: typer.Context,
    table: str = typer.Argument(help="Table to check for drift (e.g. 'mydb.events')"),
    output: str = typer.Option("table", "--output", "-o", help="Output format: table, json"),
    save: bool = typer.Option(
        True,
        "--save/--no-save",
        help="Save current profile as new snapshot (default: yes)",
    ),
    cardinality_pct: float = typer.Option(
        50.0,
        "--cardinality-pct",
        help="Alert if cardinality changes by more than N%%",
    ),
    null_pct_delta: float = typer.Option(
        10.0,
        "--null-pct-delta",
        help="Alert if null percentage changes by more than N points",
    ),
) -> None:
    """Detect schema and data quality drift vs last snapshot."""
    client = _get_conn(ctx)
    obj = ctx.obj or {}
    db, tbl = _resolve_table(table, obj.get("database"))
    snapshot_db = obj.get("database") or "default"

    _ensure_snapshot_table(client, snapshot_db)

    # Get current profile
    total_rows, current = _build_profile(client, db, tbl)

    if not current:
        console.print(f"[red]Table '{db}.{tbl}' not found or has no columns.[/red]")
        raise typer.Exit(1)

    # Get last snapshot
    previous = _get_last_snapshot(client, snapshot_db, db, tbl)

    if not previous:
        console.print(f"[yellow]No previous snapshot for {db}.{tbl}. Saving baseline.[/yellow]")
        _save_snapshot(client, snapshot_db, db, tbl, total_rows, current)
        console.print("[green]Baseline snapshot saved.[/green]")
        return

    # Build lookup from previous snapshot
    prev_by_col: dict[str, dict[str, object]] = {str(r["column_name"]): r for r in previous}
    curr_by_col: dict[str, dict[str, object]] = {str(r["column"]): r for r in current}

    prev_rows = int(str(previous[0]["total_rows"])) if previous else 0
    snapshot_at = previous[0]["snapshot_at"] if previous else "?"

    # Detect changes
    changes: list[dict[str, object]] = []

    # New columns
    for col_name in curr_by_col:
        if col_name not in prev_by_col:
            changes.append(
                {
                    "column": col_name,
                    "metric": "schema",
                    "change": "new column",
                    "previous": "-",
                    "current": str(curr_by_col[col_name]["type"]),
                    "severity": "warning",
                }
            )

    # Dropped columns
    for col_name in prev_by_col:
        if col_name not in curr_by_col:
            changes.append(
                {
                    "column": col_name,
                    "metric": "schema",
                    "change": "dropped column",
                    "previous": str(prev_by_col[col_name]["column_type"]),
                    "current": "-",
                    "severity": "critical",
                }
            )

    # Compare existing columns
    for col_name, curr in curr_by_col.items():
        prev = prev_by_col.get(col_name)
        if not prev:
            continue

        # Type change
        if str(curr["type"]) != str(prev["column_type"]):
            changes.append(
                {
                    "column": col_name,
                    "metric": "type",
                    "change": "type changed",
                    "previous": str(prev["column_type"]),
                    "current": str(curr["type"]),
                    "severity": "critical",
                }
            )

        # Cardinality drift
        prev_card = int(str(prev["cardinality"]))
        curr_card = int(str(curr["cardinality"]))
        if prev_card > 0:
            card_change_pct = abs(curr_card - prev_card) / prev_card * 100
            if card_change_pct > cardinality_pct:
                direction = "increased" if curr_card > prev_card else "decreased"
                changes.append(
                    {
                        "column": col_name,
                        "metric": "cardinality",
                        "change": f"{direction} {card_change_pct:.0f}%",
                        "previous": str(prev_card),
                        "current": str(curr_card),
                        "severity": "warning",
                    }
                )

        # Null rate drift
        prev_null = float(str(prev["null_pct"]))
        curr_null = float(str(curr["null_pct"]))
        null_delta = abs(curr_null - prev_null)
        if null_delta > null_pct_delta:
            direction = "increased" if curr_null > prev_null else "decreased"
            changes.append(
                {
                    "column": col_name,
                    "metric": "null_rate",
                    "change": f"{direction} {null_delta:.1f}pp",
                    "previous": f"{prev_null}%",
                    "current": f"{curr_null}%",
                    "severity": "warning" if curr_null > prev_null else "info",
                }
            )

    # Save new snapshot
    if save:
        _save_snapshot(client, snapshot_db, db, tbl, total_rows, current)

    # Output
    has_drift = len(changes) > 0
    critical = any(c["severity"] == "critical" for c in changes)

    if output == "json":
        typer.echo(
            json_mod.dumps(
                {
                    "table": f"{db}.{tbl}",
                    "rows_current": total_rows,
                    "rows_previous": prev_rows,
                    "snapshot_at": str(snapshot_at),
                    "drift_detected": has_drift,
                    "changes": changes,
                },
                indent=2,
                default=str,
            )
        )
    else:
        if not has_drift:
            console.print(
                f"[green]No drift detected[/green] — {db}.{tbl} "
                f"({total_rows:,} rows, snapshot: {snapshot_at})"
            )
        else:
            severity_style = "red" if critical else "yellow"
            console.print(
                f"[{severity_style}]DRIFT DETECTED[/{severity_style}] — {db}.{tbl} "
                f"({prev_rows:,} → {total_rows:,} rows)\n"
            )

            t = Table(title=f"Changes since {snapshot_at}")
            t.add_column("Column", style="bold")
            t.add_column("Metric")
            t.add_column("Change")
            t.add_column("Previous", justify="right")
            t.add_column("Current", justify="right")
            t.add_column("Severity")

            for c in changes:
                sev = str(c["severity"])
                sev_style = {"critical": "bold red", "warning": "yellow", "info": "dim"}.get(
                    sev, ""
                )
                t.add_row(
                    str(c["column"]),
                    str(c["metric"]),
                    str(c["change"]),
                    str(c["previous"]),
                    str(c["current"]),
                    f"[{sev_style}]{sev}[/{sev_style}]",
                )

            console.print(t)

        if save:
            console.print("\n[dim]Snapshot saved.[/dim]")

    if critical:
        raise typer.Exit(2)
    if has_drift:
        raise typer.Exit(1)
