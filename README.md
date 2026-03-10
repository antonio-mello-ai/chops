# chops

ClickHouse Operations CLI — health checks, data quality profiling, and observability from your terminal.

No more copy-pasting system table queries. One command to check cluster health, profile data quality, or find slow queries.

## Quick Start

```bash
# Run directly with uvx (no install needed)
uvx chops health summary

# Or install with pip
pip install chops
```

## Configuration

Set environment variables or pass flags:

```bash
export CLICKHOUSE_HOST=localhost
export CLICKHOUSE_PORT=8123
export CLICKHOUSE_USER=default
export CLICKHOUSE_PASSWORD=
```

Or use flags: `chops --host myserver --user admin health summary`

## Commands

### Health & Observability

| Command | Description |
|---------|-------------|
| `chops health summary` | Cluster overview: version, uptime, databases, tables, parts, merges, queries |
| `chops health table-sizes` | Disk usage by table, sorted by size |
| `chops health slow-queries` | Top N slowest queries from query log |
| `chops health merges` | Currently running merge operations |
| `chops health running-queries` | Active queries with elapsed time and memory |

### Data Quality

| Command | Description |
|---------|-------------|
| `chops dq profile <table>` | Column-level profiling: null rates, cardinality, min/max |
| `chops dq check <table>` | Run quality checks with configurable thresholds (CI-friendly exit codes) |
| `chops dq freshness <table>` | Time since last row — OK/WARNING/CRITICAL with exit codes |

## Examples

```bash
# Quick cluster health check
chops health summary

# Find which tables are eating disk
chops health table-sizes --limit 10

# Slowest queries in the last 6 hours
chops health slow-queries --hours 6

# Profile a table's data quality
chops dq profile mydb.events

# Run quality checks in CI (non-zero exit on failure)
chops dq check mydb.events --max-null-pct 5 --min-rows 1000

# Check if a streaming table is still receiving data
chops dq freshness mydb.events --warn 60 --critical 1440

# JSON output for automation
chops dq profile mydb.events --output json
```

## CI/CD Integration

`chops dq check` and `chops dq freshness` return non-zero exit codes on failure, making them usable in CI pipelines:

```yaml
- name: Data quality gate
  run: |
    chops dq check production.orders --max-null-pct 2 --min-rows 10000
    chops dq freshness production.orders --warn 30 --critical 120
```

## Development

```bash
git clone https://github.com/antonio-mello-ai/chops.git
cd chops
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"

# Run tests
pytest

# Lint
ruff check src/ tests/

# Type check
mypy src/
```

## License

MIT
