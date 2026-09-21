# chops

ClickHouse Operations CLI — health checks, data quality profiling, schema migrations, and observability from your terminal.

No more copy-pasting system table queries. One command to check cluster health, profile data quality, manage schema migrations, or find slow queries.

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

### Config File

Create `chops.toml` (or `.chops.toml`) in your project directory, or `~/.config/chops/config.toml` for global settings:

```toml
host = "localhost"
user = "default"

[profiles.prod]
host = "prod-cluster.example.com"
user = "admin"
password = "secret"
database = "analytics"

[profiles.staging]
host = "staging.example.com"
user = "readonly"
```

Use profiles with `--profile` or the `CHOPS_PROFILE` env var:

```bash
chops --profile prod health summary
CHOPS_PROFILE=staging chops dq profile events
```

## Commands

### Health & Observability

| Command | Description |
|---------|-------------|
| `chops health summary` | Cluster overview: version, uptime, databases, tables, parts, merges, queries |
| `chops health table-sizes` | Disk usage by table, sorted by size |
| `chops health slow-queries` | Top N slowest queries from query log |
| `chops health merges` | Currently running merge operations |
| `chops health running-queries` | Active queries with elapsed time and memory |
| `chops health replication` | Replication status with lag and queue metrics |
| `chops health partitions` | Partition analysis: detect merge pressure |

### Data Quality

| Command | Description |
|---------|-------------|
| `chops dq profile <table>` | Column-level profiling: null rates, cardinality, min/max |
| `chops dq drift <table>` | Detect schema and data quality drift vs last snapshot |
| `chops dq check <table>` | Run quality checks with configurable thresholds (CI-friendly exit codes) |
| `chops dq freshness <table>` | Time since last row — OK/WARNING/CRITICAL with exit codes |
| `chops dq anomalies <table>` | Detect anomalies in daily row counts vs historical baseline |
| `chops dq compare <t1> <t2>` | Compare schema and row counts between two tables |

### Ad-hoc Query

| Command | Description |
|---------|-------------|
| `chops query <sql>` | Run any SQL query with table, JSON, or CSV output |

### Schema Migrations

| Command | Description |
|---------|-------------|
| `chops migrate init` | Create migrations directory |
| `chops migrate new <name>` | Generate a timestamped migration file with up/down sections |
| `chops migrate status` | Show applied vs pending migrations |
| `chops migrate up` | Apply pending migrations (all or `--steps N`) |
| `chops migrate down --confirm` | Revert most recent migration(s) |
| `chops migrate validate` | Validate migration files: structure, naming, content |

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

# Detect row count anomalies (z-score based)
chops dq anomalies mydb.events --days 14

# Compare schemas between tables
chops dq compare mydb.events mydb.events_v2

# Ad-hoc query with different output formats
chops query "SELECT database, count() FROM system.tables GROUP BY database"
chops query "SELECT * FROM mydb.events LIMIT 10" --format json
chops query "SELECT * FROM mydb.events LIMIT 10" --format csv

# JSON output for automation
chops dq profile mydb.events --output json

# Schema migrations
chops migrate init
chops migrate new "add_user_scores_table"
chops migrate up
chops migrate status
chops migrate down --confirm
```

Migration files use a simple format with `-- migrate:up` and `-- migrate:down` sections:

```sql
-- migrate:up
CREATE TABLE mydb.user_scores (
    user_id UInt64,
    score Float32,
    updated_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY user_id;

-- migrate:down
DROP TABLE IF EXISTS mydb.user_scores;
```

## CI/CD Integration

`chops dq check` and `chops dq freshness` return non-zero exit codes on failure.
See [Exit codes](#exit-codes) for the full contract.

```yaml
- name: Data quality gate
  run: |
    chops dq check production.orders --max-null-pct 2 --min-rows 10000
    chops dq freshness production.orders --warn 30 --critical 120
```

## Exit codes

CI-facing checks use `0` for OK, `1` for a warning or soft failure, and `2` for a
critical failure or missing required data.

| Command | Exit code | Meaning |
|---------|-----------|---------|
| `chops dq check <table>` | `0` | All configured checks passed |
| `chops dq check <table>` | `1` | One or more configured checks failed |
| `chops dq check <table>` | `2` | The table was not found |
| `chops dq freshness <table>` | `0` | Freshness is OK |
| `chops dq freshness <table>` | `1` | Data age is at or above `--warn`, or no DateTime column was found |
| `chops dq freshness <table>` | `2` | Data age is at or above `--critical`, or the table/column has no data |
| `chops dq drift <table>` | `0` | No drift was detected, or the command saved the first baseline snapshot |
| `chops dq drift <table>` | `1` | The table was not found |
| `chops dq drift <table>` | `1` | Warning-level drift was detected |
| `chops dq drift <table>` | `2` | Critical drift was detected, such as a dropped column or type change |
| `chops dq anomalies <table>` | `0` | No anomalies were detected, or there was not enough history to evaluate |
| `chops dq anomalies <table>` | `1` | One or more anomalies were detected, or no date column was found |
| `chops dq compare <table1> <table2>` | `0` | Row counts and schemas match |
| `chops dq compare <table1> <table2>` | `1` | Row counts or schemas differ |
| `chops health replication` | `0` | Replication is healthy, or no replicated tables were found |
| `chops health replication` | `1` | Replication delay exceeded the warning threshold |
| `chops health replication` | `2` | One or more replicated tables have missing replicas |

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
