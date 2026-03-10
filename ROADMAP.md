# chops — Roadmap

## Current (v0.2.0)
- Health & Observability: summary, table-sizes, slow-queries, merges, running-queries
- Data Quality: profile, freshness, check
- Schema Migrations: init, new, status, up, down

## v0.3.0 — Data Quality Drift Detection
- [ ] `chops dq drift <table>` — detect schema/cardinality changes between runs
  - Save profile snapshot to `_chops_dq_snapshots` table in ClickHouse
  - Compare current profile vs last snapshot
  - Flag: new/dropped columns, cardinality spikes/drops, null rate changes
  - CI-friendly exit codes (non-zero on drift detected)
  - `--output json` for automation

## v0.4.0 — Replication & Partitions
- [ ] `chops health replication` — replication status for clustered setups
  - system.replicas: lag, queue size, delayed inserts
  - OK/WARNING/CRITICAL status with exit codes
- [ ] `chops health partitions` — partition analysis
  - Oversized partitions, too many parts per partition (merge pressure)
  - Detect tables needing OPTIMIZE

## v0.5.0 — Config File & Query
- [ ] Config file support (`chops.yml` or `~/.config/chops/config.yml`)
  - Named connection profiles: `chops --profile production health summary`
  - Default profile from env var `CHOPS_PROFILE`
- [ ] `chops query <sql>` — execute SQL with formatted output
  - `--format table/json/csv`
  - Stdin support: `echo "SELECT 1" | chops query`

## v0.6.0 — Advanced DQ
- [ ] `chops dq anomalies <table>` — row count anomaly detection by partition
  - Statistical deviation (z-score), no ML dependency
  - Detect spikes/drops in ingestion volume
- [ ] `chops dq compare <table1> <table2>` — compare two tables
  - Row counts, schema diff, sample data diff
  - Useful for validating migrations or replication

## v1.0.0 — Stable Release
- [ ] `chops migrate validate` — validate migrations before applying
  - Syntax check, verify down section exists
  - Detect breaking changes (DROP COLUMN warnings)
- [ ] All commands tested against ClickHouse 24.x and 25.x
- [ ] Comprehensive test suite (unit + integration)
- [ ] Documentation site or expanded README

## Future Ideas
- `chops dq rules <table>` — configurable quality rules from YAML
- `chops health alerts` — integration with alerting (Slack, PagerDuty)
- `chops migrate squash` — squash multiple migrations into one
- Docker image for CI pipelines
