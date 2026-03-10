# chops — Roadmap

## v1.0.0 (current) — Stable Release
- 22 commands: health(7), dq(6), migrate(6), query(1), version(1)
- Config file support (chops.toml with named profiles)
- 28 tests, ruff + mypy strict, Python 3.10–3.13
- Tested against ClickHouse 25.8.x

## Future Ideas
- `chops dq rules <table>` — configurable quality rules from YAML
- `chops health alerts` — integration with alerting (Slack, PagerDuty)
- `chops migrate squash` — squash multiple migrations into one
- `chops query` — stdin support: `echo "SELECT 1" | chops query -`
- Docker image for CI pipelines
- Documentation site
