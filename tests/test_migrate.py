"""Tests for migrate commands."""

from pathlib import Path

from typer.testing import CliRunner

from chops.cli import app
from chops.commands.migrate import _parse_migration, _split_statements

runner = CliRunner()


def test_migrate_help() -> None:
    result = runner.invoke(app, ["migrate", "--help"])
    assert result.exit_code == 0
    assert "Schema migration management" in result.output


def test_migrate_init(tmp_path: Path) -> None:
    d = tmp_path / "migs"
    result = runner.invoke(app, ["migrate", "init", "--dir", str(d)])
    assert result.exit_code == 0
    assert d.exists()


def test_migrate_init_already_exists(tmp_path: Path) -> None:
    d = tmp_path / "migs"
    d.mkdir()
    result = runner.invoke(app, ["migrate", "init", "--dir", str(d)])
    assert result.exit_code == 0
    assert "already exists" in result.output


def test_migrate_new(tmp_path: Path) -> None:
    d = tmp_path / "migs"
    d.mkdir()
    result = runner.invoke(app, ["migrate", "new", "create_users", "--dir", str(d)])
    assert result.exit_code == 0
    files = list(d.glob("*.sql"))
    assert len(files) == 1
    assert "create_users" in files[0].name
    content = files[0].read_text()
    assert "-- migrate:up" in content
    assert "-- migrate:down" in content


def test_migrate_new_no_dir(tmp_path: Path) -> None:
    d = tmp_path / "nonexistent"
    result = runner.invoke(app, ["migrate", "new", "foo", "--dir", str(d)])
    assert result.exit_code == 1


def test_migrate_new_sanitizes_name(tmp_path: Path) -> None:
    d = tmp_path / "migs"
    d.mkdir()
    result = runner.invoke(app, ["migrate", "new", "Create Users Table!", "--dir", str(d)])
    assert result.exit_code == 0
    files = list(d.glob("*.sql"))
    assert len(files) == 1
    # Should be lowercase with underscores
    assert "create_users_table" in files[0].name


def test_split_statements() -> None:
    sql = """
    CREATE TABLE foo (id UInt32) ENGINE = MergeTree() ORDER BY id;
    INSERT INTO foo VALUES (1);
    -- just a comment;
    ALTER TABLE foo ADD COLUMN name String;
    """
    stmts = _split_statements(sql)
    assert len(stmts) == 3
    assert "CREATE TABLE" in stmts[0]
    assert "INSERT INTO" in stmts[1]
    assert "ALTER TABLE" in stmts[2]


def test_split_statements_empty() -> None:
    assert _split_statements("") == []
    assert _split_statements("-- just comments\n-- more comments") == []


def test_parse_migration(tmp_path: Path) -> None:
    migration = tmp_path / "001_test.sql"
    migration.write_text("""-- migrate:up
CREATE TABLE foo (id UInt32) ENGINE = MergeTree() ORDER BY id;
INSERT INTO foo VALUES (1);

-- migrate:down
DROP TABLE foo;
""")
    up, down = _parse_migration(migration)
    assert len(up) == 2
    assert len(down) == 1
    assert "CREATE TABLE" in up[0]
    assert "DROP TABLE" in down[0]


def test_parse_migration_no_down(tmp_path: Path) -> None:
    migration = tmp_path / "001_test.sql"
    migration.write_text("""-- migrate:up
CREATE TABLE foo (id UInt32) ENGINE = MergeTree() ORDER BY id;

-- migrate:down
""")
    up, down = _parse_migration(migration)
    assert len(up) == 1
    assert len(down) == 0


def test_parse_migration_multiple_statements(tmp_path: Path) -> None:
    migration = tmp_path / "002_test.sql"
    migration.write_text("""-- migrate:up
CREATE TABLE foo (id UInt32) ENGINE = MergeTree() ORDER BY id;
CREATE TABLE bar (id UInt32) ENGINE = MergeTree() ORDER BY id;
ALTER TABLE foo ADD COLUMN name String;

-- migrate:down
DROP TABLE bar;
DROP TABLE foo;
""")
    up, down = _parse_migration(migration)
    assert len(up) == 3
    assert len(down) == 2
