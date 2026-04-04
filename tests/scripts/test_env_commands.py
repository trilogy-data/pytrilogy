"""End-to-end tests for deployment environments against an on-disk DuckDB:
build to prod, build changed code into an env, publish (rename cutover), and
clean up."""

from pathlib import Path

import duckdb
import pytest
from click.testing import CliRunner

from trilogy.scripts.trilogy import cli

TOML = """
[engine]
dialect = "duck_db"

[engine.config]
db_location = "wh.duckdb"

[project]
name = "envtest"

[environments]
home = "env_home"

[setup]
sql = ["setup.sql"]
"""

SETUP_SQL = """
CREATE TABLE IF NOT EXISTS raw_orders AS
SELECT * FROM (VALUES
    (1, 10, TIMESTAMP '2024-01-01 00:00:00'),
    (2, 20, TIMESTAMP '2024-01-02 00:00:00'),
    (3, 30, TIMESTAMP '2024-01-03 00:00:00')
) t(order_id, amount, updated_at);
"""

MODEL_V1 = """
key order_id int;
property order_id.amount int;

root datasource raw_orders (
    order_id: order_id,
    amount: amount
)
grain (order_id)
address raw_orders;

persist orders_summary into orders_summary from
select
    order_id,
    amount
;
"""

# v2 doubles the amounts — visibly different output for the cutover check
MODEL_V2 = MODEL_V1.replace("    amount\n", "    amount * 2 -> amount_doubled\n")


@pytest.fixture
def project(tmp_path: Path, monkeypatch) -> Path:
    (tmp_path / "trilogy.toml").write_text(TOML, encoding="utf-8")
    (tmp_path / "setup.sql").write_text(SETUP_SQL, encoding="utf-8")
    (tmp_path / "model.preql").write_text(MODEL_V1, encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    return tmp_path


def query(db: Path, sql: str):
    con = duckdb.connect(str(db))
    try:
        return con.execute(sql).fetchall()
    finally:
        con.close()


def tables(db: Path) -> set[str]:
    return {r[0] for r in query(db, "SHOW TABLES")}


def invoke(*args: str) -> str:
    result = CliRunner().invoke(cli, list(args))
    assert result.exit_code == 0, (args, result.output, result.exception)
    return result.output


def test_env_build_and_publish_cutover(project: Path):
    db = project / "wh.duckdb"

    # 1. production build
    invoke("run", "model.preql")
    assert query(db, "SELECT sum(amount) FROM orders_summary") == [(60,)]

    # 2. changed code builds into the environment, prod untouched
    (project / "model.preql").write_text(MODEL_V2, encoding="utf-8")
    output = invoke("run", "model.preql", "--environment", "dev")
    assert "dev" in output
    assert "dev_orders_summary" in tables(db)
    assert query(db, "SELECT sum(amount_doubled) FROM dev_orders_summary") == [(120,)]
    assert query(db, "SELECT sum(amount) FROM orders_summary") == [(60,)]

    # environment is registered with its built asset tracked
    listing = invoke("env", "list")
    assert "dev" in listing
    assert "1 tracked asset(s)" in listing

    # 3. dry-run shows the plan without changing anything
    plan = invoke("env", "publish", "dev", "--dry-run")
    assert "ALTER TABLE" in plan
    assert "dev_orders_summary" in tables(db)

    # 4. publish: rename cutover, backups dropped
    invoke("env", "publish", "dev")
    remaining = tables(db)
    assert "dev_orders_summary" not in remaining
    assert "orders_summary__pub_backup" not in remaining
    assert query(db, "SELECT sum(amount_doubled) FROM orders_summary") == [(120,)]

    # tracked assets cleared: delete must not try to drop promoted tables
    invoke("env", "delete", "dev")
    assert "orders_summary" in tables(db)


def test_activated_env_used_without_flag(project: Path):
    db = project / "wh.duckdb"
    invoke("env", "create", "staging")
    invoke("env", "activate", "staging")
    invoke("run", "model.preql")
    assert "staging_orders_summary" in tables(db)
    assert "orders_summary" not in tables(db)

    invoke("env", "deactivate")
    invoke("run", "model.preql")
    assert "orders_summary" in tables(db)


def test_env_delete_drops_tracked_assets(project: Path):
    db = project / "wh.duckdb"
    invoke("run", "model.preql", "--environment", "scratch")
    assert "scratch_orders_summary" in tables(db)

    invoke("env", "delete", "scratch")
    assert "scratch_orders_summary" not in tables(db)
    assert "scratch" not in invoke("env", "list")


def test_publish_requires_built_assets(project: Path):
    result = CliRunner().invoke(cli, ["env", "publish", "ghost"])
    assert result.exit_code != 0
    assert "no built assets" in result.output


def test_first_publish_without_existing_prod(project: Path):
    db = project / "wh.duckdb"
    invoke("run", "model.preql", "--environment", "dev")
    # no production build has ever run: phase 1 has nothing to back up
    invoke("env", "publish", "dev")
    assert query(db, "SELECT sum(amount) FROM orders_summary") == [(60,)]
    assert "dev_orders_summary" not in tables(db)


REFRESH_MODEL = """
key order_id int;
property order_id.amount int;
property order_id.updated_at datetime;

root datasource raw_orders (
    order_id: order_id,
    amount: amount,
    updated_at: updated_at
)
grain (order_id)
address raw_orders;

datasource orders_managed (
    order_id: order_id,
    amount: amount,
    updated_at: updated_at
)
grain (order_id)
address orders_managed
freshness by updated_at;
"""


def test_refresh_builds_into_environment(project: Path):
    db = project / "wh.duckdb"
    (project / "model.preql").write_text(REFRESH_MODEL, encoding="utf-8")
    invoke("refresh", "model.preql", "--environment", "dev")
    assert "dev_orders_managed" in tables(db)
    assert "orders_managed" not in tables(db)
    assert query(db, "SELECT sum(amount) FROM dev_orders_managed") == [(60,)]

    # a second refresh in the same environment sees the built table as fresh
    result = CliRunner().invoke(cli, ["refresh", "model.preql", "--environment", "dev"])
    assert result.exit_code == 2, (result.output, result.exception)

    # ...while production is still unbuilt and stale
    result = CliRunner().invoke(cli, ["refresh", "model.preql"])
    assert result.exit_code == 0, (result.output, result.exception)
    assert "orders_managed" in tables(db)


def test_directory_refresh_builds_into_environment(project: Path):
    """Directory refresh runs the probe pipeline (lightweight parse + owner
    probes + managed graph); all of it must see env-prefixed addresses."""
    db = project / "wh.duckdb"
    (project / "model.preql").write_text(REFRESH_MODEL, encoding="utf-8")
    invoke("refresh", ".", "--environment", "dev")
    assert "dev_orders_managed" in tables(db)
    assert "orders_managed" not in tables(db)
    assert query(db, "SELECT sum(amount) FROM dev_orders_managed") == [(60,)]

    result = CliRunner().invoke(cli, ["refresh", ".", "--environment", "dev"])
    assert result.exit_code == 2, (result.output, result.exception)


class FakeExecutor:
    """Records SQL and simulates a catalog so publish failure paths can be
    driven deterministically (a live engine can't fail a rename on cue)."""

    def __init__(self, existing: set[str], fail_on: str | None = None):
        self.sql_log: list[str] = []
        self.existing = existing
        self.fail_on = fail_on

    def execute_raw_sql(self, sql: str):
        if sql == "ROLLBACK":
            return
        if sql.startswith("SELECT 1 FROM "):
            table = sql[len("SELECT 1 FROM ") :].split(" ")[0]
            if table not in self.existing:
                raise RuntimeError(f"table {table} missing")
            return
        if self.fail_on and self.fail_on in sql:
            raise RuntimeError("injected failure")
        self.sql_log.append(sql)
        if sql.startswith("ALTER TABLE "):
            old, _, new = sql[len("ALTER TABLE ") :].partition(" RENAME TO ")
            self.existing.discard(old)
            self.existing.add(new)
        elif sql.startswith("DROP TABLE IF EXISTS "):
            self.existing.discard(sql[len("DROP TABLE IF EXISTS ") :])
        return


def make_pairs():
    from trilogy.scripts.env_commands import PublishAsset

    return [
        PublishAsset(env_address="dev_a", prod_address="a"),
        PublishAsset(env_address="dev_b", prod_address="b"),
    ]


def run_publish(fake: FakeExecutor, dry_run: bool = False, keep_backups: bool = False):
    from click.exceptions import Exit

    from trilogy.dialect.enums import Dialects
    from trilogy.scripts.env_commands import _run_publish

    try:
        _run_publish(fake, Dialects.DUCK_DB, make_pairs(), dry_run, keep_backups)
        return None
    except Exit as e:
        return e


def test_publish_success_replaces_and_drops_backups():
    fake = FakeExecutor({"a", "b", "dev_a", "dev_b"})
    assert run_publish(fake) is None
    assert fake.existing == {"a", "b"}


def test_publish_keep_backups():
    fake = FakeExecutor({"a", "b", "dev_a", "dev_b"})
    assert run_publish(fake, keep_backups=True) is None
    assert fake.existing == {"a", "b", "a__pub_backup", "b__pub_backup"}


def test_publish_dry_run_touches_nothing():
    fake = FakeExecutor({"a", "b", "dev_a", "dev_b"})
    assert run_publish(fake, dry_run=True) is None
    assert fake.existing == {"a", "b", "dev_a", "dev_b"}
    assert fake.sql_log == []


def test_publish_phase1_failure_restores_backups():
    fake = FakeExecutor({"a", "b", "dev_a", "dev_b"}, fail_on="ALTER TABLE b RENAME TO")
    assert run_publish(fake) is not None
    assert fake.existing == {"a", "b", "dev_a", "dev_b"}


def test_publish_phase2_failure_rolls_back_everything():
    fake = FakeExecutor(
        {"a", "b", "dev_a", "dev_b"}, fail_on="ALTER TABLE dev_b RENAME TO"
    )
    assert run_publish(fake) is not None
    assert fake.existing == {"a", "b", "dev_a", "dev_b"}


TWO_TABLE_MODEL = MODEL_V1 + """
persist order_ids into order_ids from
select order_id
;
"""


def test_multi_table_publish_cutover(project: Path):
    db = project / "wh.duckdb"
    (project / "model.preql").write_text(TWO_TABLE_MODEL, encoding="utf-8")
    invoke("run", "model.preql")
    invoke("run", "model.preql", "--environment", "dev")
    assert {"dev_orders_summary", "dev_order_ids"} <= tables(db)

    invoke("env", "publish", "dev")
    remaining = tables(db)
    assert {"orders_summary", "order_ids"} <= remaining
    assert not any(t.startswith("dev_") for t in remaining)
    assert not any(t.endswith("__pub_backup") for t in remaining)


def test_publish_aborts_before_touching_prod_when_env_incomplete(project: Path):
    db = project / "wh.duckdb"
    (project / "model.preql").write_text(TWO_TABLE_MODEL, encoding="utf-8")
    invoke("run", "model.preql")
    invoke("run", "model.preql", "--environment", "dev")

    con = duckdb.connect(str(db))
    con.execute("DROP TABLE dev_order_ids")
    con.close()

    result = CliRunner().invoke(cli, ["env", "publish", "dev"])
    assert result.exit_code != 0
    assert "missing built tables" in result.output
    # preflight aborted before phase 1: prod fully intact
    assert {"orders_summary", "order_ids"} <= tables(db)
    assert query(db, "SELECT sum(amount) FROM orders_summary") == [(60,)]


def test_deploy_workflow_refresh_publish_prod_fresh(project: Path):
    """The full deploy loop: backpopulate an env via refresh, cut over, and
    production's own refresh then sees the promoted table as fresh."""
    db = project / "wh.duckdb"
    (project / "model.preql").write_text(REFRESH_MODEL, encoding="utf-8")
    invoke("refresh", ".", "--environment", "deploy_1")
    invoke("env", "publish", "deploy_1")
    assert query(db, "SELECT sum(amount) FROM orders_managed") == [(60,)]

    result = CliRunner().invoke(cli, ["refresh", "."])
    assert result.exit_code == 2, (result.output, result.exception)


def test_hyphenated_env_name_rejected(project: Path):
    """Env names become unquoted SQL identifier prefixes; a hyphen would build
    (quoted DDL) and then break every raw publish statement."""
    result = CliRunner().invoke(cli, ["env", "create", "deploy-1"])
    assert result.exit_code != 0
    assert "deploy_1" in result.output

    result = CliRunner().invoke(
        cli, ["run", "model.preql", "--environment", "deploy-1"]
    )
    assert result.exit_code != 0


def test_publish_rollback_failure_reports_manual_fix(capsys):
    """Double failure: phase 2 dies AND a backup restore dies. The publish
    must still abort loudly and name the table needing manual repair."""

    class DoubleFailExecutor(FakeExecutor):
        def execute_raw_sql(self, sql: str):
            if "a__pub_backup RENAME TO a" in sql:
                raise RuntimeError("restore also failed")
            return super().execute_raw_sql(sql)

    fake = DoubleFailExecutor(
        {"a", "b", "dev_a", "dev_b"}, fail_on="ALTER TABLE dev_b RENAME TO"
    )
    assert run_publish(fake) is not None
    captured = capsys.readouterr()
    combined = captured.out + captured.err
    assert "MANUAL FIX NEEDED" in combined
    assert "a__pub_backup" in combined


FILE_MODEL = MODEL_V1 + """
auto amount_export <- amount;

datasource orders_export (
    order_id: order_id,
    amount_export: amount_export
)
grain (order_id)
file `./orders_export.parquet`
state unpublished;

overwrite orders_export;
"""


def read_parquet_sum(path: Path, column: str):
    con = duckdb.connect()
    try:
        return con.execute(
            f"SELECT sum({column}) FROM read_parquet('{path.as_posix()}')"
        ).fetchall()
    finally:
        con.close()


def test_file_backed_publish_cutover(project: Path):
    db = project / "wh.duckdb"
    (project / "model.preql").write_text(FILE_MODEL, encoding="utf-8")
    invoke("run", "model.preql")
    prod_file = project / "orders_export.parquet"
    assert prod_file.exists()
    assert read_parquet_sum(prod_file, "amount_export") == [(60,)]

    invoke("run", "model.preql", "--environment", "dev")
    env_file = project / "orders_export_dev.parquet"
    assert env_file.exists()

    # registry recorded the file with its kind, resolved absolute
    listing = invoke("env", "list")
    assert "2 tracked asset(s)" in listing

    invoke("env", "publish", "dev")
    assert not env_file.exists()
    assert not (project / "orders_export.parquet__pub_backup").exists()
    assert prod_file.exists()
    assert read_parquet_sum(prod_file, "amount_export") == [(60,)]
    # table asset cut over alongside the file
    assert "dev_orders_summary" not in tables(db)


def test_env_delete_removes_file_assets(project: Path):
    (project / "model.preql").write_text(FILE_MODEL, encoding="utf-8")
    invoke("run", "model.preql", "--environment", "scratch")
    env_file = project / "orders_export_scratch.parquet"
    assert env_file.exists()

    invoke("env", "delete", "scratch")
    assert not env_file.exists()


def make_mixed_assets(tmp_path: Path):
    from trilogy.scripts.env_commands import PublishAsset

    prod_file = tmp_path / "out.parquet"
    env_file = tmp_path / "out_dev.parquet"
    prod_file.write_text("old", encoding="utf-8")
    env_file.write_text("new", encoding="utf-8")
    return [
        PublishAsset(env_address="dev_a", prod_address="a"),
        PublishAsset(
            env_address=str(env_file), prod_address=str(prod_file), is_file=True
        ),
    ], (prod_file, env_file)


def test_mixed_publish_success(tmp_path: Path):
    from trilogy.dialect.enums import Dialects
    from trilogy.scripts.env_commands import _run_publish

    assets, (prod_file, env_file) = make_mixed_assets(tmp_path)
    fake = FakeExecutor({"a", "dev_a"})
    _run_publish(fake, Dialects.DUCK_DB, assets, False, False)
    assert fake.existing == {"a"}
    assert prod_file.read_text(encoding="utf-8") == "new"
    assert not env_file.exists()
    assert not (tmp_path / "out.parquet__pub_backup").exists()


def test_mixed_publish_table_failure_rolls_back_file(tmp_path: Path):
    """The file was already promoted when the table rename dies: the file must
    be rolled back to its pre-publish content."""
    from click.exceptions import Exit

    from trilogy.dialect.enums import Dialects
    from trilogy.scripts.env_commands import PublishAsset, _run_publish

    prod_file = tmp_path / "out.parquet"
    env_file = tmp_path / "out_dev.parquet"
    prod_file.write_text("old", encoding="utf-8")
    env_file.write_text("new", encoding="utf-8")
    assets = [
        PublishAsset(
            env_address=str(env_file), prod_address=str(prod_file), is_file=True
        ),
        PublishAsset(env_address="dev_a", prod_address="a"),
    ]
    fake = FakeExecutor({"a", "dev_a"}, fail_on="ALTER TABLE dev_a RENAME TO")
    try:
        _run_publish(fake, Dialects.DUCK_DB, assets, False, False)
        raise AssertionError("expected Exit")
    except Exit:
        pass
    assert fake.existing == {"a", "dev_a"}
    assert prod_file.read_text(encoding="utf-8") == "old"
    assert env_file.read_text(encoding="utf-8") == "new"
