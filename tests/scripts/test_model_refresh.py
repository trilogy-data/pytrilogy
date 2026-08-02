"""Model-aware refresh: a datasource whose definition changed since it was
last built reads as stale even when its watermarks look fresh.

The semantic change used here — rebinding the physical ``amount`` column to a
derived ``amount * 2`` — is invisible to every other staleness signal: the
column name and datatype are unchanged (no schema mismatch) and the freshness
watermark is unchanged (no new rows). Only the model fingerprint sees it.
"""

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
name = "modelrefresh"

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

MODEL_V2 = MODEL_V1.replace(
    "datasource orders_managed (\n    order_id: order_id,\n    amount: amount,",
    "auto amount_scaled <- amount * 2;\n\n"
    "datasource orders_managed (\n    order_id: order_id,\n    amount: amount_scaled,",
)


@pytest.fixture
def project(tmp_path: Path, monkeypatch) -> Path:
    (tmp_path / "trilogy.toml").write_text(TOML, encoding="utf-8")
    (tmp_path / "setup.sql").write_text(SETUP_SQL, encoding="utf-8")
    (tmp_path / "model.preql").write_text(MODEL_V1, encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    return tmp_path


def managed_sum(db: Path, table: str = "orders_managed") -> int:
    con = duckdb.connect(str(db))
    try:
        return con.execute(f"SELECT sum(amount) FROM {table}").fetchall()[0][0]
    finally:
        con.close()


def invoke(*args: str, expect: int = 0) -> str:
    result = CliRunner().invoke(cli, list(args))
    assert result.exit_code == expect, (args, result.output, result.exception)
    return result.output


def test_state_input_detects_model_change(project: Path):
    db = project / "wh.duckdb"
    assert MODEL_V2 != MODEL_V1

    invoke("refresh", "model.preql", "--state-file", "s1.json")
    assert managed_sum(db) == 60

    # unchanged model + recorded state: up to date
    invoke("refresh", "model.preql", "--state-input", "s1.json", expect=2)

    # semantic change with identical schema and watermarks: rebuilt
    (project / "model.preql").write_text(MODEL_V2, encoding="utf-8")
    invoke(
        "refresh",
        "model.preql",
        "--state-input",
        "s1.json",
        "--state-file",
        "s2.json",
    )
    assert managed_sum(db) == 120

    # converged: the new snapshot carries the new fingerprint
    invoke("refresh", "model.preql", "--state-input", "s2.json", expect=2)


def test_directory_state_input_detects_model_change(project: Path):
    db = project / "wh.duckdb"

    invoke("refresh", ".", "--state-file", "s1.json")
    assert managed_sum(db) == 60
    invoke("refresh", ".", "--state-input", "s1.json", expect=2)

    (project / "model.preql").write_text(MODEL_V2, encoding="utf-8")
    invoke("refresh", ".", "--state-input", "s1.json", "--state-file", "s2.json")
    assert managed_sum(db) == 120
    invoke("refresh", ".", "--state-input", "s2.json", expect=2)


def test_env_refresh_rebuilds_on_model_change(project: Path):
    db = project / "wh.duckdb"

    invoke("refresh", "model.preql", "--environment", "dev")
    assert managed_sum(db, "dev_orders_managed") == 60
    invoke("env", "fingerprint", "dev", ".")
    invoke("refresh", "model.preql", "--environment", "dev", expect=2)

    (project / "model.preql").write_text(MODEL_V2, encoding="utf-8")
    invoke("refresh", "model.preql", "--environment", "dev")
    assert managed_sum(db, "dev_orders_managed") == 120

    # the rebuild auto-recorded the fingerprint: diff clean, refresh converged
    invoke("env", "diff", "dev", ".")
    invoke("refresh", "model.preql", "--environment", "dev", expect=2)


def test_no_baseline_keeps_legacy_behavior(project: Path):
    db = project / "wh.duckdb"

    invoke("refresh", "model.preql")
    assert managed_sum(db) == 60

    # no state input, no environment: a model change stays invisible to
    # watermark-based staleness — the documented boundary of the feature
    (project / "model.preql").write_text(MODEL_V2, encoding="utf-8")
    invoke("refresh", "model.preql", expect=2)
    assert managed_sum(db) == 60
