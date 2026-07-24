"""Agent-validation runner components: workspace seeding, unit-tier mock
materialization, scoring, and the unit/integration test-type selection."""

from __future__ import annotations

import duckdb
import pytest

from trilogy import Dialects
from trilogy.core.enums import QueryComparison, ValidationScope
from trilogy.core.exceptions import ConfigurationException
from trilogy.core.models.environment import Environment
from trilogy.scripts import validate_agent as va
from trilogy.scripts.testing import (
    DEFAULT_TEST_TYPES,
    _environment_scope,
    resolve_test_types,
)

MODEL = """key order_id int;
property order_id.amount float;
property order_id.region string;
datasource orders (order_id: order_id, amount: amount, region: region) grain (order_id) address orders_tbl;
"""


@pytest.fixture
def model_dir(tmp_path):
    model = tmp_path / "model"
    model.mkdir()
    (model / "trilogy.toml").write_text(
        '[engine]\ndialect = "duck_db"\n\n[engine.config]\npath = "local.duckdb"\n',
        encoding="utf-8",
    )
    (model / "orders.preql").write_text(MODEL, encoding="utf-8")
    (model / "local.duckdb").write_bytes(b"not-actually-a-db")
    sub = model / "raw"
    sub.mkdir()
    (sub / "extra.preql").write_text("key other int;\n", encoding="utf-8")
    return model


def test_seed_workspace_copies_text_not_data(model_dir, tmp_path):
    workspace = tmp_path / "ws"
    va.seed_workspace(model_dir, workspace)
    assert (workspace / "orders.preql").exists()
    assert (workspace / "raw" / "extra.preql").exists()
    assert (workspace / "trilogy.toml").exists()
    assert not (workspace / "local.duckdb").exists()
    toml = (workspace / "trilogy.toml").read_text(encoding="utf-8")
    # relative engine path rewritten to the model's absolute location
    assert (model_dir / "local.duckdb").resolve().as_posix() in toml
    # imports fall back to the model directory
    assert toml.splitlines()[0].startswith("import_paths = [")
    assert model_dir.resolve().as_posix() in toml.splitlines()[0]


def test_seed_workspace_leaves_remote_paths(model_dir, tmp_path):
    (model_dir / "trilogy.toml").write_text(
        '[engine]\ndialect = "duck_db"\n\n[engine.config]\n'
        'path = "gs://bucket/my.duckdb"\n',
        encoding="utf-8",
    )
    workspace = tmp_path / "ws"
    va.seed_workspace(model_dir, workspace)
    assert 'path = "gs://bucket/my.duckdb"' in (workspace / "trilogy.toml").read_text(
        encoding="utf-8"
    )


def test_write_unit_toml_replaces_engine_keeps_rest(model_dir, tmp_path):
    (model_dir / "trilogy.toml").write_text(
        '[engine]\ndialect = "bigquery"\n\n[engine.config]\nproject = "x"\n\n'
        '[agent]\nprovider = "deepseek"\nmodel = "deepseek-chat"\n',
        encoding="utf-8",
    )
    workspace = tmp_path / "ws"
    workspace.mkdir()
    db = tmp_path / "mock.duckdb"
    va._write_unit_toml(model_dir, workspace, db)
    text = (workspace / "trilogy.toml").read_text(encoding="utf-8")
    assert 'provider = "deepseek"' in text
    assert "bigquery" not in text
    assert "project" not in text
    assert db.resolve().as_posix() in text
    assert 'dialect = "duck_db"' in text


def test_materialize_mock_db_original_addresses(tmp_path):
    env = Environment(working_path=tmp_path)
    env.parse(
        "key id int;\nproperty id.val float;\n"
        "datasource plain (id: id, val: val) grain (id) address plain_tbl;\n"
        "key id2 int;\n"
        "datasource dotted (id2: id2) grain (id2) address myschema.dotted_tbl;\n"
    )
    db_path = tmp_path / "mock.duckdb"
    va.materialize_mock_db(env, db_path, tmp_path / "mock_files")
    con = duckdb.connect(str(db_path))
    try:
        assert con.execute("select count(*) from plain_tbl").fetchone()[0] > 0
        assert con.execute("select count(*) from myschema.dotted_tbl").fetchone()[0] > 0
        # deterministic + fully populated
        assert (
            con.execute("select count(*) from plain_tbl where val is null").fetchone()[
                0
            ]
            == 0
        )
    finally:
        con.close()


def test_materialize_mock_db_writes_file_datasources(tmp_path):
    model = tmp_path / "model"
    (model / "data").mkdir(parents=True)
    (model / "data" / "things.parquet").write_bytes(b"")
    (model / "rows.csv").write_bytes(b"")
    env = Environment(working_path=model)
    env.parse(
        "key id int;\nproperty id.val float;\n"
        "datasource filed (id: id, val: val) grain (id) file `data/things.parquet`;\n"
        "key cid int;\n"
        "datasource csvd (cid: cid) grain (cid) file `rows.csv`;\n"
    )
    files_root = tmp_path / "mock_files"
    va.materialize_mock_db(env, tmp_path / "mock.duckdb", files_root)
    con = duckdb.connect()
    try:
        parquet = (files_root / "data" / "things.parquet").as_posix()
        assert (
            con.execute(f"select count(*) from read_parquet('{parquet}')").fetchone()[0]
            > 0
        )
        csv = (files_root / "rows.csv").as_posix()
        assert con.execute(f"select count(*) from read_csv('{csv}')").fetchone()[0] > 0
    finally:
        con.close()


def test_materialize_mock_db_rejects_outside_model_files(tmp_path):
    model = tmp_path / "model"
    model.mkdir()
    (tmp_path / "elsewhere.parquet").write_bytes(b"")
    env = Environment(working_path=model)
    env.parse(
        "key id int;\n"
        "datasource filed (id: id) grain (id) file `../elsewhere.parquet`;\n"
    )
    with pytest.raises(ConfigurationException, match="integration"):
        va.materialize_mock_db(env, tmp_path / "mock.duckdb", tmp_path / "mf")


def test_materialize_mock_db_quoted_address_is_opaque(tmp_path):
    env = Environment(working_path=tmp_path)
    env.parse(
        "key id int;\n"
        "datasource weird (id: id) grain (id) address `my weird.table`;\n"
    )
    db_path = tmp_path / "mock.duckdb"
    va.materialize_mock_db(env, db_path, tmp_path / "mock_files")
    con = duckdb.connect(str(db_path))
    try:
        assert con.execute('select count(*) from "my weird.table"').fetchone()[0] > 0
    finally:
        con.close()


@pytest.fixture
def scored_workspace(model_dir, tmp_path):
    executor = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=model_dir)
    )
    executor.parse_text((model_dir / "orders.preql").read_text(encoding="utf-8"))
    db = tmp_path / "mock.duckdb"
    va.materialize_mock_db(executor.environment, db, tmp_path / "mock_files")
    workspace = tmp_path / "ws"
    va.seed_workspace(model_dir, workspace)
    va._write_unit_toml(model_dir, workspace, db)
    expected_sql = 'select sum("amount") as "t" from orders_tbl'
    return workspace, expected_sql


def test_score_workspace_file_datasource_end_to_end(tmp_path):
    """Full unit-tier flow for a parquet-addressed model: mock file written,
    copied into the workspace, and both candidate + expected read it there."""
    model = tmp_path / "model"
    (model / "data").mkdir(parents=True)
    (model / "data" / "things.parquet").write_bytes(b"")
    (model / "trilogy.toml").write_text(
        '[engine]\ndialect = "duck_db"\n', encoding="utf-8"
    )
    (model / "things.preql").write_text(
        "key id int;\nproperty id.val float;\n"
        "datasource filed (id: id, val: val) grain (id) file `data/things.parquet`;\n",
        encoding="utf-8",
    )
    executor = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=model)
    )
    executor.parse_text((model / "things.preql").read_text(encoding="utf-8"))
    db = tmp_path / "mock.duckdb"
    files_root = tmp_path / "mock_files"
    va.materialize_mock_db(executor.environment, db, files_root)
    workspace = tmp_path / "ws"
    va.seed_workspace(model, workspace)
    va._write_unit_toml(model, workspace, db)
    import shutil

    shutil.copytree(files_root, workspace, dirs_exist_ok=True)
    mock_parquet = (workspace / "data" / "things.parquet").resolve().as_posix()
    expected_sql = f'select sum("val") as "t" from read_parquet(\'{mock_parquet}\')'
    (workspace / va.ANSWER_FILENAME).write_text(
        "import things;\nselect sum(val) -> my_total;\n", encoding="utf-8"
    )
    result = va.score_workspace(workspace, expected_sql, QueryComparison.TOLERANT)
    assert result.status == "pass", result.detail


def test_score_workspace_pass(scored_workspace):
    workspace, expected_sql = scored_workspace
    (workspace / va.ANSWER_FILENAME).write_text(
        "import orders;\nselect sum(amount) -> my_total;\n", encoding="utf-8"
    )
    result = va.score_workspace(workspace, expected_sql, QueryComparison.TOLERANT)
    assert result.status == "pass"
    assert result.candidate_rows == 1


def test_score_workspace_fail_and_missing(scored_workspace):
    workspace, expected_sql = scored_workspace
    (workspace / va.ANSWER_FILENAME).write_text(
        "import orders;\nselect count(order_id) -> my_total;\n", encoding="utf-8"
    )
    assert (
        va.score_workspace(workspace, expected_sql, QueryComparison.TOLERANT).status
        == "fail"
    )
    (workspace / va.ANSWER_FILENAME).unlink()
    assert (
        va.score_workspace(workspace, expected_sql, QueryComparison.TOLERANT).status
        == "missing"
    )


def test_score_workspace_error_on_bad_candidate(scored_workspace):
    workspace, expected_sql = scored_workspace
    (workspace / va.ANSWER_FILENAME).write_text(
        "import orders;\nselect undefined_concept;\n", encoding="utf-8"
    )
    result = va.score_workspace(workspace, expected_sql, QueryComparison.TOLERANT)
    assert result.status == "error"
    assert "generate_sql" in result.detail


def test_question_result_aggregation():
    result = va.QuestionResult(name="q", question="?", target=0.5)
    result.repetitions = [
        va.RepetitionResult(status="pass", tokens=100),
        va.RepetitionResult(status="fail", tokens=200),
    ]
    assert result.pass_rate == 0.5
    assert result.passed
    assert result.total_tokens == 300
    result.target = 0.75
    assert not result.passed


def test_resolve_test_types():
    assert resolve_test_types((), ()) == DEFAULT_TEST_TYPES
    assert "agent" in resolve_test_types((), ("agent",))
    assert "datasources" not in resolve_test_types(("datasources",), ())
    assert resolve_test_types(("agent",), ("agent",)) == DEFAULT_TEST_TYPES - {
        "agent"
    } | {"concepts", "datasources"} - {"agent"}


def test_environment_scope():
    assert _environment_scope(frozenset({"datasources", "concepts"})) is (
        ValidationScope.ALL
    )
    assert _environment_scope(frozenset({"datasources"})) is (
        ValidationScope.DATASOURCES
    )
    assert _environment_scope(frozenset({"concepts"})) is ValidationScope.CONCEPTS
    assert _environment_scope(frozenset({"agent"})) is None
