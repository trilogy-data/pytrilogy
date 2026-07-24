"""Agent-validation runner components: workspace seeding, unit-tier mock
materialization, scoring, and the unit/integration test-type selection."""

from __future__ import annotations

import duckdb
import pytest

from trilogy import Dialects
from trilogy.core.enums import QueryComparison, ValidationScope
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


def test_repoint_datasource_address():
    from trilogy.core.models.datasource import Address, Datasource
    from trilogy.parser import parse_text

    env = Environment()
    _, stmts = parse_text(
        "key id int;\n" "datasource filed (id: id) grain (id) file `data/x.parquet`;\n",
        env,
    )
    ds = next(s for s in stmts if isinstance(s, Datasource))
    assert ds.address.is_file
    ds.repoint("mock_x")
    assert isinstance(ds.address, Address)
    assert ds.address.type.value == "table"
    assert ds.address.location == "mock_x"


def test_mock_name_keyed_by_address():
    from trilogy.core.models.datasource import Datasource
    from trilogy.parser import parse_text

    def ds_of(src):
        _, stmts = parse_text(src, Environment())
        return next(s for s in stmts if isinstance(s, Datasource))

    a = ds_of("key id int;\ndatasource d (id: id) grain (id) address orders_tbl;\n")
    b = ds_of("key id int;\ndatasource other (id: id) grain (id) address orders_tbl;\n")
    c = ds_of("key id int;\ndatasource d (id: id) grain (id) address ship_tbl;\n")
    # same physical address -> same mock name (independent of ds name);
    # different address -> different name
    assert va._mock_name(a) == va._mock_name(b)
    assert va._mock_name(a) != va._mock_name(c)
    assert va._mock_name(a).startswith("mock_")


@pytest.fixture
def image(model_dir, tmp_path):
    """A built mock image + its flattened env, for scoring tests."""
    (model_dir / "validations.preql").write_text(
        "import orders;\n\nvalidate total select natural 'sum amount?'\n"
        "matches ( select sum(amount) -> t );\n",
        encoding="utf-8",
    )
    executor = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=model_dir)
    )
    executor.parse_text((model_dir / "orders.preql").read_text(encoding="utf-8"))
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    image_dir = run_dir / "mock_model"
    db = run_dir / "mock.duckdb"
    exclude = {(model_dir / "validations.preql").resolve()}
    va.build_mock_image(model_dir, image_dir, db, executor.environment, exclude)
    return image_dir, run_dir


def test_build_mock_image_repoints_and_excludes(image, model_dir):
    image_dir, _ = image
    assert (image_dir / "orders.preql").exists()
    assert (image_dir / "raw" / "extra.preql").exists()
    # validations file (holds expected answers) kept out of the agent's reach
    assert not (image_dir / "validations.preql").exists()
    # datasource repointed to a mock table (mock name keeps a readable tail)
    body = (image_dir / "orders.preql").read_text(encoding="utf-8")
    assert "address orders_tbl;" not in body
    assert "address mock_orders_tbl_" in body
    # descriptions round-trip through re-render is exercised elsewhere; here we
    # only assert the engine points at the mock db
    assert 'dialect = "duck_db"' in (image_dir / "trilogy.toml").read_text("utf-8")


def test_build_mock_image_remote_repoints_no_leak(tmp_path):
    model = tmp_path / "model"
    model.mkdir()
    (model / "trilogy.toml").write_text('[engine]\ndialect = "duck_db"\n', "utf-8")
    (model / "remote.preql").write_text(
        "key id int;\nproperty id.v float;\n"
        "datasource r (id: id, v: v) grain (id) file `gs://bucket/data.parquet`;\n",
        encoding="utf-8",
    )
    ex = Dialects.DUCK_DB.default_executor(environment=Environment(working_path=model))
    ex.parse_text((model / "remote.preql").read_text(encoding="utf-8"))
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    va.build_mock_image(
        model, run_dir / "img", run_dir / "m.duckdb", ex.environment, set()
    )
    body = (run_dir / "img" / "remote.preql").read_text(encoding="utf-8")
    assert "gs://" not in body
    assert "address mock_data_" in body


def test_materialize_mock_tables_referential_integrity(tmp_path):
    model = tmp_path / "model"
    model.mkdir()
    env = Environment(working_path=model)
    env.parse(
        "key customer_id int;\nkey order_id int;\n"
        "datasource customers (customer_id: customer_id) grain (customer_id) "
        "address cust_tbl;\n"
        "datasource orders (order_id: order_id, customer_id: customer_id) "
        "grain (order_id) address ord_tbl;\n"
    )
    db = tmp_path / "m.duckdb"
    va._materialize_mock_tables(env, db)
    con = duckdb.connect(str(db))
    try:
        tables = [r[0] for r in con.execute("show tables").fetchall()]
        cust = next(t for t in tables if "cust" in t)
        ordt = next(t for t in tables if "ord" in t)
        # shared key concept mocked consistently -> the join matches rows
        joined = con.execute(
            f'select count(*) from "{cust}" c join "{ordt}" o '
            "on c.customer_id = o.customer_id"
        ).fetchone()[0]
        assert joined > 0
    finally:
        con.close()


@pytest.fixture
def scored_workspace(image, model_dir):
    image_dir, run_dir = image
    expected_sql = va.compile_expected_against_image(
        image_dir, (model_dir / "validations.preql").read_text(encoding="utf-8")
    )[0]
    workspace = run_dir / "ws"
    import shutil

    shutil.copytree(image_dir, workspace)
    return workspace, expected_sql


def test_expected_reads_mock_not_real_file(tmp_path):
    """Regression: the expected side must read the mock, not the real model
    file. Uses a real parquet with a distinctive value so a leak would fail."""
    import shutil

    model = tmp_path / "model"
    (model / "data").mkdir(parents=True)
    parquet_path = (model / "data" / "things.parquet").as_posix()
    con = duckdb.connect()
    con.execute(f"COPY (SELECT 1 id, 999.0 val) TO '{parquet_path}' (FORMAT PARQUET)")
    con.close()
    (model / "trilogy.toml").write_text('[engine]\ndialect = "duck_db"\n', "utf-8")
    (model / "things.preql").write_text(
        "key id int;\nproperty id.val float;\n"
        "datasource filed (id: id, val: val) grain (id) file `data/things.parquet`;\n",
        encoding="utf-8",
    )
    (model / "validations.preql").write_text(
        "import things;\n\nvalidate t select natural 'sum val?'\n"
        "matches ( select sum(val) -> t );\n",
        encoding="utf-8",
    )
    ex = Dialects.DUCK_DB.default_executor(environment=Environment(working_path=model))
    ex.parse_text((model / "things.preql").read_text(encoding="utf-8"))
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    image_dir = run_dir / "img"
    va.build_mock_image(
        model,
        image_dir,
        run_dir / "m.duckdb",
        ex.environment,
        {(model / "validations.preql").resolve()},
    )
    expected_sql = va.compile_expected_against_image(
        image_dir, (model / "validations.preql").read_text(encoding="utf-8")
    )[0]
    assert "things.parquet" not in expected_sql  # reads mock table, not real file
    workspace = run_dir / "ws"
    shutil.copytree(image_dir, workspace)
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
