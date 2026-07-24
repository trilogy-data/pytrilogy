"""Agent-validation runner components: workspace seeding, unit-tier mock
materialization, scoring, and the unit/integration test-type selection."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

import duckdb
import pytest

from trilogy import Dialects
from trilogy.core.enums import QueryComparison, ValidationScope
from trilogy.core.exceptions import ModelValidationError
from trilogy.core.models.environment import Environment
from trilogy.scripts import validate_agent as va
from trilogy.scripts.agent import EXIT_ITERATION_EXHAUSTED
from trilogy.scripts.common import ExecutionStats, format_stats
from trilogy.scripts.dependency import ScriptNode
from trilogy.scripts.testing import (
    DEFAULT_TEST_TYPES,
    _environment_scope,
    execute_script_for_integration,
    execute_script_for_unit,
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


def test_pass_rate_without_repetitions():
    result = va.QuestionResult(name="q", question="?", target=1.0)
    assert result.pass_rate == 0.0
    assert not result.passed
    assert result.total_tokens == 0


def test_check_agent_ready_raises_without_provider(model_dir, monkeypatch):
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    monkeypatch.delenv("TRILOGY_AGENT_MODEL", raising=False)
    with pytest.raises(Exception, match="API key"):
        va.check_agent_ready(model_dir)


def test_check_agent_ready_passes_with_provider(model_dir, monkeypatch):
    monkeypatch.setattr("trilogy.scripts.agent._build_provider", lambda *a: object())
    va.check_agent_ready(model_dir)


def test_merged_import_paths_accepts_scalar(model_dir):
    (model_dir / "trilogy.toml").write_text(
        'import_paths = "shared"\n[engine]\ndialect = "duck_db"\n', encoding="utf-8"
    )
    paths = va._merged_import_paths(model_dir)
    assert paths[0] == model_dir.resolve().as_posix()
    assert (model_dir / "shared").resolve().as_posix() in paths


def test_multiline_import_paths_fails_loudly(model_dir, tmp_path):
    from trilogy.core.exceptions import ConfigurationException

    (model_dir / "trilogy.toml").write_text(
        'import_paths = [\n  "a",\n  "b",\n]\n[engine]\ndialect = "duck_db"\n',
        encoding="utf-8",
    )
    with pytest.raises(ConfigurationException, match="single-line array"):
        va.seed_workspace(model_dir, tmp_path / "ws")


def test_workspace_tomls_rewrite_import_paths_and_keep_other_config(
    model_dir, tmp_path
):
    (model_dir / "trilogy.toml").write_text(
        'import_paths = ["shared"]\n\n[project]\nname = "demo"\n\n'
        '[engine]\ndialect = "duck_db"\n\n[engine.config]\npath = "local.duckdb"\n',
        encoding="utf-8",
    )
    workspace = tmp_path / "ws"
    va.seed_workspace(model_dir, workspace)
    seeded = (workspace / "trilogy.toml").read_text(encoding="utf-8")
    # the model's own line is replaced by the merged one (no duplicate key)
    assert seeded.count("import_paths") == 1
    assert (model_dir / "shared").resolve().as_posix() in seeded
    assert 'name = "demo"' in seeded

    executor = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=model_dir)
    )
    executor.parse_text((model_dir / "orders.preql").read_text(encoding="utf-8"))
    image_dir = tmp_path / "img"
    va.build_mock_image(
        model_dir, image_dir, tmp_path / "m.duckdb", executor.environment, set()
    )
    unit_toml = (image_dir / "trilogy.toml").read_text(encoding="utf-8")
    assert "import_paths" not in unit_toml  # the image is self-contained
    assert 'name = "demo"' in unit_toml  # non-engine config carries over
    assert "local.duckdb" not in unit_toml  # engine repointed at the mock db


def test_seed_workspace_honors_exclude(model_dir, tmp_path):
    workspace = tmp_path / "ws"
    va.seed_workspace(
        model_dir, workspace, exclude={(model_dir / "raw" / "extra.preql").resolve()}
    )
    assert (workspace / "orders.preql").exists()
    assert not (workspace / "raw" / "extra.preql").exists()


def test_mock_name_for_non_address_location():
    from trilogy.core.models.datasource import Datasource
    from trilogy.parser import parse_text

    _, stmts = parse_text(
        "key id int;\ndatasource d (id: id) grain (id) address t;\n", Environment()
    )
    ds = next(s for s in stmts if isinstance(s, Datasource))
    ds.address = "bare_string_address"
    assert va._mock_name(ds).startswith("mock_bare_string_address_")


def test_materialize_dedupes_shared_physical_address(tmp_path):
    env = Environment(working_path=tmp_path)
    env.parse(
        "key id int;\n"
        "datasource one (id: id) grain (id) address shared_tbl;\n"
        "datasource two (id: id) grain (id) address shared_tbl;\n"
    )
    db = tmp_path / "m.duckdb"
    va._materialize_mock_tables(env, db)
    con = duckdb.connect(str(db))
    try:
        assert len(con.execute("show tables").fetchall()) == 1
    finally:
        con.close()


def test_build_mock_image_copies_schema_docs(model_dir, tmp_path):
    (model_dir / "schema.md").write_text("# notes\n", encoding="utf-8")
    executor = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=model_dir)
    )
    executor.parse_text((model_dir / "orders.preql").read_text(encoding="utf-8"))
    image_dir = tmp_path / "img"
    va.build_mock_image(
        model_dir, image_dir, tmp_path / "m.duckdb", executor.environment, set()
    )
    assert (image_dir / "schema.md").read_text(encoding="utf-8") == "# notes\n"


def test_build_mock_image_fails_on_unparseable_file(model_dir, tmp_path):
    from trilogy.core.exceptions import ConfigurationException

    (model_dir / "broken.preql").write_text("this is not trilogy;\n", encoding="utf-8")
    executor = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=model_dir)
    )
    executor.parse_text((model_dir / "orders.preql").read_text(encoding="utf-8"))
    with pytest.raises(ConfigurationException, match="standalone"):
        va.build_mock_image(
            model_dir,
            tmp_path / "img",
            tmp_path / "m.duckdb",
            executor.environment,
            set(),
        )


def test_executor_for_workspace_requires_a_dialect(tmp_path):
    from trilogy.core.exceptions import ConfigurationException

    (tmp_path / "trilogy.toml").write_text("[project]\nname = 'x'\n", encoding="utf-8")
    with pytest.raises(ConfigurationException, match="No engine.dialect"):
        va._executor_for_workspace(tmp_path)


def test_score_workspace_error_paths(scored_workspace):
    workspace, expected_sql = scored_workspace
    (workspace / va.ANSWER_FILENAME).write_text(
        "import orders;\nkey unused int;\n", encoding="utf-8"
    )
    result = va.score_workspace(workspace, expected_sql, QueryComparison.TOLERANT)
    assert result.status == "error"
    assert "no executable statement" in result.detail

    (workspace / va.ANSWER_FILENAME).write_text(
        "import orders;\nselect sum(amount) -> my_total;\n", encoding="utf-8"
    )
    result = va.score_workspace(
        workspace, "select * from nope_missing_table", QueryComparison.TOLERANT
    )
    assert result.status == "error"
    assert "expected execute" in result.detail


def test_score_workspace_reports_candidate_execution_failure(
    scored_workspace, monkeypatch
):
    workspace, expected_sql = scored_workspace
    (workspace / va.ANSWER_FILENAME).write_text(
        "import orders;\nselect sum(amount) -> my_total;\n", encoding="utf-8"
    )
    monkeypatch.setattr(
        va, "_executor_for_workspace", _executor_failing_on("my_total", workspace)
    )
    result = va.score_workspace(workspace, expected_sql, QueryComparison.TOLERANT)
    assert result.status == "error"
    assert "candidate execute" in result.detail


def _executor_failing_on(marker: str, workspace):
    """An executor whose raw-SQL execution blows up for statements containing
    ``marker`` — used to reach the candidate-execute error branch."""
    real = va._executor_for_workspace

    def factory(path):
        executor = real(path)
        original = executor.execute_raw_sql

        def guarded(sql, *args, **kwargs):
            if isinstance(sql, str) and marker in sql:
                raise RuntimeError("boom")
            return original(sql, *args, **kwargs)

        executor.execute_raw_sql = guarded
        return executor

    return factory


class _FakeProc:
    def __init__(self, output: str, returncode: int, timeout: bool):
        self._output = output
        self.returncode = returncode
        self._timeout = timeout
        self.killed = False

    def communicate(self, timeout=None):
        if self._timeout and not self.killed:
            raise subprocess.TimeoutExpired(cmd="agent", timeout=timeout or 0)
        return self._output, None

    def kill(self):
        self.killed = True
        self.returncode = -9


@pytest.fixture
def fake_popen(monkeypatch):
    """Replace the agent subprocess; returns the captured launch kwargs."""
    captured: dict = {}

    def install(output="ok", returncode=0, timeout=False):
        def popen(cmd, **kwargs):
            captured["cmd"] = cmd
            captured["cwd"] = kwargs.get("cwd")
            return _FakeProc(output, returncode, timeout)

        monkeypatch.setattr(subprocess, "Popen", popen)
        return captured

    return install


def test_run_agent_once_success(tmp_path, fake_popen):
    captured = fake_popen(output="\n".join(str(i) for i in range(30)))
    run = va.run_agent_once(tmp_path, "task", tmp_path / "log.jsonl", 5)
    assert run.exit_code == 0
    assert not run.timed_out
    assert run.duration >= 0
    # tail is capped at the last 15 lines
    assert run.output_tail.splitlines() == [str(i) for i in range(15, 30)]
    assert captured["cwd"] == tmp_path
    assert "agent" in captured["cmd"]
    assert str(tmp_path / "log.jsonl") in captured["cmd"]


def test_run_agent_once_timeout_kills_the_process(tmp_path, fake_popen):
    fake_popen(output="", timeout=True)
    run = va.run_agent_once(tmp_path, "task", tmp_path / "log.jsonl", 1)
    assert run.timed_out
    assert run.exit_code == -9
    assert run.output_tail == ""


def test_light_metrics_reads_jsonl(tmp_path):
    log = tmp_path / "log.jsonl"
    assert va.light_metrics(log) == (0, 0)
    log.write_text(
        '{"type": "llm_response", "usage": {"total_tokens": 100}}\n'
        "\n"
        "not json at all\n"
        '{"type": "tool_call"}\n'
        '{"type": "llm_response", "usage": null}\n'
        '{"type": "llm_response", "usage": {"total_tokens": 50}}\n',
        encoding="utf-8",
    )
    assert va.light_metrics(log) == (150, 3)


@pytest.mark.parametrize(
    "exit_code, timed_out, expected_status",
    [
        (0, True, "timeout"),
        (EXIT_ITERATION_EXHAUSTED, False, "exhausted"),
        (EXIT_ITERATION_EXHAUSTED + 1, False, "crashed"),
        (0, False, "fail"),
    ],
)
def test_apply_process_status(exit_code, timed_out, expected_status):
    run = va.AgentRun(
        exit_code=exit_code, timed_out=timed_out, duration=1.0, output_tail="tail"
    )
    result = va._apply_process_status(va.RepetitionResult(status="fail"), run)
    assert result.status == expected_status


def test_apply_process_status_never_downgrades_a_pass():
    run = va.AgentRun(exit_code=3, timed_out=True, duration=1.0, output_tail="")
    assert va._apply_process_status(va.RepetitionResult(status="pass"), run).status == (
        "pass"
    )


def test_run_validation_question_and_report(image, model_dir, tmp_path, monkeypatch):
    image_dir, _ = image
    expected_sql = va.compile_expected_against_image(
        image_dir, (model_dir / "validations.preql").read_text(encoding="utf-8")
    )[0]
    answers = [
        "import orders;\nselect sum(amount) -> my_total;\n",  # correct
        "import orders;\nselect count(order_id) -> my_total;\n",  # wrong
    ]

    def fake_run(workspace, task, log_path, timeout):
        assert "sum amount?" in task
        (workspace / va.ANSWER_FILENAME).write_text(
            answers[len(list(log_path.parent.glob("*.jsonl")))], encoding="utf-8"
        )
        log_path.write_text(
            '{"type": "llm_response", "usage": {"total_tokens": 10}}\n',
            encoding="utf-8",
        )
        return va.AgentRun(exit_code=0, timed_out=False, duration=0.5, output_tail="")

    monkeypatch.setattr(va, "run_agent_once", fake_run)
    question_run_dir = tmp_path / "qrun"
    result = va.run_validation_question(
        name="total",
        question="sum amount?",
        expected_sql=expected_sql,
        comparison=QueryComparison.TOLERANT,
        repetitions=2,
        target=0.5,
        timeout=None,
        tags=["smoke"],
        model_dir=model_dir,
        run_dir=question_run_dir,
        exclude=set(),
        image_dir=image_dir,
    )
    assert [r.status for r in result.repetitions] == ["pass", "fail"]
    assert result.pass_rate == 0.5
    assert result.passed
    assert result.total_tokens == 20
    assert all(r.iterations == 1 for r in result.repetitions)

    report_path = va.write_report(
        question_run_dir, model_dir / "validations.preql", [result]
    )
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["questions"][0]["name"] == "total"
    assert report["questions"][0]["tags"] == ["smoke"]
    assert report["questions"][0]["passed"]
    assert len(report["questions"][0]["repetitions"]) == 2
    assert report["questions"][0]["repetitions"][0]["workspace"]


def test_run_validation_question_seeds_the_live_model(model_dir, tmp_path, monkeypatch):
    """Integration tier (no image): each repetition gets a fresh seeded copy of
    the real model, minus the excluded validations file."""
    seen: list[Path] = []

    def fake_run(workspace, task, log_path, timeout):
        seen.append(workspace)
        assert (workspace / "orders.preql").exists()
        assert not (workspace / "validations.preql").exists()
        return va.AgentRun(exit_code=1, timed_out=False, duration=0.1, output_tail="x")

    (model_dir / "validations.preql").write_text("# secret\n", encoding="utf-8")
    monkeypatch.setattr(va, "run_agent_once", fake_run)
    result = va.run_validation_question(
        name="q",
        question="?",
        expected_sql="select 1",
        comparison=QueryComparison.TOLERANT,
        repetitions=1,
        target=1.0,
        timeout=30,
        tags=[],
        model_dir=model_dir,
        run_dir=tmp_path / "run",
        exclude={(model_dir / "validations.preql").resolve()},
    )
    assert len(seen) == 1
    assert result.repetitions[0].status == "crashed"
    assert not result.passed


def test_execute_natural_select_runs_candidate_on_caller_engine(model_dir, monkeypatch):
    monkeypatch.setattr(va, "check_agent_ready", lambda model: None)

    def fake_run(workspace, task, log_path, timeout):
        (workspace / va.ANSWER_FILENAME).write_text(
            "import orders;\nselect 42 -> answer;\n", encoding="utf-8"
        )
        return va.AgentRun(exit_code=0, timed_out=False, duration=0.1, output_tail="")

    monkeypatch.setattr(va, "run_agent_once", fake_run)
    executor = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=model_dir)
    )
    rows = va.execute_natural_select(executor, "what is the answer?").fetchall()
    assert rows[0][0] == 42
    # the caller's environment is restored after the candidate is compiled
    assert Path(executor.environment.working_path) == model_dir


@pytest.mark.parametrize(
    "run_kwargs, writes_answer, message",
    [
        ({"exit_code": 0, "timed_out": True}, False, "timed out"),
        ({"exit_code": 2, "timed_out": False}, False, "exited 2"),
        ({"exit_code": 0, "timed_out": False}, True, "no executable statement"),
    ],
)
def test_execute_natural_select_failures(
    model_dir, monkeypatch, run_kwargs, writes_answer, message
):
    from trilogy.core.exceptions import ConfigurationException

    monkeypatch.setattr(va, "check_agent_ready", lambda model: None)

    def fake_run(workspace, task, log_path, timeout):
        if writes_answer:
            (workspace / va.ANSWER_FILENAME).write_text(
                "key unused int;\n", encoding="utf-8"
            )
        return va.AgentRun(duration=0.1, output_tail="", **run_kwargs)

    monkeypatch.setattr(va, "run_agent_once", fake_run)
    executor = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=model_dir)
    )
    with pytest.raises(ConfigurationException, match=message):
        va.execute_natural_select(executor, "q?")


VALIDATIONS = (
    "import orders;\n\nvalidate total select natural 'sum amount?'\n"
    "matches ( select sum(amount) -> t );\n"
)


@pytest.fixture
def live_model(tmp_path):
    """A model whose configured DuckDB file actually holds the datasource table,
    so the integration tier can score against a live backend."""
    model = tmp_path / "live"
    model.mkdir()
    con = duckdb.connect(str(model / "local.duckdb"))
    con.execute("create table orders_tbl (order_id int, amount double, region varchar)")
    con.execute("insert into orders_tbl values (1, 10.0, 'a'), (2, 20.0, 'b')")
    con.close()
    (model / "trilogy.toml").write_text(
        '[engine]\ndialect = "duck_db"\n\n[engine.config]\npath = "local.duckdb"\n',
        encoding="utf-8",
    )
    (model / "orders.preql").write_text(MODEL, encoding="utf-8")
    (model / "validations.preql").write_text(VALIDATIONS, encoding="utf-8")
    return model


def _agent_writes(answer: str):
    def fake_run(workspace, task, log_path, timeout):
        (workspace / va.ANSWER_FILENAME).write_text(answer, encoding="utf-8")
        return va.AgentRun(exit_code=0, timed_out=False, duration=0.1, output_tail="")

    return fake_run


@pytest.fixture
def agent_tier(monkeypatch):
    """Enable the agent test type with the LLM subprocess stubbed out."""

    def install(answer: str):
        monkeypatch.setattr(va, "check_agent_ready", lambda model: None)
        monkeypatch.setattr(va, "run_agent_once", _agent_writes(answer))

    return install


def _run_tier(model, execution_fn, quiet=True, **kwargs):
    from trilogy.dialect.config import DuckDBConfig

    executor = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=model),
        conf=DuckDBConfig(path=str(model / "local.duckdb")),
    )
    node = ScriptNode(path=model / "validations.preql")
    return execution_fn(executor, node, quiet=quiet, **kwargs)


def test_unit_tier_runs_agent_questions_against_the_mock(live_model, agent_tier):
    agent_tier("import orders;\nselect sum(amount) -> mine;\n")
    stats = _run_tier(
        live_model,
        execute_script_for_unit,
        quiet=False,
        test_types=frozenset({"datasources", "concepts", "agent"}),
    )
    assert stats.agent_question_count == 1
    assert stats.agent_passed == 1
    assert stats.agent_skipped == 0
    reports = list(live_model.glob(".trilogy/validate_runs/*/report.json"))
    assert len(reports) == 1
    assert json.loads(reports[0].read_text(encoding="utf-8"))["questions"][0]["passed"]


def test_integration_tier_scores_against_the_live_backend(live_model, agent_tier):
    agent_tier("import orders;\nselect sum(amount) -> mine;\n")
    stats = _run_tier(
        live_model,
        execute_script_for_integration,
        # agent only: the environment-validation phase is skipped entirely
        test_types=frozenset({"agent"}),
        agent_report=False,
    )
    assert stats.agent_passed == 1
    assert stats.validate_count == 0
    assert not list(live_model.glob(".trilogy/validate_runs/*/report.json"))


def test_failing_question_fails_the_node(live_model, agent_tier):
    agent_tier("import orders;\nselect count(order_id) -> mine;\n")
    with pytest.raises(ModelValidationError, match="1/1 questions below target"):
        _run_tier(
            live_model,
            execute_script_for_integration,
            test_types=frozenset({"agent"}),
        )


def test_questions_are_skipped_unless_the_agent_type_is_included(live_model):
    stats = _run_tier(live_model, execute_script_for_integration)
    assert stats.agent_question_count == 0
    assert stats.agent_skipped == 1
    assert "1 agent question skipped" in format_stats(stats, ["validate"])


def test_format_stats_reports_agent_questions():
    stats = ExecutionStats(agent_question_count=3, agent_passed=2, agent_skipped=2)
    rendered = format_stats(stats, ["validate"])
    assert "2/3 agent questions passed" in rendered
    assert "2 agent questions skipped" in rendered
    assert "1/1 agent question passed" in format_stats(
        ExecutionStats(agent_question_count=1, agent_passed=1), ["validate"]
    )


def test_execution_stats_add_carries_agent_counters():
    combined = ExecutionStats(agent_question_count=1, agent_passed=1) + ExecutionStats(
        agent_question_count=2, agent_passed=1, agent_skipped=3
    )
    assert combined.agent_question_count == 3
    assert combined.agent_passed == 2
    assert combined.agent_skipped == 3


def test_resolve_test_types():
    assert resolve_test_types((), ()) == DEFAULT_TEST_TYPES
    assert "agent" in resolve_test_types((), ("agent",))
    assert "datasources" not in resolve_test_types(("datasources",), ())
    # skip wins over include
    assert resolve_test_types(("agent",), ("agent",)) == DEFAULT_TEST_TYPES


def test_environment_scope():
    assert _environment_scope(frozenset({"datasources", "concepts"})) is (
        ValidationScope.ALL
    )
    assert _environment_scope(frozenset({"datasources"})) is (
        ValidationScope.DATASOURCES
    )
    assert _environment_scope(frozenset({"concepts"})) is ValidationScope.CONCEPTS
    assert _environment_scope(frozenset({"agent"})) is None
