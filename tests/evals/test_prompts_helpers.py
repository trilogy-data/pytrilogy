"""Unit tests for ``evals/common/prompts.py`` — specifically how the
single-query task template surfaces an entry's optional ``params`` block.

A query with no params should render identically to before; a query with
params must (a) name each param + value in the body so the agent can copy
the value into its local validation, and (b) append matching ``--param``
flags to the example `trilogy run` invocation."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "evals"))

from common.prompts import (  # noqa: E402
    _render_params_block,
    build_single_query_task,
    candidate_filename,
    candidate_path,
    scoring_candidate_path,
    stage_candidate_for_scoring,
)
from common.spec import BenchmarkSpec  # noqa: E402


def _spec() -> BenchmarkSpec:
    return BenchmarkSpec(
        name="Bench",
        short_name="bench",
        duckdb_extension="bench",
        generator_sql="CALL build()",
        db_filename="bench.duckdb",
        eval_dir=Path("."),
        prompts_file=Path("."),
        default_scale_factor=0.01,
        default_num_queries=1,
    )


def test_render_params_block_empty():
    assert _render_params_block({}) == ("", "")


def test_render_params_block_lists_each_param():
    block, suffix = _render_params_block(
        {
            "zips": {
                "type": "string",
                "value": "10001,20002",
                "description": "Comma list.",
            }
        }
    )
    assert "zips (string)" in block
    assert "10001,20002" in block
    assert "Comma list." in block
    assert suffix == " --param zips=10001,20002"


def test_render_params_block_quotes_values_with_spaces():
    _, suffix = _render_params_block({"q": {"type": "string", "value": "hello world"}})
    assert suffix == ' --param q="hello world"'


def test_build_single_query_task_no_params_omits_block():
    task = build_single_query_task(
        _spec(),
        {"id": 3, "prompt": "Do the thing."},
    )
    assert "Parameters" not in task
    filename = candidate_filename(_spec(), 3, ".preql")
    assert f"trilogy run {filename}`" in task
    assert "Do the thing." in task


def test_build_single_query_task_with_params_appends_block_and_cli_flag():
    task = build_single_query_task(
        _spec(),
        {
            "id": 8,
            "prompt": "Filter by zips.",
            "params": {
                "zips": {
                    "type": "string",
                    "value": "10001,20002",
                    "description": "ZIP filter list.",
                }
            },
        },
    )
    assert "Parameters" in task
    assert "zips (string)" in task
    assert "10001,20002" in task
    filename = candidate_filename(_spec(), 8, ".preql")
    assert f"trilogy run {filename} --param zips=10001,20002`" in task


def test_build_single_query_task_with_params_preserves_question_body():
    """The params block is appended after the question text; the question
    must still appear verbatim (the agent reads it for the actual logic)."""
    task = build_single_query_task(
        _spec(),
        {
            "id": 8,
            "prompt": "Sentinel-phrase for the question body.",
            "params": {"x": {"type": "int", "value": "5"}},
        },
    )
    assert "Sentinel-phrase for the question body." in task


def test_stage_candidate_for_scoring_uses_shared_paths(tmp_path: Path):
    source_workspace = tmp_path / "worker"
    scoring_workspace = tmp_path / "scoring"
    source_workspace.mkdir()
    scoring_workspace.mkdir()
    source = candidate_path(source_workspace, _spec(), 8, ".preql")
    source.write_text("select 1;", encoding="utf-8")

    staged = stage_candidate_for_scoring(
        source_workspace,
        scoring_workspace,
        _spec(),
        8,
        ".preql",
        remove_source=True,
    )

    expected = scoring_candidate_path(scoring_workspace, 8, ".preql")
    assert staged == expected
    assert expected.read_text(encoding="utf-8") == "select 1;"
    assert not source.exists()


def test_stage_candidate_for_scoring_returns_none_when_missing(tmp_path: Path):
    assert stage_candidate_for_scoring(tmp_path, tmp_path, _spec(), 8, ".preql") is None
