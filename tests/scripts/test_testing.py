from pathlib import Path
from unittest.mock import Mock

from click.exceptions import Exit
from click.testing import CliRunner

from trilogy.core.enums import AddressType
from trilogy.core.models.datasource import Address, Datasource
from trilogy.scripts import testing
from trilogy.scripts.dependency import ScriptNode
from trilogy.scripts.parallel_execution import (
    ExecutionMode,
    ExecutionResult,
    ParallelExecutionSummary,
)
from trilogy.scripts.trilogy import cli


def _make_summary(
    results: list[ExecutionResult], skipped: int = 0
) -> ParallelExecutionSummary:
    successful = sum(1 for result in results if result.success)
    failed = sum(1 for result in results if not result.success)
    return ParallelExecutionSummary(
        total_scripts=len(results),
        successful=successful,
        skipped=skipped,
        failed=failed,
        total_duration=0.0,
        results=results,
    )


def test_get_unsuccessful_script_nodes_excludes_dependency_skips():
    failed_node = ScriptNode(path=Path("failed.preql"))
    skipped_node = ScriptNode(path=Path("skipped.preql"))
    summary = _make_summary(
        [
            ExecutionResult(node=failed_node, success=False, error=Exit(1)),
            ExecutionResult(
                node=skipped_node,
                success=False,
                error=RuntimeError("Skipped due to failed dependency"),
            ),
        ],
        skipped=1,
    )

    actual_failures = testing._get_unsuccessful_script_nodes(
        summary, include_dependency_skips=False
    )
    affected_scripts = testing._get_unsuccessful_script_nodes(
        summary, include_dependency_skips=True
    )

    assert actual_failures == [failed_node]
    assert affected_scripts == [failed_node, skipped_node]


def test_is_refreshable_derived_datasource_is_conservative():
    assert testing._is_refreshable_derived_datasource(
        Datasource(name="table_ds", columns=[], address=Address(location="tbl"))
    )
    assert testing._is_refreshable_derived_datasource(
        Datasource(
            name="csv_ds",
            columns=[],
            address=Address(location="out.csv", type=AddressType.CSV),
        )
    )
    assert not testing._is_refreshable_derived_datasource(
        Datasource(
            name="query_ds",
            columns=[],
            address=Address(location="select 1", type=AddressType.QUERY),
        )
    )
    assert not testing._is_refreshable_derived_datasource(
        Datasource(
            name="root_ds",
            columns=[],
            address=Address(location="root_tbl"),
            is_root=True,
        )
    )


def test_integration_unchanged_without_refresh_flag(tmp_path, monkeypatch):
    test_file = tmp_path / "model.preql"
    test_file.write_text("select 1;")
    run_parallel_execution = Mock(side_effect=Exit(1))
    monkeypatch.setattr(testing, "run_parallel_execution", run_parallel_execution)

    runner = CliRunner()
    result = runner.invoke(cli, ["integration", str(test_file), "duckdb"])

    assert result.exit_code == 1
    assert run_parallel_execution.call_count == 1


def test_refresh_derived_skips_refresh_when_initial_integration_passes(
    tmp_path, monkeypatch
):
    test_file = tmp_path / "model.preql"
    test_file.write_text("select 1;")
    run_parallel_execution = Mock(side_effect=[_make_summary([])])
    monkeypatch.setattr(testing, "run_parallel_execution", run_parallel_execution)
    monkeypatch.setattr(
        testing,
        "_collect_refreshable_derived_datasources",
        Mock(side_effect=AssertionError("refresh collection should not run")),
    )

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "integration",
            str(test_file),
            "duckdb",
            "--refresh-derived",
            "failed",
        ],
    )

    assert result.exit_code == 0
    assert run_parallel_execution.call_count == 1
    assert run_parallel_execution.call_args.kwargs["fail_on_error"] is False


def test_refresh_derived_exits_when_no_refreshable_datasources_found(
    tmp_path, monkeypatch
):
    test_file = tmp_path / "model.preql"
    test_file.write_text("select 1;")
    failed_node = ScriptNode(path=test_file)
    initial_summary = _make_summary(
        [ExecutionResult(node=failed_node, success=False, error=Exit(1))]
    )
    run_parallel_execution = Mock(side_effect=[initial_summary])
    monkeypatch.setattr(testing, "run_parallel_execution", run_parallel_execution)
    monkeypatch.setattr(
        testing,
        "_collect_refreshable_derived_datasources",
        Mock(return_value=[]),
    )

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "integration",
            str(test_file),
            "duckdb",
            "--refresh-derived",
            "failed",
        ],
    )

    assert result.exit_code == 1
    assert "No refreshable derived datasources were identified" in result.output
    assert run_parallel_execution.call_count == 1


def test_refresh_derived_refreshes_and_reruns_failed_scripts(tmp_path, monkeypatch):
    test_file = tmp_path / "model.preql"
    test_file.write_text("select 1;")
    failed_node = ScriptNode(path=test_file)
    initial_summary = _make_summary(
        [ExecutionResult(node=failed_node, success=False, error=Exit(1))]
    )
    refresh_summary = _make_summary([])
    rerun_summary = _make_summary([ExecutionResult(node=failed_node, success=True)])
    run_parallel_execution = Mock(side_effect=[initial_summary, rerun_summary])
    monkeypatch.setattr(testing, "run_parallel_execution", run_parallel_execution)
    run_refresh_command = Mock(return_value=refresh_summary)
    monkeypatch.setattr(testing, "run_refresh_command", run_refresh_command)
    monkeypatch.setattr(
        testing,
        "_collect_refreshable_derived_datasources",
        Mock(return_value=["derived.one", "derived.two"]),
    )

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "integration",
            str(test_file),
            "duckdb",
            "--refresh-derived",
            "failed",
        ],
    )

    assert result.exit_code == 0
    assert run_parallel_execution.call_count == 2
    assert run_refresh_command.call_count == 1
    assert run_parallel_execution.call_args_list[0].kwargs["execution_mode"] == (
        ExecutionMode.INTEGRATION
    )
    assert run_refresh_command.call_args.args[
        0
    ].refresh_params.force_sources == frozenset({"derived.one", "derived.two"})
    assert run_parallel_execution.call_args_list[1].kwargs["execution_mode"] == (
        ExecutionMode.INTEGRATION
    )
    assert "Integration failed; attempting refresh-derived=failed" in result.output
    assert "Re-running integration" in result.output


def test_refresh_derived_returns_failure_when_rerun_still_fails(tmp_path, monkeypatch):
    test_file = tmp_path / "model.preql"
    test_file.write_text("select 1;")
    failed_node = ScriptNode(path=test_file)
    initial_summary = _make_summary(
        [ExecutionResult(node=failed_node, success=False, error=Exit(1))]
    )
    refresh_summary = _make_summary([])
    rerun_summary = _make_summary(
        [ExecutionResult(node=failed_node, success=False, error=Exit(1))]
    )
    run_parallel_execution = Mock(side_effect=[initial_summary, rerun_summary])
    monkeypatch.setattr(testing, "run_parallel_execution", run_parallel_execution)
    monkeypatch.setattr(
        testing, "run_refresh_command", Mock(return_value=refresh_summary)
    )
    monkeypatch.setattr(
        testing,
        "_collect_refreshable_derived_datasources",
        Mock(return_value=["derived.one"]),
    )

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "integration",
            str(test_file),
            "duckdb",
            "--refresh-derived",
            "failed",
        ],
    )

    assert result.exit_code == 1
    assert run_parallel_execution.call_count == 2


def test_refresh_derived_directory_uses_shared_refresh_command(tmp_path, monkeypatch):
    (tmp_path / "core.preql").write_text("select 1;")
    (tmp_path / "consumer.preql").write_text("select 2;")
    failed_node = ScriptNode(path=tmp_path / "core.preql")
    initial_summary = _make_summary(
        [ExecutionResult(node=failed_node, success=False, error=Exit(1))]
    )
    rerun_summary = _make_summary([ExecutionResult(node=failed_node, success=True)])
    run_parallel_execution = Mock(side_effect=[initial_summary, rerun_summary])
    monkeypatch.setattr(testing, "run_parallel_execution", run_parallel_execution)
    run_refresh_command = Mock(return_value=_make_summary([]))
    monkeypatch.setattr(testing, "run_refresh_command", run_refresh_command)
    monkeypatch.setattr(
        testing,
        "_collect_refreshable_derived_datasources",
        Mock(return_value=["derived.one"]),
    )
    rerun_graph = object()
    build_selected_script_graph = Mock(return_value=rerun_graph)
    monkeypatch.setattr(
        testing, "_build_selected_script_graph", build_selected_script_graph
    )

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "integration",
            str(tmp_path),
            "duckdb",
            "--refresh-derived",
            "failed",
        ],
    )

    assert result.exit_code == 0
    assert run_parallel_execution.call_count == 2
    assert [
        call.kwargs["execution_mode"] for call in run_parallel_execution.call_args_list
    ] == [ExecutionMode.INTEGRATION, ExecutionMode.INTEGRATION]
    assert run_parallel_execution.call_args_list[0].kwargs["graph"] is None
    assert run_parallel_execution.call_args_list[1].kwargs["graph"] is rerun_graph
    assert run_refresh_command.call_count == 1
    assert run_refresh_command.call_args.args[
        0
    ].refresh_params.force_sources == frozenset({"derived.one"})
