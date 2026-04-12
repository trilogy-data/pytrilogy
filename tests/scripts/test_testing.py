from pathlib import Path
from unittest.mock import Mock

import networkx as nx
from click.exceptions import Exit
from click.testing import CliRunner

from trilogy.core.enums import AddressType
from trilogy.core.models.datasource import Address, Datasource
from trilogy.dialect.enums import Dialects
from trilogy.scripts import testing
from trilogy.scripts.common import CLIRuntimeParams
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


def test_collect_refreshable_derived_datasources_parses_failed_scripts(
    tmp_path, monkeypatch
):
    first_file = tmp_path / "first.preql"
    second_file = tmp_path / "second.preql"
    first_file.write_text("first")
    second_file.write_text("second")

    config = Mock()
    monkeypatch.setattr(
        testing,
        "resolve_input_information",
        Mock(return_value=(None, None, None, None, config)),
    )
    monkeypatch.setattr(
        testing,
        "merge_runtime_config",
        Mock(return_value=(Dialects.DUCK_DB, 1)),
    )

    first_executor = Mock()
    first_executor.environment.datasources = {
        "root": Datasource(
            name="root",
            columns=[],
            address=Address(location="root_table"),
            is_root=True,
        ),
        "query": Datasource(
            name="query",
            columns=[],
            address=Address(location="select 1", type=AddressType.QUERY),
        ),
        "table": Datasource(
            name="table",
            columns=[],
            address=Address(location="derived_table"),
        ),
    }
    second_executor = Mock()
    second_executor.environment.datasources = {
        "csv": Datasource(
            name="csv",
            columns=[],
            address=Address(location="derived.csv", type=AddressType.CSV),
        ),
        "table": Datasource(
            name="table",
            columns=[],
            address=Address(location="derived_table"),
        ),
    }
    create_executor_for_script = Mock(side_effect=[first_executor, second_executor])
    monkeypatch.setattr(
        testing, "create_executor_for_script", create_executor_for_script
    )

    cli_params = CLIRuntimeParams(
        input=str(tmp_path),
        dialect=Dialects.DUCK_DB,
        param=("limit=1",),
        conn_args=("conn",),
        debug=True,
        debug_file="debug.log",
    )

    result = testing._collect_refreshable_derived_datasources(
        cli_params, [ScriptNode(first_file), ScriptNode(second_file)]
    )

    assert result == ["csv", "table"]
    assert [call.args[0] for call in create_executor_for_script.call_args_list] == [
        ScriptNode(first_file),
        ScriptNode(second_file),
    ]
    assert first_executor.parse_text.call_args.args == ("first",)
    assert first_executor.parse_text.call_args.kwargs == {"root": first_file}
    assert second_executor.parse_text.call_args.args == ("second",)
    assert second_executor.parse_text.call_args.kwargs == {"root": second_file}
    first_executor.close.assert_called_once_with()
    second_executor.close.assert_called_once_with()


def test_collect_refreshable_derived_datasources_closes_executor_on_parse_error(
    tmp_path, monkeypatch
):
    test_file = tmp_path / "broken.preql"
    test_file.write_text("broken")
    executor = Mock()
    executor.parse_text.side_effect = ValueError("parse failed")

    monkeypatch.setattr(
        testing,
        "resolve_input_information",
        Mock(return_value=(None, None, None, None, Mock())),
    )
    monkeypatch.setattr(
        testing,
        "merge_runtime_config",
        Mock(return_value=(Dialects.DUCK_DB, 1)),
    )
    monkeypatch.setattr(
        testing, "create_executor_for_script", Mock(return_value=executor)
    )

    cli_params = CLIRuntimeParams(input=str(tmp_path), dialect=Dialects.DUCK_DB)

    try:
        testing._collect_refreshable_derived_datasources(
            cli_params, [ScriptNode(test_file)]
        )
    except ValueError as exc:
        assert str(exc) == "parse failed"
    else:
        raise AssertionError("expected parse error")

    executor.close.assert_called_once_with()


def test_build_selected_script_graph_for_file_input(tmp_path):
    selected_nodes = [ScriptNode(tmp_path / "model.preql")]
    graph = testing._build_selected_script_graph(selected_nodes[0].path, selected_nodes)

    assert list(graph.nodes) == selected_nodes
    assert list(graph.edges) == []


def test_build_selected_script_graph_for_directory_input(tmp_path, monkeypatch):
    core_node = ScriptNode(tmp_path / "core.preql")
    selected_node = ScriptNode(tmp_path / "selected.preql")
    skipped_node = ScriptNode(tmp_path / "skipped.preql")
    other_node = ScriptNode(tmp_path / "other.preql")
    full_graph = nx.DiGraph()
    full_graph.add_edge(core_node, selected_node)
    full_graph.add_edge(core_node, other_node)
    full_graph.add_edge(selected_node, skipped_node)

    resolver = Mock()
    resolver.build_folder_graph.return_value = full_graph
    monkeypatch.setattr(testing, "DependencyResolver", Mock(return_value=resolver))

    graph = testing._build_selected_script_graph(
        tmp_path, [core_node, selected_node, skipped_node]
    )

    assert set(graph.nodes) == {core_node, selected_node, skipped_node}
    assert set(graph.edges) == {
        (core_node, selected_node),
        (selected_node, skipped_node),
    }
    resolver.build_folder_graph.assert_called_once_with(tmp_path)


def test_build_selected_script_graph_empty_selection(tmp_path):
    graph = testing._build_selected_script_graph(tmp_path, [])

    assert list(graph.nodes) == []
    assert list(graph.edges) == []


def test_build_initial_integration_graph_only_for_file_input(tmp_path):
    test_file = tmp_path / "model.preql"
    test_file.write_text("select 1;")

    file_graph = testing._build_initial_integration_graph(test_file)
    directory_graph = testing._build_initial_integration_graph(tmp_path)

    assert file_graph is not None
    assert list(file_graph.nodes) == [ScriptNode(test_file)]
    assert directory_graph is None


def test_run_integration_with_summary_disables_fail_on_error(monkeypatch):
    cli_params = CLIRuntimeParams(input="model.preql", dialect=Dialects.DUCK_DB)
    graph = nx.DiGraph()
    summary = _make_summary([])
    run_parallel_execution = Mock(return_value=summary)
    monkeypatch.setattr(testing, "run_parallel_execution", run_parallel_execution)

    result = testing._run_integration_with_summary(cli_params, graph=graph)

    assert result is summary
    run_parallel_execution.assert_called_once_with(
        cli_params=cli_params,
        execution_fn=testing.execute_script_for_integration,
        execution_mode=ExecutionMode.INTEGRATION,
        graph=graph,
        fail_on_error=False,
    )


def test_run_refresh_for_derived_datasources_sets_force_sources(monkeypatch):
    cli_params = CLIRuntimeParams(input="model.preql", dialect=Dialects.DUCK_DB)
    summary = _make_summary([])
    run_refresh_command = Mock(return_value=summary)
    monkeypatch.setattr(testing, "run_refresh_command", run_refresh_command)

    result = testing._run_refresh_for_derived_datasources(
        cli_params, ["derived.two", "derived.one"]
    )

    assert result is summary
    refresh_cli_params = run_refresh_command.call_args.args[0]
    assert refresh_cli_params is not cli_params
    assert refresh_cli_params.refresh_params.force_sources == frozenset(
        {"derived.one", "derived.two"}
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


def test_refresh_derived_exits_when_only_dependency_skips(tmp_path, monkeypatch):
    """When all failures are dependency-skipped, no refresh targets exist."""
    test_file = tmp_path / "model.preql"
    test_file.write_text("select 1;")
    skipped_node = ScriptNode(path=test_file)
    initial_summary = _make_summary(
        [
            ExecutionResult(
                node=skipped_node,
                success=False,
                error=RuntimeError("Skipped due to failed dependency"),
            )
        ],
        skipped=1,
    )
    run_parallel_execution = Mock(side_effect=[initial_summary])
    monkeypatch.setattr(testing, "run_parallel_execution", run_parallel_execution)

    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["integration", str(test_file), "duckdb", "--refresh-derived", "failed"],
    )

    assert result.exit_code == 1
    assert "No actual failed scripts were identified" in result.output
    assert run_parallel_execution.call_count == 1


def test_refresh_derived_prints_message_on_noop_refresh(tmp_path, monkeypatch):
    """When refresh completes but updates nothing, a message is printed."""
    test_file = tmp_path / "model.preql"
    test_file.write_text("select 1;")
    failed_node = ScriptNode(path=test_file)
    initial_summary = _make_summary(
        [ExecutionResult(node=failed_node, success=False, error=Exit(1))]
    )
    noop_refresh = _make_summary([], skipped=1)
    rerun_summary = _make_summary([ExecutionResult(node=failed_node, success=True)])
    run_parallel_execution = Mock(side_effect=[initial_summary, rerun_summary])
    monkeypatch.setattr(testing, "run_parallel_execution", run_parallel_execution)
    monkeypatch.setattr(testing, "run_refresh_command", Mock(return_value=noop_refresh))
    monkeypatch.setattr(
        testing,
        "_collect_refreshable_derived_datasources",
        Mock(return_value=["derived.one"]),
    )

    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["integration", str(test_file), "duckdb", "--refresh-derived", "failed"],
    )

    assert result.exit_code == 0
    assert "without updating any derived datasources" in result.output
