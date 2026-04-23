"""Testing commands (integration and unit) for Trilogy CLI."""

from dataclasses import replace as dataclass_replace
from pathlib import Path as PathlibPath

import networkx as nx
from click import UNPROCESSED, Choice, Path, argument, option, pass_context
from click.exceptions import Exit

from trilogy import Executor
from trilogy.core.enums import AddressType
from trilogy.core.models.datasource import Address, Datasource
from trilogy.dialect.enums import Dialects
from trilogy.scripts.common import (
    CLIRuntimeParams,
    ExecutionStats,
    RefreshParams,
    count_statement_stats,
    create_executor_for_script,
    handle_execution_exception,
    merge_runtime_config,
    resolve_input_information,
    validate_environment,
)
from trilogy.scripts.dependency import DependencyResolver, ScriptNode
from trilogy.scripts.display import print_info
from trilogy.scripts.parallel_execution import (
    ExecutionMode,
    ExecutionResult,
    ParallelExecutionSummary,
    run_parallel_execution,
)
from trilogy.scripts.refresh import run_refresh_command
from trilogy.utility import safe_open

FAILED_DEPENDENCY_ERROR = "Skipped due to failed dependency"


def execute_script_for_integration(
    exec: Executor, node: ScriptNode, quiet: bool = False
) -> ExecutionStats:
    """Execute a script for the 'integration' command (parse + validate)."""
    with safe_open(node.path) as f:
        queries = exec.parse_text(f.read())
    stats = count_statement_stats(queries)
    validate_environment(exec, mock=False, quiet=quiet)
    # Count datasources validated
    stats.validate_count = len(exec.environment.datasources)
    return stats


def execute_script_for_unit(
    exec: Executor, node: ScriptNode, quiet: bool = False
) -> ExecutionStats:
    """Execute a script for the 'unit' command (parse + mock validate)."""
    with safe_open(node.path) as f:
        queries = exec.parse_text(f.read())
    stats = count_statement_stats(queries)
    validate_environment(exec, mock=True, quiet=quiet)
    # Count datasources validated
    stats.validate_count = len(exec.environment.datasources)
    return stats


def _is_dependency_skipped_result(result: ExecutionResult) -> bool:
    return (
        not result.success
        and isinstance(result.error, RuntimeError)
        and FAILED_DEPENDENCY_ERROR in str(result.error)
    )


def _get_unsuccessful_script_nodes(
    summary: ParallelExecutionSummary,
    *,
    include_dependency_skips: bool,
) -> list[ScriptNode]:
    seen: set[ScriptNode] = set()
    nodes: list[ScriptNode] = []
    for result in summary.results:
        if result.success or not isinstance(result.node, ScriptNode):
            continue
        if not include_dependency_skips and _is_dependency_skipped_result(result):
            continue
        if result.node in seen:
            continue
        seen.add(result.node)
        nodes.append(result.node)
    return nodes


def _is_refreshable_derived_datasource(datasource: Datasource) -> bool:
    if datasource.is_root:
        return False
    if not isinstance(datasource.address, Address):
        return True
    return datasource.address.type in {
        AddressType.TABLE,
        AddressType.CSV,
        AddressType.PARQUET,
    }


def _collect_refreshable_derived_datasources(
    cli_params: CLIRuntimeParams, failed_scripts: list[ScriptNode]
) -> list[str]:
    _, _, _, _, config = resolve_input_information(
        cli_params.input, cli_params.config_path
    )
    edialect, _ = merge_runtime_config(cli_params, config)
    datasources: set[str] = set()

    for node in failed_scripts:
        executor = create_executor_for_script(
            node,
            cli_params.param,
            cli_params.conn_args,
            edialect,
            cli_params.debug,
            config,
            cli_params.debug_file,
        )
        try:
            with safe_open(node.path) as handle:
                executor.parse_text(handle.read(), root=node.path)
            datasources.update(
                ds.identifier
                for ds in executor.environment.datasources.values()
                if _is_refreshable_derived_datasource(ds)
            )
        finally:
            executor.close()

    return sorted(datasources)


def _build_selected_script_graph(
    input_path: PathlibPath, selected_nodes: list[ScriptNode]
) -> nx.DiGraph:
    graph = nx.DiGraph()
    if not selected_nodes:
        return graph
    if input_path.is_dir():
        full_graph = DependencyResolver().build_folder_graph(input_path)
        return full_graph.subgraph(selected_nodes).copy()

    graph.add_nodes_from(selected_nodes)
    return graph


def _build_initial_integration_graph(input_path: PathlibPath) -> nx.DiGraph | None:
    if input_path.is_file():
        graph = nx.DiGraph()
        graph.add_node(ScriptNode(path=input_path))
        return graph
    return None


def _run_integration_with_summary(
    cli_params: CLIRuntimeParams, graph: nx.DiGraph | None = None
) -> ParallelExecutionSummary:
    return run_parallel_execution(
        cli_params=cli_params,
        execution_fn=execute_script_for_integration,
        execution_mode=ExecutionMode.INTEGRATION,
        graph=graph,
        fail_on_error=False,
    )


def _run_refresh_for_derived_datasources(
    cli_params: CLIRuntimeParams, datasource_names: list[str]
) -> ParallelExecutionSummary:
    refresh_cli_params = dataclass_replace(
        cli_params,
        refresh_params=RefreshParams(force_sources=frozenset(datasource_names)),
    )
    return run_refresh_command(refresh_cli_params)


@argument("input", type=Path())
@argument("dialect", type=str, required=False)
@option("--param", multiple=True, help="Environment parameters as key=value pairs")
@option(
    "--parallelism",
    "-p",
    default=None,
    help="Maximum parallel workers for directory execution",
)
@option(
    "--config", type=Path(exists=True), help="Path to trilogy.toml configuration file"
)
@option(
    "--env",
    "-e",
    multiple=True,
    help="Set env vars as KEY=VALUE or pass an env file path",
)
@option(
    "--refresh-derived",
    type=Choice(["failed"]),
    default=None,
    help="Attempt a targeted derived datasource refresh when integration fails.",
)
@argument("conn_args", nargs=-1, type=UNPROCESSED)
@pass_context
def integration(
    ctx,
    input,
    dialect: str | None,
    param,
    parallelism: int | None,
    config,
    env,
    refresh_derived: str | None,
    conn_args,
):
    """Run integration tests on Trilogy scripts."""
    cli_params = CLIRuntimeParams(
        input=input,
        dialect=Dialects(dialect) if dialect else None,
        parallelism=parallelism,
        param=param,
        conn_args=conn_args,
        debug=ctx.obj["DEBUG"],
        debug_file=ctx.obj.get("DEBUG_FILE"),
        config_path=PathlibPath(config) if config else None,
        execution_strategy="eager_bfs",
        env=env,
    )

    try:
        if refresh_derived is None:
            run_parallel_execution(
                cli_params=cli_params,
                execution_fn=execute_script_for_integration,
                execution_mode=ExecutionMode.INTEGRATION,
            )
            return

        input_path = PathlibPath(input)
        print_info("Initial integration run")
        initial_summary = _run_integration_with_summary(
            cli_params,
            graph=_build_initial_integration_graph(input_path),
        )
        if initial_summary.all_succeeded:
            return

        print_info("Integration failed; attempting refresh-derived=failed")
        failed_scripts = _get_unsuccessful_script_nodes(
            initial_summary, include_dependency_skips=False
        )
        affected_scripts = _get_unsuccessful_script_nodes(
            initial_summary, include_dependency_skips=True
        )

        if not failed_scripts:
            print_info(
                "No actual failed scripts were identified; dependency-skipped scripts are not refresh targets."
            )
            raise Exit(1)

        refreshable_datasources = _collect_refreshable_derived_datasources(
            cli_params, failed_scripts
        )
        if not refreshable_datasources:
            print_info(
                "No refreshable derived datasources were identified from failed scripts."
            )
            raise Exit(1)

        print_info(
            "Refreshing "
            f"{len(refreshable_datasources)} derived datasource(s) from "
            f"{len(failed_scripts)} failed script(s)"
        )
        refresh_summary = _run_refresh_for_derived_datasources(
            cli_params, refreshable_datasources
        )
        if refresh_summary.successful == 0 and refresh_summary.skipped > 0:
            print_info(
                "Refresh phase completed without updating any derived datasources."
            )

        print_info(
            f"Re-running integration for {len(affected_scripts)} affected script(s)"
        )
        rerun_summary = _run_integration_with_summary(
            cli_params,
            graph=_build_selected_script_graph(input_path, affected_scripts),
        )
        if not rerun_summary.all_succeeded:
            raise Exit(1)
    except Exit:
        raise
    except Exception as e:
        handle_execution_exception(e, debug=cli_params.debug)


@argument("input", type=Path())
@option("--param", multiple=True, help="Environment parameters as key=value pairs")
@option(
    "--parallelism",
    "-p",
    default=None,
    help="Maximum parallel workers for directory execution",
)
@option(
    "--config", type=Path(exists=True), help="Path to trilogy.toml configuration file"
)
@pass_context
def unit(
    ctx,
    input,
    param,
    parallelism: int | None,
    config,
):
    """Run unit tests on Trilogy scripts with mocked datasources."""
    # Build CLI runtime params (unit tests always use DuckDB)
    cli_params = CLIRuntimeParams(
        input=input,
        dialect=Dialects.DUCK_DB,
        parallelism=parallelism,
        param=param,
        conn_args=(),
        debug=ctx.obj["DEBUG"],
        debug_file=ctx.obj.get("DEBUG_FILE"),
        config_path=PathlibPath(config) if config else None,
        execution_strategy="eager_bfs",
    )

    try:
        run_parallel_execution(
            cli_params=cli_params,
            execution_fn=execute_script_for_unit,
            execution_mode=ExecutionMode.UNIT,
        )
    except Exit:
        raise
    except Exception as e:
        handle_execution_exception(e, debug=cli_params.debug)
