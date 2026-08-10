"""Testing commands (integration and unit) for Trilogy CLI."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import replace as dataclass_replace
from datetime import datetime
from functools import partial
from pathlib import Path as PathlibPath

from click import UNPROCESSED, Choice, Path, argument, option, pass_context
from click.exceptions import Exit

from trilogy import Executor
from trilogy.core import graph as nx
from trilogy.core.enums import AddressType, ValidationScope
from trilogy.core.exceptions import ModelValidationError
from trilogy.core.models.datasource import Address, Datasource
from trilogy.core.statements.execute import ProcessedValidateNaturalStatement
from trilogy.dialect.enums import Dialects
from trilogy.execution.config import audit_config_file
from trilogy.execution.report import report_run
from trilogy.scripts.click_utils import report_options
from trilogy.scripts.common import (
    CLIRuntimeParams,
    ExecutionStats,
    RefreshParams,
    count_statement_stats,
    create_executor_for_script,
    find_trilogy_config,
    handle_execution_exception,
    merge_runtime_config,
    resolve_input_information,
    validate_environment,
)
from trilogy.scripts.dependency import DependencyResolver, ScriptNode
from trilogy.scripts.display import print_info, print_warning
from trilogy.scripts.parallel_execution import (
    ExecutionMode,
    ExecutionResult,
    ParallelExecutionSummary,
    run_parallel_execution,
)
from trilogy.scripts.refresh import run_refresh_command
from trilogy.utility import safe_open

FAILED_DEPENDENCY_ERROR = "Skipped due to failed dependency"

# Test types selectable via --skip-type/--include-type. `datasources` and
# `concepts` are on by default (today's behavior); `agent` costs LLM tokens and
# is strictly opt-in.
TEST_TYPES = ("datasources", "concepts", "agent")
DEFAULT_TEST_TYPES = frozenset({"datasources", "concepts"})


def resolve_test_types(
    skip: tuple[str, ...], include: tuple[str, ...]
) -> frozenset[str]:
    return frozenset((DEFAULT_TEST_TYPES | set(include)) - set(skip))


def _environment_scope(test_types: frozenset[str]) -> ValidationScope | None:
    """ValidationScope for the env-validation phase, or None to skip it."""
    datasources = "datasources" in test_types
    concepts = "concepts" in test_types
    if datasources and concepts:
        return ValidationScope.ALL
    if datasources:
        return ValidationScope.DATASOURCES
    if concepts:
        return ValidationScope.CONCEPTS
    return None


def _run_agent_questions(
    exec: Executor,
    node: ScriptNode,
    questions: list[ProcessedValidateNaturalStatement],
    stats: ExecutionStats,
    *,
    mock_source_env,
    quiet: bool,
    write_report: bool,
) -> None:
    """Run the embedded agent-validation questions for one script and fail the
    node (ModelValidationError) when any question misses its target.

    ``mock_source_env`` (unit tier) is a pristine environment snapshot used to
    materialize the mock DB; None means integration tier (live backend)."""
    from trilogy.scripts import validate_agent as va
    from trilogy.scripts.display import print_info

    model_dir = node.path.parent
    va.check_agent_ready(model_dir)
    run_dir = (
        model_dir
        / ".trilogy"
        / "validate_runs"
        / f"{datetime.now().strftime('%Y%m%d-%H%M%S')}_{node.path.stem}"
    )
    # The validations file holds the expected answers — keep it out of every
    # workspace so the agent can't read them.
    exclude = {node.path.resolve()}
    image_dir: PathlibPath | None = None
    # Expected SQL, one per question in source order. Unit tier recompiles it
    # against the repointed mock image (so both sides read mock tables); the
    # integration tier uses the precompiled query against the real backend.
    if mock_source_env is not None:
        run_dir.mkdir(parents=True, exist_ok=True)
        image_dir = run_dir / "mock_model"
        va.build_mock_image(
            model_dir,
            image_dir,
            run_dir / va.MOCK_DB_FILENAME,
            mock_source_env,
            exclude,
        )
        expected_sqls = va.compile_expected_against_image(
            image_dir, node.path.read_text(encoding="utf-8")
        )
    else:
        expected_sqls = [
            exec.generator.compile_statement(q.expected) for q in questions
        ]
    results: list[va.QuestionResult] = []
    for index, question in enumerate(questions):
        name = question.name or f"{node.path.stem}_{index + 1}"
        result = va.run_validation_question(
            name=name,
            question=question.question,
            expected_sql=expected_sqls[index],
            comparison=question.comparison,
            repetitions=question.repetitions,
            target=question.target,
            timeout=question.timeout,
            tags=question.tags,
            model_dir=model_dir,
            run_dir=run_dir,
            exclude=exclude,
            image_dir=image_dir,
        )
        results.append(result)
        stats.agent_question_count += 1
        if result.passed:
            stats.agent_passed += 1
    if write_report:
        report_path = va.write_report(run_dir, node.path, results)
        if not quiet:
            print_info(f"Agent validation report: {report_path}")
    failures = [r for r in results if not r.passed]
    if failures:
        summary = "\n".join(
            f"[agent] {r.name}: pass rate {r.pass_rate:.2f} < target "
            f"{r.target:.2f} ({'; '.join(rep.status for rep in r.repetitions)})"
            for r in failures
        )
        raise ModelValidationError(
            f"Agent validation failed ({len(failures)}/{len(results)} "
            f"questions below target):\n{summary}"
        )


def _warn_unknown_config_fields(
    input_path: str, config_override: PathlibPath | None
) -> None:
    """Audit the resolved trilogy.toml and emit warnings for unknown fields."""
    if config_override is not None:
        config_path: PathlibPath | None = config_override
    else:
        start = PathlibPath(input_path)
        config_path = find_trilogy_config(start if start.exists() else None)
    if config_path is None or not config_path.exists():
        return
    for warning in audit_config_file(config_path):
        print_warning(warning)


def _execute_script_for_test(
    exec: Executor,
    node: ScriptNode,
    *,
    mock: bool,
    quiet: bool,
    test_types: frozenset[str],
    agent_report: bool,
) -> ExecutionStats:
    """Shared unit/integration body: parse, validate the environment for the
    selected scope, then run any embedded agent questions. ``mock`` selects the
    unit tier (mocked datasources + a mock DB for the agent workspace)."""
    with safe_open(node.path) as f:
        queries = exec.parse_text(f.read())
    stats = count_statement_stats(queries)
    questions = [q for q in queries if isinstance(q, ProcessedValidateNaturalStatement)]
    agent_enabled = "agent" in test_types and bool(questions)
    # Snapshot the env before validate_environment: its mock phase rewrites
    # datasource addresses in the live env, and the agent tier's mock DB needs
    # tables under the ORIGINAL addresses the workspace model files reference.
    pristine_env = exec.environment.duplicate() if mock and agent_enabled else None
    scope = _environment_scope(test_types)
    if scope is not None:
        validate_environment(exec, mock=mock, quiet=quiet, scope=scope)
        stats.validate_count = len(exec.environment.datasources)
    if agent_enabled:
        _run_agent_questions(
            exec,
            node,
            questions,
            stats,
            mock_source_env=pristine_env,
            quiet=quiet,
            write_report=agent_report,
        )
    else:
        stats.agent_skipped = len(questions)
    return stats


def execute_script_for_integration(
    exec: Executor,
    node: ScriptNode,
    quiet: bool = False,
    test_types: frozenset[str] = DEFAULT_TEST_TYPES,
    agent_report: bool = True,
) -> ExecutionStats:
    """Execute a script for the 'integration' command (parse + validate)."""
    return _execute_script_for_test(
        exec,
        node,
        mock=False,
        quiet=quiet,
        test_types=test_types,
        agent_report=agent_report,
    )


def execute_script_for_unit(
    exec: Executor,
    node: ScriptNode,
    quiet: bool = False,
    test_types: frozenset[str] = DEFAULT_TEST_TYPES,
    agent_report: bool = True,
) -> ExecutionStats:
    """Execute a script for the 'unit' command (parse + mock validate)."""
    return _execute_script_for_test(
        exec,
        node,
        mock=True,
        quiet=quiet,
        test_types=test_types,
        agent_report=agent_report,
    )


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
        return full_graph.subgraph(str(node.path) for node in selected_nodes).copy()

    graph.add_nodes_from(str(node.path) for node in selected_nodes)
    return graph


def _build_initial_integration_graph(input_path: PathlibPath) -> nx.DiGraph | None:
    if input_path.is_file():
        graph = nx.DiGraph()
        graph.add_node(str(input_path))
        return graph
    return None


def _run_integration_with_summary(
    cli_params: CLIRuntimeParams,
    graph: nx.DiGraph | None = None,
    execution_fn=execute_script_for_integration,
) -> ParallelExecutionSummary:
    return run_parallel_execution(
        cli_params=cli_params,
        execution_fn=execution_fn,
        execution_mode=ExecutionMode.INTEGRATION,
        graph=graph,
        fail_on_error=False,
    )


def _test_type_options(fn):
    fn = option(
        "--skip-type",
        "skip_types",
        multiple=True,
        type=Choice(TEST_TYPES),
        help="Test types to skip (repeatable).",
    )(fn)
    fn = option(
        "--include-type",
        "include_types",
        multiple=True,
        type=Choice(TEST_TYPES),
        help="Test types to include beyond the defaults (repeatable). The "
        "'agent' type runs embedded `validate ... matches` LLM questions and "
        "is off by default because it spends provider tokens.",
    )(fn)
    fn = option(
        "--report/--no-report",
        "agent_report",
        default=True,
        help="Write an agent-validation report.json under "
        ".trilogy/validate_runs/ (default on; only applies with "
        "--include-type agent).",
    )(fn)
    return fn


@contextmanager
def _environment_activation(
    environment: str | None, input: str, cli_params: CLIRuntimeParams
) -> Iterator[None]:
    """Enter the deployment environment a test run builds against: flag >
    activated > none (mirrors `run`/`refresh`)."""
    from trilogy.execution.envs import env_activation_scope
    from trilogy.scripts.env_commands import announce_activation, resolve_activation

    activation = resolve_activation(environment, str(input), cli_params.config_path)
    announce_activation(activation)
    with env_activation_scope(activation):
        yield


def _run_integration(
    cli_params: CLIRuntimeParams,
    execution_fn: Callable[..., ExecutionStats],
    input: str,
    *,
    refresh_derived: str | None,
) -> None:
    if refresh_derived is None:
        run_parallel_execution(
            cli_params=cli_params,
            execution_fn=execution_fn,
            execution_mode=ExecutionMode.INTEGRATION,
        )
        return

    input_path = PathlibPath(input)
    print_info("Initial integration run")
    initial_summary = _run_integration_with_summary(
        cli_params,
        graph=_build_initial_integration_graph(input_path),
        execution_fn=execution_fn,
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
        print_info("Refresh phase completed without updating any derived datasources.")

    print_info(f"Re-running integration for {len(affected_scripts)} affected script(s)")
    rerun_summary = _run_integration_with_summary(
        cli_params,
        graph=_build_selected_script_graph(input_path, affected_scripts),
        execution_fn=execution_fn,
    )
    if not rerun_summary.all_succeeded:
        raise Exit(1)


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
    type=int,
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
@_test_type_options
@option(
    "--environment",
    default=None,
    help="Build into this deployment environment (overrides the activated one)",
)
@report_options
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
    skip_types: tuple[str, ...],
    include_types: tuple[str, ...],
    agent_report: bool,
    environment: str | None,
    report_file: str | None,
    run_id: str | None,
    conn_args,
):
    """Run integration tests on Trilogy scripts."""
    try:
        # The config audit and param construction live INSIDE report_run so a
        # --report-file consumer still gets the guaranteed fallback summary for
        # errors that die before the file loop — the common case for validation.
        with report_run(
            "integration",
            report_file,
            run_id,
            target=str(input)[:200],
            dialect=dialect,
            parallelism=parallelism,
            config_path=str(config) if config else None,
        ):
            _warn_unknown_config_fields(input, PathlibPath(config) if config else None)
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
            execution_fn = partial(
                execute_script_for_integration,
                test_types=resolve_test_types(skip_types, include_types),
                agent_report=agent_report,
            )
            with _environment_activation(environment, input, cli_params):
                _run_integration(
                    cli_params, execution_fn, input, refresh_derived=refresh_derived
                )
    except Exit:
        raise
    except Exception as e:
        handle_execution_exception(e, debug=ctx.obj["DEBUG"])


@argument("input", type=Path())
@option("--param", multiple=True, help="Environment parameters as key=value pairs")
@option(
    "--parallelism",
    "-p",
    type=int,
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
@_test_type_options
@option(
    "--environment",
    default=None,
    help="Build into this deployment environment (overrides the activated one)",
)
@report_options
@pass_context
def unit(
    ctx,
    input,
    param,
    parallelism: int | None,
    config,
    env,
    skip_types: tuple[str, ...],
    include_types: tuple[str, ...],
    agent_report: bool,
    environment: str | None,
    report_file: str | None,
    run_id: str | None,
):
    """Run unit tests on Trilogy scripts with mocked datasources."""
    try:
        with report_run(
            "unit",
            report_file,
            run_id,
            target=str(input)[:200],
            dialect=Dialects.DUCK_DB.value,
            parallelism=parallelism,
            config_path=str(config) if config else None,
        ):
            _warn_unknown_config_fields(input, PathlibPath(config) if config else None)
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
                env=env,
            )
            with _environment_activation(environment, input, cli_params):
                run_parallel_execution(
                    cli_params=cli_params,
                    execution_fn=partial(
                        execute_script_for_unit,
                        test_types=resolve_test_types(skip_types, include_types),
                        agent_report=agent_report,
                    ),
                    execution_mode=ExecutionMode.UNIT,
                )
    except Exit:
        raise
    except Exception as e:
        handle_execution_exception(e, debug=ctx.obj["DEBUG"])
