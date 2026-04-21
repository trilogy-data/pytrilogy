import traceback
from datetime import datetime
from typing import Any, Union

from trilogy import Executor
from trilogy.core.statements.execute import (
    PROCESSED_STATEMENT_TYPES,
    ProcessedValidateStatement,
)
from trilogy.dialect.results import ChartResult
from trilogy.execution.state import RefreshPlan
from trilogy.execution.state import RefreshResult as StateRefreshResult
from trilogy.scripts.common import (
    ExecutionStats,
    RefreshQuery,
    validate_force_sources,
)
from trilogy.scripts.dependency import ScriptNode
from trilogy.scripts.display import (
    FETCH_LIMIT,
    RICH_AVAILABLE,
    ResultSet,
    create_progress_context,
    print_chart_terminal,
    print_error,
    print_info,
    print_results_table,
    print_success,
    print_warning,
    show_asset_status_summary,
    show_execution_start,
    show_execution_summary,
    show_root_probe_breakdown,
    show_statement_result,
    show_statement_type,
    show_watermarks,
)
from trilogy.utility import safe_open


def get_statement_type(statement: PROCESSED_STATEMENT_TYPES) -> str:
    """Get the type/class name of a statement."""
    return type(statement).__name__


def execute_single_statement(
    exec: Executor,
    query: PROCESSED_STATEMENT_TYPES,
    idx: int,
    total_queries: int,
    use_progress=False,
) -> tuple[bool, ResultSet | ChartResult | None, Any, Union[Exception, None]]:
    """Execute a single statement and handle results/errors consistently."""
    # Log the statement type before execution
    statement_type = get_statement_type(query)
    if not use_progress:  # Only show type when not using progress bar
        show_statement_type(idx, total_queries, statement_type)

    start_time = datetime.now()

    try:
        raw_results = exec.execute_statement(query)

        # Handle chart results specially
        if isinstance(raw_results, ChartResult):
            duration = datetime.now() - start_time
            if not use_progress:
                show_statement_result(idx, total_queries, duration, True)
            return True, raw_results, duration, None

        results = (
            ResultSet(
                rows=raw_results.fetchmany(FETCH_LIMIT + 1), columns=raw_results.keys()
            )
            if raw_results
            else None
        )
        duration = datetime.now() - start_time

        if not use_progress:
            show_statement_result(idx, total_queries, duration, bool(results))

        return True, results, duration, None

    except Exception as e:
        duration = datetime.now() - start_time

        if not use_progress:
            show_statement_result(idx, total_queries, duration, False, str(e), type(e))

        return False, None, duration, e


def execute_queries_with_progress(
    exec: Executor, queries: list[PROCESSED_STATEMENT_TYPES]
) -> Exception | None:
    """Execute queries with Rich progress bar. Returns True if all succeeded, False if any failed."""
    progress = create_progress_context()
    results_to_print = []
    exception = None

    with progress:
        task = progress.add_task("Executing statements...", total=len(queries))

        for idx, query in enumerate(queries):
            statement_type = get_statement_type(query)
            progress.update(
                task, description=f"Statement {idx+1}/{len(queries)} ({statement_type})"
            )

            success, results, duration, error = execute_single_statement(
                exec, query, idx, len(queries), use_progress=True
            )

            if not success:
                exception = error

            # Store results for printing after progress is done
            results_to_print.append(
                (idx, len(queries), duration, success, results, error)
            )
            progress.advance(task)
            if exception:
                break

    # Print all results after progress bar is finished
    for idx, total_queries, duration, success, results, error in results_to_print:
        if error:
            show_statement_result(
                idx, total_queries, duration, False, str(error), type(error)
            )
            print_error(f"Full traceback:\n{traceback.format_exc()}")
        else:
            show_statement_result(idx, total_queries, duration, bool(results))
            if results and not error:
                if isinstance(results, ChartResult):
                    print_chart_terminal(results.data, results.statement)
                else:
                    print_results_table(results)

    return exception


def execute_queries_simple(
    exec: Executor, queries: list[PROCESSED_STATEMENT_TYPES]
) -> Exception | None:
    """Execute queries with simple output. Returns True if all succeeded, False if any failed."""
    exception = None

    for idx, query in enumerate(queries):
        if len(queries) > 1:
            print_info(f"Executing statement {idx+1} of {len(queries)}...")

        success, results, duration, error = execute_single_statement(
            exec, query, idx, len(queries), use_progress=False
        )

        if not success:
            exception = error

        if results and not error:
            if isinstance(results, ChartResult):
                print_chart_terminal(results.data, results.statement)
            else:
                print_results_table(results)

    return exception


def execute_run_mode(exec: Executor, queries: list[PROCESSED_STATEMENT_TYPES]) -> None:
    """Execute queries in run mode with progress tracking."""
    start = datetime.now()
    show_execution_start(len(queries))

    progress = (
        create_progress_context() if len(queries) > 1 and RICH_AVAILABLE else None
    )

    if progress:
        exception = execute_queries_with_progress(exec, queries)
    else:
        exception = execute_queries_simple(exec, queries)

    total_duration = datetime.now() - start
    show_execution_summary(len(queries), total_duration, exception is None)

    if exception:
        raise exception


def execute_integration_mode(exec: Executor) -> None:
    """Execute integration tests (validate datasources without mocking)."""
    from trilogy.scripts.common import validate_datasources

    validate_datasources(exec, mock=False, quiet=False)
    print_success("Integration tests passed successfully!")


def execute_unit_mode(exec: Executor) -> None:
    """Execute unit tests (validate datasources with mocking)."""
    from trilogy.scripts.common import validate_datasources

    validate_datasources(exec, mock=True, quiet=False)
    print_success("Unit tests passed successfully!")


def _plan_and_execute_refresh(
    exec: Executor,
    plan: RefreshPlan,
    quiet: bool,
    dry_run: bool,
    interactive: bool,
    print_watermarks: bool,
    addr_map: dict[str, str],
    name: str,
    stats: ExecutionStats | None = None,
) -> StateRefreshResult:
    from trilogy.execution.state import execute_refresh_plan

    if plan.stale_count == 0 and not quiet:
        suffix = f" in {name}" if name else ""
        print_info(
            f"No stale assets found{suffix} ({plan.root_assets}/{plan.all_assets} root assets)"
        )
    elif not quiet:
        label = "Would refresh" if dry_run else "Found"
        suffix = f" in {name}" if name else ""
        print_warning(f"{label} {plan.stale_count} stale asset(s){suffix}")

    if plan.concept_max_watermarks and not quiet:
        show_root_probe_breakdown(plan.root_watermarks, plan.concept_max_watermarks)

    if plan.refresh_assets and not quiet:
        env_max = plan.concept_max_watermarks if print_watermarks else None
        show_asset_status_summary(
            plan.watermarks, addr_map, plan.refresh_assets, env_max=env_max
        )
    elif print_watermarks and not quiet:
        show_watermarks(plan.watermarks, plan.concept_max_watermarks)

    approved = True
    if interactive and plan.refresh_assets:
        import click

        approved = click.confirm("\nProceed with refresh?", default=True)

    if not approved:
        result = StateRefreshResult(
            stale_count=plan.stale_count,
            refreshed_count=0,
            root_assets=plan.root_assets,
            all_assets=plan.all_assets,
        )
    else:

        def on_refresh(asset_id: str, reason: str) -> None:
            if quiet:
                return
            label = "Would refresh" if dry_run else "Refreshing"
            print_info(f"  {label} {asset_id}: {reason}")

        def on_refresh_query(ds_id: str, sql: str) -> None:
            if stats is not None:
                stats.refresh_queries.append(RefreshQuery(datasource_id=ds_id, sql=sql))
            if dry_run and not quiet:
                print_info(f"\n-- {ds_id}\n{sql}")

        result = execute_refresh_plan(
            exec,
            plan,
            on_refresh=on_refresh,
            on_refresh_query=on_refresh_query,
            dry_run=dry_run,
        )

    if result.had_stale and not quiet:
        suffix = f" in {name}" if name else ""
        if dry_run:
            print_info(
                f"Dry run: {result.refreshed_count} asset(s) would be refreshed{suffix}"
            )
        elif result.refreshed_count > 0:
            print_success(f"Refreshed {result.refreshed_count} asset(s){suffix}")
        else:
            print_info(f"Refresh skipped by user{' for ' + name if name else ''}")

    return result


def execute_script_for_refresh(
    exec: Executor,
    node: ScriptNode,
    quiet: bool = False,
    print_watermarks: bool = False,
    force_sources: frozenset[str] = frozenset(),
    interactive: bool = False,
    dry_run: bool = False,
) -> ExecutionStats:
    """Refresh stale assets in a single script file."""
    from trilogy.execution.state import create_refresh_plan

    validation = []
    with safe_open(node.path) as f:
        statements = exec.parse_text(f.read(), root=node.path)
    for x in statements:
        if isinstance(x, ProcessedValidateStatement):
            validation.append(x)

    validate_force_sources(force_sources, exec.environment.datasources)

    plan = create_refresh_plan(
        exec, force_sources=set(force_sources) if force_sources else None
    )
    addr_map = {
        ds_id: ds.safe_address for ds_id, ds in exec.environment.datasources.items()
    }
    stats = ExecutionStats()
    result = _plan_and_execute_refresh(
        exec,
        plan,
        quiet,
        dry_run,
        interactive,
        print_watermarks,
        addr_map,
        node.path.name,
        stats,
    )
    stats.update_count = result.refreshed_count

    if not dry_run:
        for x in validation:
            exec.execute_statement(x)

    return stats


def execute_refresh_mode(
    exec: Executor,
    force_sources: set[str] | None = None,
    print_watermarks: bool = False,
    dry_run: bool = False,
    interactive: bool = False,
    script_path: Any = None,
) -> StateRefreshResult:
    """Execute refresh mode on an already-parsed executor."""
    from trilogy.execution.state import create_refresh_plan

    validate_force_sources(force_sources, exec.environment.datasources)
    plan = create_refresh_plan(exec, force_sources=force_sources)
    addr_map = {
        ds_id: ds.safe_address for ds_id, ds in exec.environment.datasources.items()
    }
    return _plan_and_execute_refresh(
        exec,
        plan,
        quiet=False,
        dry_run=dry_run,
        interactive=interactive,
        print_watermarks=print_watermarks,
        addr_map=addr_map,
        name="",
    )
