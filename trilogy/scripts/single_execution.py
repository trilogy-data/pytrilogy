import traceback
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Union

from trilogy import Executor
from trilogy.core.statements.execute import PROCESSED_STATEMENT_TYPES
from trilogy.dialect.results import ChartResult
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
    show_execution_start,
    show_execution_summary,
    show_statement_result,
    show_statement_type,
)


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
                    print_chart_terminal(results.data, results.config)
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
                print_chart_terminal(results.data, results.config)
            else:
                print_results_table(results)

    return exception


@dataclass
class RefreshResult:
    refreshed_count: int
    had_stale: bool


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


def execute_refresh_mode(
    exec: Executor,
    force_sources: set[str] | None = None,
    print_watermarks: bool = False,
) -> RefreshResult:
    """Execute refresh mode to update stale assets."""
    from trilogy.execution.state.state_store import (
        DatasourceWatermark,
        refresh_stale_assets,
    )
    from trilogy.scripts.display import print_warning
    from trilogy.scripts.refresh import _format_watermarks

    def on_stale_found(stale_count: int, root_assets: int, all_assets: int) -> None:
        if stale_count == 0:
            print_info(
                f"No stale assets found ({root_assets}/{all_assets} root assets)"
            )
        else:
            print_warning(f"Found {stale_count} stale asset(s)")

    def on_refresh(asset_id: str, reason: str) -> None:
        print_info(f"  Refreshing {asset_id}: {reason}")

    def on_watermarks(watermarks: dict[str, DatasourceWatermark]) -> None:
        if print_watermarks:
            _format_watermarks(watermarks)

    result = refresh_stale_assets(
        exec,
        on_stale_found=on_stale_found,
        on_refresh=on_refresh,
        on_watermarks=on_watermarks,
        force_sources=force_sources,
    )

    if result.had_stale:
        print_success(f"Refreshed {result.refreshed_count} asset(s)")

    return RefreshResult(
        refreshed_count=result.refreshed_count, had_stale=result.had_stale
    )
