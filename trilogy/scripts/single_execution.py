import traceback
from datetime import datetime
from typing import Any, Union

from trilogy import Executor
from trilogy.core.statements.execute import PROCESSED_STATEMENT_TYPES
from trilogy.scripts.display import (
    FETCH_LIMIT,
    ResultSet,
    create_progress_context,
    print_error,
    print_info,
    print_results_table,
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
) -> tuple[bool, ResultSet | None, Any, Union[Exception, None]]:
    """Execute a single statement and handle results/errors consistently."""
    # Log the statement type before execution
    statement_type = get_statement_type(query)
    if not use_progress:  # Only show type when not using progress bar
        show_statement_type(idx, total_queries, statement_type)

    start_time = datetime.now()

    try:
        raw_results = exec.execute_statement(query)
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
            print_results_table(results)

    return exception
