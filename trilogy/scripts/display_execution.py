"""Display helpers for single-script execution output."""

from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from trilogy.core.statements.execute import ProcessedChartStatement

from click import echo

import trilogy.scripts.display_core as _core
from trilogy.scripts.display_core import (
    emit_event,
    format_duration,
    is_json_mode,
    print_error,
    print_info,
    print_success,
    print_warning,
)
from trilogy.scripts.display_models import ResultSet


def _duration_ms(duration: object) -> "float | None":
    """Best-effort milliseconds from a timedelta-like duration for JSON events."""
    total = getattr(duration, "total_seconds", None)
    if callable(total):
        return round(total() * 1000, 3)
    return None


try:
    from rich import box
    from rich.panel import Panel
    from rich.progress import (
        BarColumn,
        Progress,
        SpinnerColumn,
        TextColumn,
        TimeElapsedColumn,
    )
    from rich.table import Table
except ImportError:
    pass


def show_execution_info(
    input_type: str,
    input_name: str,
    dialect: str,
    debug: bool,
    config_path: Optional[str] = None,
    debug_file: str | None = None,
) -> None:
    """Display execution information in a clean format."""
    if is_json_mode():
        # Chrome for an agent: it already knows the input/dialect it passed.
        # The result/error + summary events carry everything actionable.
        return
    debug_str = (
        f"enabled (file: {debug_file})"
        if debug and debug_file
        else "enabled" if debug else None
    )
    if _core.RICH_AVAILABLE and _core.console is not None:
        info_text = (
            f"Input: {input_type} ({input_name})\n" f"Dialect: [cyan]{dialect}[/cyan]"
        )
        if debug_str:
            info_text += f"\nDebug: {debug_str}"
        if config_path:
            info_text += f"\nConfig: [dim]{config_path}[/dim]"
        panel = Panel.fit(info_text, style="blue", title="Execution Info")
        _core.console.print(panel)
    else:
        msg = f"Executing {input_type}: {input_name} | Dialect: {dialect}"
        if debug_str:
            msg += f" | Debug: {debug_str}"
        if config_path:
            msg += f" | Config: {config_path}"
        print_info(msg)


def show_environment_params(env_params: dict) -> None:
    """Display environment parameters if any."""
    if env_params:
        if is_json_mode():
            emit_event("environment_params", params=env_params)
            return
        if _core.RICH_AVAILABLE and _core.console is not None:
            _core.console.print(
                f"Environment parameters: {env_params}", style="dim cyan"
            )
        else:
            from click import style

            echo(style(f"Environment parameters: {env_params}", fg="cyan"))


def show_debug_mode() -> None:
    """Show debug mode indicator."""
    if is_json_mode():
        emit_event("debug_mode", enabled=True)
        return
    if _core.RICH_AVAILABLE and _core.console is not None:
        panel = Panel.fit("Debug mode enabled", style="yellow", title="Debug")
        _core.console.print(panel)
    else:
        from click import style

        echo(style("Debug mode enabled", fg="yellow"))


def show_statement_type(idx: int, total: int, statement_type: str) -> None:
    """Show the type of statement before execution."""
    if is_json_mode():
        return  # chrome; statement type isn't actionable for the caller
    statement_num = f"Statement {idx+1}"
    if total > 1:
        statement_num += f"/{total}"

    if _core.RICH_AVAILABLE and _core.console is not None:
        _core.console.print(
            f"[bold cyan]{statement_num}[/bold cyan] [dim]({statement_type})[/dim]"
        )
    else:
        from click import style

        echo(style(f"{statement_num} ({statement_type})", fg="cyan", bold=True))


def show_execution_start(num_queries: int) -> None:
    """Show execution start message."""
    if is_json_mode():
        return  # chrome; the summary event reports the statement count
    statement_word = "statement" if num_queries == 1 else "statements"
    if _core.RICH_AVAILABLE and _core.console is not None:
        _core.console.print(
            f"\n[bold]Executing {num_queries} {statement_word}...[/bold]"
        )
    else:
        print_info(f"Executing {num_queries} {statement_word}...")


def create_progress_context() -> "Progress":
    return Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TimeElapsedColumn(),
        console=_core.console,
    )


def show_statement_result(
    idx: int,
    total: int,
    duration: object,
    has_results: bool,
    error: object = None,
    exception_type: type | None = None,
) -> None:
    """Show result of individual statement execution."""
    if is_json_mode():
        # Success is pure chrome — the `result` event + exit code already convey
        # it. Only surface a per-statement FAILURE (carries the error detail);
        # the top-level `error` event covers it too, this just keeps the
        # statement index for multi-statement runs.
        if error is not None:
            emit_event(
                "statement_result",
                index=idx,
                total=total,
                duration_ms=_duration_ms(duration),
                success=False,
                error=str(error).strip(),
                error_type=exception_type.__name__ if exception_type else None,
            )
        return
    statement_num = f"Statement {idx+1}"
    if total > 1:
        statement_num += f"/{total}"

    duration_str = f"({format_duration(duration)})"

    if error is not None:
        error_str = str(error).strip()
        if not error_str or error_str in ["0", "None", "null", ""]:
            if exception_type:
                error_msg = f"{statement_num} failed with {exception_type.__name__}"
                if error_str and error_str not in ["None", "null"]:
                    error_msg += f" (code: {error_str})"
            else:
                error_msg = f"{statement_num} failed with unclear error"
                if error_str:
                    error_msg += f": '{error_str}'"
        else:
            error_msg = f"{statement_num} failed: {error_str}"

        print_error(error_msg)
    elif has_results:
        if _core.RICH_AVAILABLE and _core.console is not None:
            _core.console.print(
                f"\n[bold green]{statement_num} Results[/bold green] [dim]{duration_str}[/dim]"
            )
        else:
            print_success(f"{statement_num} completed {duration_str}")
    else:
        if _core.RICH_AVAILABLE and _core.console is not None:
            _core.console.print(
                f"[green]{statement_num} completed[/green] [dim]{duration_str}[/dim]"
            )
        else:
            print_success(f"{statement_num} completed {duration_str}")


def show_execution_summary(
    num_queries: int,
    total_duration: object,
    all_succeeded: bool,
    total_rows: int | None = None,
) -> None:
    """Show final execution summary. In JSON mode this is the run's "info"
    section — duration, statement count, ok flag, and total result rows — the
    bits worth keeping after the per-statement chrome is dropped."""
    if is_json_mode():
        emit_event(
            "summary",
            statements=num_queries,
            duration_ms=_duration_ms(total_duration),
            ok=all_succeeded,
            rows=total_rows,
        )
        return
    if _core.RICH_AVAILABLE and _core.console is not None:
        if all_succeeded:
            rich_style = "green"
            state = "Complete"
        else:
            rich_style = "red"
            state = "Failed"
        summary_text = (
            f"[bold {rich_style}]Execution {state}[/bold {rich_style}]\n"
            f"Total time: [cyan]{format_duration(total_duration)}[/cyan]\n"
            f"Statements: [cyan]{num_queries}[/cyan]"
        )

        summary = Panel.fit(summary_text, style=rich_style, title="Summary")
        _core.console.print("\n")
        _core.console.print(summary)
    else:
        if not all_succeeded:
            print_error(f"Statements failed in {format_duration(total_duration)}")
        else:
            print_success(
                f"Statements: {num_queries} Completed in: {format_duration(total_duration)}"
            )


def show_formatting_result(
    num_queries: int,
    duration: object,
    file_path: str | None = None,
    error: str | None = None,
) -> None:
    if is_json_mode():
        emit_event(
            "format_result",
            statements=num_queries,
            duration_ms=_duration_ms(duration),
            file=file_path,
            ok=error is None,
            error=error,
        )
        return
    if error is not None:
        location = f" {file_path}" if file_path else ""
        print_error(f"Failed to format{location}: {error}")
        return
    print_success(f"Formatted {num_queries} statements in {format_duration(duration)}")


def _emit_results_json(results: ResultSet, cap: int) -> None:
    """Emit a query result set as a single ``result`` NDJSON event.

    Mirrors the rich table's display contract: the full result is fetched (so
    ``row_count`` is exact) but only ``cap`` rows are emitted, middle-truncated
    head+tail, with ``displayed``/``truncated``/``omitted`` reporting the gap.
    Rows are plain lists; non-JSON cell values fall back to ``str`` via the
    event serializer."""
    rows = results.rows
    total = len(rows)
    hit_fetch_ceiling = total >= _core.DISPLAY_FETCH_CEILING
    head, tail, omitted = _slice_for_middle_truncation(rows, cap)
    shown: list = [list(r) for r in head]
    # Inject a visible marker where the middle was cut so the agent doesn't
    # mistake the head/tail join for a contiguous run (e.g. read a gap in a
    # sorted sequence as missing data).
    if omitted:
        shown.append(f"<redacted {omitted} rows>")
    shown += [list(r) for r in tail]
    emit_event(
        "result",
        columns=list(results.columns),
        rows=shown,
        row_count=total,
        displayed=len(head) + len(tail),
        truncated=omitted > 0 or None,
        omitted=omitted or None,
        fetch_ceiling_hit=hit_fetch_ceiling or None,
    )


def print_results_table(results: ResultSet, row_limit: int | None = None) -> None:
    """Print query results using Rich tables or fallback.

    ``row_limit`` is the displayed-rows ceiling. ``None`` keeps the legacy
    default (50 rows shown, one extra fetched as a "more exist" sentinel)."""
    cap = _core.FETCH_LIMIT - 1 if row_limit is None else row_limit
    if is_json_mode():
        _emit_results_json(results, cap)
        return
    if _core.RICH_AVAILABLE and _core.console is not None:
        _print_rich_table(results.rows, headers=results.columns, row_limit=cap)
    else:
        _print_fallback_table(results.rows, results.columns, row_limit=cap)


def print_chart_terminal(
    layer_data: list[list[dict]], statement: "ProcessedChartStatement"
) -> bool:
    """Render chart to terminal using plotext. Returns True if rendered."""
    if is_json_mode():
        # Charts are inherently visual; in JSON mode surface the underlying
        # per-layer data rows so the information is still available.
        emit_event(
            "chart",
            layers=layer_data,
            # "chart_type" is the first layer's type (mirrors report _chart_type);
            # ChartType is a plain Enum, so emit its .value for clean JSON.
            chart_type=(
                statement.layers[0].layer_type.value if statement.layers else None
            ),
        )
        return True
    from trilogy.rendering.terminal_renderer import PLOTEXT_AVAILABLE

    if not PLOTEXT_AVAILABLE:
        print_info("Install plotext for terminal charts: pip install plotext")
        return False

    if not any(layer_data):
        print_info("Chart produced no data")
        return True

    from trilogy.rendering.terminal_renderer import TerminalRenderer

    renderer = TerminalRenderer()
    output = renderer.render(statement, layer_data)
    echo(output)
    return True


def _slice_for_middle_truncation(result: list, cap: int) -> tuple[list, list, int]:
    """Split rows into (head, tail, omitted_count) for middle-truncated display.

    Cap is the total displayed row count (head + tail). Splitting around the
    middle preserves both the leading rows the agent uses for "shape" checks
    and the trailing rows it uses for "order" checks (sort tail, last bucket,
    etc.) — losing either was the failure mode in q13 where the agent kept
    bumping `--displayed-rows` looking for the last bucket.
    """
    if cap <= 0 or len(result) <= cap:
        return result, [], 0
    head_size = cap // 2 + cap % 2
    tail_size = cap - head_size
    head = result[:head_size]
    tail = result[-tail_size:] if tail_size else []
    omitted = len(result) - cap
    return head, tail, omitted


def _row_count_footer(displayed: int, total: int, hit_fetch_ceiling: bool) -> str:
    """Trailing line under every result table — gives the agent the displayed
    vs. actual total so it doesn't bump `--displayed-rows` blindly looking
    for rows that may not exist."""
    if hit_fetch_ceiling:
        return (
            f"WARNING: {displayed} of {total:,}+ rows shown — fetch ceiling "
            f"({_core.DISPLAY_FETCH_CEILING:,}) hit, the result set is even "
            "larger and full introspection isn't possible. Add a WHERE clause "
            "or aggregation to bound the query before relying on the output."
        )
    if displayed >= total:
        return f"{total} row(s)."
    omitted = total - displayed
    return (
        f"{displayed} of {total} rows shown — middle {omitted} omitted; "
        "raise --displayed-rows or pass --all-rows for the full set."
    )


def _print_rich_table(
    result: list, headers: list[str] | None = None, row_limit: int | None = None
) -> None:
    """Print query results using Rich tables with middle truncation."""
    if _core.console is None:
        return

    if not result:
        _core.console.print("No results returned.", style="dim")
        return

    cap = _core.FETCH_LIMIT - 1 if row_limit is None else row_limit
    hit_fetch_ceiling = len(result) >= _core.DISPLAY_FETCH_CEILING

    table = Table(
        box=box.MINIMAL_DOUBLE_HEAD,
        show_header=True,
        header_style=_core.HEADER_BLUE,
    )

    column_names = headers or []
    for col in column_names:
        table.add_column(str(col), style=_core.COL_WHITE, no_wrap=False)

    head, tail, _ = _slice_for_middle_truncation(result, cap)
    for row in head:
        row_data = [str(val) if val is not None else "[dim]NULL[/dim]" for val in row]
        table.add_row(*row_data)
    if tail:
        table.add_row(*["…" for _ in column_names], style="dim")
        for row in tail:
            row_data = [
                str(val) if val is not None else "[dim]NULL[/dim]" for val in row
            ]
            table.add_row(*row_data)

    _core.console.print(table)
    footer = _row_count_footer(len(head) + len(tail), len(result), hit_fetch_ceiling)
    style = "bold yellow" if hit_fetch_ceiling else "dim"
    _core.console.print(f"[{style}]{footer}[/{style}]")


def _print_fallback_table(
    rows: list, headers: list[str], row_limit: int | None = None
) -> None:
    """Fallback table printing when Rich is not available."""
    print_warning("Install rich for prettier table output")
    print(", ".join(headers))
    cap = _core.FETCH_LIMIT - 1 if row_limit is None else row_limit
    hit_fetch_ceiling = len(rows) >= _core.DISPLAY_FETCH_CEILING
    head, tail, _ = _slice_for_middle_truncation(rows, cap)
    for row in head:
        print(row)
    if tail:
        print("...")
        for row in tail:
            print(row)
    print(_row_count_footer(len(head) + len(tail), len(rows), hit_fetch_ceiling))
    print("---")
