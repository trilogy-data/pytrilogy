"""Display helpers for single-script execution output."""

import os
from typing import TYPE_CHECKING, Any, Optional

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


def _pluralize(label: str, count: int) -> str:
    if count == 1:
        return f"1 {label}"
    plural = label + ("es" if label.endswith(("s", "x", "z")) else "s")
    return f"{count} {plural}"


def summarize_definitions(definitions: list) -> str:
    """Human breakdown of parsed-but-non-executable statements, e.g.
    "1 rowset, 2 concepts". Falls back to a plain count for unknown types."""
    # deferred: keep this display module import-light for CLI startup
    from trilogy.executor import label_definition_statement

    counts: dict[str, int] = {}
    for d in definitions:
        label = label_definition_statement(d)
        counts[label] = counts.get(label, 0) + 1
    # Stable, count-descending order so the dominant kind reads first.
    ordered = sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))
    return ", ".join(_pluralize(label, count) for label, count in ordered)


def show_execution_start(num_queries: int) -> None:
    """Show execution start message."""
    if is_json_mode():
        return  # chrome; the summary event reports the statement count
    statement_word = "statement" if num_queries == 1 else "statements"
    hint = " (are you missing a select?)" if num_queries == 0 else ""
    if _core.RICH_AVAILABLE and _core.console is not None:
        _core.console.print(
            f"\n[bold]Executing {num_queries} {statement_word}...[/bold]{hint}"
        )
    else:
        print_info(f"Executing {num_queries} {statement_word}...{hint}")


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


def _column_stats(columns: list, rows: list) -> list[dict]:
    """Per-column summary over the FULL fetched result. Surfaced only when rows
    are elided, so the agent reads the whole set's shape (non-null / distinct /
    range) instead of inferring it from the truncated head+tail — the q05
    failure mode where a sparse, id-sorted page of nulls was misread as a
    structural "missing data" problem rather than expected sparsity."""
    n = len(rows)
    stats: list[dict] = []
    for i, col in enumerate(columns):
        non_null = [r[i] for r in rows if r[i] is not None]
        entry: dict[str, Any] = {
            "column": col,
            "non_null": len(non_null),
            "nulls": n - len(non_null),
        }
        try:  # unhashable cells (lists/dicts) — skip rather than crash
            entry["distinct"] = len(set(non_null))
        except TypeError:
            pass
        if non_null:
            try:  # mixed/unorderable types — skip the range
                entry["min"] = min(non_null)
                entry["max"] = max(non_null)
            except TypeError:
                pass
        stats.append(entry)
    return stats


_LIMIT_BOUNDED_STATS_NOTE = (
    "column_stats cover only the returned rows, and this result reached the "
    "query's LIMIT — so it is a prefix of the full result. An ORDER BY biases "
    "distinct/min/max toward that prefix (e.g. distinct=1 may just mean other "
    "values sort past the LIMIT). Use count_distinct / aggregates for true "
    "cardinality rather than reading it off a LIMIT-bounded sample."
)


def _emit_results_json(
    results: ResultSet, cap: int, query_limit: int | None = None
) -> None:
    """Emit a query result set as a single ``result`` NDJSON event.

    Mirrors the rich table's display contract: the full result is fetched (so
    ``row_count`` is exact) but only ``cap`` rows are emitted, middle-truncated
    head+tail, with ``displayed``/``truncated``/``omitted`` reporting the gap.
    When rows are elided, ``column_stats`` carries per-column non-null/distinct/
    range over the full fetched set. ``limit_bounded`` flags that the result hit
    the query's own ``LIMIT`` — so the rows (and the stats) are a prefix of the
    full result, not the whole thing. Rows are plain lists; non-JSON cell values
    fall back to ``str`` via the event serializer."""
    rows = results.rows
    total = len(rows)
    hit_fetch_ceiling = total >= _core.DISPLAY_FETCH_CEILING
    # The result reached the query's own LIMIT, so it is a deliberate prefix —
    # callers must read distinct/min/max as describing that prefix, not the
    # full data (the q14 "only CATALOG" misread of a `limit 100` sorted by
    # channel).
    limit_bounded = (
        query_limit if query_limit is not None and total >= query_limit else None
    )
    head, tail, omitted = _slice_for_middle_truncation(rows, cap)
    shown: list = [list(r) for r in head]
    # Inject a visible marker where the middle was cut so the agent doesn't
    # mistake the head/tail join for a contiguous run (e.g. read a gap in a
    # sorted sequence as missing data).
    if omitted:
        shown.append(f"<redacted {omitted} rows>")
    shown += [list(r) for r in tail]
    # Prefer FULL-result stats (SPIKE: query re-run without its LIMIT) when
    # available — they describe the whole result, not the displayed prefix. Else
    # fall back to prefix stats (+ the LIMIT-bias caveat) only when rows elided.
    if results.full_column_stats is not None:
        col_stats: "list[dict] | None" = results.full_column_stats
        stats_note: "str | None" = (
            "column_stats are computed over the FULL result "
            f"({results.full_row_count} rows, without limit applied)."
        )
    else:
        col_stats = _column_stats(list(results.columns), rows) if omitted else None
        stats_note = _LIMIT_BOUNDED_STATS_NOTE if (limit_bounded and omitted) else None
    emit_event(
        "result",
        columns=list(results.columns),
        rows=shown,
        row_count=total,
        displayed=len(head) + len(tail),
        truncated=omitted > 0 or None,
        omitted=omitted or None,
        column_stats=col_stats,
        column_stats_note=stats_note,
        full_row_count=results.full_row_count,
        limit_bounded=limit_bounded,
        fetch_ceiling_hit=hit_fetch_ceiling or None,
        # factual per-computation scope report (input filters vs output
        # filters, grouping/partitioning) — compare against the business
        # question before trusting a value that merely ran cleanly. Omitted
        # entirely when the statement computes no aggregate/window values.
        derived_value_scopes=(
            [s.to_dict() for s in results.derived_value_scopes]
            if results.derived_value_scopes
            and os.environ.get("TRILOGY_AGENT_SCOPE_DIAGNOSTICS", "1").lower()
            not in ("0", "false", "no", "off")
            else None
        ),
    )


def print_results_table(
    results: ResultSet, row_limit: int | None = None, query_limit: int | None = None
) -> None:
    """Print query results using Rich tables or fallback.

    ``row_limit`` is the displayed-rows ceiling. ``None`` keeps the legacy
    default (one extra fetched as a "more exist" sentinel).
    ``query_limit`` is the statement's own ``LIMIT`` (if any), surfaced in JSON
    mode so a consumer knows the result is a prefix of the full set."""
    cap = _core.FETCH_LIMIT - 1 if row_limit is None else row_limit
    if is_json_mode():
        _emit_results_json(results, cap, query_limit)
        return
    if _core.RICH_AVAILABLE and _core.console is not None:
        _print_rich_table(results.rows, headers=results.columns, row_limit=cap)
    else:
        _print_fallback_table(results.rows, results.columns, row_limit=cap)


def show_derived_value_scopes(results: ResultSet) -> None:
    """Render the derived-value scope block after a result table (rich mode
    only — JSON mode carries the same data on the ``result`` event)."""
    if is_json_mode():
        return
    if not results.derived_value_scopes:
        return
    from trilogy.core.scope_diagnostics import render_derived_value_scopes

    block = render_derived_value_scopes(results.derived_value_scopes)
    if not block:
        return
    if _core.RICH_AVAILABLE and _core.console is not None:
        header, _, body = block.partition("\n")
        _core.console.print(f"[{_core.HEADER_BLUE}]{header}[/{_core.HEADER_BLUE}]")
        _core.console.print(body.strip("\n"), style="dim", highlight=False)
    else:
        echo(block)


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
