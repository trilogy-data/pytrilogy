"""Display helpers for single-script execution output."""

from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from trilogy.core.statements.author import ChartConfig

from click import echo

import trilogy.scripts.display_core as _core
from trilogy.scripts.display_core import (
    format_duration,
    print_error,
    print_info,
    print_success,
    print_warning,
)
from trilogy.scripts.display_models import ResultSet

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
    if debug and debug_file:
        debug_str = f"enabled (file: {debug_file})"
    elif debug:
        debug_str = "enabled"
    else:
        debug_str = "disabled"
    if _core.RICH_AVAILABLE and _core.console is not None:
        info_text = (
            f"Input: {input_type} ({input_name})\n"
            f"Dialect: [cyan]{dialect}[/cyan]\n"
            f"Debug: {debug_str}"
        )
        if config_path:
            info_text += f"\nConfig: [dim]{config_path}[/dim]"
        panel = Panel.fit(info_text, style="blue", title="Execution Info")
        _core.console.print(panel)
    else:
        msg = f"Executing {input_type}: {input_name} | Dialect: {dialect} | Debug: {debug_str}"
        if config_path:
            msg += f" | Config: {config_path}"
        print_info(msg)


def show_environment_params(env_params: dict) -> None:
    """Display environment parameters if any."""
    if env_params:
        if _core.RICH_AVAILABLE and _core.console is not None:
            _core.console.print(
                f"Environment parameters: {env_params}", style="dim cyan"
            )
        else:
            from click import style

            echo(style(f"Environment parameters: {env_params}", fg="cyan"))


def show_debug_mode() -> None:
    """Show debug mode indicator."""
    if _core.RICH_AVAILABLE and _core.console is not None:
        panel = Panel.fit("Debug mode enabled", style="yellow", title="Debug")
        _core.console.print(panel)
    else:
        from click import style

        echo(style("Debug mode enabled", fg="yellow"))


def show_statement_type(idx: int, total: int, statement_type: str) -> None:
    """Show the type of statement before execution."""
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
    num_queries: int, total_duration: object, all_succeeded: bool
) -> None:
    """Show final execution summary."""
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


def show_formatting_result(filename: str, num_queries: int, duration: object) -> None:
    """Show formatting operation result."""
    if _core.RICH_AVAILABLE and _core.console is not None:
        _core.console.print(f"File: [bold]{filename}[/bold]")
        _core.console.print(
            f"Processed [cyan]{num_queries}[/cyan] queries in [cyan]{format_duration(duration)}[/cyan]"
        )
    else:
        print_success(f"Formatted {num_queries} queries in {format_duration(duration)}")


def print_results_table(results: ResultSet) -> None:
    """Print query results using Rich tables or fallback."""
    if _core.RICH_AVAILABLE and _core.console is not None:
        _print_rich_table(results.rows, headers=results.columns)
    else:
        _print_fallback_table(results.rows, results.columns)


def print_chart_terminal(data: list[dict], config: "ChartConfig") -> bool:
    """Render chart to terminal using plotext. Returns True if rendered."""
    from trilogy.rendering.terminal_renderer import PLOTEXT_AVAILABLE

    if not PLOTEXT_AVAILABLE:
        print_info("Install plotext for terminal charts: pip install plotext")
        return False

    if not data:
        print_info("Chart produced no data")
        return True

    from trilogy.rendering.terminal_renderer import TerminalRenderer

    renderer = TerminalRenderer()
    output = renderer.render(config, data)
    echo(output)
    return True


def _print_rich_table(result: list, headers: list[str] | None = None) -> None:
    """Print query results using Rich tables."""
    if _core.console is None:
        return

    if not result:
        _core.console.print("No results returned.", style="dim")
        return

    table = Table(
        box=box.MINIMAL_DOUBLE_HEAD,
        show_header=True,
        header_style=_core.HEADER_BLUE,
    )

    column_names = headers or []
    for col in column_names:
        table.add_column(str(col), style=_core.COL_WHITE, no_wrap=False)

    for i, row in enumerate(result):
        if i >= _core.FETCH_LIMIT:
            table.add_row(*["..." for _ in column_names], style="dim")
            _core.console.print(
                f"[dim]Showing first {_core.FETCH_LIMIT-1} rows. Result set was larger.[/dim]"
            )
            break
        row_data = [str(val) if val is not None else "[dim]NULL[/dim]" for val in row]
        table.add_row(*row_data)

    _core.console.print(table)


def _print_fallback_table(rows: list, headers: list[str]) -> None:
    """Fallback table printing when Rich is not available."""
    print_warning("Install rich for prettier table output")
    print(", ".join(headers))
    for row in rows:
        print(row)
    print("---")
