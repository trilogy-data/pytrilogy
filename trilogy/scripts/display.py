"""Display helpers for prettier CLI output with configurable Rich support."""

from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional

from click import echo, style

# Type checking imports for forward references
if TYPE_CHECKING:
    from trilogy.scripts.parallel_execution import (
        ExecutionResult,
        ParallelExecutionSummary,
    )

# Try to import Rich for enhanced output
try:
    from rich import box
    from rich.console import Console
    from rich.panel import Panel
    from rich.progress import (
        BarColumn,
        Progress,
        SpinnerColumn,
        TextColumn,
        TimeElapsedColumn,
    )
    from rich.table import Table

    RICH_AVAILABLE = True
    console: Optional[Console] = Console()
except ImportError:
    RICH_AVAILABLE = False
    console = None

    Progress = lambda: None  # type: ignore  # noqa: E731
    Table = lambda: None  # type: ignore  # noqa: E731
    Panel = lambda: None  # type: ignore  # noqa: E731

FETCH_LIMIT = 51


@dataclass
class ResultSet:
    rows: list[tuple]
    columns: list[str]


class SetRichMode:
    """
    Callable class that can be used as both a function and a context manager.

    Regular usage:
        set_rich_mode(True)   # Enable Rich for output formatting for CL
        set_rich_mode(False)  # Disables Rich for output formatting

    Context manager usage:
        with set_rich_mode(True):
            # Rich output mode temporarily disabled
            pass
        # Previous state automatically restored
    """

    def __call__(self, enabled: bool):
        current = is_rich_available()
        prior = RichModeContext(enabled, current)
        self._set_mode(enabled)
        return prior

    def _set_mode(self, enabled: bool):
        global RICH_AVAILABLE, console

        if enabled:
            try:
                from rich.console import Console

                RICH_AVAILABLE = True
                console = Console()
            except ImportError:
                RICH_AVAILABLE = False
                console = None
        else:
            RICH_AVAILABLE = False
            console = None


class RichModeContext:
    """Context manager returned by SetRichMode for 'with' statement usage."""

    def __init__(self, enabled: bool, current: bool):
        self.enabled = enabled
        self.old_rich_available = current
        self.old_console = None

    def __enter__(self):
        global RICH_AVAILABLE, console

        self.old_console = console
        # The mode was already set by __call__, so we're good
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        global RICH_AVAILABLE, console

        # Restore previous state
        RICH_AVAILABLE = self.old_rich_available
        console = self.old_console


set_rich_mode = SetRichMode()


def is_rich_available() -> bool:
    """Check if Rich mode is currently available."""
    return RICH_AVAILABLE


def print_success(message: str):
    """Print success message with styling."""
    if RICH_AVAILABLE and console is not None:
        console.print(message, style="bold green")
    else:
        echo(style(message, fg="green", bold=True))


def print_info(message: str):
    """Print info message with styling."""
    if RICH_AVAILABLE and console is not None:
        console.print(message, style="bold blue")
    else:
        echo(style(message, fg="blue", bold=True))


def print_warning(message: str):
    """Print warning message with styling."""
    if RICH_AVAILABLE and console is not None:
        console.print(message, style="bold yellow")
    else:
        echo(style(message, fg="yellow", bold=True))


def print_error(message: str):
    """Print error message with styling."""
    if RICH_AVAILABLE and console is not None:
        console.print(message, style="bold red")
    else:
        echo(style(message, fg="red", bold=True))


def print_header(message: str):
    """Print header message with styling."""
    if RICH_AVAILABLE and console is not None:
        console.print(message, style="bold magenta")
    else:
        echo(style(message, fg="magenta", bold=True))


def format_duration(duration):
    """Format duration nicely."""
    total_seconds = duration.total_seconds()
    if total_seconds < 1:
        return f"{total_seconds*1000:.0f}ms"
    elif total_seconds < 60:
        return f"{total_seconds:.2f}s"
    else:
        minutes = int(total_seconds // 60)
        seconds = total_seconds % 60
        return f"{minutes}m {seconds:.2f}s"


def show_execution_info(input_type: str, input_name: str, dialect: str, debug: bool):
    """Display execution information in a clean format."""
    if RICH_AVAILABLE and console is not None:
        info_text = (
            f"Input: {input_type} ({input_name})\n"
            f"Dialect: [cyan]{dialect}[/cyan]\n"
            f"Debug: {'enabled' if debug else 'disabled'}"
        )
        panel = Panel.fit(info_text, style="blue", title="Execution Info")
        console.print(panel)
    else:
        print_info(
            f"Executing {input_type}: {input_name} | Dialect: {dialect} | Debug: {debug}"
        )


def show_environment_params(env_params: dict):
    """Display environment parameters if any."""
    if env_params:
        if RICH_AVAILABLE and console is not None:
            console.print(f"Environment parameters: {env_params}", style="dim cyan")
        else:
            echo(style(f"Environment parameters: {env_params}", fg="cyan"))


def show_debug_mode():
    """Show debug mode indicator."""
    if RICH_AVAILABLE and console is not None:
        panel = Panel.fit("Debug mode enabled", style="yellow", title="Debug")
        console.print(panel)


def show_statement_type(idx: int, total: int, statement_type: str):
    """Show the type of statement before execution."""
    statement_num = f"Statement {idx+1}"
    if total > 1:
        statement_num += f"/{total}"

    if RICH_AVAILABLE and console is not None:
        console.print(
            f"[bold cyan]{statement_num}[/bold cyan] [dim]({statement_type})[/dim]"
        )
    else:
        echo(style(f"{statement_num} ({statement_type})", fg="cyan", bold=True))


def print_results_table(results: ResultSet):
    """Print query results using Rich tables or fallback."""
    if RICH_AVAILABLE and console is not None:
        _print_rich_table(results.rows, headers=results.columns)
    else:
        _print_fallback_table(results.rows, results.columns)


def _print_rich_table(result, headers=None):
    """Print query results using Rich tables."""
    if console is None:
        return

    if not result:
        console.print("No results returned.", style="dim")
        return

    # Create Rich table
    table = Table(
        box=box.MINIMAL_DOUBLE_HEAD, show_header=True, header_style="bold blue"
    )

    # Add columns
    column_names = headers
    for col in column_names:
        table.add_column(str(col), style="white", no_wrap=False)

    # Add rows (limit to reasonable number for display)
    for i, row in enumerate(result):
        if i >= FETCH_LIMIT:
            table.add_row(*["..." for _ in column_names], style="dim")
            console.print(
                f"[dim]Showing first {FETCH_LIMIT-1} rows. Result set was larger.[/dim]"
            )
            break
        # Convert all values to strings and handle None
        row_data = [str(val) if val is not None else "[dim]NULL[/dim]" for val in row]
        table.add_row(*row_data)

    console.print(table)


def _print_fallback_table(rows, headers: list[str]):
    """Fallback table printing when Rich is not available."""
    print_warning("Install rich for prettier table output")
    print(", ".join(headers))
    for row in rows:
        print(row)
    print("---")


def show_execution_start(num_queries: int):
    """Show execution start message."""
    statement_word = "statement" if num_queries == 1 else "statements"
    if RICH_AVAILABLE and console is not None:
        console.print(f"\n[bold]Executing {num_queries} {statement_word}...[/bold]")
    else:
        print_info(f"Executing {num_queries} {statement_word}...")


def create_progress_context() -> Progress:
    return Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TimeElapsedColumn(),
        console=console,
    )


def show_statement_result(
    idx: int, total: int, duration, has_results: bool, error=None, exception_type=None
):
    """Show result of individual statement execution."""
    statement_num = f"Statement {idx+1}"
    if total > 1:
        statement_num += f"/{total}"

    duration_str = f"({format_duration(duration)})"

    if error is not None:
        # Provide more context for unclear error messages
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
        if RICH_AVAILABLE and console is not None:
            console.print(
                f"\n[bold green]{statement_num} Results[/bold green] [dim]{duration_str}[/dim]"
            )
        else:
            print_success(f"{statement_num} completed {duration_str}")
    else:
        if RICH_AVAILABLE and console is not None:
            console.print(
                f"[green]{statement_num} completed[/green] [dim]{duration_str}[/dim]"
            )
        else:
            print_success(f"{statement_num} completed {duration_str}")


def show_execution_summary(num_queries: int, total_duration, all_succeeded: bool):
    """Show final execution summary."""
    if RICH_AVAILABLE and console is not None:
        if all_succeeded:
            style = "green"
            state = "Complete"
        else:
            style = "red"
            state = "Failed"
        summary_text = (
            f"[bold {style}]Execution {state}[/bold {style}]\n"
            f"Total time: [cyan]{format_duration(total_duration)}[/cyan]\n"
            f"Statements: [cyan]{num_queries}[/cyan]"
        )

        summary = Panel.fit(summary_text, style=style, title="Summary")
        console.print("\n")
        console.print(summary)
    else:
        if not all_succeeded:
            print_error(f"Statements failed in {format_duration(total_duration)}")
        else:
            print_success(
                f"Statements: {num_queries} Completed in: {format_duration(total_duration)}"
            )


def show_formatting_result(filename: str, num_queries: int, duration):
    """Show formatting operation result."""
    if RICH_AVAILABLE and console is not None:
        console.print(f"File: [bold]{filename}[/bold]")
        console.print(
            f"Processed [cyan]{num_queries}[/cyan] queries in [cyan]{format_duration(duration)}[/cyan]"
        )
    else:
        print_success(f"Formatted {num_queries} queries in {format_duration(duration)}")


def with_status(message: str):
    """Context manager for showing status."""
    if RICH_AVAILABLE and console is not None:
        return console.status(f"[bold green]{message}...")
    else:
        print_info(f"{message}...")
        return _DummyContext()


class _DummyContext:
    """Dummy context manager for fallback."""

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass


def show_parallel_execution_start(
    num_files: int, num_edges: int, parallelism: int, strategy: str = "eager_bfs"
) -> None:
    """Display parallel execution start information."""
    if RICH_AVAILABLE and console is not None:
        console.print("\n[bold blue]Starting parallel execution:[/bold blue]")
        console.print(f"  Files: {num_files}")
        console.print(f"  Dependencies: {num_edges}")
        console.print(f"  Max parallelism: {parallelism}")
        console.print(f"  Strategy: {strategy}")
    else:
        print("\nStarting parallel execution:")
        print(f"  Files: {num_files}")
        print(f"  Dependencies: {num_edges}")
        print(f"  Max parallelism: {parallelism}")
        print(f"  Strategy: {strategy}")


def show_parallel_execution_summary(summary: "ParallelExecutionSummary") -> None:
    """Display parallel execution summary."""
    if RICH_AVAILABLE and console is not None:
        # Summary table
        table = Table(title="Execution Summary", show_header=False)
        table.add_column("Metric", style="cyan")
        table.add_column("Value", style="green")

        table.add_row("Total Scripts", str(summary.total_scripts))
        table.add_row("Successful", str(summary.successful))
        table.add_row("Failed", str(summary.failed))
        table.add_row("Total Duration", f"{summary.total_duration:.2f}s")

        console.print(table)

        # Failed scripts details
        if summary.failed > 0:
            console.print("\n[bold red]Failed Scripts:[/bold red]")
            for result in summary.results:
                if not result.success:
                    console.print(f"  [red]✗[/red] {result.node.path}")
                    if result.error:
                        console.print(f"    Error: {result.error}")
    else:
        print("Execution Summary:")
        print(f"  Total Scripts: {summary.total_scripts}")
        print(f"  Successful: {summary.successful}")
        print(f"  Failed: {summary.failed}")
        print(f"  Total Duration: {summary.total_duration:.2f}s")

        if summary.failed > 0:
            print("\nFailed Scripts:")
            for result in summary.results:
                if not result.success:
                    print(f"  ✗ {result.node.path}")
                    if result.error:
                        print(f"    Error: {result.error}")


def show_script_result(result: "ExecutionResult") -> None:
    """Display result of a single script execution."""
    if RICH_AVAILABLE and console is not None:
        if result.success:
            console.print(
                f"  [green]✓[/green] {result.node.path.name} ({result.duration:.2f}s)"
            )
        else:
            console.print(
                f"  [red]✗[/red] {result.node.path.name} ({result.duration:.2f}s) - {result.error}"
            )
    else:
        if result.success:
            print(f"  ✓ {result.node.path.name} ({result.duration:.2f}s)")
        else:
            print(
                f"  ✗ {result.node.path.name} ({result.duration:.2f}s) - {result.error}"
            )
