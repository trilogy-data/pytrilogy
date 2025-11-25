"""Display helpers for prettier CLI output."""

from click import echo, style

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
    console = Console()
except ImportError:
    RICH_AVAILABLE = False
    console = None


def print_success(message: str):
    """Print success message with styling."""
    if RICH_AVAILABLE:
        console.print(message, style="bold green")
    else:
        echo(style(message, fg="green", bold=True))


def print_info(message: str):
    """Print info message with styling."""
    if RICH_AVAILABLE:
        console.print(message, style="bold blue")
    else:
        echo(style(message, fg="blue", bold=True))


def print_warning(message: str):
    """Print warning message with styling."""
    if RICH_AVAILABLE:
        console.print(message, style="bold yellow")
    else:
        echo(style(message, fg="yellow", bold=True))


def print_error(message: str):
    """Print error message with styling."""
    if RICH_AVAILABLE:
        console.print(message, style="bold red")
    else:
        echo(style(message, fg="red", bold=True))


def print_header(message: str):
    """Print header message with styling."""
    if RICH_AVAILABLE:
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
    if RICH_AVAILABLE:
        info_text = (
            f"[bold]Execution Info[/bold]\n"
            f"Input: {input_type} ({input_name})\n"
            f"Dialect: [cyan]{dialect}[/cyan]\n"
            f"Debug: {'enabled' if debug else 'disabled'}"
        )
        panel = Panel.fit(info_text, style="blue", title="Setup")
        console.print(panel)
    else:
        print_info(
            f"Executing {input_type}: {input_name} | Dialect: {dialect} | Debug: {debug}"
        )


def show_environment_params(env_params: dict):
    """Display environment parameters if any."""
    if env_params:
        if RICH_AVAILABLE:
            console.print(f"Environment parameters: {env_params}", style="dim cyan")
        else:
            echo(style(f"Environment parameters: {env_params}", fg="cyan"))


def show_debug_mode():
    """Show debug mode indicator."""
    if RICH_AVAILABLE:
        panel = Panel.fit("Debug mode enabled", style="yellow", title="Debug")
        console.print(panel)


def show_statement_type(idx: int, total: int, statement_type: str):
    """Show the type of statement before execution."""
    statement_num = f"Statement {idx+1}"
    if total > 1:
        statement_num += f"/{total}"

    if RICH_AVAILABLE:
        console.print(
            f"[bold cyan]{statement_num}[/bold cyan] [dim]({statement_type})[/dim]"
        )
    else:
        echo(style(f"{statement_num} ({statement_type})", fg="cyan", bold=True))


def print_results_table(q, headers=None):
    """Print query results using Rich tables or fallback."""
    if RICH_AVAILABLE:
        _print_rich_table(q, headers)
    else:
        _print_fallback_table(q)


def _print_rich_table(q, headers=None):
    """Print query results using Rich tables."""
    result = q.fetchall()
    if not result:
        console.print("No results returned.", style="dim")
        return

    # Create Rich table
    table = Table(
        box=box.MINIMAL_DOUBLE_HEAD, show_header=True, header_style="bold blue"
    )

    # Add columns
    column_names = headers or q.keys()
    for col in column_names:
        table.add_column(str(col), style="white", no_wrap=False)

    # Add rows (limit to reasonable number for display)
    display_limit = 50
    for i, row in enumerate(result):
        if i >= display_limit:
            table.add_row(*["..." for _ in column_names], style="dim")
            console.print(
                f"[dim]Showing first {display_limit} rows of {len(result)} total.[/dim]"
            )
            break
        # Convert all values to strings and handle None
        row_data = [str(val) if val is not None else "[dim]NULL[/dim]" for val in row]
        table.add_row(*row_data)

    console.print(table)


def _print_fallback_table(q):
    """Fallback table printing when Rich is not available."""
    print_warning("Install rich for prettier table output")
    result = q.fetchall()
    print(", ".join(q.keys()))
    for row in result:
        print(row)
    print("---")


def show_execution_start(num_queries: int):
    """Show execution start message."""
    statement_word = "statement" if num_queries == 1 else "statements"
    if RICH_AVAILABLE:
        console.print(f"\n[bold]Executing {num_queries} {statement_word}...[/bold]")
    else:
        print_info(f"Executing {num_queries} {statement_word}...")


def create_progress_context(num_queries: int):
    """Create progress context for multiple statements."""
    if RICH_AVAILABLE and num_queries > 1:
        return Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeElapsedColumn(),
            console=console,
        )
    return None


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
        if RICH_AVAILABLE:
            console.print(
                f"\n[bold green]{statement_num} Results[/bold green] [dim]{duration_str}[/dim]"
            )
        else:
            print_success(f"{statement_num} completed {duration_str}")
    else:
        if RICH_AVAILABLE:
            console.print(
                f"[green]{statement_num} completed[/green] [dim]{duration_str}[/dim]"
            )
        else:
            print_success(f"{statement_num} completed {duration_str}")


def show_execution_summary(num_queries: int, total_duration):
    """Show final execution summary."""
    if RICH_AVAILABLE:
        summary_text = (
            f"[bold green]Execution Complete[/bold green]\n"
            f"Total time: [cyan]{format_duration(total_duration)}[/cyan]\n"
            f"Statements: [cyan]{num_queries}[/cyan]"
        )
        summary = Panel.fit(summary_text, style="green", title="Summary")
        console.print("\n")
        console.print(summary)
    else:
        print_success(
            f"All {num_queries} statements completed in {format_duration(total_duration)}"
        )


def show_formatting_result(filename: str, num_queries: int, duration):
    """Show formatting operation result."""
    if RICH_AVAILABLE:
        console.print(f"File: [bold]{filename}[/bold]")
        console.print(
            f"Processed [cyan]{num_queries}[/cyan] queries in [cyan]{format_duration(duration)}[/cyan]"
        )
    else:
        print_success(f"Formatted {num_queries} queries in {format_duration(duration)}")


def with_status(message: str):
    """Context manager for showing status."""
    if RICH_AVAILABLE:
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
