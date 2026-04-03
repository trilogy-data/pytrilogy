"""Display helpers for parallel execution output."""

import threading
from typing import TYPE_CHECKING, Any

import trilogy.scripts.display_core as _core
from trilogy.scripts.display_core import _FdStderrCapture

if TYPE_CHECKING:
    from trilogy.scripts.parallel_execution import (
        ExecutionResult,
        ParallelExecutionSummary,
    )

try:
    from rich import box
    from rich.progress import (
        Progress,
        SpinnerColumn,
        TextColumn,
        TimeElapsedColumn,
    )
    from rich.table import Table
except ImportError:
    pass


def show_parallel_execution_start(
    num_files: int, num_edges: int, parallelism: int, strategy: str = "eager_bfs"
) -> None:
    """Display parallel execution start information."""
    if _core.RICH_AVAILABLE and _core.console is not None:
        _core.console.print("\n[bold blue]Starting parallel execution:[/bold blue]")
        _core.console.print(f"  Files: {num_files}")
        _core.console.print(f"  Dependencies: {num_edges}")
        _core.console.print(f"  Max parallelism: {parallelism}")
        _core.console.print(f"  Strategy: {strategy}")
    else:
        print("\nStarting parallel execution:")
        print(f"  Files: {num_files}")
        print(f"  Dependencies: {num_edges}")
        print(f"  Max parallelism: {parallelism}")
        print(f"  Strategy: {strategy}")


def show_parallel_execution_summary(summary: "ParallelExecutionSummary") -> None:
    """Display parallel execution summary."""
    from trilogy.scripts.common import ExecutionStats
    from trilogy.scripts.dependency import ScriptNode

    total_stats = ExecutionStats()
    for result in summary.results:
        if result.stats:
            total_stats = total_stats + result.stats

    if _core.RICH_AVAILABLE and _core.console is not None:
        table = Table(title="Execution Summary", show_header=False)
        table.add_column("Metric", style=_core.COL_CYAN)
        table.add_column("Value", style=_core.COL_WHITE)

        table.add_row("Total Scripts", str(summary.total_scripts))
        table.add_row("Successful", str(summary.successful))
        table.add_row("Failed", str(summary.failed))
        table.add_row("Total Duration", f"{summary.total_duration:.2f}s")

        if total_stats.update_count > 0:
            table.add_row("Datasources Updated", str(total_stats.update_count))
        if total_stats.validate_count > 0:
            table.add_row("Datasources Validated", str(total_stats.validate_count))
        if total_stats.persist_count > 0:
            table.add_row("Tables Persisted", str(total_stats.persist_count))

        _core.console.print(table)

        if summary.failed > 0:
            _core.console.print("\n[bold red]Failed Scripts:[/bold red]")
            for result in summary.results:
                if not result.success:
                    node_label = (
                        result.node.path
                        if isinstance(result.node, ScriptNode)
                        else result.node.address
                    )
                    _core.console.print(f"  [red]\u2717[/red] {node_label}")
                    if result.error:
                        _core.console.print(f"    Error: {result.error}")
    else:
        print("Execution Summary:")
        print(f"  Total Scripts: {summary.total_scripts}")
        print(f"  Successful: {summary.successful}")
        print(f"  Failed: {summary.failed}")
        print(f"  Total Duration: {summary.total_duration:.2f}s")

        if total_stats.update_count > 0:
            print(f"  Datasources Updated: {total_stats.update_count}")
        if total_stats.validate_count > 0:
            print(f"  Datasources Validated: {total_stats.validate_count}")
        if total_stats.persist_count > 0:
            print(f"  Tables Persisted: {total_stats.persist_count}")

        if summary.failed > 0:
            print("\nFailed Scripts:")
            for result in summary.results:
                if not result.success:
                    node_label = (
                        result.node.path
                        if isinstance(result.node, ScriptNode)
                        else result.node.address
                    )
                    print(f"  \u2717 {node_label}")
                    if result.error:
                        print(f"    Error: {result.error}")


def show_script_result(
    result: "ExecutionResult", stat_types: list[str] | None = None
) -> None:
    """Display result of a single script execution."""
    from trilogy.scripts.common import format_stats
    from trilogy.scripts.dependency import ManagedRefreshNode, ScriptNode

    stats_str = ""
    if result.stats:
        formatted = format_stats(result.stats, stat_types)
        if formatted:
            stats_str = f" [{formatted}]"

    if _core.RICH_AVAILABLE and _core.console is not None:
        if result.success:
            if isinstance(result.node, ScriptNode):
                _core.console.print(
                    f"  [green]\u2713[/green] {result.node.path.name} ({result.duration:.2f}s){stats_str}"
                )
            elif isinstance(result.node, ManagedRefreshNode):
                _core.console.print(
                    f"  [green]\u2713[/green] {result.node.address} ({result.duration:.2f}s){stats_str}"
                )
            else:
                _core.console.print(str(result))
        else:
            if isinstance(result.node, ScriptNode):
                _core.console.print(
                    f"  [red]\u2717[/red] {result.node.path.name} ({result.duration:.2f}s) - {result.error}"
                )
            elif isinstance(result.node, ManagedRefreshNode):
                _core.console.print(
                    f"  [red]\u2717[/red] {result.node.address} ({result.duration:.2f}s) - {result.error}"
                )
            else:
                _core.console.print(str(result))
    else:
        if result.success:
            if isinstance(result.node, ScriptNode):
                print(
                    f"  \u2713 {result.node.path.name} ({result.duration:.2f}s){stats_str}"
                )
            else:
                print(
                    f"  \u2713 {result.node.address} ({result.duration:.2f}s){stats_str}"
                )
        else:
            if isinstance(result.node, ScriptNode):
                print(
                    f"  \u2717 {result.node.path.name} ({result.duration:.2f}s) - {result.error}"
                )
            else:
                print(
                    f"  \u2717 {result.node.address} ({result.duration:.2f}s) - {result.error}"
                )


def _make_futures_context_getter(futures: dict) -> Any:
    """Return a callable that yields labels of futures not yet done."""
    from trilogy.scripts.dependency import ScriptNode

    def get_ctx() -> str:
        active = []
        for f, node in futures.items():
            if not f.done():
                label = node.path.name if isinstance(node, ScriptNode) else str(node)
                active.append(label)
        return " | ".join(sorted(active))

    return get_ctx


class ParallelProgressTracker:
    """Context manager that shows an animated spinner for each in-progress node."""

    def __init__(self) -> None:
        self._task_ids: dict[int, Any] = {}
        self._in_progress_labels: dict[int, str] = {}
        self._lock = threading.Lock()
        self._progress: Any = None
        self._stderr_cap = _FdStderrCapture(
            get_context=lambda: " | ".join(sorted(self._in_progress_labels.values()))
        )

    def __enter__(self) -> "ParallelProgressTracker":
        if _core.RICH_AVAILABLE and _core.console is not None:
            self._progress = Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                TimeElapsedColumn(),
                console=_core.console,
                transient=True,
                redirect_stderr=True,
            )
            self._progress.__enter__()
            self._stderr_cap.__enter__()
        return self

    def __exit__(self, *args: Any) -> None:
        if self._progress is not None:
            self._stderr_cap.__exit__(*args)
            self._progress.__exit__(*args)
            self._progress = None

    def on_start(self, node: Any) -> None:
        from trilogy.scripts.dependency import ManagedRefreshNode, ScriptNode

        if isinstance(node, ScriptNode):
            label = node.path.name
        elif isinstance(node, ManagedRefreshNode):
            label = node.address
        else:
            label = str(node)
        if self._progress is not None:
            with self._lock:
                task_id = self._progress.add_task(
                    f"[{_core.COL_CYAN}]{label}[/{_core.COL_CYAN}]"
                )
                self._task_ids[id(node)] = task_id
                self._in_progress_labels[id(node)] = label
        else:
            print(f"  \u2192 {label}")

    def on_complete(self, result: Any) -> None:
        if self._progress is not None:
            with self._lock:
                task_id = self._task_ids.pop(id(result.node), None)
                self._in_progress_labels.pop(id(result.node), None)
                if task_id is not None:
                    self._progress.remove_task(task_id)
        show_script_result(result)


def show_execution_plan(
    nodes: list[str],
    edges: list[tuple[str, str]],
    execution_order: list[list[str]],
    required_files: list | None = None,
) -> None:
    """Display execution plan in human-readable format."""
    required_files = required_files or []
    if _core.RICH_AVAILABLE and _core.console is not None:
        from rich.panel import Panel

        info_text = (
            f"Scripts: [cyan]{len(nodes)}[/cyan]\n"
            f"Dependencies: [cyan]{len(edges)}[/cyan]\n"
            f"Execution Levels: [cyan]{len(execution_order)}[/cyan]\n"
            f"Required Files: [cyan]{len(required_files)}[/cyan]"
        )
        panel = Panel.fit(info_text, style="blue", title="Execution Plan")
        _core.console.print(panel)

        if execution_order:
            table = Table(
                title="Execution Order",
                show_header=True,
                header_style=_core.HEADER_BLUE,
                box=box.MINIMAL_DOUBLE_HEAD,
            )
            table.add_column("Level", style=_core.COL_CYAN, no_wrap=True)
            table.add_column("Scripts (can run in parallel)", style=_core.COL_WHITE)

            for level, scripts in enumerate(execution_order):
                table.add_row(str(level + 1), ", ".join(scripts))

            _core.console.print(table)

        if edges:
            _core.console.print("\n[bold]Dependencies:[/bold]")
            for from_node, to_node in edges:
                _core.console.print(
                    f"  [dim]{from_node}[/dim] -> [white]{to_node}[/white]"
                )

        if required_files:
            _core.console.print("\n[bold]Required Files (for bundling):[/bold]")
            for path in required_files:
                _core.console.print(f"  [dim]{path}[/dim]")
    else:
        print("Execution Plan:")
        print(f"  Scripts: {len(nodes)}")
        print(f"  Dependencies: {len(edges)}")
        print(f"  Execution Levels: {len(execution_order)}")
        print(f"  Required Files: {len(required_files)}")

        if execution_order:
            print("\nExecution Order:")
            for level, scripts in enumerate(execution_order):
                print(f"  Level {level + 1}: {', '.join(scripts)}")

        if edges:
            print("\nDependencies:")
            for from_node, to_node in edges:
                print(f"  {from_node} -> {to_node}")

        if required_files:
            print("\nRequired Files (for bundling):")
            for path in required_files:
                print(f"  {path}")
