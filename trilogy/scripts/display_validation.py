"""Display helpers for environment validation (unit / integration test commands)."""

from collections import defaultdict
from dataclasses import dataclass
from typing import Any

from click import echo, style
from typing_extensions import Self

import trilogy.scripts.display_core as _core
from trilogy.scripts.display_core import _FdStderrCapture
from trilogy.scripts.display_refresh import _ProbeProgressContext

try:
    from rich import box
    from rich.table import Table
except ImportError:
    pass


@dataclass(frozen=True)
class ValidationFailure:
    """A single failure surfaced by environment validation."""

    kind: str  # "datasource" or "concept"
    target: str
    message: str


def show_validation_targets(
    datasource_names: list[str],
    concept_count: int,
    *,
    mock: bool,
) -> None:
    """Print the list of datasources and the count of concepts to validate.

    Datasources are listed individually because each carries SQL execution
    cost; concepts are aggregated since the vast majority short-circuit with
    no work.
    """
    mode = "unit (mocked)" if mock else "integration"
    if _core.RICH_AVAILABLE and _core.console is not None:
        _core.console.print(
            f"[bold]Validating {mode}:[/bold] "
            f"[{_core.COL_CYAN}]{len(datasource_names)}[/{_core.COL_CYAN}] datasource(s), "
            f"[{_core.COL_CYAN}]{concept_count}[/{_core.COL_CYAN}] concept(s)"
        )
        for name in datasource_names:
            _core.console.print(f"  [{_core.COL_CYAN}]{name}[/{_core.COL_CYAN}]")
    else:
        echo(
            style(
                f"Validating {mode}: {len(datasource_names)} datasource(s), "
                f"{concept_count} concept(s)",
                bold=True,
            )
        )
        for name in datasource_names:
            echo(f"  {name}")


def validation_progress(total: int) -> _ProbeProgressContext:
    """Progress bar context for stepping through validation targets.

    Reuses the refresh probe context so look-and-feel matches the refresh CLI
    and we get the same fd-2 capture for subprocess stderr.
    """
    return _ProbeProgressContext(total, task_label="Validating")


class ValidationProgressContext:
    """Wrapper that updates the progress description per target.

    Adapts `_ProbeProgressContext` so callers can drive both progress and the
    per-task description without leaking Rich primitives.
    """

    def __init__(self, total: int) -> None:
        self._inner = _ProbeProgressContext(total, task_label="Validating")

    def __enter__(self) -> Self:
        self._inner.__enter__()
        return self

    def __exit__(self, *args: object) -> None:
        self._inner.__exit__(*args)

    def set_label(self, label: str) -> None:
        progress = self._inner._progress
        task = self._inner._task
        if progress is not None and task is not None:
            progress.update(task, description=f"Validating {label}")

    def advance(self) -> None:
        self._inner.advance()

    def register_capture_context(self, get_ctx: Any) -> None:
        self._inner._stderr_cap.get_context = get_ctx


def show_validation_failures(
    failures: list[ValidationFailure],
    *,
    script_label: str | None = None,
) -> None:
    """Render validation failures grouped by datasource/concept.

    `script_label` is included in the title so multi-script runs can prefix
    each block with the source script name.
    """
    if not failures:
        return

    grouped: dict[tuple[str, str], list[str]] = defaultdict(list)
    for failure in failures:
        grouped[(failure.kind, failure.target)].append(failure.message)

    title = "Validation Failures"
    if script_label:
        title = f"Validation Failures ({script_label})"

    if _core.RICH_AVAILABLE and _core.console is not None:
        table = Table(
            title=title,
            show_header=True,
            header_style="bold red",
            box=box.MINIMAL_DOUBLE_HEAD,
        )
        table.add_column("Kind", style=_core.COL_DIM, no_wrap=True)
        table.add_column("Target", style=_core.COL_CYAN, no_wrap=True)
        table.add_column("Error", style="red")

        for (kind, target), messages in sorted(grouped.items()):
            first = True
            for message in messages:
                table.add_row(
                    kind if first else "",
                    target if first else "",
                    message,
                )
                first = False

        _core.console.print(table)
    else:
        echo(style(title + ":", fg="red", bold=True))
        for (kind, target), messages in sorted(grouped.items()):
            echo(f"  [{kind}] {target}")
            for message in messages:
                echo(f"    - {message}")


def show_validation_success(
    *,
    mock: bool,
    datasource_count: int,
    duration_seconds: float | None = None,
) -> None:
    """Single-line success summary mirroring the refresh-style output."""
    label = "Unit" if mock else "Integration"
    detail = f"{datasource_count} datasource(s)"
    if duration_seconds is not None:
        detail += f" in {duration_seconds:.2f}s"
    if _core.RICH_AVAILABLE and _core.console is not None:
        _core.console.print(
            f"[bold green]{label} validation passed[/bold green] "
            f"[dim]({detail})[/dim]"
        )
    else:
        echo(style(f"{label} validation passed ({detail})", fg="green", bold=True))


__all__ = [
    "ValidationFailure",
    "ValidationProgressContext",
    "_FdStderrCapture",
    "show_validation_failures",
    "show_validation_success",
    "show_validation_targets",
    "validation_progress",
]
