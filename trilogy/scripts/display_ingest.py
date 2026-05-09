"""Display helpers for the ingest command — Rich panels, progress, and summary."""

from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING, Iterator, Sequence

import trilogy.scripts.display_core as _core
from trilogy.scripts.display_core import print_info, print_success

if TYPE_CHECKING:
    from trilogy.scripts.ingest import IngestSummaryRow

try:
    from rich import box
    from rich.panel import Panel
    from rich.progress import (
        BarColumn,
        Progress,
        SpinnerColumn,
        TaskID,
        TextColumn,
        TimeElapsedColumn,
    )
    from rich.table import Table
except ImportError:
    pass


def show_ingest_header(
    sources: Sequence[str],
    output_dir: str,
    dialect: str,
    config_path: str | None = None,
) -> None:
    """Show a startup panel summarizing the ingest job."""
    if _core.RICH_AVAILABLE and _core.console is not None:
        body = (
            f"Sources: [cyan]{len(sources)}[/cyan]\n"
            f"Dialect: [cyan]{dialect}[/cyan]\n"
            f"Output:  [dim]{output_dir}[/dim]"
        )
        if config_path:
            body += f"\nConfig:  [dim]{config_path}[/dim]"
        _core.console.print(Panel.fit(body, style="blue", title="Trilogy Ingest"))
    else:
        msg = (
            f"Trilogy Ingest | sources={len(sources)} | dialect={dialect} "
            f"| output={output_dir}"
        )
        if config_path:
            msg += f" | config={config_path}"
        print_info(msg)


@contextmanager
def ingest_progress(total: int) -> "Iterator[_IngestProgress]":
    """Context manager wrapping a Rich progress bar for the ingest run.

    Falls back to plain prints when Rich is unavailable.
    """
    if _core.RICH_AVAILABLE and _core.console is not None:
        progress = Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("{task.completed}/{task.total}"),
            TimeElapsedColumn(),
            console=_core.console,
            transient=True,
        )
        with progress:
            task_id = progress.add_task("Ingesting...", total=total)
            yield _RichIngestProgress(progress, task_id)
    else:
        yield _PlainIngestProgress(total)


class _IngestProgress:
    """Common interface so the caller doesn't branch on Rich/non-Rich."""

    def step(self, source: str, stage: str) -> None: ...
    def advance(self) -> None: ...


class _RichIngestProgress(_IngestProgress):
    def __init__(self, progress: "Progress", task_id: "TaskID") -> None:
        self._progress = progress
        self._task_id = task_id
        self._current_source: str | None = None

    def step(self, source: str, stage: str) -> None:
        self._current_source = source
        self._progress.update(
            self._task_id, description=f"[cyan]{source}[/cyan] — {stage}"
        )

    def advance(self) -> None:
        self._progress.update(self._task_id, advance=1)


class _PlainIngestProgress(_IngestProgress):
    def __init__(self, total: int) -> None:
        self._total = total
        self._done = 0

    def step(self, source: str, stage: str) -> None:
        print_info(f"[{self._done + 1}/{self._total}] {source}: {stage}")

    def advance(self) -> None:
        self._done += 1


def _shorten(path: str, max_len: int = 50) -> str:
    """Shorten a long path for display.

    Tries `.../parent/basename` first; if that's still too long, falls back to
    a tail-truncated form `...<last N chars>`.
    """
    if len(path) <= max_len:
        return path
    p = Path(path)
    candidate = f".../{p.parent.name}/{p.name}" if p.parent.name else p.name
    if len(candidate) <= max_len:
        return candidate
    return "..." + path[-(max_len - 3) :]


def show_ingest_summary(rows: list["IngestSummaryRow"]) -> None:
    """Print a final summary table across all ingested sources."""
    if not rows:
        return
    successes = sum(1 for r in rows if r.ok)
    if _core.RICH_AVAILABLE and _core.console is not None:
        table = Table(
            title=f"Ingest Summary ({successes}/{len(rows)} ok)",
            box=box.SIMPLE_HEAVY,
            show_lines=False,
        )
        table.add_column("Source", style="cyan", overflow="ellipsis", max_width=50)
        table.add_column("Output", style="dim", overflow="ellipsis", max_width=40)
        table.add_column("Cols", justify="right")
        table.add_column("Grain", style="green", overflow="ellipsis", max_width=30)
        table.add_column("Status")
        for r in rows:
            status_cell = "[green]ok[/green]" if r.ok else f"[red]{r.status}[/red]"
            table.add_row(
                _shorten(r.source, 50),
                _shorten(r.output, 40),
                r.columns,
                r.grain,
                status_cell,
            )
        _core.console.print(table)
    else:
        for r in rows:
            print_info(
                f"{r.source} -> {r.output} "
                f"[{r.columns} cols, grain={r.grain}, {r.status}]"
            )
    print_success(
        f"\nIngested {successes} of {len(rows)} source(s)."
        if successes < len(rows)
        else f"\nIngested {successes} source(s)."
    )
