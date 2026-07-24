"""Display helpers for the ingest command — Rich panels, progress, and summary."""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING

import trilogy.scripts.display_core as _core
from trilogy.scripts.display_core import (
    emit_event,
    is_json_mode,
    print_info,
    print_success,
)

if TYPE_CHECKING:
    from trilogy.scripts.ingest import IngestSummaryRow
    from trilogy.scripts.ingest_helpers.fk_inference import FKBinding, InferredFK

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
    if is_json_mode():
        emit_event(
            "ingest_start",
            sources=list(sources),
            count=len(sources),
            dialect=dialect,
            output=output_dir,
            config_path=config_path,
        )
        return
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
def ingest_progress(total: int) -> Iterator[_IngestProgress]:
    """Context manager wrapping a Rich progress bar for the ingest run.

    Falls back to plain prints when Rich is unavailable.
    """
    if is_json_mode():
        # No progress chatter in JSON mode — the per-source outcome arrives in
        # the final ``ingest_summary`` event.
        yield _IngestProgress()
        return
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
    def __init__(self, progress: Progress, task_id: TaskID) -> None:
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


def show_ingest_summary(rows: list[IngestSummaryRow]) -> None:
    """Print a final summary table across all ingested sources."""
    if not rows:
        return
    successes = sum(1 for r in rows if r.ok)
    if is_json_mode():
        emit_event(
            "ingest_summary",
            total=len(rows),
            successful=successes,
            sources=[
                {
                    "source": r.source,
                    "output": r.output,
                    "columns": r.columns,
                    "grain": r.grain,
                    "status": r.status,
                    "ok": r.ok,
                }
                for r in rows
            ],
        )
        return
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


def show_fk_summary(
    inferred: list[InferredFK],
    explicit: dict[str, dict[str, FKBinding]],
) -> None:
    """Print the foreign keys wired into the model, inferred distinct from explicit."""
    overridden = {
        (table, column) for table, columns in explicit.items() for column in columns
    }
    # (column, references, origin, overlap, coverage)
    rows: list[tuple[str, str, str, str, str]] = []
    for fk in inferred:
        if (fk.from_table, fk.from_column) in overridden:
            continue  # an explicit --fks entry takes precedence; shown below
        overlap = "name" if fk.overlap is None else f"{fk.overlap:.0%}"
        rows.append(
            (
                f"{fk.from_table}.{fk.from_column}",
                fk.target_ref,
                f"inferred ({fk.match_kind})",
                overlap,
                "partial" if fk.partial else "complete",
            )
        )
    for table, columns in explicit.items():
        for column, binding in columns.items():
            rows.append(
                (
                    f"{table}.{column}",
                    binding.target_ref,
                    "explicit",
                    "-",
                    "partial" if binding.partial else "complete",
                )
            )
    if not rows:
        return
    if is_json_mode():
        emit_event(
            "foreign_keys",
            count=len(rows),
            foreign_keys=[
                {
                    "column": column,
                    "references": ref,
                    "origin": origin,
                    "overlap": overlap,
                    "coverage": coverage,
                }
                for column, ref, origin, overlap, coverage in rows
            ],
        )
        return
    if _core.RICH_AVAILABLE and _core.console is not None:
        table_view = Table(title="Foreign Keys", box=box.SIMPLE_HEAVY, show_lines=False)
        table_view.add_column("Column", style="cyan")
        table_view.add_column("References", style="green")
        table_view.add_column("Origin")
        table_view.add_column("Overlap", justify="right")
        table_view.add_column("Coverage")
        for column, ref, origin, overlap, coverage in rows:
            style = "yellow" if origin.startswith("inferred") else "blue"
            cov_style = "magenta" if coverage == "partial" else "green"
            table_view.add_row(
                column,
                ref,
                f"[{style}]{origin}[/{style}]",
                overlap,
                f"[{cov_style}]{coverage}[/{cov_style}]",
            )
        _core.console.print(table_view)
    else:
        for column, ref, origin, overlap, coverage in rows:
            print_info(
                f"FK {column} -> {ref} [{origin}, overlap={overlap}, {coverage}]"
            )
