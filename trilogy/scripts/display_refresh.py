"""Display helpers for refresh/watermark pipeline output."""

from typing import Any

from click import echo, style

import trilogy.scripts.display_core as _core
from trilogy.scripts.display_core import _FdStderrCapture, print_info
from trilogy.scripts.display_models import ManagedDataGroup, StaleDataSourceEntry

try:
    from rich import box
    from rich.progress import (
        BarColumn,
        MofNCompleteColumn,
        Progress,
        SpinnerColumn,
        TextColumn,
    )
    from rich.table import Table
except ImportError:
    pass


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


class _ProbeProgressContext:
    """Context manager for watermark probe progress tracking."""

    def __init__(self, total: int, task_label: str = "Checking assets"):
        self._total = total
        self._task_label = task_label
        self._progress: Any = None
        self._task: Any = None
        self._stderr_cap = _FdStderrCapture()

    def __enter__(self) -> "_ProbeProgressContext":
        if _core.RICH_AVAILABLE and _core.console is not None:
            self._progress = Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                MofNCompleteColumn(),
                console=_core.console,
                redirect_stderr=True,
            )
            self._progress.start()
            self._task = self._progress.add_task(self._task_label, total=self._total)
            self._stderr_cap.__enter__()
        return self

    def register_futures(self, futures: dict) -> None:
        """Set provenance getter from a future→node map (call after pool.submit)."""
        self._stderr_cap.get_context = _make_futures_context_getter(futures)

    def advance(self) -> None:
        if self._progress is not None and self._task is not None:
            self._progress.advance(self._task)

    def __exit__(self, *args: Any) -> None:
        if self._progress is not None:
            self._stderr_cap.__exit__(*args)
            self._progress.stop()


def probe_progress(total: int) -> _ProbeProgressContext:
    """Context manager showing progress while collecting managed asset watermarks."""
    return _ProbeProgressContext(total)


def root_probe_progress(total: int) -> _ProbeProgressContext:
    """Context manager showing progress while collecting root watermarks."""
    return _ProbeProgressContext(total, task_label="Probing root watermarks")


def _common_prefix(strings: list[str]) -> str:
    if not strings:
        return ""
    prefix = strings[0]
    for s in strings[1:]:
        while not s.startswith(prefix):
            prefix = prefix[:-1]
            if not prefix:
                return ""
    last_sep = max(prefix.rfind("/"), prefix.rfind("\\"))
    return prefix[: last_sep + 1] if last_sep >= 0 else prefix


def show_managed_asset_list(addresses: list[str]) -> None:
    """Print the list of managed assets found before probing begins."""
    if _core.RICH_AVAILABLE and _core.console is not None:
        _core.console.print("[bold]Managed assets found:[/bold]")
        for addr in addresses:
            _core.console.print(f"  [{_core.COL_CYAN}]{addr}[/{_core.COL_CYAN}]")
    else:
        echo(style("Managed assets found:", bold=True))
        for addr in addresses:
            echo(f"  {addr}")


def show_root_concepts(root_addr_to_needed_concepts: dict[str, set[str]]) -> None:
    """Show root physical addresses and the concepts being probed from each."""
    if not root_addr_to_needed_concepts:
        return
    addrs = list(root_addr_to_needed_concepts.keys())
    prefix = _common_prefix(addrs) if len(addrs) > 1 else ""

    def _short(addr: str) -> str:
        return addr[len(prefix) :] if prefix else addr

    if _core.RICH_AVAILABLE and _core.console is not None:
        title = "Root Watermark Concepts"
        if prefix:
            title += f"\n[dim](prefix: {prefix})[/dim]"
        table = Table(
            title=title,
            show_header=True,
            header_style=_core.HEADER_BLUE,
            box=box.MINIMAL_DOUBLE_HEAD,
        )
        table.add_column("Root Address", style=_core.COL_CYAN)
        table.add_column("Concept", style=_core.COL_WHITE)
        for addr, concepts in sorted(root_addr_to_needed_concepts.items()):
            first = True
            for concept in sorted(concepts):
                table.add_row(_short(addr) if first else "", concept)
                first = False
        _core.console.print(table)
    else:
        echo(style("Root Watermark Concepts:", bold=True))
        if prefix:
            echo(f"  (prefix: {prefix})")
        for addr, concepts in sorted(root_addr_to_needed_concepts.items()):
            echo(f"  {_short(addr)}:")
            for concept in sorted(concepts):
                echo(f"    {concept}")


def show_root_probe_breakdown(
    root_watermarks: dict,
    concept_max_watermarks: dict,
) -> None:
    """Show per-root values for each probe concept and the derived max."""
    if not concept_max_watermarks:
        return

    if _core.RICH_AVAILABLE and _core.console is not None:
        table = Table(
            title="Root Probe Results",
            show_header=True,
            header_style=_core.HEADER_BLUE,
            box=box.MINIMAL_DOUBLE_HEAD,
        )
        table.add_column("Concept", style=_core.COL_CYAN)
        table.add_column("Root Datasource", style=_core.COL_WHITE)
        table.add_column("Value", style=_core.COL_WHITE)

        for concept_name in sorted(concept_max_watermarks):
            max_val = concept_max_watermarks[concept_name]
            roots_for_concept = [
                (ds_id, wm.keys[concept_name])
                for ds_id, wm in sorted(root_watermarks.items())
                if concept_name in wm.keys
            ]
            first = True
            for ds_id, update_key in roots_for_concept:
                table.add_row(
                    concept_name if first else "",
                    ds_id,
                    str(update_key.value),
                )
                first = False
            table.add_row(
                concept_name if not roots_for_concept else "",
                "[dim]\u2192 max[/dim]",
                f"[{_core.COL_GREEN}]{max_val.value}[/{_core.COL_GREEN}]",
            )
        _core.console.print(table)
    else:
        echo(style("Root Probe Results:", bold=True))
        for concept_name in sorted(concept_max_watermarks):
            max_val = concept_max_watermarks[concept_name]
            echo(f"  {concept_name}:")
            for ds_id, wm in sorted(root_watermarks.items()):
                if concept_name in wm.keys:
                    echo(f"    {ds_id}: {wm.keys[concept_name].value}")
            echo(f"    \u2192 max: {max_val.value}")


def _print_env_max_table(env_max: dict) -> None:
    """Print environment max watermarks table (rich or fallback)."""
    if _core.RICH_AVAILABLE and _core.console is not None:
        max_table = Table(
            title="Environment Max Watermarks",
            show_header=True,
            header_style=_core.HEADER_GREEN,
            box=box.MINIMAL_DOUBLE_HEAD,
        )
        max_table.add_column("Key", style=_core.COL_WHITE)
        max_table.add_column("Max Value", style=_core.COL_GREEN)
        max_table.add_column("Type", style=_core.COL_DIM)
        for key_name, update_key in sorted(env_max.items()):
            max_table.add_row(
                key_name,
                str(update_key.value),
                update_key.type.value,
            )
        _core.console.print(max_table)
    else:
        print_info("Environment max watermarks:")
        for key_name, update_key in sorted(env_max.items()):
            print_info(f"  {key_name}: {update_key.value} ({update_key.type.value})")


def show_watermarks(watermarks: dict, env_max: dict | None = None) -> None:
    """Display datasource watermark information."""
    if _core.RICH_AVAILABLE and _core.console is not None:
        wm_table = Table(
            title="Datasource Watermarks",
            show_header=True,
            header_style=_core.HEADER_BLUE,
            box=box.MINIMAL_DOUBLE_HEAD,
        )
        wm_table.add_column("Datasource", style=_core.COL_CYAN)
        wm_table.add_column("Key", style=_core.COL_WHITE)
        wm_table.add_column("Value", style=_core.COL_WHITE)
        wm_table.add_column("Type", style=_core.COL_DIM)

        for ds_id, watermark in sorted(watermarks.items()):
            if not watermark.keys:
                wm_table.add_row(ds_id, "-", "(no watermarks)", "")
            else:
                for key_name, update_key in watermark.keys.items():
                    wm_table.add_row(
                        ds_id,
                        key_name,
                        str(update_key.value),
                        update_key.type.value,
                    )

        _core.console.print(wm_table)
    else:
        print_info("Watermarks:")
        for ds_id, watermark in sorted(watermarks.items()):
            if not watermark.keys:
                print_info(f"  {ds_id}: (no watermarks)")
            else:
                for key_name, update_key in watermark.keys.items():
                    print_info(
                        f"  {ds_id}.{key_name}: {update_key.value} ({update_key.type.value})"
                    )

    if env_max:
        _print_env_max_table(env_max)


def show_stale_assets(stale_assets: list[StaleDataSourceEntry]) -> None:
    """Display stale assets that need refresh."""
    if _core.RICH_AVAILABLE and _core.console is not None:
        stale_table = Table(
            title="Stale Assets to Refresh",
            show_header=True,
            header_style=_core.HEADER_YELLOW,
            box=box.MINIMAL_DOUBLE_HEAD,
        )
        stale_table.add_column("Datasource", style=_core.COL_CYAN)
        stale_table.add_column("Reason", style="yellow")

        for asset in stale_assets:
            stale_table.add_row(asset.datasource_id, asset.reason)

        _core.console.print(stale_table)
    else:
        echo(style("Stale Assets to Refresh:", fg="yellow", bold=True))
        for asset in stale_assets:
            echo(f"  {asset.datasource_id}: {asset.reason}")


def show_grouped_refresh_assets(
    grouped_assets: list[ManagedDataGroup],
) -> None:
    """Display stale assets grouped by physical data address."""
    if _core.RICH_AVAILABLE and _core.console is not None:
        stale_table = Table(
            title="Stale Assets to Refresh",
            show_header=True,
            header_style=_core.HEADER_YELLOW,
            box=box.MINIMAL_DOUBLE_HEAD,
        )
        stale_table.add_column("Data Object", style=_core.COL_CYAN)
        stale_table.add_column("Datasource", style=_core.COL_WHITE)
        stale_table.add_column("Reason", style="yellow")
        stale_table.add_column("Files", style=_core.COL_DIM)

        for group in grouped_assets:
            first_group_row = True
            for ds in group.datasources:
                files_str = ", ".join(str(f) for f in ds.referenced_in)
                reason_to_show = ""
                if first_group_row and group.common_reason:
                    reason_to_show = group.common_reason
                elif ds.reason:
                    reason_to_show = ds.reason

                stale_table.add_row(
                    group.data_address if first_group_row else "",
                    ds.datasource_id,
                    reason_to_show,
                    files_str,
                )
                first_group_row = False

        _core.console.print(stale_table)
        return

    echo(style("Stale Assets to Refresh:", fg="yellow", bold=True))
    for group in grouped_assets:
        reason_label = f" [{group.common_reason}]" if group.common_reason else ""
        echo(f"  {group.data_address}{reason_label}")
        for ds in group.datasources:
            files_str = ", ".join(str(f) for f in ds.referenced_in)
            ds_reason = f": {ds.reason}" if ds.reason else ""
            echo(f"    {ds.datasource_id}{ds_reason} [{files_str}]")


def show_asset_status_summary(
    watermarks: dict,
    address_map: dict[str, str],
    stale_assets: list,
    env_max: dict | None = None,
) -> None:
    """Display combined watermark and staleness status grouped by managed asset."""
    stale_reasons: dict[str, str] = {a.datasource_id: a.reason for a in stale_assets}
    all_ds_ids = sorted(set(watermarks.keys()) | set(stale_reasons.keys()))

    addr_to_ds: dict[str, list[str]] = {}
    for ds_id in all_ds_ids:
        addr = address_map.get(ds_id, ds_id)
        addr_to_ds.setdefault(addr, []).append(ds_id)

    if _core.RICH_AVAILABLE and _core.console is not None:
        table = Table(
            title="Asset Status",
            show_header=True,
            header_style=_core.HEADER_BLUE,
            box=box.MINIMAL_DOUBLE_HEAD,
        )
        table.add_column("Managed Asset", style=_core.COL_CYAN)
        table.add_column("Datasource", style=_core.COL_WHITE)
        table.add_column("Watermark", style=_core.COL_WHITE)
        table.add_column("Status", style=_core.COL_WHITE)

        for addr in sorted(addr_to_ds):
            first = True
            for ds_id in addr_to_ds[addr]:
                wm = watermarks.get(ds_id)
                wm_str = (
                    ", ".join(f"{k}={v.value}" for k, v in wm.keys.items())
                    if wm and wm.keys
                    else "-"
                )
                reason = stale_reasons.get(ds_id)
                status = (
                    f"[yellow]stale: {reason}[/yellow]"
                    if reason
                    else f"[{_core.COL_GREEN}]up to date[/{_core.COL_GREEN}]"
                )
                table.add_row(addr if first else "", ds_id, wm_str, status)
                first = False

        _core.console.print(table)
    else:
        echo(style("Asset Status:", bold=True))
        for addr in sorted(addr_to_ds):
            echo(f"  {addr}")
            for ds_id in addr_to_ds[addr]:
                wm = watermarks.get(ds_id)
                wm_str = (
                    ", ".join(f"{k}={v.value}" for k, v in wm.keys.items())
                    if wm and wm.keys
                    else "-"
                )
                reason = stale_reasons.get(ds_id)
                status = f"stale: {reason}" if reason else "up to date"
                echo(f"    {ds_id} | wm: {wm_str} | {status}")

    if env_max:
        _print_env_max_table(env_max)


def show_dry_run_queries(results: list) -> None:
    """Display collected dry-run SQL after parallel refresh completes."""
    from trilogy.scripts.dependency import ScriptNode

    for r in results:
        if not (r.success and r.stats and r.stats.refresh_queries):
            continue
        for q in r.stats.refresh_queries:
            node_name = (
                r.node.path.name if isinstance(r.node, ScriptNode) else r.node.address
            )
            header = f"-- {node_name}: {q.datasource_id}"
            if _core.RICH_AVAILABLE and _core.console is not None:
                _core.console.print(f"\n[dim]{header}[/dim]")
                _core.console.print(q.sql)
            else:
                echo(f"\n{header}\n{q.sql}")


def show_refresh_plan(
    stale_assets: list,
    watermarks: dict,
    grouped_assets: list[ManagedDataGroup] | None = None,
) -> None:
    """Display refresh plan showing watermarks and stale assets for approval."""
    show_watermarks(watermarks)
    if grouped_assets:
        show_grouped_refresh_assets(grouped_assets)
    else:
        show_stale_assets(stale_assets)
