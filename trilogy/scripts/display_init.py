"""Display helpers for the init command — the startup panel."""

from __future__ import annotations

import trilogy.scripts.display_core as _core
from trilogy.scripts.display_core import emit_event, is_json_mode, print_info

try:
    from rich.panel import Panel
except ImportError:
    pass


def show_init_header(path: str, dialect: str | None, overwrite: bool) -> None:
    """Show a startup panel summarizing the workspace about to be created."""
    if is_json_mode():
        emit_event("init_start", path=path, dialect=dialect, overwrite=overwrite)
        return
    dialect_label = dialect or "unset (defaults to duck_db)"
    if _core.RICH_AVAILABLE and _core.console is not None:
        body = f"Path:    [dim]{path}[/dim]\nDialect: [cyan]{dialect_label}[/cyan]"
        if overwrite:
            body += "\nForce:   [yellow]overwriting existing trilogy.toml[/yellow]"
        _core.console.print(Panel.fit(body, style="blue", title="Trilogy Init"))
        return
    msg = f"Trilogy Init | path={path} | dialect={dialect_label}"
    if overwrite:
        msg += " | overwriting existing trilogy.toml"
    print_info(msg)
