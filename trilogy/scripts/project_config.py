"""Locating a project's ``trilogy.toml``.

Deliberately stdlib-only. ``common`` pulls in the executor and dialect stack —
roughly a quarter-second — which commands that only need to find the config
file (``trilogy cloud``) should not pay for. ``common`` re-exports both names,
so its importers are unaffected.
"""

from __future__ import annotations

from pathlib import Path

TRILOGY_CONFIG_NAME = "trilogy.toml"

# Directory holding root datasource definitions, relative to the project root.
# `init` scaffolds it and `ingest` writes generated models into it by default —
# one constant so the two cannot drift apart.
MODEL_ROOT_DIR = "root"


def find_trilogy_config(start_path: Path | None = None) -> Path | None:
    """The nearest ``trilogy.toml`` at or above *start_path* (default: cwd).

    A file argument searches from its directory, so callers can pass the script
    they are about to run rather than having to strip the name first.
    """
    search_path = start_path if start_path else Path.cwd()
    if not search_path.is_dir():
        search_path = search_path.parent

    for parent in [search_path] + list(search_path.parents):
        candidate = parent / TRILOGY_CONFIG_NAME
        if candidate.exists():
            return candidate
    return None
