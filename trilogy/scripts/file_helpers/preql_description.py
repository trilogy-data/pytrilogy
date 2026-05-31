"""Read the leading ``#`` comment block from a ``.preql`` file as a one-line
description, for surfaced-in-list-output usage.

The leading comment block is by convention the file's purpose statement
(e.g. ``# Unified sales fact across all three TPC-DS channels …``). Both
``trilogy file list`` and the agent's bespoke ``list_files`` tool surface
it next to the path so a reader (human or LLM) can route to the right
import without opening every model file.

Shared so the two CLIs render the same string with the same truncation
behaviour — drift between the agent-facing and human-facing tools is the
exact failure mode we hit on the TPC-DS eval (the agent saw the truncated
``unified_sales`` hint and missed the cross-channel value prop)."""

from __future__ import annotations

from pathlib import Path

LIST_FILES_DESC_LIMIT = 500
LIST_FILES_DESC_PREFIX = "    ↳ "
PREQL_HEAD_SCAN_BYTES = 4096


def read_preql_description(path: Path) -> str | None:
    """First-block ``#`` comment at the top of a ``.preql`` file, flattened
    to a single line. Returns ``None`` if the file does not start with a
    comment block or cannot be read."""
    try:
        with path.open("r", encoding="utf-8", errors="replace") as f:
            head = f.read(PREQL_HEAD_SCAN_BYTES)
    except OSError:
        return None
    lines = head.splitlines()
    i = 0
    while i < len(lines) and not lines[i].strip():
        i += 1
    parts: list[str] = []
    while i < len(lines):
        stripped = lines[i].strip()
        if not stripped or not stripped.startswith("#"):
            break
        parts.append(stripped.lstrip("#").strip())
        i += 1
    text = " ".join(p for p in parts if p)
    return text or None


def truncate_description(text: str, limit: int = LIST_FILES_DESC_LIMIT) -> str:
    """Clip ``text`` to ``limit`` characters, replacing the trailing slice
    with an ellipsis so the truncation is visible to the reader."""
    return text if len(text) <= limit else text[: max(limit - 1, 0)] + "…"


def format_preql_description(path: Path) -> str | None:
    """``    ↳ <description>`` line for a ``.preql`` file with a leading
    comment block. Returns ``None`` when no description exists, so callers
    can branch on presence without re-reading the file."""
    if path.suffix != ".preql":
        return None
    desc = read_preql_description(path)
    if not desc:
        return None
    return LIST_FILES_DESC_PREFIX + truncate_description(desc)
