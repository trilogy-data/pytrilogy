"""Render the CLI's JSON event stream as indented plain text for an LLM.

The CLI speaks JSON to programs. A model reading it pays for every quote,
escape, bracket and comma line; the same events as ``key: value`` lines carry
the same information in fewer tokens. Result rows stay one compact JSON array
per line, so string and number cells remain distinguishable.
"""

from __future__ import annotations

import json
from typing import Any

# A short scalar list (result columns, grain keys) reads fine on one line.
INLINE_LIST_LIMIT = 120


def _compact(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), default=str)


def _scalar(value: Any) -> str:
    return value if isinstance(value, str) else json.dumps(value, default=str)


def _is_scalar(value: Any) -> bool:
    return not isinstance(value, (list, dict))


def _is_row(value: Any) -> bool:
    return isinstance(value, list) and all(_is_scalar(c) for c in value)


def _block(key: str, value: Any, pad: str) -> list[str]:
    if isinstance(value, str) and "\n" in value:
        return [f"{pad}{key}: |", *(f"{pad}  {line}" for line in value.splitlines())]
    if _is_scalar(value) or not value:
        shown = (
            json.dumps(value)
            if not _is_scalar(value) or value == ""
            else _scalar(value)
        )
        return [f"{pad}{key}: {shown}"]
    if _is_row(value):
        inline = _compact(value)
        if len(inline) <= INLINE_LIST_LIMIT:
            return [f"{pad}{key}: {inline}"]
    return [f"{pad}{key}:", *_lines(value, pad + "  ")]


def _lines(obj: Any, pad: str = "") -> list[str]:
    if isinstance(obj, dict):
        return [line for k, v in obj.items() for line in _block(str(k) or '""', v, pad)]
    if isinstance(obj, list):
        if any(isinstance(e, list) for e in obj) and all(
            isinstance(e, str) or _is_row(e) for e in obj
        ):
            return [pad + (e if isinstance(e, str) else _compact(e)) for e in obj]
        out: list[str] = []
        for e in obj:
            sub = (
                _lines(e, pad + "  ")
                if not _is_scalar(e)
                else [pad + "  " + _scalar(e)]
            )
            out.append(f"{pad}- {sub[0].lstrip()}")
            out.extend(sub[1:])
        return out
    return [pad + _scalar(obj)]


def _decode_events(text: str) -> list[Any] | None:
    decoder = json.JSONDecoder()
    events: list[Any] = []
    idx, n = 0, len(text)
    while idx < n:
        while idx < n and text[idx].isspace():
            idx += 1
        if idx >= n:
            break
        try:
            obj, idx = decoder.raw_decode(text, idx)
        except json.JSONDecodeError:
            return None
        events.append(obj)
    return events


def render_event(event: Any) -> str:
    return "\n".join(_lines(event))


def render_events(text: str) -> str:
    """Plain-text form of a JSON event stream; non-JSON text passes through."""
    events = _decode_events(text)
    if not events:
        return text
    return "\n\n".join(render_event(e) for e in events)
