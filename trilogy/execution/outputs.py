"""Run outputs: values a script hands back to whoever ran it.

A program run by a ``call`` statement reports an output by printing one
marker line on stdout::

    ::trilogy-output name=fix_pr kind=link value=https://github.com/o/r/pull/45

``name`` is required; ``kind`` is ``link``, ``text`` or ``json`` and defaults
to ``link`` for an http(s) URL and ``text`` otherwise; ``value`` must come
last and takes the rest of the line verbatim, so URLs carrying ``=`` or ``&``
survive. Every other stdout line is left alone.

Each output becomes an ``output`` report record and is printed with the
run's summary, so a script's result reads the same on a laptop and in an
orchestrator that parses the report. Outputs are collected per process,
like the report sink: parallel execution emits them from worker threads.
"""

from __future__ import annotations

import json
import re
import threading
from dataclasses import dataclass
from typing import Any

from trilogy.constants import logger
from trilogy.execution.report import emit_report

MARKER = "::trilogy-output"
OUTPUT_KINDS = ("link", "text", "json")

_MARKER_RE = re.compile(rf"^{re.escape(MARKER)}\s+((?:\w+=\S*\s+)*)value=(.*)$")
_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_.-]*$")
_URL_RE = re.compile(r"^https?://\S+$")


@dataclass(frozen=True)
class RunOutput:
    name: str
    value: Any
    kind: str
    source: str | None = None

    def as_record(self) -> dict[str, Any]:
        record: dict[str, Any] = {
            "name": self.name,
            "kind": self.kind,
            "value": self.value,
        }
        if self.source:
            record["source"] = self.source
        return record


def parse_output_line(line: str, source: str | None = None) -> RunOutput | None:
    """The output a marker line declares, or ``None`` for any other line.

    A line that starts with the marker but does not parse is a script author's
    mistake, so it is logged rather than silently dropped."""
    stripped = line.strip()
    if not stripped.startswith(MARKER):
        return None
    match = _MARKER_RE.match(stripped)
    if match is None:
        logger.warning(f"ignoring malformed output marker: {stripped}")
        return None
    attrs: dict[str, str] = {}
    for token in match.group(1).split():
        key, _, val = token.partition("=")
        attrs[key] = val
    value: Any = match.group(2).strip()
    name = attrs.pop("name", "")
    kind = attrs.pop("kind", None)
    if attrs or not _NAME_RE.match(name):
        logger.warning(f"ignoring malformed output marker: {stripped}")
        return None
    if kind is None:
        kind = "link" if _URL_RE.match(value) else "text"
    if kind not in OUTPUT_KINDS:
        logger.warning(f"ignoring output '{name}' with unknown kind '{kind}'")
        return None
    if kind == "json":
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            logger.warning(f"output '{name}' is not valid JSON; keeping it as text")
            kind = "text"
    return RunOutput(name=name, value=value, kind=kind, source=source)


def scan_outputs(text: str, source: str | None = None) -> list[RunOutput]:
    """Every output declared in a program's stdout, in order."""
    outputs = []
    for line in text.splitlines():
        output = parse_output_line(line, source)
        if output is not None:
            outputs.append(output)
    return outputs


def is_output_line(line: str) -> bool:
    return line.strip().startswith(MARKER)


_COLLECTED: list[RunOutput] = []
_LOCK = threading.Lock()


def record_outputs(outputs: list[RunOutput]) -> None:
    """Keep outputs for the run summary and write each to the report."""
    if not outputs:
        return
    with _LOCK:
        _COLLECTED.extend(outputs)
    for output in outputs:
        emit_report("output", **output.as_record())


def collected_outputs() -> list[RunOutput]:
    with _LOCK:
        return list(_COLLECTED)


def reset_outputs() -> None:
    """Start a run with no outputs; the CLI calls this per invocation."""
    with _LOCK:
        _COLLECTED.clear()
