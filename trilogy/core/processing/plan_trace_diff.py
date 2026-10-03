"""Compare two plan traces (``plan_trace.PlanTrace.to_dict`` JSON) of one
statement, e.g. before and after a planner change.

Steps align by plan, phase, build context and title, the n-th occurrence of a
key against the n-th; timing and call paths are not compared. CLI:
``local_scripts/plan_debugger/trace_diff.py``.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from difflib import unified_diff
from typing import Any

StepKey = tuple[str, str, str, str, int]
# a long value reads as its first characters
_CLIP = 120


@dataclass
class TraceDiff:
    only_a: list[StepKey] = field(default_factory=list)
    only_b: list[StepKey] = field(default_factory=list)
    # step -> "path: a -> b" per differing leaf
    changed: dict[StepKey, list[str]] = field(default_factory=dict)
    # phase -> (a ms, b ms), timed steps only
    phase_ms: dict[str, tuple[float, float]] = field(default_factory=dict)
    total_ms: tuple[float | None, float | None] = (None, None)
    sql: list[str] = field(default_factory=list)

    @property
    def same_plan(self) -> bool:
        return not (self.only_a or self.only_b or self.changed or self.sql)


def _plan_paths(trace: dict[str, Any]) -> dict[str | None, str]:
    plans = {p["id"]: p for p in trace["plans"]}
    paths: dict[str | None, str] = {None: "statement"}
    for plan_id, plan in plans.items():
        chain = [plan["label"]]
        parent = plan["parent"]
        while parent is not None:
            chain.append(plans[parent]["label"])
            parent = plans[parent]["parent"]
        paths[plan_id] = " / ".join(reversed(chain))
    return paths


def keyed_steps(trace: dict[str, Any]) -> dict[StepKey, dict[str, Any]]:
    paths = _plan_paths(trace)
    seen: Counter[tuple[str, str, str, str]] = Counter()
    out: dict[StepKey, dict[str, Any]] = {}
    for step in trace["steps"]:
        base = (
            paths[step["plan"]],
            step["phase"],
            step["context"] or "",
            step["title"],
        )
        out[(*base, seen[base])] = step
        seen[base] += 1
    return out


def _clip(value: Any) -> str:
    text = repr(value)
    return text if len(text) <= _CLIP else text[: _CLIP - 3] + "..."


def value_diff(a: Any, b: Any, path: str = "") -> list[str]:
    if isinstance(a, dict) and isinstance(b, dict):
        return [
            line
            for k in sorted(a.keys() | b.keys())
            for line in value_diff(a.get(k), b.get(k), f"{path}.{k}")
        ]
    if isinstance(a, list) and isinstance(b, list) and len(a) == len(b):
        return [
            line
            for i, (x, y) in enumerate(zip(a, b))
            for line in value_diff(x, y, f"{path}[{i}]")
        ]
    return [] if a == b else [f"{path or '.'}: {_clip(a)} -> {_clip(b)}"]


def _phase_ms(trace: dict[str, Any]) -> Counter[str]:
    totals: Counter[str] = Counter()
    for step in trace["steps"]:
        if step["ms"] is not None:
            totals[step["phase"]] += step["ms"]
    return totals


def _sql(trace: dict[str, Any]) -> str | None:
    return next((s["data"]["sql"] for s in trace["steps"] if s["phase"] == "sql"), None)


def diff_traces(a: dict[str, Any], b: dict[str, Any]) -> TraceDiff:
    steps_a, steps_b = keyed_steps(a), keyed_steps(b)
    out = TraceDiff(
        only_a=[k for k in steps_a if k not in steps_b],
        only_b=[k for k in steps_b if k not in steps_a],
        total_ms=(a.get("total_ms"), b.get("total_ms")),
    )
    for key in steps_a.keys() & steps_b.keys():
        lines = value_diff(steps_a[key]["data"], steps_b[key]["data"])
        if lines:
            out.changed[key] = lines
    out.changed = {k: out.changed[k] for k in steps_a if k in out.changed}
    ms_a, ms_b = _phase_ms(a), _phase_ms(b)
    out.phase_ms = {
        p: (round(ms_a[p], 3), round(ms_b[p], 3))
        for p in a.get("phases", [])
        if p in ms_a or p in ms_b
    }
    sql_a, sql_b = _sql(a), _sql(b)
    if sql_a is not None and sql_b is not None and sql_a != sql_b:
        out.sql = list(
            unified_diff(sql_a.splitlines(), sql_b.splitlines(), "a", "b", lineterm="")
        )
    return out


def _key_label(key: StepKey) -> str:
    plan, phase, context, title, n = key
    where = f"{plan} [{context}]" if context else plan
    return f"{phase:<13} {where}: {title}" + (f" #{n + 1}" if n else "")


def format_diff(diff: TraceDiff, max_lines: int = 20) -> str:
    out: list[str] = []
    for label, keys in (("only in a", diff.only_a), ("only in b", diff.only_b)):
        if keys:
            out.append(f"{label} ({len(keys)}):")
            out.extend(f"  {_key_label(k)}" for k in keys)
    if diff.changed:
        out.append(f"changed ({len(diff.changed)}):")
        for key, lines in diff.changed.items():
            out.append(f"  {_key_label(key)}")
            out.extend(f"    {line}" for line in lines[:max_lines])
            if len(lines) > max_lines:
                out.append(f"    ... {len(lines) - max_lines} more")
    if diff.sql:
        out.append("sql:")
        out.extend(f"  {line}" for line in diff.sql)
    if diff.same_plan:
        out.append("same plan")
    out.append("planner ms (a -> b):")
    out.extend(f"  {p:<13} {a} -> {b}" for p, (a, b) in diff.phase_ms.items())
    out.append(f"  {'total':<13} {diff.total_ms[0]} -> {diff.total_ms[1]}")
    return "\n".join(out)
