"""A step-by-step record of one statement's discovery, for the visual plan
debugger (``local_scripts/plan_debugger``).

The planner calls ``record`` at its phase seams; when no recorder is active
that is one boolean check. Activate with ``start()``/``stop()`` around a
``process_query`` call, or set ``TRILOGY_PLAN_TRACE=<file>`` and every
top-level ``process_query`` writes its own trace: the first to ``<file>``, the
n-th after it to ``<stem>.<n><suffix>``.

Each step carries the planner time since the previous step (``ms``), on a
clock that stops while the recorder itself works (``off_clock``). A step that
only records its inputs or result for the viewer (``TIMED = False``) has none.

This module is what the planner calls on every statement. The step payloads
and the snapshots inside them (``plan_trace_model``) load on first use, which
is only while a recorder is active; ``plan_trace.<name>`` reaches them.
"""

from __future__ import annotations

import os
from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager
from contextvars import ContextVar
from functools import wraps
from pathlib import Path
from time import perf_counter
from typing import TYPE_CHECKING, Any, ParamSpec, TypeVar

if TYPE_CHECKING:
    from trilogy.core.models.execute import CTE, UnionCTE
    from trilogy.core.processing.nodes import StrategyNode
    from trilogy.core.processing.plan_trace_model import *
    from trilogy.core.processing.plan_trace_model import (
        CteTombstone,
        CteTrace,
        PlanTrace,
        StepData,
    )
    from trilogy.dialect.base import BaseDialect


def __getattr__(name: str) -> Any:
    from trilogy.core.processing import plan_trace_model

    try:
        return getattr(plan_trace_model, name)
    except AttributeError:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}") from None


TRACE_ENV = "TRILOGY_PLAN_TRACE"
# a step's origin is the planner call path below the plan's own entry point;
# these are function names, so renaming one of those functions must update this
_ORIGIN_ROOTS = frozenset({"_build_from_graph_traced", "_process_query"})

P = ParamSpec("P")
R = TypeVar("R")


# per context, so a planning thread never records into another's trace
_ACTIVE: ContextVar[PlanTrace | None] = ContextVar("plan_trace", default=None)


def active() -> bool:
    return _ACTIVE.get() is not None


def current() -> PlanTrace | None:
    return _ACTIVE.get()


def start(
    statement: str | None = None,
    renderer: BaseDialect | None = None,
    statement_lines: tuple[int, int] | None = None,
) -> PlanTrace:
    from trilogy.core.processing.plan_trace_model import PlanTrace

    trace = PlanTrace(statement, renderer, statement_lines=statement_lines)
    trace._token = _ACTIVE.set(trace)
    return trace


def stop() -> PlanTrace | None:
    trace = _ACTIVE.get()
    if trace is None:
        return None
    if trace._token is None:
        _ACTIVE.set(None)
    else:
        _ACTIVE.reset(trace._token)
        trace._token = None
    trace.total_ms = round(trace.clock(), 3)
    return trace


@contextmanager
def recording(
    statement: str | None = None,
    renderer: BaseDialect | None = None,
    statement_lines: tuple[int, int] | None = None,
) -> Iterator[PlanTrace]:
    trace = start(statement, renderer, statement_lines)
    try:
        yield trace
    finally:
        stop()


def off_clock(fn: Callable[P, R]) -> Callable[P, R]:
    """Keep ``fn``'s time off the planner clock: snapshotting for the trace,
    or anything else the recording run does that planning does not."""

    @wraps(fn)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
        trace = _ACTIVE.get()
        if trace is None or trace._off_since is not None:
            return fn(*args, **kwargs)
        trace._off_since = perf_counter()
        try:
            return fn(*args, **kwargs)
        finally:
            trace._overhead += perf_counter() - trace._off_since
            trace._off_since = None

    return wrapper


# per env path, how many statements have written a trace to it
_ENV_TRACES_WRITTEN: dict[str, int] = {}


def env_output_path() -> str | None:
    return os.environ.get(TRACE_ENV) or None


def next_env_output_path(path: str) -> Path:
    """`path` for the process's first traced statement, `<stem>.<n><suffix>`
    for the n-th after it, so no statement overwrites another's trace."""
    written = _ENV_TRACES_WRITTEN[path] = _ENV_TRACES_WRITTEN.get(path, 0) + 1
    out = Path(path)
    if written == 1:
        return out
    return out.with_name(f"{out.stem}.{written}{out.suffix}")


@off_clock
def record(title: str, data: StepData) -> None:
    trace = _ACTIVE.get()
    if trace is not None:
        trace.record(title, data)


def note_sourced(node: StrategyNode | None) -> None:
    trace = _ACTIVE.get()
    if trace is not None and node is not None:
        trace._sourced[id(node)] = (node, trace._contexts[-1])


def _sourced_in(node: StrategyNode) -> str | None:
    trace = _ACTIVE.get()
    hit = trace._sourced.get(id(node)) if trace is not None else None
    return hit[1] if hit else None


def note_removed_ctes(
    phase: str, rule: str, names: set[str], merged: dict[str, str]
) -> None:
    trace = _ACTIVE.get()
    if trace is not None:
        from trilogy.core.processing.plan_trace_model import CteTombstone

        trace._removed_ctes.extend(
            CteTombstone(name, phase, rule, merged.get(name)) for name in sorted(names)
        )


@off_clock
def cte_snapshot(ctes: Sequence[CTE | UnionCTE]) -> dict[str, CteTrace]:
    """Every CTE by name, for `optimizer_step`; empty when not recording."""
    if _ACTIVE.get() is None:
        return {}
    from trilogy.core.processing.plan_trace_model import cte

    return {c.name: cte(c) for c in ctes}


def removed_ctes() -> list[CteTombstone]:
    trace = _ACTIVE.get()
    return list(trace._removed_ctes) if trace is not None else []


def set_context(label: str | None) -> None:
    """Name what the current plan is building (a group id, "FINAL") for the
    steps recorded until the next call."""
    trace = _ACTIVE.get()
    if trace is not None:
        trace._contexts[-1] = label


@contextmanager
def plan_scope(
    label: str, depth: int, outputs: list[str], conditions: list[str | None]
) -> Iterator[str | None]:
    trace = _ACTIVE.get()
    if trace is None:
        yield None
        return
    plan_id = trace.push_plan(label, depth, outputs, conditions)
    try:
        yield plan_id
    finally:
        trace.pop_plan()
