"""A step-by-step record of one statement's discovery, for the visual plan
debugger (``local_scripts/plan_debugger``, docs/keyspace_phase_plan.md).

The planner calls ``record`` at its phase seams; when no recorder is active
that is one boolean check. Activate with ``start()``/``stop()`` around a
``process_query`` call, or set ``TRILOGY_PLAN_TRACE=<file>`` and every
top-level ``process_query`` writes its own trace there.

Each step carries the planner time since the previous step (``ms``), on a
clock that stops while the recorder itself works (``off_clock``). A step that
only records its inputs or result for the viewer (``TIMED = False``) has none.

A step's payload is a ``StepData`` dataclass naming its phase; the snapshots
inside it (``ConceptTrace``, ``NodeTrace``, ``CteTrace``...) are copies taken
at record time, since the planner mutates its objects afterwards. Planner
dataclasses recorded whole (group attrs, buckets, contracts) are snapshotted
by ``jsonable``. Nothing here is read back by the planner.
"""

from __future__ import annotations

import json
import os
import sys
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from copy import copy
from dataclasses import InitVar, dataclass, field, fields, is_dataclass, replace
from enum import Enum
from functools import wraps
from pathlib import Path
from time import perf_counter
from typing import TYPE_CHECKING, Any, ClassVar, ParamSpec, TypeVar

if TYPE_CHECKING:
    from trilogy.core import graph as nx
    from trilogy.core.models.build import BuildConcept, BuildDatasource
    from trilogy.core.models.build_environment import BuildEnvironment, SpanScope
    from trilogy.core.models.execute import CTE, Join, QueryDatasource, UnionCTE
    from trilogy.core.models.keyspace import Keyspace
    from trilogy.core.processing.nodes import StrategyNode
    from trilogy.dialect.base import BaseDialect

    from .v4_helper.edges import EdgeMap

TRACE_ENV = "TRILOGY_PLAN_TRACE"
_PACKAGE = str(Path(__file__).resolve().parents[2])
# a step's origin is the planner call path below the plan's own entry point
_ORIGIN_ROOTS = frozenset({"_build_from_graph_traced", "_process_query"})
TRACE_VERSION = 1

P = ParamSpec("P")
R = TypeVar("R")

# Phase names, in pipeline order. The viewer orders its legend by this list.
PHASES: tuple[str, ...] = (
    "request",
    "concept_graph",
    "keyspace",
    "grouping",
    "group_graph",
    "source",
    "node",
    "final",
    "strategy",
    "resolve",
    "ctes",
    "sql",
)

# A planner object snapshotted by `jsonable`.
Json = Any


# ------------------------------------------------------------------ snapshots


@dataclass(frozen=True)
class ConceptTrace:
    address: str
    purpose: str
    derivation: str
    granularity: str
    datatype: str
    keys: list[str]
    grain: list[str]
    lineage: str | None
    pseudonyms: list[str]
    modifiers: list[str]


@dataclass(frozen=True)
class ColumnTrace:
    alias: str
    concept: str
    partial: bool
    nullable: bool
    origin: str | None


@dataclass(frozen=True)
class DatasourceTrace:
    name: str
    identifier: str
    address: str
    grain: list[str]
    where: str | None
    non_partial_for: str | None
    columns: list[ColumnTrace]
    column_level_partial: list[str]


@dataclass(frozen=True)
class EnvironmentTrace:
    datasources: list[DatasourceTrace]
    statement_outputs: list[str] | None
    statement_hidden: list[str] | None


@dataclass(frozen=True)
class SpanScopeTrace:
    owned: list[str]
    unextended: list[str]
    extent_free: list[str]
    extent_free_carried: dict[str, list[str]]


@dataclass(frozen=True)
class RegionTrace:
    index: int
    present: list[str]
    spans: list[str]
    has_own_rows: bool
    emptied_by: list[str]
    witnesses: list[str]
    completions: list[Json]
    reach: list[str]
    describe: str


@dataclass(frozen=True)
class KeyspaceTrace:
    describe: str
    regions: list[RegionTrace]
    outputs: list[str]
    keys_by_address: dict[str, list[str]]
    span_reach: dict[str, list[str]]
    witnessed: dict[str, str]
    unread_spans: list[str]
    demanded_spans: list[str]
    output_demanded_spans: list[str]
    in_play_spans: list[str]
    families: list[list[str]]
    # output address -> "defined" | "carried" | "absent", one per region
    matrix: dict[str, list[str]]


@dataclass(frozen=True)
class EdgeTrace:
    u: str
    v: str
    kind: str | None = None
    phase: str | None = None
    alt_group: str | None = None


@dataclass(frozen=True)
class GraphTrace:
    nodes: dict[str, Json]
    edges: list[EdgeTrace]


@dataclass(frozen=True)
class NodeJoinTrace:
    # "Type<group>" of each side
    left: str
    right: str
    type: str
    concepts: list[str]
    pairs: list[str]
    modifiers: list[str]


@dataclass(frozen=True)
class NodeRef:
    """A node already expanded elsewhere in the same tree."""

    ref: str
    type: str


@dataclass(frozen=True)
class NodeTrace:
    id: str
    type: str
    outputs: list[str]
    inputs: list[str]
    hidden: list[str]
    grain: list[str] | None
    conditions: str | None
    preexisting_conditions: str | None
    partial: list[str]
    nullable: list[str]
    rollup: list[str]
    existence: list[str]
    force_group: bool | None
    limit: int | None
    region_spans: list[str]
    region_boundary: bool
    parents: list[NodeTrace | NodeRef]
    # the group whose build produced this node, when it carries one
    group: str | None = None
    # the build context (group id or "FINAL") whose `plan_source` made it
    sourced_in: str | None = None
    # SelectNode
    datasource: str | None = None
    # MergeNode
    host_stitch: bool | None = None
    preserve_parents: bool | None = None
    whole_grain: bool | None = None
    force_join_type: str | None = None
    span_scope: SpanScopeTrace | None = None
    joins: list[NodeJoinTrace] | None = None


@dataclass(frozen=True)
class QdsJoinTrace:
    left: str | None
    right: str
    type: str
    concepts: list[str]
    pairs: list[str]
    modifiers: list[str]


@dataclass(frozen=True)
class UnnestTrace:
    unnest: list[str] | str
    alias: str


@dataclass(frozen=True)
class TableRef:
    name: str
    kind: str = "table"


@dataclass(frozen=True)
class QdsRef:
    """A query datasource already expanded elsewhere in the same tree."""

    ref: str
    identifier: str


@dataclass(frozen=True)
class QdsTrace:
    id: str
    identifier: str
    source_type: str
    grain: list[str]
    outputs: list[str]
    hidden: list[str]
    condition: str | None
    group_required: bool
    force_group: bool | None
    limit: int | None
    partial: list[str]
    nullable: list[str]
    region_spans: list[str]
    zero_filled: list[str]
    extent_free_spans: list[str]
    extent_free_carried: list[str]
    joins: list[QdsJoinTrace | UnnestTrace]
    datasources: list[QdsTrace | QdsRef | TableRef]
    group: str | None = None
    kind: str = "query"


@dataclass(frozen=True)
class CteJoinTrace:
    left: str | None
    right: str
    type: str
    pairs: list[str]
    condition: str | None
    modifiers: list[str]


@dataclass(frozen=True)
class CteTrace:
    name: str
    kind: str
    outputs: list[str]
    hidden: list[str]
    grain: list[str]
    parents: list[str]
    partial: list[str]
    rollup: list[str]
    limit: int | None
    order_by: str | None
    # the group whose node this CTE renders, when it carries one
    group: str | None = None
    # CTE
    base: str | None = None
    group_to_grain: bool | None = None
    condition: str | None = None
    nullable: list[str] | None = None
    zero_filled: list[str] | None = None
    inlined: list[str] | None = None
    base_alias: str | None = None
    joins: list[CteJoinTrace | UnnestTrace] | None = None
    # UnionCTE
    operator: str | None = None


# ---------------------------------------------------------------------- steps


@dataclass(frozen=True)
class StepData:
    PHASE: ClassVar[str]
    # False for a step that only snapshots inputs or a result: the time before
    # it is not its work
    TIMED: ClassVar[bool] = True


@dataclass(frozen=True)
class RequestStep(StepData):
    PHASE: ClassVar[str] = "request"
    TIMED: ClassVar[bool] = False
    concepts: list[ConceptTrace]
    conditions: list[str | None]
    staged_conditions: list[str | None]
    materialized_roots: list[str]
    complete_partials: bool
    span_scope: SpanScopeTrace
    environment: EnvironmentTrace


@dataclass(frozen=True)
class HistoryHitStep(StepData):
    PHASE: ClassVar[str] = "request"
    concepts: list[str]
    conditions: list[str | None]
    exists: bool


@dataclass(frozen=True)
class ConceptGraphStep(StepData):
    PHASE: ClassVar[str] = "concept_graph"
    graph: GraphTrace
    # every concept in the graph, planner-minted ones (`_virt_*`) included
    concepts: list[ConceptTrace]


@dataclass(frozen=True)
class KeyspaceStep(StepData):
    PHASE: ClassVar[str] = "keyspace"
    keyspace: KeyspaceTrace


@dataclass(frozen=True)
class PlacementTrace:
    atom: str | None
    groups: list[str]
    reason: str


@dataclass(frozen=True)
class PlacementStep(StepData):
    PHASE: ClassVar[str] = "grouping"
    placements: list[PlacementTrace]


@dataclass(frozen=True)
class BucketsStep(StepData):
    PHASE: ClassVar[str] = "grouping"
    buckets: dict[str, Json]
    primary_group: dict[str, str]


@dataclass(frozen=True)
class GroupGraphStep(StepData):
    PHASE: ClassVar[str] = "group_graph"
    graph: GraphTrace


@dataclass(frozen=True)
class SourceRequestTrace:
    outputs: list[str]
    conditions: str | None
    deferred_conditions: str | None
    require_full: bool
    complete_partials: bool
    depth: int


@dataclass(frozen=True)
class SourceStep(StepData):
    PHASE: ClassVar[str] = "source"
    request: SourceRequestTrace
    span_scope: SpanScopeTrace
    node: NodeTrace | None


@dataclass(frozen=True)
class BindingTrace:
    strength: str
    stored: bool
    injected: bool


@dataclass(frozen=True)
class CandidateTrace:
    datasource: str | None
    condition: str
    is_union: bool
    grain: list[str]
    bindings: dict[str, BindingTrace]


@dataclass(frozen=True)
class SolutionTrace:
    sources: list[str]
    assignments: dict[str, list[str]]
    # "left ~ right" -> join keys
    join_keys: dict[str, list[str]]
    partial_terminals: list[str]
    completions: list[str]
    connectors: list[str]
    cost: Json


@dataclass(frozen=True)
class SearchStep(StepData):
    PHASE: ClassVar[str] = "source"
    terminals: list[str]
    candidates: dict[str, CandidateTrace]
    solution: SolutionTrace | None
    unreachable: list[str]
    split: list[str]
    limit: str | None


@dataclass(frozen=True)
class NodeBuiltStep(StepData):
    PHASE: ClassVar[str] = "node"
    group: str
    derivation: str
    attrs: Json
    outputs: list[str]
    needed: list[str]
    atoms: list[str | None]
    preexisting: str | None
    parent_groups: list[str]
    join_keys: list[str]
    span_scope: SpanScopeTrace
    node: NodeTrace | None


@dataclass(frozen=True)
class FinalStep(StepData):
    PHASE: ClassVar[str] = "final"
    contract: Json
    extent_ownership: Json
    built: dict[str, str]
    node: NodeTrace | None


@dataclass(frozen=True)
class StrategyStep(StepData):
    PHASE: ClassVar[str] = "strategy"
    TIMED: ClassVar[bool] = False
    node: NodeTrace | None


@dataclass(frozen=True)
class ResolveStep(StepData):
    PHASE: ClassVar[str] = "resolve"
    node: NodeTrace | None
    datasource: QdsTrace


@dataclass(frozen=True)
class CtesStep(StepData):
    PHASE: ClassVar[str] = "ctes"
    root: str
    ctes: list[CteTrace]
    # CTEs the optimizer removed, in removal order (after optimization only)
    removed: list[CteTombstone] = field(default_factory=list)


@dataclass(frozen=True)
class CteTombstone:
    name: str
    # the optimization phase and its rule; the phase removed it by merging it
    # into `merged_into`, or it went unreferenced (e.g. inlined) and was swept
    phase: str
    rule: str
    merged_into: str | None


@dataclass(frozen=True)
class SqlStep(StepData):
    PHASE: ClassVar[str] = "sql"
    dialect: str
    statement_index: int
    outputs: list[str]
    ctes: dict[str, str]
    sql: str
    columns: list[str] | None
    rows: list[list[Any]] | None


@dataclass(frozen=True)
class TraceStep:
    seq: int
    plan: str | None
    phase: str
    title: str
    data: StepData
    # the group being built when this step was recorded, or "FINAL"
    context: str | None
    # planner call path from the plan's entry point to the recording seam
    origin: list[str]
    # planner milliseconds at the step, and since the previous step when timed
    at_ms: float
    ms: float | None


@dataclass(frozen=True)
class PlanScope:
    """One `_build_from_graph` call: the statement's own plan, or a nested one
    (a rowset body)."""

    id: str
    label: str
    depth: int
    parent: str | None
    outputs: list[str]
    conditions: list[str | None]


# ------------------------------------------------------------------- recorder


@dataclass
class PlanTrace:
    statement: str | None = None
    dialect: InitVar[BaseDialect | None] = None
    renderer: BaseDialect = field(init=False)
    steps: list[TraceStep] = field(default_factory=list)
    plans: list[PlanScope] = field(default_factory=list)
    _stack: list[str] = field(default_factory=list)
    # one build context per open plan scope (the statement's is the first)
    _contexts: list[str | None] = field(default_factory=lambda: [None])
    # id(QueryDatasource) -> group, noted when the statement's tree resolves
    _qds_groups: dict[int, str] = field(default_factory=dict)
    # id(node) -> (node, context) for every `plan_source` result; the node is
    # held so its id is not reused
    _sourced: dict[int, tuple[Any, str | None]] = field(default_factory=dict)
    # the traced statement's first and last line within `statement`, when
    # `statement` is a whole source file
    statement_lines: tuple[int, int] | None = None
    _removed_ctes: list[CteTombstone] = field(default_factory=list)
    _t0: float = field(default_factory=perf_counter)
    # seconds spent in the recorder, and when the current off-clock span began
    _overhead: float = 0.0
    _off_since: float | None = None
    _last_ms: float = 0.0
    total_ms: float | None = None

    def __post_init__(self, dialect: BaseDialect | None) -> None:
        self.renderer = _display_renderer(dialect)

    def clock(self) -> float:
        now = perf_counter() if self._off_since is None else self._off_since
        return (now - self._t0 - self._overhead) * 1000

    @property
    def current_plan(self) -> str | None:
        return self._stack[-1] if self._stack else None

    def push_plan(
        self, label: str, depth: int, outputs: list[str], conditions: list[str | None]
    ) -> str:
        plan_id = f"p{len(self.plans)}"
        self.plans.append(
            PlanScope(plan_id, label, depth, self.current_plan, outputs, conditions)
        )
        self._stack.append(plan_id)
        self._contexts.append(None)
        return plan_id

    def pop_plan(self) -> None:
        self._stack.pop()
        self._contexts.pop()

    def record(self, title: str, data: StepData) -> None:
        at = self.clock()
        ms = at - self._last_ms if data.TIMED else None
        self._last_ms = at
        self.steps.append(
            TraceStep(
                len(self.steps) + 1,
                self.current_plan,
                data.PHASE,
                title,
                data,
                self._contexts[-1],
                _origin(),
                round(at, 3),
                None if ms is None else round(ms, 3),
            )
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "version": TRACE_VERSION,
            "statement": self.statement,
            "statement_lines": self.statement_lines,
            "total_ms": self.total_ms,
            "phases": list(PHASES),
            "plans": jsonable(self.plans),
            "steps": jsonable(self.steps),
        }

    def write(self, path: str | Path) -> Path:
        out = Path(path)
        out.write_text(
            json.dumps(self.to_dict(), ensure_ascii=False, indent=1), encoding="utf-8"
        )
        return out


_active: PlanTrace | None = None


def active() -> bool:
    return _active is not None


def current() -> PlanTrace | None:
    return _active


def start(
    statement: str | None = None,
    renderer: BaseDialect | None = None,
    statement_lines: tuple[int, int] | None = None,
) -> PlanTrace:
    global _active
    _active = PlanTrace(statement, renderer, statement_lines=statement_lines)
    return _active


def stop() -> PlanTrace | None:
    global _active
    trace, _active = _active, None
    if trace is not None:
        trace.total_ms = round(trace.clock(), 3)
    return trace


def off_clock(fn: Callable[P, R]) -> Callable[P, R]:
    """Keep ``fn``'s time off the planner clock: snapshotting for the trace,
    or anything else the recording run does that planning does not."""

    @wraps(fn)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
        trace = _active
        if trace is None or trace._off_since is not None:
            return fn(*args, **kwargs)
        trace._off_since = perf_counter()
        try:
            return fn(*args, **kwargs)
        finally:
            trace._overhead += perf_counter() - trace._off_since
            trace._off_since = None

    return wrapper


def env_output_path() -> str | None:
    return os.environ.get(TRACE_ENV) or None


@off_clock
def record(title: str, data: StepData) -> None:
    if _active is not None:
        _active.record(title, data)


def note_sourced(node: StrategyNode | None) -> None:
    if _active is not None and node is not None:
        _active._sourced[id(node)] = (node, _active._contexts[-1])


def _sourced_in(node: StrategyNode) -> str | None:
    hit = _active._sourced.get(id(node)) if _active is not None else None
    return hit[1] if hit else None


def note_removed_ctes(
    phase: str, rule: str, names: set[str], merged: dict[str, str]
) -> None:
    if _active is not None:
        _active._removed_ctes.extend(
            CteTombstone(name, phase, rule, merged.get(name)) for name in sorted(names)
        )


def removed_ctes() -> list[CteTombstone]:
    return list(_active._removed_ctes) if _active is not None else []


def set_context(label: str | None) -> None:
    """Name what the current plan is building (a group id, "FINAL") for the
    steps recorded until the next call."""
    if _active is not None:
        _active._contexts[-1] = label


def _origin() -> list[str]:
    path: list[str] = []
    frame = sys._getframe(2)
    while frame is not None:
        code = frame.f_code
        if code.co_name in _ORIGIN_ROOTS:
            break
        if (
            code.co_filename.startswith(_PACKAGE)
            and code.co_filename != __file__
            and not code.co_name.startswith("_trace")
        ):
            path.append(code.co_name)
        frame = frame.f_back  # type: ignore[assignment]
    return path[::-1]


@contextmanager
def plan_scope(
    label: str, depth: int, outputs: list[str], conditions: list[str | None]
) -> Iterator[str | None]:
    if _active is None:
        yield None
        return
    plan_id = _active.push_plan(label, depth, outputs, conditions)
    try:
        yield plan_id
    finally:
        _active.pop_plan()


# ---------------------------------------------------------------- serializers


def _display_renderer(renderer: BaseDialect | None) -> BaseDialect:
    """A renderer for expressions outside any CTE. Without a CTE, `render_expr`
    renders an aggregate collapsed over one row (`count` as a 0/1 flag); a
    lineage reads as the grouped aggregate, so this instance renders that."""
    if renderer is None:
        from trilogy.dialect.duckdb import DuckDBDialect

        renderer = DuckDBDialect()
    display = copy(renderer)
    display.FUNCTION_GRAIN_MATCH_MAP = display.FUNCTION_MAP  # type: ignore[misc]
    return display


@off_clock
def expression(e: Any) -> str | None:
    """An expression (or where clause) as the dialect renders it, with
    unqualified columns. None when no trace is recording, so eager call sites
    cost nothing. Rowsets, unions and subselects have no SQL outside their
    CTE; those keep their own spelling."""
    from trilogy.core.models.build import BuildWhereClause

    if e is None or _active is None:
        return None
    if isinstance(e, BuildWhereClause):
        e = e.conditional
    try:
        return _active.renderer.render_expr(e)
    except (TypeError, KeyError, AssertionError):
        return str(e)


@off_clock
def jsonable(value: Any) -> Any:
    """A JSON value for any planner object: dataclasses by field, enums by
    value, sets sorted, and anything else by ``str``."""
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, dict):
        return {str(k): jsonable(v) for k, v in value.items()}
    if isinstance(value, (set, frozenset)):
        return sorted(jsonable(v) for v in value)
    if isinstance(value, (list, tuple)):
        return [jsonable(v) for v in value]
    if is_dataclass(value) and not isinstance(value, type):
        own = vars(value)
        return {f.name: jsonable(own[f.name]) for f in fields(value) if f.name in own}
    return str(value)


def addresses(concepts: list[BuildConcept] | tuple[BuildConcept, ...]) -> list[str]:
    return [c.address for c in concepts]


def _lineage(c: BuildConcept) -> str | None:
    from trilogy.core.models.build import BuildAggregateWrapper

    rendered = expression(c.lineage)
    if isinstance(c.lineage, BuildAggregateWrapper) and c.lineage.by:
        return f"{rendered} by {', '.join(b.address for b in c.lineage.by)}"
    return rendered


@off_clock
def graph_concepts(env: BuildEnvironment, attrs: dict[str, Any]) -> list[ConceptTrace]:
    found = (env.concepts.get(a.address) for a in attrs.values())
    return [concept(c) for c in {c.address: c for c in found if c}.values()]


@off_clock
def concept(c: BuildConcept) -> ConceptTrace:
    return ConceptTrace(
        address=c.address,
        purpose=c.purpose.value,
        derivation=c.derivation.value,
        granularity=c.granularity.value,
        datatype=str(c.datatype),
        keys=sorted(c.keys or ()),
        grain=sorted(c.grain.components) if c.grain else [],
        lineage=_lineage(c),
        pseudonyms=sorted(c.pseudonyms),
        modifiers=[m.value for m in c.modifiers],
    )


@off_clock
def datasource(ds: BuildDatasource) -> DatasourceTrace:
    from trilogy.core.enums import Modifier

    return DatasourceTrace(
        name=ds.name,
        identifier=ds.identifier,
        address=str(ds.address),
        grain=sorted(ds.grain.components),
        where=expression(ds.where),
        non_partial_for=expression(ds.non_partial_for),
        columns=[
            ColumnTrace(
                alias=str(col.alias),
                concept=col.concept.address,
                partial=Modifier.PARTIAL in col.modifiers,
                nullable=Modifier.NULLABLE in col.modifiers,
                origin=col.origin_address,
            )
            for col in ds.columns
        ],
        column_level_partial=sorted(ds.column_level_partial_addresses),
    )


@off_clock
def environment(env: BuildEnvironment) -> EnvironmentTrace:
    outputs, hidden = env.statement_output_addresses, env.statement_hidden_addresses
    return EnvironmentTrace(
        datasources=[datasource(ds) for ds in env.datasources.values()],
        statement_outputs=sorted(outputs) if outputs is not None else None,
        statement_hidden=sorted(hidden) if hidden is not None else None,
    )


@off_clock
def span_scope(scope: SpanScope) -> SpanScopeTrace:
    return SpanScopeTrace(
        owned=sorted(scope.owned),
        unextended=sorted(scope.unextended),
        extent_free=sorted(scope.extent_free),
        extent_free_carried={
            k: sorted(v) for k, v in sorted(scope.extent_free_carried.items())
        },
    )


@off_clock
def keyspace(ks: Keyspace) -> KeyspaceTrace:
    regions = [
        RegionTrace(
            index=i,
            present=sorted(r.present),
            spans=sorted(r.spans),
            has_own_rows=r.has_own_rows,
            emptied_by=sorted(r.emptied_by),
            witnesses=sorted(r.witnesses),
            completions=[jsonable(c) for c in r.completions],
            reach=sorted(r.reach),
            describe=r.describe(),
        )
        for i, r in enumerate(ks.regions)
    ]
    matrix = {
        output: [
            (
                "defined"
                if ks.defined_on(output, r)
                else "carried" if ks.carried_on(output, r) else "absent"
            )
            for r in ks.regions
        ]
        for output in ks.outputs
    }
    return KeyspaceTrace(
        describe=ks.describe(),
        regions=regions,
        outputs=list(ks.outputs),
        keys_by_address={k: sorted(v) for k, v in sorted(ks.keys_by_address.items())},
        span_reach={k: sorted(v) for k, v in sorted(ks.span_reach.items())},
        witnessed=dict(sorted(ks.witnessed.items())),
        unread_spans=sorted(ks.unread_spans),
        demanded_spans=sorted(ks.demanded_spans),
        output_demanded_spans=sorted(ks.output_demanded_spans),
        in_play_spans=sorted(ks.in_play_spans),
        families=[sorted(f) for f in ks.families],
        matrix=matrix,
    )


@off_clock
def graph(g: nx.DiGraph, edges: EdgeMap, attrs: dict[str, Any]) -> GraphTrace:
    """A topology graph with its side maps: nodes carry their attrs, edges
    their kind and phase."""
    edge_traces = []
    for u, v in g.edges:
        e = edges.get((u, v))
        edge_traces.append(
            EdgeTrace(u, v)
            if e is None
            else EdgeTrace(
                u,
                v,
                e.kind.value,
                e.phase.value if e.phase else None,
                e.alt_group,
            )
        )
    return GraphTrace(
        nodes={node: jsonable(attrs.get(node)) for node in g.nodes},
        edges=edge_traces,
    )


@off_clock
def strategy_node(node: StrategyNode | None) -> NodeTrace | None:
    """The node tree. A parent shared by two consumers is expanded once and
    referenced by id afterwards, so a diamond stays a diamond."""
    if node is None:
        return None
    out = _node(node, {})
    assert isinstance(out, NodeTrace)
    return out


def _pairs(pairs: Any, right: str) -> list[str]:
    return [
        f"{p.existing_datasource.identifier}.{p.left.address} = {right}.{p.right.address}"
        for p in pairs or []
    ]


def _node_label(node: StrategyNode) -> str:
    group = f"<{node.origin_group}>" if node.origin_group else ""
    return f"{type(node).__name__}{group}"


def _node(node: StrategyNode, seen: dict[int, str]) -> NodeTrace | NodeRef:
    from trilogy.core.processing.nodes import MergeNode, SelectNode

    key = id(node)
    if key in seen:
        return NodeRef(seen[key], type(node).__name__)
    ident = f"n{len(seen)}"
    seen[key] = ident
    out = NodeTrace(
        id=ident,
        type=type(node).__name__,
        outputs=addresses(node.output_concepts),
        inputs=addresses(node.input_concepts),
        hidden=sorted(node.hidden_concepts),
        grain=sorted(node.grain.components) if node.grain else None,
        conditions=expression(node.conditions),
        preexisting_conditions=expression(node.preexisting_conditions),
        partial=addresses(node.partial_concepts),
        nullable=addresses(node.nullable_concepts),
        rollup=addresses(node.rollup_concepts),
        existence=addresses(node.existence_concepts),
        force_group=node.force_group,
        limit=node.limit,
        region_spans=sorted(node.region_spans),
        region_boundary=node.region_boundary,
        parents=[_node(p, seen) for p in node.parents],
        group=node.origin_group,
        sourced_in=_sourced_in(node),
    )
    if isinstance(node, SelectNode) and node.datasource:
        return replace(out, datasource=node.datasource.identifier)
    if isinstance(node, MergeNode):
        return replace(
            out,
            host_stitch=node.host_stitch,
            preserve_parents=node.preserve_parents,
            whole_grain=node.whole_grain,
            force_join_type=(
                node.force_join_type.value if node.force_join_type else None
            ),
            span_scope=span_scope(node.span_scope),
            joins=[
                NodeJoinTrace(
                    left=_node_label(j.left_node),
                    right=_node_label(j.right_node),
                    type=j.join_type.value,
                    concepts=addresses(j.concepts),
                    pairs=[
                        f"{p.left.address} = {p.right.address}"
                        for p in j.concept_pairs or []
                    ],
                    modifiers=[m.value for m in j.modifiers],
                )
                for j in node.node_joins or []
            ],
        )
    return out


@off_clock
def query_datasource(qds: QueryDatasource, root: StrategyNode) -> QdsTrace:
    """The resolved tree. Each strategy node caches the datasource it
    resolved to, so walking `root` ties every datasource (and the CTE later
    built from it) back to the group that built its node."""
    from trilogy.core.processing.nodes import StrategyNode as Node

    assert _active is not None
    pending: list[Node] = [root]
    while pending:
        node = pending.pop()
        if node.resolution_cache is not None and node.origin_group:
            _active._qds_groups.setdefault(id(node.resolution_cache), node.origin_group)
        pending.extend(node.parents)
    out = _qds(qds, {})
    assert isinstance(out, QdsTrace)
    return out


def _qds(source: Any, seen: dict[int, str]) -> QdsTrace | QdsRef | TableRef:
    from trilogy.core.models.build import BuildDatasource
    from trilogy.core.models.execute import BaseJoin

    if isinstance(source, BuildDatasource):
        return TableRef(source.identifier)
    key = id(source)
    if key in seen:
        return QdsRef(seen[key], source.identifier)
    ident = f"q{len(seen)}"
    seen[key] = ident
    joins: list[QdsJoinTrace | UnnestTrace] = [
        (
            QdsJoinTrace(
                left=j.left_datasource.identifier if j.left_datasource else None,
                right=j.right_datasource.identifier,
                type=j.join_type.value,
                concepts=addresses(j.concepts or []),
                pairs=_pairs(j.concept_pairs, j.right_datasource.identifier),
                modifiers=[m.value for m in j.modifiers],
            )
            if isinstance(j, BaseJoin)
            else UnnestTrace(addresses(j.concepts), j.alias)
        )
        for j in source.joins
    ]
    return QdsTrace(
        id=ident,
        identifier=source.identifier,
        source_type=source.source_type.value,
        grain=sorted(source.grain.components),
        outputs=addresses(source.output_concepts),
        hidden=sorted(source.hidden_concepts),
        condition=expression(source.condition),
        group_required=source.group_required,
        force_group=source.force_group,
        limit=source.limit,
        partial=addresses(source.partial_concepts),
        nullable=addresses(source.nullable_concepts),
        region_spans=sorted(source.region_spans),
        zero_filled=sorted(source.zero_filled),
        extent_free_spans=sorted(source.extent_free_spans),
        extent_free_carried=sorted(source.extent_free_carried),
        joins=joins,
        datasources=[_qds(d, seen) for d in source.datasources],
        group=_group_of(source),
    )


def _group_of(source: Any) -> str | None:
    return _active._qds_groups.get(id(source)) if _active is not None else None


def _cte_ref(consumer: CTE, join: Join, node: CTE | UnionCTE) -> str:
    """The alias the SQL uses, and the CTE behind it when they differ (an
    inlined datasource renders under its table alias)."""
    alias = join.name_for(consumer, node)
    return alias if alias == node.name else f"{alias} ({node.name})"


def _cte_join_left(consumer: CTE, join: Join) -> str | None:
    """A join names its left side only when it has one; otherwise its key
    pairs do, one CTE per pair."""
    if join.left_cte is not None:
        return _cte_ref(consumer, join, join.left_cte)
    sides = sorted({_cte_ref(consumer, join, k.cte) for k in join.joinkey_pairs or []})
    return ", ".join(sides) or None


@off_clock
def cte(c: CTE | UnionCTE) -> CteTrace:
    from trilogy.core.models.execute import CTE, Join

    out = CteTrace(
        name=c.name,
        kind=type(c).__name__,
        outputs=addresses(c.output_columns),
        hidden=sorted(c.hidden_concepts),
        grain=sorted(c.grain.components),
        parents=[p.name for p in c.parent_ctes],
        partial=addresses(c.partial_concepts),
        rollup=addresses(c.rollup_concepts),
        limit=c.limit,
        order_by=str(c.order_by) if c.order_by else None,
        group=_group_of(c.source),
    )
    if not isinstance(c, CTE):
        return replace(out, operator=c.operator)
    return replace(
        out,
        group_to_grain=c.group_to_grain,
        condition=expression(c.condition),
        nullable=addresses(c.nullable_concepts),
        zero_filled=sorted(c.zero_filled),
        inlined=[p.name for p in c.inlined_parents],
        base_alias=c.base_alias_override,
        base=c.base_alias,
        joins=[
            (
                CteJoinTrace(
                    left=_cte_join_left(c, j),
                    right=_cte_ref(c, j, j.right_cte),
                    type=j.jointype.value,
                    pairs=[
                        f"{j.name_for(c, k.cte)}.{k.left.address}"
                        f" = {j.name_for(c, j.right_cte)}.{k.right.address}"
                        for k in j.joinkey_pairs or []
                    ],
                    condition=expression(j.condition),
                    modifiers=[m.value for m in j.modifiers],
                )
                if isinstance(j, Join)
                else UnnestTrace(str(j.object_to_unnest), j.alias)
            )
            for j in c.joins
        ],
    )
