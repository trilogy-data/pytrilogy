"""A step-by-step record of one statement's discovery, for the visual plan
debugger (``local_scripts/plan_debugger``, docs/keyspace_phase_plan.md).

The planner calls ``record`` at its phase seams; when no recorder is active
that is one boolean check. Activate with ``start()``/``stop()`` around a
``process_query`` call, or set ``TRILOGY_PLAN_TRACE=<file>`` and every
top-level ``process_query`` writes its own trace there.

Everything recorded is a plain JSON value built by the serializers below;
nothing here is read back by the planner.
"""

from __future__ import annotations

import json
import os
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import fields, is_dataclass
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from trilogy.core import graph as nx
    from trilogy.core.models.build import BuildConcept, BuildDatasource
    from trilogy.core.models.build_environment import BuildEnvironment, SpanScope
    from trilogy.core.models.execute import CTE, QueryDatasource, UnionCTE
    from trilogy.core.models.keyspace import Keyspace
    from trilogy.core.processing.nodes import StrategyNode

    from .v4_helper.edges import EdgeMap

TRACE_ENV = "TRILOGY_PLAN_TRACE"
TRACE_VERSION = 1

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


class PlanTrace:
    def __init__(self, statement: str | None = None) -> None:
        self.statement = statement
        self.steps: list[dict[str, Any]] = []
        self.plans: list[dict[str, Any]] = []
        self._stack: list[str] = []
        self._seq = 0

    @property
    def current_plan(self) -> str | None:
        return self._stack[-1] if self._stack else None

    def push_plan(self, label: str, depth: int, **data: Any) -> str:
        plan_id = f"p{len(self.plans)}"
        self.plans.append(
            {
                "id": plan_id,
                "label": label,
                "depth": depth,
                "parent": self.current_plan,
                **data,
            }
        )
        self._stack.append(plan_id)
        return plan_id

    def pop_plan(self) -> None:
        self._stack.pop()

    def record(self, phase: str, title: str, data: dict[str, Any]) -> None:
        self._seq += 1
        self.steps.append(
            {
                "seq": self._seq,
                "plan": self.current_plan,
                "phase": phase,
                "title": title,
                "data": data,
            }
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "version": TRACE_VERSION,
            "statement": self.statement,
            "phases": list(PHASES),
            "plans": self.plans,
            "steps": self.steps,
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


def start(statement: str | None = None) -> PlanTrace:
    global _active
    _active = PlanTrace(statement)
    return _active


def stop() -> PlanTrace | None:
    global _active
    trace, _active = _active, None
    return trace


def env_output_path() -> str | None:
    return os.environ.get(TRACE_ENV) or None


def record(phase: str, title: str, **data: Any) -> None:
    if _active is not None:
        _active.record(phase, title, data)


@contextmanager
def plan_scope(label: str, depth: int, **data: Any) -> Iterator[str | None]:
    if _active is None:
        yield None
        return
    plan_id = _active.push_plan(label, depth, **data)
    try:
        yield plan_id
    finally:
        _active.pop_plan()


# ---------------------------------------------------------------- serializers


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


def concept(c: BuildConcept) -> dict[str, Any]:
    return {
        "address": c.address,
        "purpose": c.purpose.value,
        "derivation": c.derivation.value,
        "granularity": c.granularity.value,
        "datatype": str(c.datatype),
        "keys": sorted(c.keys or ()),
        "grain": sorted(c.grain.components) if c.grain else [],
        "lineage": str(c.lineage) if c.lineage is not None else None,
        "pseudonyms": sorted(c.pseudonyms),
        "modifiers": [m.value for m in c.modifiers],
    }


def addresses(concepts: list[BuildConcept] | tuple[BuildConcept, ...]) -> list[str]:
    return [c.address for c in concepts]


def datasource(ds: BuildDatasource) -> dict[str, Any]:
    from trilogy.core.enums import Modifier

    return {
        "name": ds.name,
        "identifier": ds.identifier,
        "address": str(ds.address),
        "grain": sorted(ds.grain.components),
        "where": str(ds.where) if ds.where else None,
        "non_partial_for": str(ds.non_partial_for) if ds.non_partial_for else None,
        "columns": [
            {
                "alias": str(col.alias),
                "concept": col.concept.address,
                "partial": Modifier.PARTIAL in col.modifiers,
                "nullable": Modifier.NULLABLE in col.modifiers,
                "origin": col.origin_address,
            }
            for col in ds.columns
        ],
        "column_level_partial": sorted(ds.column_level_partial_addresses),
    }


def environment(env: BuildEnvironment) -> dict[str, Any]:
    return {
        "datasources": [datasource(ds) for ds in env.datasources.values()],
        "statement_outputs": (
            sorted(env.statement_output_addresses)
            if env.statement_output_addresses is not None
            else None
        ),
        "statement_hidden": (
            sorted(env.statement_hidden_addresses)
            if env.statement_hidden_addresses is not None
            else None
        ),
    }


def span_scope(scope: SpanScope) -> dict[str, Any]:
    return {
        "owned": sorted(scope.owned),
        "unextended": sorted(scope.unextended),
        "extent_free": sorted(scope.extent_free),
        "extent_free_carried": {
            k: sorted(v) for k, v in sorted(scope.extent_free_carried.items())
        },
    }


def keyspace(ks: Keyspace) -> dict[str, Any]:
    regions = [
        {
            "index": i,
            "present": sorted(r.present),
            "spans": sorted(r.spans),
            "has_own_rows": r.has_own_rows,
            "emptied_by": sorted(r.emptied_by),
            "witnesses": sorted(r.witnesses),
            "completions": [jsonable(c) for c in r.completions],
            "reach": sorted(r.reach),
            "describe": r.describe(),
        }
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
    return {
        "describe": ks.describe(),
        "regions": regions,
        "outputs": list(ks.outputs),
        "keys_by_address": {
            k: sorted(v) for k, v in sorted(ks.keys_by_address.items())
        },
        "span_reach": {k: sorted(v) for k, v in sorted(ks.span_reach.items())},
        "witnessed": dict(sorted(ks.witnessed.items())),
        "unread_spans": sorted(ks.unread_spans),
        "demanded_spans": sorted(ks.demanded_spans),
        "output_demanded_spans": sorted(ks.output_demanded_spans),
        "in_play_spans": sorted(ks.in_play_spans),
        "families": [sorted(f) for f in ks.families],
        "matrix": matrix,
    }


def graph(g: nx.DiGraph, edges: EdgeMap, attrs: dict[str, Any]) -> dict[str, Any]:
    """A topology graph with its side maps: nodes carry their attrs, edges
    their kind and phase."""
    return {
        "nodes": {node: jsonable(attrs.get(node)) for node in g.nodes},
        "edges": [
            {"u": u, "v": v, **(jsonable(edges.get((u, v))) or {})} for u, v in g.edges
        ],
    }


def strategy_node(node: StrategyNode | None) -> dict[str, Any] | None:
    """The node tree. A parent shared by two consumers is expanded once and
    referenced by id afterwards, so a diamond stays a diamond."""
    if node is None:
        return None
    return _node(node, {})


def _node(node: StrategyNode, seen: dict[int, str]) -> dict[str, Any]:
    from trilogy.core.processing.nodes import MergeNode, SelectNode

    key = id(node)
    if key in seen:
        return {"ref": seen[key], "type": type(node).__name__}
    ident = f"n{len(seen)}"
    seen[key] = ident
    out: dict[str, Any] = {
        "id": ident,
        "type": type(node).__name__,
        "outputs": addresses(node.output_concepts),
        "inputs": addresses(node.input_concepts),
        "hidden": sorted(node.hidden_concepts),
        "grain": sorted(node.grain.components) if node.grain else None,
        "conditions": str(node.conditions) if node.conditions else None,
        "preexisting_conditions": (
            str(node.preexisting_conditions) if node.preexisting_conditions else None
        ),
        "partial": addresses(node.partial_concepts),
        "nullable": addresses(node.nullable_concepts),
        "rollup": addresses(node.rollup_concepts),
        "existence": addresses(node.existence_concepts),
        "force_group": node.force_group,
        "limit": node.limit,
        "region_spans": sorted(node.region_spans),
        "region_boundary": node.region_boundary,
    }
    if isinstance(node, SelectNode):
        out["datasource"] = node.datasource.identifier if node.datasource else None
    if isinstance(node, MergeNode):
        out["host_stitch"] = node.host_stitch
        out["preserve_parents"] = node.preserve_parents
        out["whole_grain"] = node.whole_grain
        out["force_join_type"] = (
            node.force_join_type.value if node.force_join_type else None
        )
        out["span_scope"] = span_scope(node.span_scope)
        out["joins"] = [
            {
                "left": repr(j.left_node),
                "right": repr(j.right_node),
                "type": j.join_type.value,
                "concepts": addresses(j.concepts),
                "pairs": [
                    f"{p.left.address} = {p.right.address}"
                    for p in j.concept_pairs or []
                ],
                "modifiers": [m.value for m in j.modifiers],
            }
            for j in node.node_joins or []
        ]
    out["parents"] = [_node(p, seen) for p in node.parents]
    return out


def query_datasource(qds: QueryDatasource) -> dict[str, Any]:
    return _qds(qds, {})


def _qds(source: Any, seen: dict[int, str]) -> dict[str, Any]:
    from trilogy.core.models.build import BuildDatasource
    from trilogy.core.models.execute import BaseJoin

    if isinstance(source, BuildDatasource):
        return {"kind": "table", "name": source.identifier}
    key = id(source)
    if key in seen:
        return {"ref": seen[key], "identifier": source.identifier}
    ident = f"q{len(seen)}"
    seen[key] = ident
    joins = []
    for j in source.joins:
        if isinstance(j, BaseJoin):
            joins.append(
                {
                    "left": j.left_datasource.identifier if j.left_datasource else None,
                    "right": j.right_datasource.identifier,
                    "type": j.join_type.value,
                    "concepts": addresses(j.concepts or []),
                    "pairs": [
                        f"{p.left.address} = {p.right.address}"
                        for p in j.concept_pairs or []
                    ],
                    "modifiers": [m.value for m in j.modifiers],
                }
            )
        else:
            joins.append({"unnest": addresses(j.concepts), "alias": j.alias})
    return {
        "id": ident,
        "kind": "query",
        "identifier": source.identifier,
        "source_type": source.source_type.value,
        "grain": sorted(source.grain.components),
        "outputs": addresses(source.output_concepts),
        "hidden": sorted(source.hidden_concepts),
        "condition": str(source.condition) if source.condition else None,
        "group_required": source.group_required,
        "force_group": source.force_group,
        "limit": source.limit,
        "partial": addresses(source.partial_concepts),
        "nullable": addresses(source.nullable_concepts),
        "region_spans": sorted(source.region_spans),
        "zero_filled": sorted(source.zero_filled),
        "extent_free_spans": sorted(source.extent_free_spans),
        "extent_free_carried": sorted(source.extent_free_carried),
        "joins": joins,
        "datasources": [_qds(d, seen) for d in source.datasources],
    }


def cte(c: CTE | UnionCTE) -> dict[str, Any]:
    from trilogy.core.models.execute import CTE, Join

    out: dict[str, Any] = {
        "name": c.name,
        "kind": type(c).__name__,
        "outputs": addresses(c.output_columns),
        "hidden": sorted(c.hidden_concepts),
        "grain": sorted(c.grain.components),
        "parents": [p.name for p in c.parent_ctes],
        "partial": addresses(c.partial_concepts),
        "rollup": addresses(c.rollup_concepts),
        "limit": c.limit,
        "order_by": str(c.order_by) if c.order_by else None,
    }
    if isinstance(c, CTE):
        out.update(
            {
                "group_to_grain": c.group_to_grain,
                "condition": str(c.condition) if c.condition else None,
                "nullable": addresses(c.nullable_concepts),
                "zero_filled": sorted(c.zero_filled),
                "inlined": [p.name for p in c.inlined_parents],
                "base_alias": c.base_alias_override,
                "joins": [
                    (
                        {
                            "left": j.left_cte.name if j.left_cte else None,
                            "right": j.right_cte.name,
                            "type": j.jointype.value,
                            "pairs": [
                                f"{p.left.address} = {p.right.address}"
                                for p in j.joinkey_pairs or []
                            ],
                            "condition": str(j.condition) if j.condition else None,
                            "modifiers": [m.value for m in j.modifiers],
                        }
                        if isinstance(j, Join)
                        else {"unnest": str(j.object_to_unnest), "alias": j.alias}
                    )
                    for j in c.joins
                ],
            }
        )
    else:
        out["operator"] = c.operator
    return out
