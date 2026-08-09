"""Sourcing the inputs a condition needs, then injecting it at a node.

Used wherever v4 applies a clause over an already-materialized producer — a
rowset boundary, a multiselect's post-join WHERE, a nested select's HAVING.

ROOT forks only the ROW branch (`root._resolve_root_condition_sources`), because
it re-sources from datasources rather than consuming parents and so must widen
the search to grain keys, seed a correlation identity, and carry ancestor atoms.
Existence args are shared outright via `resolve_existence_sources`.

The two were written from one template in June 2026 and drifted three times
before being reconciled: the existence-feeder slice below was added here and
never mirrored (costing an extra pass-through CTE on every ROOT-scan
membership), and both unresolvable-feeder cases were silently skipped there
while raising here. Nothing outside the row branch should diverge again — if a
fix belongs to one path, ask first whether it belongs to both.
"""

from trilogy.core.exceptions import UnresolvableQueryException
from trilogy.core.graph_models import ReferenceGraph
from trilogy.core.models.build import BuildConcept, BuildGrain, BuildWhereClause
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.nodes import StrategyNode
from trilogy.core.processing.v4_helper.condition_injection import (
    ConditionSources,
    inject_condition_at_node,
)
from trilogy.core.processing.v4_helper.history import V4History
from trilogy.utility import unique

from .common import search_parent


def resolve_condition_sources(
    node: StrategyNode,
    condition: BuildWhereClause,
    environment: BuildEnvironment,
    graph: ReferenceGraph,
    history: V4History,
    depth: int,
) -> ConditionSources:
    """Resolve condition row inputs and existence inputs without mixing them."""
    sources = ConditionSources()
    produced_addrs = {o.address for o in node.usable_outputs}
    row_args = unique(
        [c for c in condition.row_arguments if c.address not in produced_addrs],
        "address",
    )
    if row_args:
        feeder = search_parent(row_args, environment, history, graph, depth=depth + 1)
        if feeder is None:
            raise UnresolvableQueryException(
                "Could not resolve condition row arguments "
                f"{[c.address for c in row_args]}"
            )
        # The standalone feeder plan hides its own grain keys at its FINAL
        # layer (non-mandatory there), but hidden outputs are invisible to
        # downstream join inference — the merge back onto `node` degrades to
        # a cartesian. Un-hide any key the consumer also carries so the pair
        # joins keyed (a keyless feeder, e.g. a `by *` global, still
        # cross-joins).
        shared_hidden = {
            o.address for o in feeder.output_concepts if o.address in produced_addrs
        } & set(feeder.hidden_concepts)
        if shared_hidden:
            feeder.hidden_concepts = set(feeder.hidden_concepts) - shared_hidden
            feeder.rebuild_cache()
        sources.row_concepts = row_args
        sources.row_parents.append(feeder)

    resolve_existence_sources(
        sources, condition, environment, graph, history, depth=depth + 1
    )
    return sources


def resolve_existence_sources(
    sources: ConditionSources,
    condition: BuildWhereClause,
    environment: BuildEnvironment,
    graph: ReferenceGraph,
    history: V4History,
    depth: int,
) -> None:
    """Source each existence (`x IN <subselect>`) arg group onto `sources`.

    Shared verbatim by the generic and ROOT paths — an existence feeder is a
    side-channel subselect, so nothing about how the consumer sourced its own
    rows changes how the feeder is built. Only the search `depth` differs.
    """
    seen_existence_addrs: set[str] = set()
    seen_parent_ids: set[int] = set()
    for arg_group in condition.existence_arguments or ():
        # Search the FULL group even when an address was already seen in an
        # earlier group: this group's subselect must project its own columns,
        # and pre-filtering would build a feeder missing one of them.
        existence_args = unique(list(arg_group), "address")
        if not existence_args:
            continue
        ex_node = search_parent(
            existence_args, environment, history, graph, depth=depth
        )
        if ex_node is None:
            raise UnresolvableQueryException(
                "Could not resolve condition existence arguments "
                f"{[c.address for c in existence_args]}"
            )
        # An existence feeder is side-channel-only: slice its outputs to the
        # subselect columns (its mandatory contract). The nested plan can come
        # back carrying its predicate args as extra row outputs (`max_total`
        # for a HAVING membership), and any of those shared with the consumer
        # promotes the feeder to a row-join candidate in MergeNode resolution —
        # a spurious value-join whose grain then leaks the plan-local virt
        # across the rowset boundary. The feeder renders as the bare
        # subselect column; match it.
        existence_addrs = {c.address for c in existence_args}
        sliced = [o for o in ex_node.output_concepts if o.address in existence_addrs]
        if sliced and len(sliced) < len(ex_node.output_concepts):
            ex_node.set_output_concepts(sliced)
        for concept in existence_args:
            if concept.address not in seen_existence_addrs:
                seen_existence_addrs.add(concept.address)
                sources.existence_concepts.append(concept)
        if id(ex_node) not in seen_parent_ids:
            seen_parent_ids.add(id(ex_node))
            sources.existence_parents.append(ex_node)


def resolve_and_inject_condition(
    node: StrategyNode,
    condition: BuildWhereClause,
    output_concepts: list[BuildConcept],
    environment: BuildEnvironment,
    graph: ReferenceGraph,
    history: V4History,
    depth: int,
    *,
    partial_concepts: list[BuildConcept] | None = None,
    grain: BuildGrain | None = None,
    hidden_concepts: set[str] | None = None,
) -> StrategyNode:
    sources = resolve_condition_sources(
        node, condition, environment, graph, history, depth
    )
    return inject_condition_at_node(
        node,
        condition,
        output_concepts,
        environment,
        sources,
        partial_concepts=partial_concepts,
        grain=grain,
        hidden_concepts=hidden_concepts,
    )
