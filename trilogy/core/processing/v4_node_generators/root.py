"""ROOT generator: pick or join datasources for requested concepts."""

from typing import cast

from trilogy.core.enums import BooleanOperator, Derivation, Purpose
from trilogy.core.exceptions import UnresolvableQueryException
from trilogy.core.models.build import BuildConcept, BuildConditional, BuildWhereClause
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.condition_utility import (
    combine_condition_atoms,
    decompose_condition,
)
from trilogy.core.processing.nodes import History, SelectNode, StrategyNode
from trilogy.core.processing.v4_helper.condition_injection import (
    ConditionSources,
    condition_row_args,
    has_existence_args,
    inject_condition_at_node,
    split_existence_atoms,
)
from trilogy.core.processing.v4_helper.history import V4History
from trilogy.core.processing.v4_helper.projection import lineage_existence_only
from trilogy.core.processing.v4_helper.source_planning import SourceRequest, plan_source

from .common import search_parent
from .condition_sources import resolve_existence_sources


def _outputs_with_grain_keys(
    outputs: list[BuildConcept],
    environment: BuildEnvironment,
) -> list[BuildConcept]:
    addresses: set[str] = set()
    for concept in outputs:
        addresses.add(concept.address)
        # A lineage-existence arg (the RHS of `auto flag <- a in b`) feeds the
        # concept through a side-channel subselect, never its row identity —
        # demanding it as a row output here joins an unrelated model into the
        # stream (a disconnected-model cartesian for the derived-membership
        # flag, whose authored keys include the RHS).
        existence = lineage_existence_only(concept)
        if concept.grain is not None:
            addresses.update(set(concept.grain.components) - existence)
        addresses.update((concept.keys or set()) - existence)
    return [
        environment.concepts[address]
        for address in sorted(addresses)
        if address in environment.concepts
    ]


def _condition_source_search_outputs(
    row_args: list[BuildConcept], environment: BuildEnvironment
) -> list[BuildConcept]:
    if _condition_source_uses_aggregate_contract(row_args):
        addresses = {c.address for c in row_args}
        for concept in row_args:
            if concept.grain is not None:
                addresses.update(concept.grain.components)
        return [
            environment.concepts[address]
            for address in sorted(addresses)
            if address in environment.concepts
        ]
    return _outputs_with_grain_keys(row_args, environment)


def _condition_source_uses_aggregate_contract(
    row_args: list[BuildConcept],
) -> bool:
    return bool(row_args) and all(
        concept.derivation == Derivation.AGGREGATE for concept in row_args
    )


def _inheritable_atoms(
    preexisting_conditions: BuildWhereClause | None,
    request: list[BuildConcept],
) -> list[BuildWhereClause]:
    """The ancestor atoms a condition-source sub-search must re-apply.

    ROOT re-sources from datasources rather than from `parents`, so a derived
    row arg it re-plans is rebuilt from unfiltered rows — an atom an ancestor
    group applied is genuinely absent, and dropping it loses the filter
    outright (`where key is not null and sum(x) by key > 0` rebuilds the
    aggregate over NULL keys too, and the NULL group survives the outer join to
    the dimension: tpc-ds q11).

    Only atoms expressible on what is being re-planned may come along. An atom
    over the request's own concepts -- the derived args and their grain keys --
    selects which GROUPS exist and cannot change any group's value. An atom over
    any other row column narrows the aggregate's INPUT, which is exactly the
    scope-narrowing the population/select dual-scope split exists to prevent: a
    population-only `sum(z) by x` gated beside `where f = 1` must still see
    every row (test_where_select_dual_scope).
    """
    if preexisting_conditions is None:
        return []
    available = {concept.address for concept in request}
    available |= {alias for concept in request for alias in concept.pseudonyms}
    keep = [
        atom
        for atom in decompose_condition(preexisting_conditions.conditional)
        if not has_existence_args(atom)
        and all(arg.address in available for arg in atom.row_arguments)
    ]
    if not keep:
        return []
    combined = combine_condition_atoms(keep)
    return [BuildWhereClause(conditional=combined)] if combined is not None else []


def _resolve_root_condition_sources(
    node: StrategyNode,
    conditions: BuildWhereClause,
    environment: BuildEnvironment,
    g,
    history: History,
    preexisting_conditions: BuildWhereClause | None = None,
) -> ConditionSources:
    """ROOT's fork of `condition_sources.resolve_condition_sources`.

    Only the ROW branch forks, and only where re-sourcing from datasources
    demands it: the search is widened to the args' grain keys, seeded with the
    node's own row identity as a correlation, and re-applies the ancestor atoms
    the rows it re-plans never saw (`_inheritable_atoms`). The generic path's
    un-hide step has no analogue because demanding those keys as mandatory
    outputs stops them being hidden in the first place.

    Existence args are NOT forked — they go through the shared
    `resolve_existence_sources`, since a side-channel subselect is built the
    same way regardless of how the consumer sourced its own rows.
    """
    sources = ConditionSources()
    v4_history = cast(V4History, history)
    produced = {concept.address for concept in node.usable_outputs}
    row_args = [
        concept
        for concept in condition_row_args(conditions)
        if concept.address not in produced
    ]
    if row_args:
        row_search = _condition_source_search_outputs(row_args, environment)
        # This source is rejoined to `node` on whatever the two share, so it
        # must carry the node's OWN row identity or the rejoin silently
        # coarsens: `where supplier.nation.region.name = 'EUROPE'` beside an
        # aggregate sourced region at (region, part) grain and rejoined on part
        # alone, asking "does this part have SOME European supplier" instead of
        # "is THIS supplier European". Identity is the node's grain, or the KEY
        # concepts it outputs when it has none yet (a freshly sourced ROOT
        # scan). Best-effort: an identity the row source cannot bind must not
        # cost us the filter entirely, so retry without it.
        seeded = {c.address for c in row_search}
        identity = set(node.grain.components) if node.grain else set()
        identity |= {
            c.address for c in node.output_concepts if c.purpose == Purpose.KEY
        }
        aggregate_only = _condition_source_uses_aggregate_contract(row_args)
        correlation = (
            []
            if aggregate_only
            else [
                environment.concepts[address]
                for address in sorted(identity - seeded)
                if address in environment.concepts
            ]
        )
        inherited = _inheritable_atoms(preexisting_conditions, row_search + correlation)
        row_node = search_parent(
            row_search + correlation,
            environment,
            v4_history,
            g,
            depth=1,
            conditions=inherited,
            # This search rebuilds an aggregate's fact input, where a key
            # carried by the fact is the intended population rather than an
            # incomplete dimension projection.
            complete_partials=not aggregate_only,
        )
        if correlation and row_node is None:
            row_node = search_parent(
                row_search,
                environment,
                v4_history,
                g,
                depth=1,
                conditions=inherited,
                complete_partials=not aggregate_only,
            )
        if row_node is None:
            raise UnresolvableQueryException(
                "Could not resolve condition row arguments "
                f"{[c.address for c in row_args]}"
            )
        sources.row_concepts = row_args
        sources.row_parents.append(row_node)

    resolve_existence_sources(sources, conditions, environment, g, v4_history, depth=1)
    return sources


def _has_upgradable_outer_join(node: StrategyNode, guard_addresses: set[str]) -> bool:
    """Whether the sourced node renders a preserved (outer) join whose
    NULL-padded side carries a guard column — the only case where co-locating
    the rejecting WHERE with the joins lets the join-upgrade pass tighten the
    scan (q70: `state in top_states` over the LEFT-joined store dim). Anywhere
    else the inline form buys nothing and costs a scan-CTE split when the
    unfiltered scan is also consumed elsewhere (q33's size regression)."""
    from trilogy.core.enums import JoinType
    from trilogy.core.models.execute import BaseJoin

    resolved = node.resolve()
    for join in resolved.joins or []:
        if not isinstance(join, BaseJoin):
            continue
        padded = []
        if join.join_type in (JoinType.LEFT_OUTER, JoinType.FULL):
            padded.append(join.right_datasource)
        if join.join_type == JoinType.FULL:
            padded.extend(ds.existing_datasource for ds in join.concept_pairs or [])
        for side in padded:
            if guard_addresses & {c.address for c in side.output_concepts}:
                return True
    return False


def gen_root(
    outputs: list[BuildConcept],
    parents: list[StrategyNode],
    environment: BuildEnvironment,
    conditions: BuildWhereClause | None = None,
    *,
    preexisting_conditions: BuildWhereClause | None = None,
    complete_partials: bool = True,
    history: History,
    g,
) -> StrategyNode | None:
    """Source ROOT concepts through the v4 source planner.

    Existence-bearing atoms (`x IN <subselect>`) are applied in a wrapper so
    the existence feeder remains a side-channel parent rather than being pulled
    into the row stream.
    """
    row_conditions, existence_conditions = split_existence_atoms(conditions)

    inner_outputs: list[BuildConcept] = list(outputs)
    if existence_conditions is not None:
        seen = {c.address for c in inner_outputs}
        for atom in decompose_condition(existence_conditions.conditional):
            for arg in atom.row_arguments:
                if arg.address not in seen:
                    inner_outputs.append(arg)
                    seen.add(arg.address)

    node = plan_source(
        SourceRequest(
            outputs=inner_outputs,
            environment=environment,
            graph=g,
            history=history,
            conditions=row_conditions,
            complete_partials=complete_partials,
        )
    )
    if node is None and conditions is not None:
        fallback_outputs = _outputs_with_grain_keys(inner_outputs, environment)
        node = plan_source(
            SourceRequest(
                outputs=fallback_outputs,
                environment=environment,
                graph=g,
                history=history,
                conditions=None,
                complete_partials=complete_partials,
            )
        )
        if node is None:
            return None
        sources = _resolve_root_condition_sources(
            node,
            conditions,
            environment,
            g,
            history,
            preexisting_conditions,
        )
        hidden = {concept.address for concept in fallback_outputs} - {
            concept.address for concept in outputs
        }
        return inject_condition_at_node(
            node,
            conditions,
            fallback_outputs,
            environment=environment,
            sources=sources,
            input_concepts=list(node.output_concepts) + sources.row_concepts,
            condition_on_merge=bool(sources.row_parents),
            hidden_concepts=hidden or None,
            combine_existing=False,
        )
    if node is None or existence_conditions is None:
        return node

    # Resolve every existence arg's source up front (single- and multi-arg alike).
    # Deferring multi-arg cases to `_attach_existence_sources` left the union-member
    # scans rendering the subselect with no wired source (INVALID_REFERENCE_BUG).
    sources = _resolve_root_condition_sources(
        node, existence_conditions, environment, g, history
    )
    if not sources.row_parents:
        node_addresses = {c.address for c in node.output_concepts}
        feeder_addresses = {
            o.address
            for parent in sources.existence_parents
            for o in parent.output_concepts
        }
        extra = {c.address for c in inner_outputs} - {c.address for c in outputs}
        if (
            not node.force_group
            and not (node_addresses & feeder_addresses)
            and not extra
            and _has_upgradable_outer_join(
                node,
                {
                    arg.address
                    for atom in decompose_condition(existence_conditions.conditional)
                    for arg in atom.row_arguments
                },
            )
        ):
            # Attach the existence gate ON the sourced node, not in a
            # pass-through wrapper CTE: the join-upgrade pass can only prove a
            # preserved dim join INNER when the rejecting WHERE and the join
            # render in the SAME select (q70's `state in top_states` guard must
            # upgrade the nullable store join exactly as the inline form
            # does), and a wrapper hides the joins from the proof. Gated to
            # feeders whose outputs are fully disjoint from the row stream — a
            # shared address makes node resolution treat the feeder as a row
            # parent and fan the scan (q16/q23) — and to memberships whose row
            # args are all demanded outputs (a hidden extra breaks downstream
            # input validation, q23). Copy-first: plan_source results are
            # history-cached and may be shared.
            gated = node.copy()
            gated.conditions = (
                BuildConditional(
                    left=gated.conditions,
                    right=existence_conditions.conditional,
                    operator=BooleanOperator.AND,
                )
                if gated.conditions is not None
                else existence_conditions.conditional
            )
            gated.parents = list(gated.parents) + list(sources.existence_parents)
            gated.add_existence_concepts(sources.existence_concepts, rebuild=False)
            gated.rebuild_cache()
            return gated
        return SelectNode(
            input_concepts=list(node.output_concepts),
            output_concepts=list(outputs),
            environment=environment,
            parents=[node, *sources.existence_parents],
            partial_concepts=list(node.partial_concepts),
            conditions=existence_conditions.conditional,
            existence_concepts=sources.existence_concepts,
        )
    return inject_condition_at_node(
        node,
        existence_conditions,
        list(outputs),
        environment=environment,
        sources=sources,
        input_concepts=list(node.output_concepts),
        combine_existing=False,
    )
