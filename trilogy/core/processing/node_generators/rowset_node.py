from typing import List

from trilogy.constants import logger
from trilogy.core.enums import Derivation, JoinType
from trilogy.core.exceptions import UnresolvableQueryException
from trilogy.core.models.author import MultiSelectLineage, SelectLineage
from trilogy.core.models.build import (
    BuildConcept,
    BuildRowsetItem,
    BuildRowsetLineage,
    BuildWhereClause,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.node_generators.common import unsatisfied_optionals
from trilogy.core.processing.nodes import (
    History,
    MergeNode,
    RowsetNode,
    StrategyNode,
)
from trilogy.core.processing.utility import concept_to_relevant_joins, padding
from trilogy.utility import unique

LOGGER_PREFIX = "[GEN_ROWSET_NODE]"


def _condition_operands_resolved(
    conditions: BuildWhereClause, node: StrategyNode
) -> bool:
    """True when `node` can produce every row-operand the predicate references
    (directly or via a pseudonym). A cross-rowset merge that drops an operand
    must not have the predicate applied -- the renderer would emit a dangling
    INVALID_REFERENCE_BUG CTE for the missing column."""
    available: set[str] = set()
    for c in node.output_concepts:
        available.add(c.address)
        available.update(c.pseudonyms)
    return all(r.address in available for r in conditions.row_arguments)


def _scoped_joins_for_rowset(
    scoped_joins: list[tuple[str, str, JoinType]],
    derived_concepts: list[str],
) -> list[tuple[str, str, JoinType]]:
    """A query-scoped `join`/`merge` relates the rowset's *output* to an outer
    concept; it must not be applied inside the rowset's own (independent-scope)
    build. Such a join collapses the outer concept onto the rowset output via
    the merge map/pseudonym — so if the rowset's WHERE references that outer
    concept (e.g. a membership existence feeder), sourcing the feeder redirects
    back to the rowset's own output and the rowset depends on itself (infinite
    recursion). Drop any join referencing a concept this rowset derives."""
    derived = set(derived_concepts)
    return [
        (s, t, jt)
        for (s, t, jt) in scoped_joins
        if s not in derived and t not in derived
    ]


def _pseudonym_bridge_keys(
    outputs: List[BuildConcept], environment: BuildEnvironment
) -> list[tuple[BuildConcept, BuildConcept]]:
    """Pair each rowset-derived FK output with the non-rowset (dim) key it was
    merged/joined onto. A query-scoped `join`/`merge` collapses the FK's address
    onto the dim key, leaving the FK only as a non-rowset pseudonym; offering
    that canonical key lets the rowset be enriched from the dim's datasource."""
    pairs: list[tuple[BuildConcept, BuildConcept]] = []
    for fk in outputs:
        if fk.derivation != Derivation.ROWSET:
            continue
        for address in fk.pseudonyms:
            canonical = environment.concepts.get(address)
            if canonical is not None and canonical.derivation != Derivation.ROWSET:
                pairs.append((fk, canonical))
    return pairs


def gen_rowset_node(
    concept: BuildConcept,
    local_optional: List[BuildConcept],
    environment: BuildEnvironment,
    g,
    depth: int,
    source_concepts,
    history: History,
    conditions: BuildWhereClause | None = None,
) -> StrategyNode | None:
    from trilogy.core.query_processor import get_query_node

    if not isinstance(concept.lineage, BuildRowsetItem):
        raise SyntaxError(
            f"Invalid lineage passed into rowset fetch, got {type(concept.lineage)}, expected {BuildRowsetItem}"
        )
    lineage: BuildRowsetItem = concept.lineage
    rowset: BuildRowsetLineage = lineage.rowset
    select: SelectLineage | MultiSelectLineage = lineage.rowset.select

    # Cache the parent select-node by rowset name: when an outer SELECT
    # mixes an aliased rowset concept (BASIC derivation) with bare rowset
    # references, the search loop visits gen_rowset_node twice (once for the
    # rowset priority, once via gen_basic_node's parent re-sourcing). The
    # select's structure is deterministic per rowset, so memoize the
    # pre-mutation node and hand back a copy on cache hit.
    cached = history.rowset_history.get(rowset.name)
    if cached is not None:
        node = cached.copy()
    else:
        # The rowset body's OWN query-scoped joins live on its SelectLineage; feed
        # them to the inner build (like the union-arm path) so the body applies them
        # — otherwise its datasources come back disconnected. Combine with the outer
        # query's joins (filtered to avoid self-referential recursion).
        own_scoped = (
            list(select.scoped_joins) if isinstance(select, SelectLineage) else []
        )
        scoped_joins = (
            _scoped_joins_for_rowset(
                history.build_caches.scoped_joins, lineage.rowset.derived_concepts
            )
            + own_scoped
        )
        node = get_query_node(
            history.base_environment,
            select,
            history=None,
            scoped_joins=scoped_joins or None,
        )
        if node is not None:
            history.rowset_history[rowset.name] = node.copy()

    if not node:
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} Cannot generate parent rowset node for {concept}"
        )
        raise UnresolvableQueryException(
            f"Cannot generate parent select for concept {concept} in rowset {rowset.name}; ensure the rowset is a valid statement."
        )
    enrichment = set([x.address for x in local_optional])

    logger.info(
        f"{padding(depth)}{LOGGER_PREFIX} rowset derived concepts are {lineage.rowset.derived_concepts}"
    )
    concept_pool = list(environment.concepts.values()) + list(
        environment.alias_origin_lookup.values()
    )
    rowset_outputs = [
        x.address for x in concept_pool if x.address in lineage.rowset.derived_concepts
    ]
    rowset_relevant: list[BuildConcept] = [
        v
        for v in concept_pool
        if v.address in rowset_outputs and v.address not in node.hidden_concepts
    ]

    present_map: dict[str, BuildConcept] = {v.address: v for v in rowset_relevant}
    scoped_partial: list[BuildConcept] = []
    for derived_address in lineage.rowset.derived_concepts:
        if derived_address in present_map:
            advertised = present_map[derived_address]
        else:
            collapsed = environment.concepts.get(derived_address)
            if collapsed is None:
                continue
            advertised = collapsed
            if collapsed.address not in present_map:
                rowset_relevant.append(collapsed)
                present_map[collapsed.address] = collapsed
        if derived_address in environment.scoped_partial_sources:
            scoped_partial.append(advertised)

    additional_relevant = [
        environment.concepts[x.address]
        for x in select.output_components
        if x.address in enrichment
    ]
    # Wrap the body node in a translation SelectNode rather than mutating its
    # outputs. The body materializes the rowset-local concepts (`local._rs_*`)
    # against its own scoped-join-collapsed env; keeping it as a parent (with
    # those locals as this node's inputs) preserves their source mapping, so
    # `rs.*` resolves across the query boundary — in particular a collapsed join
    # key whose authored source (e.g. `a.aid`) only exists inside the body as the
    # join canonical (`b.bid`). We can optimize this extra node away later.
    base_node = node
    node = RowsetNode(
        input_concepts=list(
            [
                x
                for x in base_node.output_concepts
                if x.address not in base_node.hidden_concepts
            ]
        ),
        output_concepts=rowset_relevant + additional_relevant,
        environment=environment,
        parents=[base_node],
        depth=depth,
        partial_concepts=list(base_node.partial_concepts),
    )
    if select.where_clause:
        for item in additional_relevant:
            logger.info(
                f"{padding(depth)}{LOGGER_PREFIX} adding {item} to partial concepts"
            )
            node.partial_concepts.append(item)

    existing_partial = {c.address for c in node.partial_concepts}
    for item in scoped_partial:
        if item.address not in existing_partial:
            node.partial_concepts.append(item)
            existing_partial.add(item.address)

    node.rebuild_cache()
    logger.info(
        f"{padding(depth)}{LOGGER_PREFIX} final output is {[x.address for x in node.output_concepts]} with grain {node.grain}"
    )
    # A WHERE pushed up to this rowset can compare its outputs against *other*
    # scoped-joined rowsets (q11/q23 period/channel comparisons). Those operands
    # aren't in this rowset's outputs; source them alongside this rowset's output
    # as one scoped-join merge and apply the predicate to that merge. Returning
    # the bare node would silently drop the filter (or strand it as an
    # unsatisfiable condition upstream). Sourcing without the condition avoids
    # re-entering this branch for each operand rowset.
    if conditions:
        have = {x.address for x in node.output_concepts}
        condition_targets = [
            environment.concepts[r.address]
            for r in conditions.row_arguments
            if r.address not in have and r.address in environment.concepts
        ]
        # Only intercept when an operand lives in ANOTHER scoped rowset (the
        # q11/q23 cross-rowset comparison): those operands can't be pushed down,
        # so we merge the rowsets and apply the predicate post-join. A plain base
        # concept (e.g. an outer `yr=1999` filter feeding an outer aggregate) must
        # instead flow through the normal enrich path so the condition is pushed
        # down into the scan — applying it here would compute the aggregate
        # unfiltered and filter too late.
        if any(t.derivation == Derivation.ROWSET for t in condition_targets):
            # Source the merge against EVERY concept the predicate references, not
            # just the operands missing from this rowset (`condition_targets`). An
            # operand that lives in *this* rowset (e.g. a measure compared against
            # another rowset's measure) is in `node` but NOT in the fresh merge --
            # the merge is sourced anew, it doesn't inherit `node`'s outputs. Omit
            # it and the applied predicate references a column the merge never
            # produced -> dangling INVALID_REFERENCE_BUG CTE (q64).
            merge_inputs = unique(
                [concept] + local_optional + list(conditions.row_arguments),
                "address",
            )
            merged = source_concepts(
                mandatory_list=merge_inputs,
                environment=environment,
                g=g,
                depth=depth + 1,
                conditions=None,
                history=history,
            )
            # Only apply the predicate if the merge actually produced every
            # operand; otherwise fall through rather than emit a dangling CTE.
            if merged and _condition_operands_resolved(conditions, merged):
                merged.add_condition(conditions.conditional)
                # A membership predicate (`x in <set>`) needs its existence set
                # sourced as a parent here too -- otherwise the subselect renders
                # against a dangling CTE (INVALID_REFERENCE_BUG). The normal
                # completion-merge path appends this; this cross-rowset branch
                # short-circuits it, so mirror it explicitly.
                if conditions.existence_arguments:
                    from trilogy.core.processing.concept_strategies_v3 import (
                        append_existence_check,
                    )

                    append_existence_check(merged, environment, g, conditions, history)
                merged.set_preexisting_conditions(conditions.conditional)
                return merged

    remaining = unsatisfied_optionals(local_optional, node)
    if not remaining:
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} no enrichment required for rowset node as all optional {[x.address for x in local_optional]} found or no optional; exiting early."
        )
        return node
    bridge_keys = _pseudonym_bridge_keys(node.output_concepts, environment)
    possible_joins = concept_to_relevant_joins(
        [x for x in node.output_concepts if x.derivation != Derivation.ROWSET]
        + [canonical for _, canonical in bridge_keys]
    )
    if not possible_joins:
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} no possible joins for rowset node to get {[x.address for x in local_optional]}; have {[x.address for x in node.output_concepts]}"
        )
        return node
    if any(x.derivation == Derivation.ROWSET for x in possible_joins):

        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} cannot enrich rowset node with rowset concepts; exiting early"
        )
        return node
    logger.info([x.address for x in possible_joins + local_optional])
    enrich_node: MergeNode = source_concepts(  # this fetches the parent + join keys
        # to then connect to the rest of the query
        mandatory_list=possible_joins + remaining,
        environment=environment,
        g=g,
        depth=depth + 1,
        conditions=conditions,
        history=history,
    )
    if not enrich_node:
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} Cannot generate rowset enrichment node for {concept} with optional {local_optional}, returning just rowset node"
        )
        return node

    for x in possible_joins:
        if x.address in node.hidden_concepts:
            node.unhide_output_concepts([x])
    # keep the bridge FK visible on the rowset side so the merge joins on it
    for fk, _ in bridge_keys:
        if fk.address in node.hidden_concepts:
            node.unhide_output_concepts([fk])
    non_hidden = [
        x for x in node.output_concepts if x.address not in node.hidden_concepts
    ]
    non_hidden_enrich = [
        x
        for x in enrich_node.output_concepts
        if x.address not in enrich_node.hidden_concepts
    ]

    return MergeNode(
        input_concepts=non_hidden + non_hidden_enrich,
        output_concepts=non_hidden + local_optional,
        environment=environment,
        depth=depth,
        parents=[
            node,
            enrich_node,
        ],
        preexisting_conditions=conditions.conditional if conditions else None,
    )
