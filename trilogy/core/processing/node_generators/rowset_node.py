from typing import List

from trilogy.constants import logger
from trilogy.core.enums import Derivation
from trilogy.core.exceptions import UnresolvableQueryException
from trilogy.core.models.author import MultiSelectLineage, SelectLineage
from trilogy.core.models.build import (
    BuildConcept,
    BuildGrain,
    BuildRowsetItem,
    BuildRowsetLineage,
    BuildWhereClause,
    Factory,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.node_generators.common import unsatisfied_optionals
from trilogy.core.processing.nodes import History, MergeNode, StrategyNode
from trilogy.core.processing.utility import concept_to_relevant_joins, padding

LOGGER_PREFIX = "[GEN_ROWSET_NODE]"


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
        node = get_query_node(history.base_environment, select, history)
        if node is not None:
            history.rowset_history[rowset.name] = node.copy()

    if not node:
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} Cannot generate parent rowset node for {concept}"
        )
        raise UnresolvableQueryException(
            f"Cannot generate parent select for concept {concept} in rowset {rowset.name}; ensure the rowset is a valid statement."
        )
    # A LEFT scoped-join key is partial against its anchor; this arm keeps the
    # anchor's rows, so re-enriching the key back to a shared base dimension fans
    # the arm out against that base (a 1=1 cross product). Drop it from the
    # enrichment set — the rowset already outputs the key directly. FULL keys are
    # intentionally NOT dropped: a FULL join must source both sides in full (so a
    # complete dimension on one side contributes all its rows).
    enrichment = set([x.address for x in local_optional])

    factory = Factory(environment=history.base_environment, grain=select.grain)
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
        v for v in concept_pool if v.address in rowset_outputs
    ]
    # A query-scoped `join` collapses one of this rowset's derived keys onto the
    # join's canonical concept. A distinct-base key keeps its own address (with a
    # pseudonym back to the canonical) and is already advertised above. But a key
    # that's a passthrough of a base dimension *shared* with the other rowset has
    # its address rewritten to the canonical, so it no longer matches its own
    # derived address and drops out — leaving the rowset with no join key to merge
    # on. For those, follow the collapse and advertise the canonical concept.
    # `scoped_partial` collects the keys whose join is LEFT (the source side of a
    # `left join`), keyed off the *original* derived address, marking whichever
    # concept actually carries the key.
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
        factory.build(x) for x in select.output_components if x.address in enrichment
    ]
    # add in other other concepts
    node.set_output_concepts(rowset_relevant + additional_relevant)
    if select.where_clause:
        for item in additional_relevant:
            logger.info(
                f"{padding(depth)}{LOGGER_PREFIX} adding {item} to partial concepts"
            )
            node.partial_concepts.append(item)
    # The LEFT-join key is partial against its anchor — neither the column-partial
    # path (no Modifier.PARTIAL on a derived binding) nor get_node_joins
    # (scoped_partial_derived excludes ROWSET keys) marks it. Mark it here so the
    # outer merge keeps the anchor's unmatched rows (LEFT, not INNER).
    existing_partial = {c.address for c in node.partial_concepts}
    for item in scoped_partial:
        if item.address not in existing_partial:
            node.partial_concepts.append(item)
            existing_partial.add(item.address)

    node.grain = BuildGrain.from_concepts(
        [
            x
            for x in node.output_concepts
            if x.address
            not in [
                y
                for y in node.hidden_concepts
                if y in environment.concepts
                and environment.concepts[y].derivation != Derivation.ROWSET
            ]
        ],
    )

    node.rebuild_cache()
    logger.info(
        f"{padding(depth)}{LOGGER_PREFIX} final output is {[x.address for x in node.output_concepts]} with grain {node.grain}"
    )
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
