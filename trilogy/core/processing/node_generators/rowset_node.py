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
from trilogy.core.processing.nodes import (
    History,
    MergeNode,
    RowsetNode,
    StrategyNode,
)
from trilogy.core.processing.utility import concept_to_relevant_joins, padding

LOGGER_PREFIX = "[GEN_ROWSET_NODE]"


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

    # Memoize the parent select-node by rowset name. The select's structure
    # is deterministic per rowset; consumers downstream wrap this frozen
    # inner node in a RowsetNode boundary.
    cached = history.rowset_history.get(rowset.name)
    if cached is not None:
        inner = cached.copy()
    else:
        inner = get_query_node(history.base_environment, select)
        if inner is not None:
            history.rowset_history[rowset.name] = inner.copy()

    if not inner:
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} Cannot generate parent rowset node for {concept}"
        )
        raise UnresolvableQueryException(
            f"Cannot generate parent select for concept {concept} in rowset {rowset.name}; ensure the rowset is a valid statement."
        )

    enrichment = set([x.address for x in local_optional])
    factory = Factory(environment=history.base_environment, grain=select.grain)
    derived_addresses: set[str] = set(lineage.rowset.derived_concepts)
    # Build the rowset's full output set as one BuildConcept per declared
    # address. ``environment.concepts`` and ``alias_origin_lookup`` may
    # both hold an entry for the same address; the dict-by-address
    # collapses those.
    rowset_relevant_by_address: dict[str, BuildConcept] = {}
    for v in list(environment.concepts.values()) + list(
        environment.alias_origin_lookup.values()
    ):
        if (
            v.address in derived_addresses
            and v.address not in rowset_relevant_by_address
        ):
            rowset_relevant_by_address[v.address] = v
    # Collapse name-level duplicates: when two declared rowset concepts
    # share a ``safe_address`` (e.g. ``dn.customer_id`` and
    # ``dn.customer.id`` both resolve to ``dn_customer_id`` because the
    # namespace path collapses), they would project as duplicate columns
    # in the rowset CTE. Keep one per ``safe_address``; downstream
    # consumers reach the dropped name through the kept concept's
    # keys/pseudonyms.
    rowset_relevant: list[BuildConcept] = []
    seen_safe_addresses: set[str] = set()
    for c in rowset_relevant_by_address.values():
        if c.safe_address in seen_safe_addresses:
            continue
        seen_safe_addresses.add(c.safe_address)
        rowset_relevant.append(c)
    # Concepts that are part of the rowset's source SELECT but aren't
    # declared rowset items — included only when a consumer specifically
    # asks for them (rare; would expand the boundary's identity).
    additional_relevant: list[BuildConcept] = [
        factory.build(x) for x in select.output_components if x.address in enrichment
    ]
    logger.info(
        f"{padding(depth)}{LOGGER_PREFIX} rowset derived concepts are {lineage.rowset.derived_concepts}"
    )

    # Rename the inner node's outputs to the rowset-qualified addresses.
    # The inner select projects its source columns under their unqualified
    # names (e.g. local.x); a rowset declaration aliases them under the
    # rowset's namespace (e.g. my_rowset.x). The RowsetNode boundary above
    # presents only those qualified outputs to downstream consumers, so the
    # inner node must already project them.
    inner.set_output_concepts(rowset_relevant + additional_relevant)
    if select.where_clause:
        for item in additional_relevant:
            inner.partial_concepts.append(item)
    inner.grain = BuildGrain.from_concepts(
        [
            x
            for x in inner.output_concepts
            if x.address
            not in [
                y
                for y in inner.hidden_concepts
                if y in environment.concepts
                and environment.concepts[y].derivation != Derivation.ROWSET
            ]
        ],
    )
    inner.rebuild_cache()

    # The boundary: a typed wrapper exposing the rowset's declared outputs
    # at the rowset's full grain. Consumers may attach BASIC derivations
    # (e.g. ``_virt_filter_*`` CASE wrappers, alias projections) without
    # changing the underlying CTE — RowsetNode.resolve() inlines through to
    # the inner select's QueryDatasource, so no extra CTE layer appears.
    full_outputs = list(rowset_relevant) + list(additional_relevant)
    full_grain = BuildGrain.from_concepts(full_outputs, environment=environment)
    # Only the SELECT's incidental outputs (additional_relevant) need to
    # propagate partiality from the rowset's WHERE — the declared rowset
    # items are themselves the filtered grain, so they are complete.
    partial_for_rowset: list[BuildConcept] = (
        list(additional_relevant) if select.where_clause else []
    )
    rowset_node = RowsetNode(
        rowset_name=rowset.name,
        full_outputs=full_outputs,
        environment=environment,
        parents=[inner],
        depth=depth,
        partial_concepts=partial_for_rowset,
        grain=full_grain,
    )

    logger.info(
        f"{padding(depth)}{LOGGER_PREFIX} rowset boundary outputs {[x.address for x in rowset_node.full_outputs]} grain {rowset_node.grain}"
    )

    # Quick exit when no enrichment is needed (all optional concepts are
    # rowset items already exposed by the boundary).
    full_addresses = {c.address for c in full_outputs}
    if not local_optional or all(
        (x.address in full_addresses or any(z in full_addresses for z in x.pseudonyms))
        for x in local_optional
    ):
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} no enrichment required for rowset node as all optional {[x.address for x in local_optional]} are rowset items; exiting early."
        )
        return rowset_node

    remaining = [x for x in local_optional if x.address not in full_addresses]
    possible_joins = concept_to_relevant_joins(
        [x for x in full_outputs if x.derivation != Derivation.ROWSET]
    )
    if not possible_joins:
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} no possible joins for rowset node to get {[x.address for x in remaining]}; have {[x.address for x in full_outputs]}"
        )
        return rowset_node
    if any(x.derivation == Derivation.ROWSET for x in possible_joins):
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} cannot enrich rowset node with rowset concepts; exiting early"
        )
        return rowset_node
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
        return rowset_node

    non_hidden_rowset = [
        x for x in full_outputs if x.address not in rowset_node.hidden_concepts
    ]
    non_hidden_enrich = [
        x
        for x in enrich_node.output_concepts
        if x.address not in enrich_node.hidden_concepts
    ]
    return MergeNode(
        input_concepts=non_hidden_rowset + non_hidden_enrich,
        output_concepts=non_hidden_rowset + remaining,
        environment=environment,
        depth=depth,
        parents=[
            rowset_node,
            enrich_node,
        ],
        preexisting_conditions=conditions.conditional if conditions else None,
    )
