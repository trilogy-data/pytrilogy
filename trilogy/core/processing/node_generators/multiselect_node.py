from collections import defaultdict

from trilogy.constants import logger
from trilogy.core.enums import JoinType, Modifier, Purpose
from trilogy.core.models.build import (
    BuildConcept,
    BuildGrain,
    BuildMultiSelectLineage,
    BuildWhereClause,
    get_concept_arguments,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.models.execute import ConceptPair
from trilogy.core.processing.node_generators.common import (
    gen_enrichment_node,
    unsatisfied_optionals,
)
from trilogy.core.processing.nodes import (
    History,
    MergeNode,
    MultiSelectMergeNode,
    NodeJoin,
)
from trilogy.core.processing.nodes.base_node import StrategyNode
from trilogy.core.processing.utility import (
    concept_to_relevant_joins,
    create_log_lambda,
    padding,
)

LOGGER_PREFIX = "[GEN_MULTISELECT_NODE]"


def extra_align_joins(
    base: BuildMultiSelectLineage,
    environment: BuildEnvironment,
    parents: list[StrategyNode],
) -> list[NodeJoin]:
    """Build the FULL-JOIN chain that aligns multiselect rowset CTEs.

    For N parent CTEs, emit N-1 joins anchored on the first parent. The Nth
    parent's join binds its aligned concepts against EVERY prior parent (not
    just the anchor). This matters for ROLLUP-style cascades where coarser
    levels emit NULLs for some aligned columns: with only the anchor in the
    ON clause, the grand-total CTE's (NULL, NULL) row would `IS NOT DISTINCT
    FROM` the anchor's NULLs and get absorbed into per-channel rows. Binding
    against every prior parent (rendered as `coalesce(prior1, prior2, ...) =
    rightN` by `_build_joinkeys`) keeps the level-N row distinct.
    """
    node_merge_concept_map: dict[StrategyNode, list[BuildConcept]] = defaultdict(list)
    for align in base.align.items:
        jc = environment.concepts[align.aligned_concept]
        if jc.purpose == Purpose.CONSTANT:
            continue
        for node in parents:
            for item in align.concepts:
                if item in node.output_lcl:
                    node_merge_concept_map[node].append(jc)

    relevant = list(node_merge_concept_map.keys())
    if len(relevant) < 2:
        return []

    anchor = relevant[0]
    output: list[NodeJoin] = []
    for i in range(1, len(relevant)):
        right = relevant[i]
        priors = relevant[:i]
        right_concepts = [
            c
            for c in node_merge_concept_map[right]
            if any(c in node_merge_concept_map[p] for p in priors)
        ]
        concept_pairs: list[ConceptPair] = []
        for c in right_concepts:
            for prior in priors:
                if c not in node_merge_concept_map[prior]:
                    continue
                concept_pairs.append(
                    ConceptPair(
                        left=c,
                        right=c,
                        existing_datasource=prior.resolve(),
                        modifiers=[Modifier.NULLABLE],
                    )
                )
        output.append(
            NodeJoin(
                left_node=anchor,
                right_node=right,
                concepts=right_concepts,
                concept_pairs=concept_pairs or None,
                join_type=JoinType.FULL,
                modifiers=[Modifier.NULLABLE],
            )
        )
    return output


def gen_multiselect_node(
    concept: BuildConcept,
    local_optional: list[BuildConcept],
    environment: BuildEnvironment,
    g,
    depth: int,
    source_concepts,
    history: History,
    conditions: BuildWhereClause | None = None,
) -> MergeNode | None:
    from trilogy.core.query_processor import get_query_node

    if not isinstance(concept.lineage, BuildMultiSelectLineage):
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} Cannot generate multiselect node for {concept}"
        )
        return None
    lineage: BuildMultiSelectLineage = concept.lineage

    # Hidden inner-select columns are normally dropped from each branch's output,
    # but the multiselect still consumes some of them internally: align keys (the
    # FULL-JOIN machinery) and derive arguments (referenced by the derived output
    # expressions). If a branch hides one of these, it never reaches the merge
    # node, so the planner recomputes it at the outer level where the source
    # column is out of scope and the renderer emits INVALID_REFERENCE_BUG. Keep
    # them visible on the branch; the outer projection still hides them from the
    # final output via the lineage's hidden_components.
    internal_needed: set[str] = set()
    for align_item in lineage.align.items:
        internal_needed.update(c.address for c in align_item.concepts)
    if lineage.derive:
        for ditem in lineage.derive.items:
            internal_needed.update(a.address for a in get_concept_arguments(ditem.expr))

    base_parents: list[StrategyNode] = []
    partial = []
    for select in lineage.selects:

        snode: StrategyNode = get_query_node(
            history.base_environment, select, None, scoped_joins=select.scoped_joins
        )

        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} Fetched parent node with outputs {select.output_components}"
        )
        if not snode:
            logger.info(
                f"{padding(depth)}{LOGGER_PREFIX} Cannot generate multiselect node for {concept}"
            )
            return None
        merge_concepts: list[BuildConcept] = []
        for x in [*snode.output_concepts]:
            merge_name = lineage.get_merge_concept(x)
            if merge_name:
                merge = environment.concepts[merge_name]
                snode.output_concepts.append(merge)
                merge_concepts.append(merge)
        # keep internally-consumed (align/derive) columns visible to the merge node
        snode.hidden_concepts = {
            h for h in snode.hidden_concepts if h not in internal_needed
        }
        # clear cache so QPS
        snode.rebuild_cache()
        for mc in merge_concepts:
            assert (
                mc.address in snode.resolve().output_concepts
            ), f"missing {mc} in {snode.resolve().output_concepts}"
        base_parents.append(snode)
        if select.where_clause:
            for item in select.output_components:
                partial.append(item)
        logger.info(snode.hidden_concepts)

    node_joins = extra_align_joins(lineage, environment, base_parents)
    merge_concepts_in = [
        x
        for y in base_parents
        for x in y.output_concepts
        if x.address not in y.hidden_concepts
    ]
    logger.info(f"Non-hidden {merge_concepts_in}")
    node = MultiSelectMergeNode(
        input_concepts=list(merge_concepts_in),
        output_concepts=list(merge_concepts_in),
        environment=environment,
        depth=depth,
        parents=base_parents,
        node_joins=node_joins,
        # A multiselect outer is a pure FULL JOIN of already-aggregated arms on
        # the align keys; it must never re-group. The arms can carry hidden
        # derive-arg columns (e.g. a `--date.year as yr_a` consumed only by a
        # `derive coalesce(yr_a, 0)`) whose source key is absent from the align
        # keys. Those inflate the joined pregrain past the align-key grain and
        # would otherwise trigger a spurious GROUP BY that omits the raw
        # aggregate projections, producing invalid SQL.
        whole_grain=True,
    )

    enrichment = set([x.address for x in local_optional])

    multiselect_relevant = [
        environment.concepts[x]
        for x in lineage.derived_concepts
        if x == concept.address or x in enrichment
    ]
    additional_relevant = [x for x in node.output_concepts if x.address in enrichment]
    # add in other other concepts

    node.set_output_concepts(multiselect_relevant + additional_relevant)

    # node.add_partial_concepts(partial)
    # if select.where_clause:
    #     for item in additional_relevant:
    #         node.partial_concepts.append(item)
    node.grain = BuildGrain.from_concepts(node.output_concepts, environment=environment)
    node.rebuild_cache()
    # we need a better API for refreshing a nodes QDS
    possible_joins = concept_to_relevant_joins(additional_relevant)
    if not local_optional:
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} no enrichment required for multiselect node; exiting early"
        )
        return node
    if not possible_joins:
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} no possible joins for multiselect node; exiting early"
        )
        return node
    if not unsatisfied_optionals(local_optional, node):
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} all enriched concepts returned from base multiselect node; exiting early"
        )
        return node
    logger.info(
        f"{padding(depth)}{LOGGER_PREFIX} Missing required concepts {[x for x in local_optional if x.address not in [y.address for y in node.output_concepts]]}"
    )
    return gen_enrichment_node(
        node,
        join_keys=additional_relevant,
        local_optional=local_optional,
        environment=environment,
        g=g,
        depth=depth,
        source_concepts=source_concepts,
        log_lambda=create_log_lambda(LOGGER_PREFIX, depth, logger),
        history=history,
        conditions=conditions,
        partial_concepts=node.partial_concepts,
    )
