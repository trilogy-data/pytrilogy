from typing import List

from trilogy.constants import logger
from trilogy.core.models.build import (
    BuildConcept,
    BuildGrain,
    BuildUnionSelectLineage,
    BuildWhereClause,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.node_generators.common import (
    gen_enrichment_node,
    unsatisfied_optionals,
)
from trilogy.core.processing.nodes import (
    History,
    MergeNode,
    SelectNode,
    StrategyNode,
    UnionNode,
)
from trilogy.core.processing.utility import create_log_lambda, padding

LOGGER_PREFIX = "[GEN_UNION_SELECT_NODE]"


def gen_union_select_node(
    concept: BuildConcept,
    local_optional: List[BuildConcept],
    environment: BuildEnvironment,
    g,
    depth: int,
    source_concepts,
    history: History,
    conditions: BuildWhereClause | None = None,
) -> StrategyNode | None:
    """Build a relational `union(...)` TVF: a column-positional row stack.

    Sources each arm independently (like the multiselect generator) but stacks
    them with a `UnionNode` (SQL UNION ALL) instead of FULL-joining on align
    keys. Each arm projects its i-th column onto the shared union output so the
    stacked SELECTs line up by column."""
    from trilogy.core.query_processor import get_query_node

    if not isinstance(concept.lineage, BuildUnionSelectLineage):
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} Cannot generate union node for {concept}"
        )
        return None
    lineage: BuildUnionSelectLineage = concept.lineage

    # Canonical output order = align-item order; every arm exposes exactly these.
    ordered_outputs = [
        environment.concepts[item.aligned_concept] for item in lineage.align.items
    ]

    # Each arm applies its own query-scoped joins (carried on the arm lineage),
    # combined with any joins already in scope. Restored after so one arm's joins
    # don't bleed into a sibling.
    caches = history.build_caches
    prior_scoped = list(caches.scoped_joins)

    arm_nodes: List[StrategyNode] = []
    for select in lineage.selects:
        arm_scoped = prior_scoped + list(select.scoped_joins)
        snode: StrategyNode = get_query_node(
            history.base_environment,
            select,
            history,
            scoped_joins=arm_scoped or None,
        )
        if not snode:
            logger.info(
                f"{padding(depth)}{LOGGER_PREFIX} could not source union arm "
                f"{select.output_components}"
            )
            return None
        # Wrap the arm in a rename-only SELECT: it exposes the shared union
        # outputs (sourced from the arm's columns via find_source) plus the arm's
        # own columns (hidden), and does nothing else. Renaming in a separate
        # SELECT — rather than mutating the arm node — keeps the arm's own
        # grain/grouping intact. A GROUP arm would otherwise re-derive its grain
        # from the union outputs and group by its own aggregate; a SELECT node
        # never emits a GROUP BY, so the union output typing can't bleed through.
        arm_cols = list(snode.output_concepts)
        # Expose the union outputs in the canonical align order (identical for
        # every arm), not the arm's own column order — a UNION ALL stacks by
        # position, so an arm that happens to source its columns in a different
        # order must still project them in the shared order. find_source maps each
        # union output back to this arm's matching column via the align clause.
        rename = SelectNode(
            input_concepts=arm_cols,
            output_concepts=arm_cols + list(ordered_outputs),
            environment=environment,
            depth=depth,
            parents=[snode],
            hidden_concepts={c.address for c in arm_cols},
        )
        rename.rebuild_cache()
        arm_nodes.append(rename)
    caches.scoped_joins = prior_scoped

    # Union grain is the stacked output columns; reused by the pass-through wrap.
    union_grain = BuildGrain.from_concepts(ordered_outputs, environment=environment)
    union_node = UnionNode(
        input_concepts=list(ordered_outputs),
        output_concepts=list(ordered_outputs),
        environment=environment,
        parents=arm_nodes,
        preexisting_conditions=conditions.conditional if conditions else None,
        grain=union_grain,
    )

    # Wrap in a pass-through projection so a consumer (e.g. gen_rowset_node) can
    # re-project the outputs onto rowset handles without mutating the UnionNode,
    # whose add_output_concepts propagates to the arm parents (which cannot
    # source the handle addresses).
    node: StrategyNode = MergeNode(
        input_concepts=list(ordered_outputs),
        output_concepts=list(ordered_outputs),
        environment=environment,
        depth=depth,
        parents=[union_node],
        grain=union_grain,
    )

    if not unsatisfied_optionals(local_optional, node):
        return node

    # Enrich via the shared machinery: source the join keys (the union outputs)
    # plus the missing optionals and FULL-merge them back onto the union.
    return gen_enrichment_node(
        node,
        join_keys=list(ordered_outputs),
        local_optional=local_optional,
        environment=environment,
        g=g,
        depth=depth,
        source_concepts=source_concepts,
        log_lambda=create_log_lambda(LOGGER_PREFIX, depth, logger),
        history=history,
        conditions=conditions,
    )
