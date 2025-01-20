from typing import List

from trilogy.constants import logger
from trilogy.core.models.build import BuildConcept, BuildFunction, BuildWhereClause
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.nodes import (
    GroupNode,
    History,
    MergeNode,
    StrategyNode,
)
from trilogy.core.processing.utility import padding

LOGGER_PREFIX = "[GEN_GROUP_TO_NODE]"


def gen_group_to_node(
    concept: BuildConcept,
    local_optional,
    environment: BuildEnvironment,
    g,
    depth: int,
    source_concepts,
    history: History | None = None,
    conditions: BuildWhereClause | None = None,
) -> GroupNode | MergeNode:
    # aggregates MUST always group to the proper grain
    if not isinstance(concept.lineage, BuildFunction):
        raise SyntaxError(
            f"Group to should have function lineage, is {type(concept.lineage)}"
        )
    group_arg = concept.lineage.arguments[0]
    parent_concepts: List[BuildConcept] = concept.lineage.concept_arguments
    logger.info(
        f"{padding(depth)}{LOGGER_PREFIX} group by node has required parents {[x.address for x in parent_concepts]}"
    )
    parents: List[StrategyNode] = [
        source_concepts(
            mandatory_list=parent_concepts,
            environment=environment,
            g=g,
            depth=depth + 1,
            history=history,
            conditions=conditions,
        )
    ]

    group_node = GroupNode(
        output_concepts=parent_concepts + [concept],
        input_concepts=parent_concepts,
        environment=environment,
        parents=parents,
        depth=depth,
        preexisting_conditions=conditions.conditional if conditions else None,
        hidden_concepts=set(
            [group_arg.address]
            if isinstance(group_arg, BuildConcept)
            and group_arg.address not in local_optional
            else []
        ),
    )

    # early exit if no optional
    if not local_optional:
        return group_node

    # the keys we group by
    # are what we can use for enrichment
    enrich_node = source_concepts(  # this fetches the parent + join keys
        # to then connect to the rest of the query
        mandatory_list=parent_concepts + local_optional,
        environment=environment,
        g=g,
        depth=depth + 1,
        history=history,
        conditions=conditions,
    )
    if not enrich_node:
        logger.info(
            f"{padding(depth)}{LOGGER_PREFIX} group by node enrich node, returning group node only."
        )
        return group_node

    return MergeNode(
        input_concepts=[concept]
        + local_optional
        + [x for x in parent_concepts if x.address != concept.address],
        output_concepts=[concept] + local_optional,
        environment=environment,
        parents=[
            # this node gets the group
            group_node,
            # this node gets enrichment
            enrich_node,
        ],
        whole_grain=True,
        depth=depth,
        preexisting_conditions=conditions.conditional if conditions else None,
    )
