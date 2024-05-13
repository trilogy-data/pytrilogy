from preql.core.models import Concept, Environment
from preql.utility import unique
from preql.core.processing.nodes import GroupNode, StrategyNode, MergeNode, NodeJoin
from typing import List
from preql.core.processing.node_generators.common import (
    resolve_function_parent_concepts,
)
from preql.core.enums import JoinType


def gen_group_to_node(
    concept: Concept,
    local_optional,
    environment: Environment,
    g,
    depth: int,
    source_concepts,
):
    # aggregates MUST always group to the proper grain
    # except when the
    parent_concepts: List[Concept] = unique(
        resolve_function_parent_concepts(concept), "address"
    )

    # if the aggregation has a grain, we need to ensure these are the ONLY optional in the output of the select
    output_concepts = [concept]
    grain_components = [x for x in  (
                concept.grain.components_copy if not concept.grain.abstract else []
            ) if x.address != concept.address]
    
    if concept.grain and len(grain_components) > 0:
        parent_concepts += grain_components
        output_concepts += grain_components

    if parent_concepts:
        parent_concepts = unique([x for x in parent_concepts if x.address != concept.address], "address")
        parents: List[StrategyNode] = [
            source_concepts(
                mandatory_list=parent_concepts,
                environment=environment,
                g=g,
                depth=depth + 1,
            )
        ]
    else:
        parents = []


    group_node = GroupNode(
        output_concepts=output_concepts,
        input_concepts=parent_concepts,
        environment=environment,
        g=g,
        parents=parents,
        depth=depth,
    )

    # early exit if no optional
    if not local_optional:
        return group_node
    
    # the keys we group by
    # are what we can use for enrichment
    group_key_parents = grain_components
    enrich_node = source_concepts(  # this fetches the parent + join keys
        # to then connect to the rest of the query
        mandatory_list=group_key_parents + local_optional,
        environment=environment,
        g=g,
        depth=depth + 1,
    )

    final_output = unique(output_concepts + local_optional +concept.lineage.concept_arguments , 'address')
    return MergeNode(
        input_concepts=output_concepts + local_optional,
        output_concepts=final_output,
        environment=environment,
        g=g,
        parents=[
            # this node gets the group
            group_node,
            # this node gets enrichment
            enrich_node,
        ],
        node_joins=[
            NodeJoin(
                left_node=group_node,
                right_node=enrich_node,
                concepts=group_key_parents,
                filter_to_mutual=False,
                join_type=JoinType.LEFT_OUTER,
            )
        ],
        partial_concepts = concept.lineage.concept_arguments 
    )
