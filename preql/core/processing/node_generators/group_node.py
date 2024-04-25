from preql.core.models import Concept, Environment
from preql.utility import unique
from preql.core.processing.nodes import GroupNode, StrategyNode
from typing import List
from preql.core.processing.node_generators.common import (
    resolve_function_parent_concepts,
)


def gen_group_node(
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

    if concept.grain and len(concept.grain.components_copy) > 0:
        parent_concepts += concept.grain.components_copy
        output_concepts += concept.grain.components_copy
    # otherwise, local optional can be included
    else:
        parent_concepts += local_optional
        output_concepts += local_optional
    if parent_concepts:
        parents: List[StrategyNode] = [
            source_concepts(parent_concepts, [], environment, g, depth=depth + 1)
        ]
    else:
        parents = []
    partials = []
    for x in output_concepts:
        sources = [p for p in parents if x in p.output_concepts]
        if not sources:
            continue
        if all(x in source.partial_concepts for source in sources):
            partials.append(x)
    # partials = [x for x in output_concepts if all([x in p.partial_concepts for p in parents if x in p.output_concepts])]
    return GroupNode(
        output_concepts=output_concepts,
        input_concepts=parent_concepts,
        environment=environment,
        g=g,
        parents=parents,
        partial_concepts=partials,
        depth=depth,
    )
