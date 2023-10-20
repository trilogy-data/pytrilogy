from preql.core.models import (
    Concept,
)
from preql.utility import unique
from preql.core.processing.nodes import (
    GroupNode,
)
from preql.core.processing.node_generators.common import (
    resolve_function_parent_concepts,
)


def gen_group_node(
    concept: Concept, local_optional, environment, g, depth, source_concepts
):
    # aggregates MUST always group to the proper grain
    # except when the
    parent_concepts = unique(resolve_function_parent_concepts(concept), "address")

    # if the aggregation has a grain, we need to ensure these are the ONLY optional in the output of the select
    if concept.grain and len(concept.grain.components_copy) > 0:
        local_optional = concept.grain.components_copy
    # otherwise, local optional are mandatory
    else:
        parent_concepts += local_optional
    if parent_concepts:
        parents = [
            source_concepts(parent_concepts, [], environment, g, depth=depth + 1)
        ]
    else:
        parents = []
    return GroupNode(
        [concept],
        local_optional,
        environment,
        g,
        parents=parents,
    )
