from typing import List

from trilogy.core.models.build import BuildConcept, BuildWhereClause
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.nodes import History, StrategyNode

LOGGER_PREFIX = "[GEN_CONSTANT_NODE]"


def gen_constant_node(
    concept: BuildConcept,
    local_optional: List[BuildConcept],
    environment: BuildEnvironment,
    g,
    depth: int,
    source_concepts,
    history: History | None = None,
    conditions: BuildWhereClause | None = None,
    accept_partial: bool = False,
):
    """our only goal here is to generate a row if conditions exist, or none if they do not"""

    targets = [concept] + local_optional
    if conditions:
        targets += conditions.row_arguments
    parent_node: StrategyNode | None = source_concepts(
        mandatory_list=targets,
        environment=environment,
        g=g,
        depth=depth + 1,
        history=history,
        conditions=conditions,
        accept_partial=accept_partial,
    )
    if not parent_node:
        return None
    parent_node.set_output_concepts([concept] + local_optional)
    return parent_node
