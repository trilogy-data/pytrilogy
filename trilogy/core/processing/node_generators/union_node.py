from typing import List

from trilogy.constants import logger
from trilogy.core.enums import FunctionType, Purpose
from trilogy.core.models.build import BuildConcept, BuildFunction, BuildWhereClause
from trilogy.core.processing.nodes import History, StrategyNode, UnionNode
from trilogy.core.processing.utility import padding

LOGGER_PREFIX = "[GEN_UNION_NODE]"


def is_union(c: BuildConcept):
    return (
        isinstance(c.lineage, BuildFunction)
        and c.lineage.operator == FunctionType.UNION
    )


def gen_union_node(
    concept: BuildConcept,
    local_optional: List[BuildConcept],
    environment,
    g,
    depth: int,
    source_concepts,
    history: History | None = None,
    conditions: BuildWhereClause | None = None,
) -> StrategyNode | None:
    all_unions = [x for x in local_optional if is_union(x)] + [concept]

    parents = []
    keys = [x for x in all_unions if x.purpose == Purpose.KEY]
    base = keys.pop()
    remaining = [x for x in all_unions if x.address != base.address]
    arguments = []
    if isinstance(base.lineage, BuildFunction):
        arguments = base.lineage.concept_arguments
    for arg in arguments:
        relevant_parents: list[BuildConcept] = []
        for other_union in remaining:
            assert other_union.lineage
            potential_parents = [z for z in other_union.lineage.concept_arguments]
            relevant_parents += [
                x for x in potential_parents if x.keys and arg.address in x.keys
            ]
        logger.info(
            f"For parent arg {arg.address}, including additional union inputs {[c.address for c in relevant_parents]}"
        )
        parent: StrategyNode = source_concepts(
            mandatory_list=[arg] + relevant_parents,
            environment=environment,
            g=g,
            depth=depth + 1,
            history=history,
            conditions=conditions,
        )
        parent.hide_output_concepts(parent.output_concepts)
        # parent.remove_output_concepts(parent.output_concepts)
        parent.add_output_concept(concept)
        for x in remaining:
            parent.add_output_concept(x)

        parents.append(parent)
        if not parent:
            logger.info(
                f"{padding(depth)}{LOGGER_PREFIX} could not find union node parents"
            )
            return None

    return UnionNode(
        input_concepts=[concept] + local_optional,
        output_concepts=[concept] + local_optional,
        environment=environment,
        parents=parents,
    )
