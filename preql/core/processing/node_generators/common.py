from typing import List


from preql.core.enums import PurposeLineage
from preql.core.models import (
    Concept,
    Function,
    AggregateWrapper,
)
from preql.utility import unique


def resolve_function_parent_concepts(concept: Concept) -> List[Concept]:
    if not isinstance(concept.lineage, (Function, AggregateWrapper)):
        raise ValueError(f"Concept {concept} is not an aggregate function")
    if concept.derivation == PurposeLineage.AGGREGATE:
        if concept.grain:
            return unique(
                concept.lineage.concept_arguments + concept.grain.components_copy,
                "address",
            )
        return concept.lineage.concept_arguments
    # TODO: handle basic lineage chains?

    return unique(concept.lineage.concept_arguments, "address")
