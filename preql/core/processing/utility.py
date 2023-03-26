from typing import List, Optional, Tuple, Dict, TypedDict, Union

from preql.core.enums import BooleanOperator
from preql.core.graph_models import ReferenceGraph
from preql.core.models import (
    Concept,
    Datasource,
    JoinType,
    BaseJoin,
    Conditional,
    Comparison,
    FilterItem,
    QueryDatasource,
    Grain,
)
from preql.utility import unique


class PathInfo(TypedDict):
    paths: Dict[str, List[str]]
    datasource: Datasource


def path_to_joins(input: List[str], g: ReferenceGraph) -> List[BaseJoin]:
    """ Build joins and ensure any required CTEs are also created/tracked"""
    out = []
    zipped = parse_path_to_matches(input)
    for row in zipped:
        left_ds, right_ds, raw_concepts = row
        concepts = [g.nodes[concept]["concept"] for concept in raw_concepts]
        left_value = g.nodes[left_ds]["datasource"]
        if not right_ds:
            continue
        right_value = g.nodes[right_ds]["datasource"]
        out.append(
            BaseJoin(
                left_datasource=left_value,
                right_datasource=right_value,
                join_type=JoinType.LEFT_OUTER,
                concepts=concepts,
            )
        )
    return out


def parse_path_to_matches(
    input: List[str]
) -> List[Tuple[Optional[str], Optional[str], List[str]]]:
    """Parse a networkx path to a set of join relations"""
    left_ds = None
    right_ds = None
    concept = None
    output: List[Tuple[Optional[str], Optional[str], List[str]]] = []
    while input:
        ds = None
        next = input.pop(0)
        if next.startswith("ds~"):
            ds = next
        elif next.startswith("c~"):
            concept = next
        if ds and not left_ds:
            left_ds = ds
            continue
        elif ds and concept:
            right_ds = ds
            output.append((left_ds, right_ds, [concept]))
            left_ds = right_ds
            concept = None
    if left_ds and concept and not right_ds:
        output.append((left_ds, None, [concept]))
    return output


def concepts_to_inputs(concepts: List[Concept]) -> List[Concept]:
    output = []
    for concept in concepts:
        output += concept_to_inputs(concept)
    return unique(output, hash)


def concept_to_inputs(concept: Concept) -> List[Concept]:
    """Given a concept, return all relevant root inputs"""
    output = []
    if not concept.lineage:
        return [concept]
    for source in concept.sources:
        # if something is a transformation of something with a lineage
        # then we need to persist the original type
        # ex: avg() of sum() @ grain
        output += concept_to_inputs(source.with_default_grain())
    return unique(output, hash)


def concepts_to_conditions(
    concepts: List[Concept]
) -> Optional[Union[Comparison, Conditional]]:
    conditions: List[Union[Comparison, Conditional]] = []
    for concept in concepts:
        if isinstance(concept.lineage, FilterItem):
            conditions.append(concept.lineage.where.conditional)
    if not conditions:
        return None
    base_condition = conditions.pop()
    while conditions:
        condition = conditions.pop()
        base_condition = Conditional(
            left=base_condition, right=condition, operator=BooleanOperator.AND
        )
    return base_condition


from pydantic import BaseModel


class DynamicConditionReturn(BaseModel):
    condition: Union[Comparison, Conditional]
    remove_concept: Concept
    add_concept: Concept


def concepts_to_conditions_mapping(
    concepts: List[Concept]
) -> List[DynamicConditionReturn]:
    conditions: List[DynamicConditionReturn] = []
    for concept in concepts:
        if isinstance(concept.lineage, FilterItem):
            conditions.append(
                DynamicConditionReturn(
                    condition=concept.lineage.where.conditional,
                    remove_concept=concept.lineage.content,
                    add_concept=concept,
                )
            )
    return conditions


def get_nested_source_for_condition(
    base: QueryDatasource,
    conditions: Union[Comparison, Conditional],
    extra_concept: Optional[Concept],
    remove_concepts: Optional[List[Concept]] = None,
) -> QueryDatasource:

    outputs = base.output_concepts
    if extra_concept:
        outputs = base.output_concepts + [extra_concept]
    if remove_concepts:
        outputs = [
            x for x in outputs if x.address not in [z.address for z in remove_concepts]
        ]
    return QueryDatasource(
        input_concepts=base.output_concepts,
        output_concepts=outputs,
        source_map={concept.address: {base} for concept in base.output_concepts},
        datasources=[base],
        grain=Grain(components=outputs),
        joins=[],
        condition=conditions,
    )
