from collections import defaultdict
from typing import List, Optional, Union, Tuple, Set, Dict, TypedDict

import networkx as nx

from preql.constants import logger
from preql.core.enums import Purpose, PurposeLineage
from preql.core.env_processor import generate_graph
from preql.core.graph_models import ReferenceGraph, concept_to_node, datasource_to_node
from preql.core.models import (
    Concept,
    Environment,
    Datasource,
    Grain,
    QueryDatasource,
    JoinType,
    BaseJoin,
    Function,
    WindowItem,
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
    return output
