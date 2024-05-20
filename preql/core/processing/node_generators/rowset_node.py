from preql.core.models import Concept, Environment, Select, RowsetDerivation, RowsetItem
from preql.utility import unique
from preql.core.processing.nodes import GroupNode, StrategyNode, MergeNode, NodeJoin
from typing import List
from preql.core.processing.node_generators.common import (
    resolve_function_parent_concepts,
)

from preql.core.enums import JoinType
from preql.constants import logger

LOGGER_PREFIX = "[GEN_ROWSET_NODE]"


def padding(x: int):
    return "\t" * x


def gen_rowset_node(
    concept: Concept,
    local_optional: List[Concept],
    environment: Environment,
    g,
    depth: int,
    source_concepts,
) -> MergeNode:
    lineage: RowsetItem = concept.lineage
    rowset: RowsetDerivation = lineage.rowset
    select: Select = lineage.rowset.select
    node: MergeNode = source_concepts(
        mandatory_list=select.output_components,
        environment=environment,
        g=g,
        depth=depth + 1,
    )
    if select.where_clause:
        node.conditions = select.where_clause.conditional
    # add in other other concepts
    for item in rowset.derived_concepts:
        node.output_concepts.append(item)
    if select.where_clause:
        for item in select.output_components:
            node.partial_concepts.append(item)
    # we need a better API for this
    node.resolution_cache = node._resolve()
    return node
