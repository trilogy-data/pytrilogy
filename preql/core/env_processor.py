import uuid
from typing import List, Optional, Union

import networkx as nx

from preql.constants import logger
from preql.core.enums import Purpose
from preql.core.hooks import BaseProcessingHook
from preql.core.models import (
    Concept,
    Environment,
    Select,
    Datasource,
    CTE,
    Join,
    JoinKey,
    ProcessedQuery,
    Grain,
    JoinedDataSource,
    JoinType,
    Address,
)
from preql.utility import string_to_hash
from preql.core.graph_models import ReferenceGraph, concept_to_node, datasource_to_node


def generate_graph(environment: Environment,) -> ReferenceGraph:
    g = ReferenceGraph()
    # statement.input_components, statement.output_components
    for name, concept in environment.concepts.items():
        g.add_node(concept)

        # if we have sources, recursively add them
        if concept.sources:
            node_name = concept_to_node(concept)
            for source in concept.sources:
                g.add_node(source)
                g.add_edge(source, node_name)
    for key, dataset in environment.datasources.items():
        node = datasource_to_node(dataset)
        g.add_node(dataset, type="datasource", datasource=dataset)
        for concept in dataset.concepts:
            g.add_edge(node, concept)
            g.add_edge(concept, node)
            if concept.purpose == Purpose.KEY:
                g.add_edge(concept, concept.with_grain(Grain(components=[concept])))
    return g
