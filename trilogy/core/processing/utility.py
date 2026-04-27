from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from logging import Logger
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from trilogy.core import graph as nx

from trilogy.core.enums import Derivation, Granularity, JoinType, Purpose
from trilogy.core.models.build import (
    BuildConcept,
    BuildDatasource,
    BuildGrain,
    LooseBuildConceptList,
)
from trilogy.core.models.execute import (
    CTE,
    BaseJoin,
    QueryDatasource,
    UnionCTE,
    UnnestJoin,
)
from trilogy.core.statements.author import MultiSelectStatement, SelectStatement
from trilogy.core.statements.execute import ProcessedQuery
from trilogy.utility import unique


class NodeType(Enum):
    CONCEPT = 1
    NODE = 2


@dataclass
class GroupRequiredResponse:
    target: BuildGrain
    upstream: BuildGrain
    required: bool


def padding(x: int) -> str:
    return "\t" * x


def create_log_lambda(prefix: str, depth: int, logger: Logger):
    pad = padding(depth)

    def log_lambda(msg: str):
        logger.info(f"{pad}{prefix} {msg}")

    return log_lambda


def calculate_graph_relevance(
    g: nx.Graph | nx.DiGraph, subset_nodes: set[str], concepts: set[BuildConcept]
) -> int:
    """Calculate the relevance of each node in a graph.
    Relevance is used to prune irrelevant nodes from the graph.
    """
    concept_lookup = {c.address: c for c in concepts}
    relevance = 0
    for node in g.nodes:
        if node not in subset_nodes:
            continue
        if g.nodes[node]["type"] != NodeType.CONCEPT:
            continue
        concept = concept_lookup[node]
        # a single row concept can always be crossjoined
        if concept.granularity == Granularity.SINGLE_ROW:
            continue
        if concept.derivation == Derivation.CONSTANT:
            continue
        # if it's an aggregate up to an arbitrary grain, it can be joined in later
        if concept.purpose == Purpose.METRIC:
            if not concept.grain:
                continue
            if len(concept.grain.components) == 0:
                continue
        if concept.grain and len(concept.grain.components) > 0:
            relevance += 1
            continue
        # Added 2023-10-18 since we seemed to be strangely dropping things
        relevance += 1
    return relevance


def get_disconnected_components(
    concept_map: dict[str, set[BuildConcept]],
) -> tuple[int, list]:
    """Find if any of the datasources are not linked"""
    from trilogy.core import graph as nx

    graph = nx.Graph()
    all_concepts = set()
    for datasource, concepts in concept_map.items():
        graph.add_node(datasource, type=NodeType.NODE)
        for concept in concepts:
            graph.add_node(concept.address, type=NodeType.CONCEPT)
            graph.add_edge(datasource, concept.address)
            all_concepts.add(concept)
    sub_graphs = list(nx.connected_components(graph))
    sub_graphs = [
        x for x in sub_graphs if calculate_graph_relevance(graph, x, all_concepts) > 0
    ]
    return len(sub_graphs), sub_graphs


def find_nullable_concepts(
    source_map: dict[str, set[BuildDatasource | QueryDatasource | UnnestJoin]],
    datasources: list[BuildDatasource | QueryDatasource],
    joins: list[BaseJoin | UnnestJoin],
) -> list[str]:
    """Give a set of datasources and joins, find the concepts
    that may contain nulls in the output set.
    """
    nullable_datasources = set()
    datasource_map = {
        x.identifier: x
        for x in datasources
        if isinstance(x, (BuildDatasource, QueryDatasource))
    }
    # pre-build address sets for O(1) lookup in inner loops
    nullable_addrs: dict[str, set[str]] = {
        ds.identifier: {c.address for c in ds.nullable_concepts}
        for ds in datasources
        if isinstance(ds, (BuildDatasource, QueryDatasource))
    }
    output_addrs: dict[str, set[str]] = {
        ds.identifier: {c.address for c in ds.output_concepts}
        for ds in datasources
        if isinstance(ds, (BuildDatasource, QueryDatasource))
    }
    for join in joins:
        is_on_nullable_condition = False
        if not isinstance(join, BaseJoin):
            continue
        # The JOIN type itself can introduce NULLs. LEFT/RIGHT/FULL outer
        # joins make the corresponding side's concepts nullable in the
        # output, regardless of the source's own nullability.
        if join.join_type in (JoinType.LEFT_OUTER, JoinType.FULL):
            right_ds = datasource_map.get(join.right_datasource.identifier)
            if right_ds is not None:
                nullable_datasources.add(right_ds)
        if (
            join.join_type in (JoinType.RIGHT_OUTER, JoinType.FULL)
            and join.left_datasource is not None
        ):
            left_ds = datasource_map.get(join.left_datasource.identifier)
            if left_ds is not None:
                nullable_datasources.add(left_ds)
        if not join.concept_pairs:
            continue
        right_nullables = nullable_addrs.get(join.right_datasource.identifier, set())
        for pair in join.concept_pairs:
            if pair.right.address in right_nullables:
                is_on_nullable_condition = True
                break
            left_check = (
                join.left_datasource.identifier
                if join.left_datasource is not None
                else pair.existing_datasource.identifier
            )
            if pair.left.address in nullable_addrs.get(left_check, set()):
                is_on_nullable_condition = True
                break
        if is_on_nullable_condition:
            nullable_datasources.add(datasource_map[join.right_datasource.identifier])
    final_nullable = set()

    for k, v in source_map.items():
        local_nullable = [
            x for x in datasources if k in nullable_addrs.get(x.identifier, set())
        ]
        nullable_matches = [
            k in nullable_addrs.get(x.identifier, set())
            for x in datasources
            if k in output_addrs.get(x.identifier, set())
        ]
        if all(nullable_matches) and len(nullable_matches) > 0:
            final_nullable.add(k)
        all_ds = set(local_nullable).union(nullable_datasources)
        if nullable_datasources:
            if set(v).issubset(all_ds):
                final_nullable.add(k)
    return list(sorted(final_nullable))


def concept_to_relevant_joins(concepts: list[BuildConcept]) -> list[BuildConcept]:
    sub_props = LooseBuildConceptList(
        concepts=[
            x for x in concepts if x.keys and all(key in concepts for key in x.keys)
        ]
    )
    final = [c for c in concepts if c.address not in sub_props]
    return unique(final, "address")


def sort_select_output_processed(
    cte: CTE | UnionCTE, query: SelectStatement | MultiSelectStatement | ProcessedQuery
) -> CTE | UnionCTE:
    if isinstance(query, ProcessedQuery):
        targets = query.output_columns
        hidden = query.hidden_columns
    else:
        targets = query.output_components
        hidden = query.hidden_components

    output_addresses = {c.address for c in targets}
    mapping = {x.address: x for x in cte.output_columns}

    new_output: list[BuildConcept] = []
    for x in targets:
        if x.address in mapping:
            new_output.append(mapping[x.address])
        for oc in cte.output_columns:
            if x.address in oc.pseudonyms:
                # create a wrapper BuildConcept to render the pseudonym under the original name
                if any(x.address == y for y in mapping.keys()):
                    continue
                new_output.append(
                    BuildConcept(
                        name=x.name,
                        canonical_name=x.name,
                        namespace=x.namespace,
                        pseudonyms={oc.address},
                        datatype=oc.datatype,
                        purpose=oc.purpose,
                        grain=oc.grain,
                        build_is_aggregate=oc.build_is_aggregate,
                    )
                )
                break

    for oc in cte.output_columns:
        # add hidden back
        if oc.address not in output_addresses:
            new_output.append(oc)

    cte.hidden_concepts = {
        c.address
        for c in cte.output_columns
        if (c.address not in targets or c.address in hidden)
    }
    cte.output_columns = new_output
    return cte


def sort_select_output(
    cte: CTE | UnionCTE, query: SelectStatement | MultiSelectStatement | ProcessedQuery
) -> CTE | UnionCTE:
    return sort_select_output_processed(cte, query)
