from typing import List, Tuple, Dict, Set, Any
import networkx as nx
from trilogy.core.models import (
    Datasource,
    JoinType,
    BaseJoin,
    Concept,
    QueryDatasource,
    LooseConceptList,
    Environment,
    Conditional,
    SubselectComparison,
    Comparison,
    Parenthetical,
    Function,
    FilterItem,
    MagicConstants,
    WindowItem,
    AggregateWrapper,
    DataType,
    ConceptPair,
    UnnestJoin,
    CaseWhen,
    CaseElse,
    MapWrapper,
    ListWrapper,
    MapType,
    DatePart,
    NumericType,
    ListType,
)

from trilogy.core.enums import Purpose, Granularity, BooleanOperator, Modifier
from trilogy.core.constants import CONSTANT_DATASET
from enum import Enum
from trilogy.utility import unique
from collections import defaultdict
from logging import Logger
from pydantic import BaseModel

from trilogy.core.enums import FunctionClass


class NodeType(Enum):
    CONCEPT = 1
    NODE = 2


class PathInfo(BaseModel):
    paths: Dict[str, List[str]]
    datasource: Datasource
    reduced_concepts: Set[str]
    concept_subgraphs: List[List[Concept]]


def concept_to_relevant_joins(concepts: list[Concept]) -> List[Concept]:
    addresses = LooseConceptList(concepts=concepts)
    sub_props = LooseConceptList(
        concepts=[
            x for x in concepts if x.keys and all([key in addresses for key in x.keys])
        ]
    )
    final = [c for c in concepts if c not in sub_props]
    return unique(final, "address")


def padding(x: int) -> str:
    return "\t" * x


def create_log_lambda(prefix: str, depth: int, logger: Logger):
    pad = padding(depth)

    def log_lambda(msg: str):
        logger.info(f"{pad}{prefix} {msg}")

    return log_lambda


def calculate_graph_relevance(
    g: nx.DiGraph, subset_nodes: set[str], concepts: set[Concept]
) -> int:
    """Calculate the relevance of each node in a graph
    Relevance is used to prune irrelevant nodes from the graph
    """
    relevance = 0
    for node in g.nodes:
        if node not in subset_nodes:
            continue
        if not g.nodes[node]["type"] == NodeType.CONCEPT:
            continue
        concept = [x for x in concepts if x.address == node].pop()

        # a single row concept can always be crossjoined
        # therefore a graph with only single row concepts is always relevant
        if concept.granularity == Granularity.SINGLE_ROW:
            continue
        # if it's an aggregate up to an arbitrary grain, it can be joined in later
        # and can be ignored in subgraph
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


def resolve_join_order(joins: List[BaseJoin]) -> List[BaseJoin]:
    available_aliases: set[str] = set()
    final_joins_pre = [*joins]
    final_joins = []
    partial = set()
    while final_joins_pre:
        new_final_joins_pre: List[BaseJoin] = []
        for join in final_joins_pre:
            if join.join_type != JoinType.INNER:
                partial.add(join.right_datasource.identifier)
            # an inner join after a left outer implicitly makes that outer an inner
            # so fix that
            if (
                join.left_datasource.identifier in partial
                and join.join_type == JoinType.INNER
            ):
                join.join_type = JoinType.LEFT_OUTER
            if not available_aliases:
                final_joins.append(join)
                available_aliases.add(join.left_datasource.identifier)
                available_aliases.add(join.right_datasource.identifier)
            elif join.left_datasource.identifier in available_aliases:
                # we don't need to join twice
                # so whatever join we found first, works
                if join.right_datasource.identifier in available_aliases:
                    continue
                final_joins.append(join)
                available_aliases.add(join.left_datasource.identifier)
                available_aliases.add(join.right_datasource.identifier)
            else:
                new_final_joins_pre.append(join)
        if len(new_final_joins_pre) == len(final_joins_pre):
            remaining = [
                join.left_datasource.identifier for join in new_final_joins_pre
            ]
            remaining_right = [
                join.right_datasource.identifier for join in new_final_joins_pre
            ]
            raise SyntaxError(
                f"did not find any new joins, available {available_aliases} remaining is {remaining + remaining_right} "
            )
        final_joins_pre = new_final_joins_pre
    return final_joins


def add_node_join_concept(
    graph: nx.DiGraph,
    concept: Concept,
    datasource: Datasource | QueryDatasource,
    concepts: List[Concept],
):

    concepts.append(concept)

    graph.add_node(concept.address, type=NodeType.CONCEPT)
    graph.add_edge(datasource.identifier, concept.address)
    for _, v in concept.pseudonyms.items():
        if v in concepts:
            continue
        if v.address != concept.address:
            add_node_join_concept(graph, v, datasource, concepts)


def get_node_joins(
    datasources: List[QueryDatasource],
    grain: List[Concept],
    environment: Environment,
    # concepts:List[Concept],
) -> List[BaseJoin]:
    graph = nx.Graph()
    concepts: List[Concept] = []
    for datasource in datasources:
        graph.add_node(datasource.identifier, type=NodeType.NODE)
        for concept in datasource.output_concepts:
            add_node_join_concept(graph, concept, datasource, concepts)

    # add edges for every constant to every datasource
    for datasource in datasources:
        for concept in datasource.output_concepts:
            if concept.granularity == Granularity.SINGLE_ROW:
                for node in graph.nodes:
                    if graph.nodes[node]["type"] == NodeType.NODE:
                        graph.add_edge(node, concept.address)

    joins: defaultdict[str, set] = defaultdict(set)
    identifier_map: dict[str, Datasource | QueryDatasource] = {
        x.identifier: x for x in datasources
    }
    grain_pseudonyms: set[str] = set()
    for g in grain:
        env_lookup = environment.concepts[g.address]
        # if we're looking up a pseudonym, we would have gotten the remapped value
        # so double check we got what we were looking for
        if env_lookup.address == g.address:
            grain_pseudonyms.update(env_lookup.pseudonyms.keys())

    node_list = sorted(
        [x for x in graph.nodes if graph.nodes[x]["type"] == NodeType.NODE],
        # sort so that anything with a partial match on the target is later
        key=lambda x: len(
            [
                partial
                for partial in identifier_map[x].partial_concepts
                if partial in grain
            ]
            + [
                output
                for output in identifier_map[x].output_concepts
                if output.address in grain_pseudonyms
            ]
        ),
    )

    for left in node_list:
        # the constant dataset is a special case
        # and can never be on the left of a join
        if left == CONSTANT_DATASET:
            continue

        for cnode in graph.neighbors(left):
            if graph.nodes[cnode]["type"] == NodeType.CONCEPT:
                for right in graph.neighbors(cnode):
                    # skip concepts
                    if graph.nodes[right]["type"] == NodeType.CONCEPT:
                        continue
                    if left == right:
                        continue
                    identifier = [left, right]
                    joins["-".join(identifier)].add(cnode)

    final_joins_pre: List[BaseJoin] = []

    for key, join_concepts in joins.items():
        left, right = key.split("-")
        local_concepts: List[Concept] = unique(
            [c for c in concepts if c.address in join_concepts], "address"
        )
        if all([c.granularity == Granularity.SINGLE_ROW for c in local_concepts]):
            # for the constant join, make it a full outer join on 1=1
            join_type = JoinType.FULL
            local_concepts = []
        elif any(
            [
                c.address in [x.address for x in identifier_map[left].partial_concepts]
                for c in local_concepts
            ]
        ):
            join_type = JoinType.FULL
            local_concepts = [
                c for c in local_concepts if c.granularity != Granularity.SINGLE_ROW
            ]
        elif any(
            [
                c.address in [x.address for x in identifier_map[right].partial_concepts]
                for c in local_concepts
            ]
        ) or any(
            [
                c.address in [x.address for x in identifier_map[left].nullable_concepts]
                for c in local_concepts
            ]
        ):
            join_type = JoinType.LEFT_OUTER
            local_concepts = [
                c for c in local_concepts if c.granularity != Granularity.SINGLE_ROW
            ]
        else:
            join_type = JoinType.INNER
            # remove any constants if other join keys exist
            local_concepts = [
                c for c in local_concepts if c.granularity != Granularity.SINGLE_ROW
            ]

        relevant = concept_to_relevant_joins(local_concepts)
        left_datasource = identifier_map[left]
        right_datasource = identifier_map[right]
        join_tuples: list[ConceptPair] = []
        for joinc in relevant:
            left_arg = joinc
            right_arg = joinc
            if joinc.address not in [
                c.address for c in left_datasource.output_concepts
            ]:
                try:
                    left_arg = [
                        x
                        for x in left_datasource.output_concepts
                        if x.address in joinc.pseudonyms
                        or joinc.address in x.pseudonyms
                    ].pop()
                except IndexError:
                    raise SyntaxError(
                        f"Could not find {joinc.address} in {left_datasource.identifier} output {[c.address for c in left_datasource.output_concepts]}"
                    )
            if joinc.address not in [
                c.address for c in right_datasource.output_concepts
            ]:
                try:
                    right_arg = [
                        x
                        for x in right_datasource.output_concepts
                        if x.address in joinc.pseudonyms
                        or joinc.address in x.pseudonyms
                    ].pop()
                except IndexError:
                    raise SyntaxError(
                        f"Could not find {joinc.address} in {right_datasource.identifier} output {[c.address for c in right_datasource.output_concepts]}"
                    )
            narg = (left_arg, right_arg)
            if narg not in join_tuples:
                modifiers = set()
                if left_arg.address in [
                    x.address for x in left_datasource.nullable_concepts
                ] and right_arg.address in [
                    x.address for x in right_datasource.nullable_concepts
                ]:
                    modifiers.add(Modifier.NULLABLE)
                join_tuples.append(
                    ConceptPair(
                        left=left_arg, right=right_arg, modifiers=list(modifiers)
                    )
                )

        # deduplication
        all_right = []
        for tuple in join_tuples:
            all_right.append(tuple.right.address)
        right_grain = identifier_map[right].grain
        # if the join includes all the right grain components
        # we only need to join on those, not everything
        if all([x.address in all_right for x in right_grain.components]):
            join_tuples = [
                x for x in join_tuples if x.right.address in right_grain.components
            ]

        final_joins_pre.append(
            BaseJoin(
                left_datasource=identifier_map[left],
                right_datasource=identifier_map[right],
                join_type=join_type,
                concepts=[],
                concept_pairs=join_tuples,
            )
        )
    final_joins = resolve_join_order(final_joins_pre)

    # this is extra validation
    non_single_row_ds = [x for x in datasources if not x.grain.abstract]
    if len(non_single_row_ds) > 1:
        for x in datasources:
            if x.grain.abstract:
                continue
            found = False
            for join in final_joins:
                if (
                    join.left_datasource.identifier == x.identifier
                    or join.right_datasource.identifier == x.identifier
                ):
                    found = True
            if not found:
                raise SyntaxError(
                    f"Could not find join for {x.identifier} with output {[c.address for c in x.output_concepts]}, all {[z.identifier for z in datasources]}"
                )
    return final_joins


def get_disconnected_components(
    concept_map: Dict[str, Set[Concept]]
) -> Tuple[int, List]:
    """Find if any of the datasources are not linked"""
    import networkx as nx

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


def is_scalar_condition(
    element: (
        int
        | str
        | float
        | list[Any]
        | WindowItem
        | FilterItem
        | Concept
        | Comparison
        | Conditional
        | Parenthetical
        | Function
        | AggregateWrapper
        | MagicConstants
        | DataType
        | CaseWhen
        | CaseElse
        | MapWrapper[Any, Any]
        | ListType
        | MapType
        | NumericType
        | DatePart
        | ListWrapper[Any]
    ),
    materialized: set[str] | None = None,
) -> bool:
    if isinstance(element, Parenthetical):
        return is_scalar_condition(element.content, materialized)
    elif isinstance(element, SubselectComparison):
        return True
    elif isinstance(element, Comparison):
        return is_scalar_condition(element.left, materialized) and is_scalar_condition(
            element.right, materialized
        )
    elif isinstance(element, Function):
        if element.operator in FunctionClass.AGGREGATE_FUNCTIONS.value:
            return False
        return all([is_scalar_condition(x, materialized) for x in element.arguments])
    elif isinstance(element, Concept):
        if materialized and element.address in materialized:
            return True
        if element.lineage and isinstance(element.lineage, AggregateWrapper):
            return is_scalar_condition(element.lineage, materialized)
        return True
    elif isinstance(element, AggregateWrapper):
        return is_scalar_condition(element.function, materialized)
    elif isinstance(element, Conditional):
        return is_scalar_condition(element.left, materialized) and is_scalar_condition(
            element.right, materialized
        )
    elif isinstance(element, CaseWhen):
        return is_scalar_condition(
            element.comparison, materialized
        ) and is_scalar_condition(element.expr, materialized)
    elif isinstance(element, CaseElse):
        return is_scalar_condition(element.expr, materialized)
    elif isinstance(element, MagicConstants):
        return True
    return True


def decompose_condition(
    conditional: Conditional | Comparison | Parenthetical,
) -> list[SubselectComparison | Comparison | Conditional | Parenthetical]:
    chunks: list[SubselectComparison | Comparison | Conditional | Parenthetical] = []
    if not isinstance(conditional, Conditional):
        return [conditional]
    if conditional.operator == BooleanOperator.AND:
        if not (
            isinstance(
                conditional.left,
                (SubselectComparison, Comparison, Conditional, Parenthetical),
            )
            and isinstance(
                conditional.right,
                (SubselectComparison, Comparison, Conditional, Parenthetical),
            )
        ):
            chunks.append(conditional)
        else:
            for val in [conditional.left, conditional.right]:
                if isinstance(val, Conditional):
                    chunks.extend(decompose_condition(val))
                else:
                    chunks.append(val)
    else:
        chunks.append(conditional)
    return chunks


def find_nullable_concepts(
    source_map: Dict[str, set[Datasource | QueryDatasource | UnnestJoin]],
    datasources: List[Datasource | QueryDatasource],
    joins: List[BaseJoin | UnnestJoin],
) -> List[str]:
    """give a set of datasources and joins, find the concepts
    that may contain nulls in the output set
    """
    nullable_datasources = set()
    datasource_map = {
        x.identifier: x
        for x in datasources
        if isinstance(x, (Datasource, QueryDatasource))
    }
    for join in joins:
        is_on_nullable_condition = False
        if not isinstance(join, BaseJoin):
            continue
        if not join.concept_pairs:
            continue
        for pair in join.concept_pairs:
            if pair.right.address in [
                y.address
                for y in datasource_map[
                    join.right_datasource.identifier
                ].nullable_concepts
            ]:
                is_on_nullable_condition = True
                break
            if pair.left.address in [
                y.address
                for y in datasource_map[
                    join.left_datasource.identifier
                ].nullable_concepts
            ]:
                is_on_nullable_condition = True
                break
        if is_on_nullable_condition:
            nullable_datasources.add(datasource_map[join.right_datasource.identifier])
    final_nullable = set()

    for k, v in source_map.items():
        local_nullable = [
            x for x in datasources if k in [v.address for v in x.nullable_concepts]
        ]
        if all(
            [
                k in [v.address for v in x.nullable_concepts]
                for x in datasources
                if k in [z.address for z in x.output_concepts]
            ]
        ):
            final_nullable.add(k)
        all_ds = set([ds for ds in local_nullable]).union(nullable_datasources)
        if nullable_datasources:
            if set(v).issubset(all_ds):
                final_nullable.add(k)
    return list(sorted(final_nullable))
