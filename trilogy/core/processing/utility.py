from dataclasses import dataclass
from datetime import date, datetime
from enum import Enum
from logging import Logger
from typing import Any, Dict, List, Set, Tuple

import networkx as nx

from trilogy.constants import MagicConstants
from trilogy.core.enums import (
    BooleanOperator,
    DatePart,
    FunctionClass,
    Granularity,
    JoinType,
    Purpose,
)
from trilogy.core.models.build import (
    BuildAggregateWrapper,
    BuildCaseElse,
    BuildCaseWhen,
    BuildComparison,
    BuildConcept,
    BuildConditional,
    BuildDatasource,
    BuildFilterItem,
    BuildFunction,
    BuildParenthetical,
    BuildSubselectComparison,
    BuildWindowItem,
    LooseBuildConceptList,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.models.core import (
    DataType,
    ListType,
    ListWrapper,
    MapType,
    MapWrapper,
    NumericType,
    TraitDataType,
    TupleWrapper,
)
from trilogy.core.models.execute import (
    CTE,
    BaseJoin,
    ConceptPair,
    QueryDatasource,
    UnionCTE,
    UnnestJoin,
)
from trilogy.core.statements.author import MultiSelectStatement, SelectStatement
from trilogy.core.statements.execute import ProcessedQuery
from trilogy.utility import unique

AGGREGATE_TYPES = (BuildAggregateWrapper,)
SUBSELECT_TYPES = (BuildSubselectComparison,)
COMPARISON_TYPES = (BuildComparison,)
FUNCTION_TYPES = (BuildFunction,)
PARENTHETICAL_TYPES = (BuildParenthetical,)
CONDITIONAL_TYPES = (BuildConditional,)
CONCEPT_TYPES = (BuildConcept,)
WINDOW_TYPES = (BuildWindowItem,)


class NodeType(Enum):
    CONCEPT = 1
    NODE = 2


@dataclass
class JoinOrderOutput:
    right: str
    type: JoinType
    keys: dict[str, set[str]]
    left: str | None = None

    @property
    def lefts(self):
        return set(self.keys.keys())


def resolve_join_order_v2(
    g: nx.Graph, partials: dict[str, list[str]]
) -> list[JoinOrderOutput]:
    datasources = [x for x in g.nodes if x.startswith("ds~")]
    concepts = [x for x in g.nodes if x.startswith("c~")]

    output: list[JoinOrderOutput] = []
    pivot_map = {
        concept: [x for x in g.neighbors(concept) if x in datasources]
        for concept in concepts
    }
    pivots = list(
        sorted(
            [x for x in pivot_map if len(pivot_map[x]) > 1],
            key=lambda x: (len(pivot_map[x]), len(x), x),
        )
    )
    solo = [x for x in pivot_map if len(pivot_map[x]) == 1]
    eligible_left = set()

    while pivots:
        next_pivots = [
            x for x in pivots if any(y in eligible_left for y in pivot_map[x])
        ]
        if next_pivots:
            root = next_pivots[0]
            pivots = [x for x in pivots if x != root]
        else:
            root = pivots.pop()

        # sort so less partials is last and eligible lefts are
        def score_key(x: str) -> tuple[int, int, str]:
            base = 1
            # if it's left, higher weight
            if x in eligible_left:
                base += 3
            # if it has the concept as a partial, lower weight
            if root in partials.get(x, []):
                base -= 1
            return (base, len(x), x)

        # get remainig un-joined datasets
        to_join = sorted(
            [x for x in pivot_map[root] if x not in eligible_left], key=score_key
        )
        while to_join:
            # need to sort this to ensure we join on the best match
            base = sorted(
                [x for x in pivot_map[root] if x in eligible_left], key=score_key
            )
            if not base:
                new = to_join.pop()
                eligible_left.add(new)
                base = [new]
            right = to_join.pop()
            # we already joined it
            # this could happen if the same pivot is shared with multiple Dses
            if right in eligible_left:
                continue
            joinkeys: dict[str, set[str]] = {}
            # sorting puts the best candidate last for pop
            # so iterate over the reversed list
            join_types = set()
            for left_candidate in reversed(base):
                common = nx.common_neighbors(g, left_candidate, right)

                if not common:
                    continue
                exists = False
                for _, v in joinkeys.items():
                    if v == common:
                        exists = True
                if exists:
                    continue
                left_is_partial = any(
                    key in partials.get(left_candidate, []) for key in common
                )
                right_is_partial = any(key in partials.get(right, []) for key in common)
                # we don't care if left is nullable for join type (just keys), but if we did
                # ex: left_is_nullable = any(key in partials.get(left_candidate, [])
                right_is_nullable = any(
                    key in partials.get(right, []) for key in common
                )
                if left_is_partial:
                    join_type = JoinType.FULL
                elif right_is_partial or right_is_nullable:
                    join_type = JoinType.LEFT_OUTER
                # we can't inner join if the left was an outer join
                else:
                    join_type = JoinType.INNER
                join_types.add(join_type)
                joinkeys[left_candidate] = common

            final_join_type = JoinType.INNER
            if any([x == JoinType.LEFT_OUTER for x in join_types]):
                final_join_type = JoinType.LEFT_OUTER
            elif any([x == JoinType.FULL for x in join_types]):
                final_join_type = JoinType.FULL
            output.append(
                JoinOrderOutput(
                    # left=left_candidate,
                    right=right,
                    type=final_join_type,
                    keys=joinkeys,
                )
            )
            eligible_left.add(right)

    for concept in solo:
        for ds in pivot_map[concept]:
            # if we already have it, skip it

            if ds in eligible_left:
                continue
            # if we haven't had ANY left datasources yet
            # this needs to become it
            if not eligible_left:
                eligible_left.add(ds)
                continue
            # otherwise do a full out join
            output.append(
                JoinOrderOutput(
                    # pick random one to be left
                    left=list(eligible_left)[0],
                    right=ds,
                    type=JoinType.FULL,
                    keys={},
                )
            )
            eligible_left.add(ds)
    # only once we have all joins
    # do we know if some inners need to be left outers
    for review_join in output:
        if review_join.type in (JoinType.LEFT_OUTER, JoinType.FULL):
            continue
        if any(
            [
                join.right in review_join.lefts
                for join in output
                if join.type in (JoinType.LEFT_OUTER, JoinType.FULL)
            ]
        ):
            review_join.type = JoinType.LEFT_OUTER
    return output


def concept_to_relevant_joins(concepts: list[BuildConcept]) -> List[BuildConcept]:
    sub_props = LooseBuildConceptList(
        concepts=[
            x for x in concepts if x.keys and all([key in concepts for key in x.keys])
        ]
    )
    final = [c for c in concepts if c.address not in sub_props]
    return unique(final, "address")


def padding(x: int) -> str:
    return "\t" * x


def create_log_lambda(prefix: str, depth: int, logger: Logger):
    pad = padding(depth)

    def log_lambda(msg: str):
        logger.info(f"{pad}{prefix} {msg}")

    return log_lambda


def calculate_graph_relevance(
    g: nx.DiGraph, subset_nodes: set[str], concepts: set[BuildConcept]
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


def add_node_join_concept(
    graph: nx.DiGraph,
    concept: BuildConcept,
    concept_map: dict[str, BuildConcept],
    ds_node: str,
    environment: BuildEnvironment,
):
    name = f"c~{concept.address}"
    graph.add_node(name, type=NodeType.CONCEPT)
    graph.add_edge(ds_node, name)
    concept_map[name] = concept
    for v_address in concept.pseudonyms:
        v = environment.alias_origin_lookup.get(
            v_address, environment.concepts[v_address]
        )
        if f"c~{v.address}" in graph.nodes:
            continue
        if v != concept.address:
            add_node_join_concept(
                graph=graph,
                concept=v,
                concept_map=concept_map,
                ds_node=ds_node,
                environment=environment,
            )


def resolve_instantiated_concept(
    concept: BuildConcept, datasource: QueryDatasource | BuildDatasource
) -> BuildConcept:
    if concept.address in datasource.output_concepts:
        return concept
    for k in concept.pseudonyms:
        if k in datasource.output_concepts:
            return [x for x in datasource.output_concepts if x.address == k].pop()
    raise SyntaxError(
        f"Could not find {concept.address} in {datasource.identifier} output {[c.address for c in datasource.output_concepts]}"
    )


def reduce_concept_pairs(input: list[ConceptPair]) -> list[ConceptPair]:
    left_keys = set()
    right_keys = set()
    for pair in input:
        if pair.left.purpose == Purpose.KEY:
            left_keys.add(pair.left.address)
        if pair.right.purpose == Purpose.KEY:
            right_keys.add(pair.right.address)
    final: list[ConceptPair] = []
    for pair in input:
        if (
            pair.left.purpose == Purpose.PROPERTY
            and pair.left.keys
            and pair.left.keys.issubset(left_keys)
        ):
            continue
        if (
            pair.right.purpose == Purpose.PROPERTY
            and pair.right.keys
            and pair.right.keys.issubset(right_keys)
        ):
            continue
        final.append(pair)
    return final


def get_node_joins(
    datasources: List[QueryDatasource | BuildDatasource],
    environment: BuildEnvironment,
    # concepts:List[Concept],
) -> List[BaseJoin]:
    graph = nx.Graph()
    partials: dict[str, list[str]] = {}
    ds_node_map: dict[str, QueryDatasource | BuildDatasource] = {}
    concept_map: dict[str, BuildConcept] = {}
    for datasource in datasources:
        ds_node = f"ds~{datasource.identifier}"
        ds_node_map[ds_node] = datasource
        graph.add_node(ds_node, type=NodeType.NODE)
        partials[ds_node] = [f"c~{c.address}" for c in datasource.partial_concepts]
        for concept in datasource.output_concepts:
            if concept.address in datasource.hidden_concepts:
                continue
            add_node_join_concept(
                graph=graph,
                concept=concept,
                concept_map=concept_map,
                ds_node=ds_node,
                environment=environment,
            )

    joins = resolve_join_order_v2(graph, partials=partials)
    return [
        BaseJoin(
            left_datasource=ds_node_map[j.left] if j.left else None,
            right_datasource=ds_node_map[j.right],
            join_type=j.type,
            # preserve empty field for maps
            concepts=[] if not j.keys else None,
            concept_pairs=reduce_concept_pairs(
                [
                    ConceptPair.model_construct(
                        left=resolve_instantiated_concept(
                            concept_map[concept], ds_node_map[k]
                        ),
                        right=resolve_instantiated_concept(
                            concept_map[concept], ds_node_map[j.right]
                        ),
                        existing_datasource=ds_node_map[k],
                    )
                    for k, v in j.keys.items()
                    for concept in v
                ]
            ),
        )
        for j in joins
    ]


def get_disconnected_components(
    concept_map: Dict[str, Set[BuildConcept]]
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
        | date
        | datetime
        | list[Any]
        | BuildConcept
        | BuildWindowItem
        | BuildFilterItem
        | BuildConditional
        | BuildComparison
        | BuildParenthetical
        | BuildFunction
        | BuildAggregateWrapper
        | BuildCaseWhen
        | BuildCaseElse
        | MagicConstants
        | TraitDataType
        | DataType
        | MapWrapper[Any, Any]
        | ListType
        | MapType
        | NumericType
        | DatePart
        | ListWrapper[Any]
        | TupleWrapper[Any]
    ),
    materialized: set[str] | None = None,
) -> bool:
    if isinstance(element, PARENTHETICAL_TYPES):
        return is_scalar_condition(element.content, materialized)
    elif isinstance(element, SUBSELECT_TYPES):
        return True
    elif isinstance(element, COMPARISON_TYPES):
        return is_scalar_condition(element.left, materialized) and is_scalar_condition(
            element.right, materialized
        )
    elif isinstance(element, FUNCTION_TYPES):
        if element.operator in FunctionClass.AGGREGATE_FUNCTIONS.value:
            return False
        return all([is_scalar_condition(x, materialized) for x in element.arguments])
    elif isinstance(element, CONCEPT_TYPES):
        if materialized and element.address in materialized:
            return True
        if element.lineage and isinstance(element.lineage, AGGREGATE_TYPES):
            return is_scalar_condition(element.lineage, materialized)
        if element.lineage and isinstance(element.lineage, FUNCTION_TYPES):
            return is_scalar_condition(element.lineage, materialized)
        return True
    elif isinstance(element, AGGREGATE_TYPES):
        return is_scalar_condition(element.function, materialized)
    elif isinstance(element, CONDITIONAL_TYPES):
        return is_scalar_condition(element.left, materialized) and is_scalar_condition(
            element.right, materialized
        )
    elif isinstance(element, (BuildCaseWhen,)):
        return is_scalar_condition(
            element.comparison, materialized
        ) and is_scalar_condition(element.expr, materialized)
    elif isinstance(element, (BuildCaseElse,)):
        return is_scalar_condition(element.expr, materialized)
    elif isinstance(element, MagicConstants):
        return True
    return True


CONDITION_TYPES = (
    BuildSubselectComparison,
    BuildComparison,
    BuildConditional,
    BuildParenthetical,
)


def decompose_condition(
    conditional: BuildConditional | BuildComparison | BuildParenthetical,
) -> list[
    BuildSubselectComparison | BuildComparison | BuildConditional | BuildParenthetical
]:
    chunks: list[
        BuildSubselectComparison
        | BuildComparison
        | BuildConditional
        | BuildParenthetical
    ] = []
    if not isinstance(conditional, BuildConditional):
        return [conditional]
    if conditional.operator == BooleanOperator.AND:
        if not (
            isinstance(conditional.left, CONDITION_TYPES)
            and isinstance(
                conditional.right,
                CONDITION_TYPES,
            )
        ):
            chunks.append(conditional)
        else:
            for val in [conditional.left, conditional.right]:
                if isinstance(val, BuildConditional):
                    chunks.extend(decompose_condition(val))
                else:
                    chunks.append(val)
    else:
        chunks.append(conditional)
    return chunks


def find_nullable_concepts(
    source_map: Dict[str, set[BuildDatasource | QueryDatasource | UnnestJoin]],
    datasources: List[BuildDatasource | QueryDatasource],
    joins: List[BaseJoin | UnnestJoin],
) -> List[str]:
    """give a set of datasources and joins, find the concepts
    that may contain nulls in the output set
    """
    nullable_datasources = set()
    datasource_map = {
        x.identifier: x
        for x in datasources
        if isinstance(x, (BuildDatasource, QueryDatasource))
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
            left_check = (
                join.left_datasource.identifier
                if join.left_datasource is not None
                else pair.existing_datasource.identifier
            )
            if pair.left.address in [
                y.address for y in datasource_map[left_check].nullable_concepts
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


def sort_select_output_processed(
    cte: CTE | UnionCTE, query: ProcessedQuery
) -> CTE | UnionCTE:
    output_addresses = [
        c.address for c in query.output_columns if c.address not in query.hidden_columns
    ]

    mapping = {x.address: x for x in cte.output_columns}

    new_output = []
    for x in output_addresses:
        new_output.append(mapping[x])
    cte.output_columns = new_output
    return cte


def sort_select_output(
    cte: CTE | UnionCTE, query: SelectStatement | MultiSelectStatement | ProcessedQuery
) -> CTE | UnionCTE:
    if isinstance(query, ProcessedQuery):
        return sort_select_output_processed(cte, query)
    output_addresses = [
        c.address
        for c in query.output_components
        if c.address not in query.hidden_components
    ]

    mapping = {x.address: x for x in cte.output_columns}

    new_output = []
    for x in output_addresses:
        new_output.append(mapping[x])
    cte.output_columns = new_output
    return cte
