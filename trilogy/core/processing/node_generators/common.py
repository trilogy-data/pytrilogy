from typing import List, Tuple


from trilogy.core.enums import PurposeLineage, Purpose
from trilogy.core.models import (
    Concept,
    Function,
    AggregateWrapper,
    FilterItem,
    Environment,
    LooseConceptList,
)
from trilogy.utility import unique
from trilogy.core.processing.nodes.base_node import StrategyNode
from trilogy.core.processing.nodes.merge_node import MergeNode
from trilogy.core.processing.nodes import History
from trilogy.core.enums import JoinType
from trilogy.core.processing.nodes import (
    NodeJoin,
)
from collections import defaultdict
from trilogy.core.processing.utility import concept_to_relevant_joins


def resolve_function_parent_concepts(concept: Concept) -> List[Concept]:
    if not isinstance(concept.lineage, (Function, AggregateWrapper)):
        raise ValueError(f"Concept {concept} lineage is not function or aggregate")
    if concept.derivation == PurposeLineage.AGGREGATE:
        if not concept.grain.abstract:
            base = concept.lineage.concept_arguments + concept.grain.components_copy
            # if the base concept being aggregated is a property with a key
            # keep the key as a parent
        else:
            base = concept.lineage.concept_arguments
        if isinstance(concept.lineage, AggregateWrapper):
            # for aggregate wrapper, don't include the by
            extra_property_grain = concept.lineage.function.concept_arguments
        else:
            extra_property_grain = concept.lineage.concept_arguments
        for x in extra_property_grain:
            if isinstance(x, Concept) and x.purpose == Purpose.PROPERTY and x.keys:
                base += x.keys
        return unique(base, "address")
    # TODO: handle basic lineage chains?
    return unique(concept.lineage.concept_arguments, "address")


def resolve_filter_parent_concepts(
    concept: Concept,
) -> Tuple[Concept, List[Concept], List[Concept]]:
    if not isinstance(concept.lineage, FilterItem):
        raise ValueError(
            f"Concept {concept} lineage is not filter item, is {type(concept.lineage)}"
        )
    direct_parent = concept.lineage.content
    base_existence = []
    base_rows = [direct_parent]
    base_rows += concept.lineage.where.row_arguments
    # TODO: pass tuple groups through
    for ctuple in concept.lineage.where.existence_arguments:
        base_existence += list(ctuple)
    if direct_parent.grain:
        base_rows += direct_parent.grain.components_copy
    if (
        isinstance(direct_parent, Concept)
        and direct_parent.purpose == Purpose.PROPERTY
        and direct_parent.keys
    ):
        base_rows += direct_parent.keys
    if concept.lineage.where.existence_arguments:
        return (
            concept.lineage.content,
            unique(base_rows, "address"),
            unique(base_existence, "address"),
        )
    return concept.lineage.content, unique(base_rows, "address"), []


def gen_property_enrichment_node(
    base_node: StrategyNode,
    extra_properties: list[Concept],
    environment: Environment,
    g,
    depth: int,
    source_concepts,
    history: History | None = None,
):
    required_keys: dict[str, set[str]] = defaultdict(set)
    for x in extra_properties:
        if not x.keys:
            raise SyntaxError(f"Property {x.address} missing keys in lookup")
        keys = "-".join([y.address for y in x.keys])
        required_keys[keys].add(x.address)
    final_nodes = []
    node_joins = []
    for _k, vs in required_keys.items():
        ks = _k.split("-")
        enrich_node: StrategyNode = source_concepts(
            mandatory_list=[environment.concepts[k] for k in ks]
            + [environment.concepts[v] for v in vs],
            environment=environment,
            g=g,
            depth=depth + 1,
            history=history,
        )
        final_nodes.append(enrich_node)
        node_joins.append(
            NodeJoin(
                left_node=enrich_node,
                right_node=base_node,
                concepts=concept_to_relevant_joins(
                    [environment.concepts[k] for k in ks]
                ),
                filter_to_mutual=False,
                join_type=JoinType.LEFT_OUTER,
            )
        )
    return MergeNode(
        input_concepts=unique(
            base_node.output_concepts
            + extra_properties
            + [
                environment.concepts[v]
                for k, values in required_keys.items()
                for v in values
            ],
            "address",
        ),
        output_concepts=base_node.output_concepts + extra_properties,
        environment=environment,
        g=g,
        parents=[
            base_node,
            enrich_node,
        ],
        node_joins=node_joins,
    )


def gen_enrichment_node(
    base_node: StrategyNode,
    join_keys: List[Concept],
    local_optional: list[Concept],
    environment: Environment,
    g,
    depth: int,
    source_concepts,
    log_lambda,
    history: History | None = None,
):

    local_opts = LooseConceptList(concepts=local_optional)

    if local_opts.issubset(LooseConceptList(concepts=base_node.output_concepts)):
        log_lambda(
            f"{str(type(base_node).__name__)} has all optional { base_node.output_lcl}, skipping enrichmennt"
        )
        return base_node
    extra_required = [
        x
        for x in local_opts
        if x not in base_node.output_lcl or x in base_node.partial_lcl
    ]

    # property lookup optimization
    # this helps when evaluating a normalized star schema as you only want to lookup the missing properties based on the relevant keys
    if all([x.purpose == Purpose.PROPERTY for x in extra_required]):
        if all(
            x.keys and all([key in base_node.output_lcl for key in x.keys])
            for x in extra_required
        ):
            log_lambda(
                f"{str(type(base_node).__name__)} returning property optimized enrichment node"
            )
            return gen_property_enrichment_node(
                base_node,
                extra_required,
                environment,
                g,
                depth,
                source_concepts,
                history=history,
            )

    enrich_node: StrategyNode = source_concepts(  # this fetches the parent + join keys
        # to then connect to the rest of the query
        mandatory_list=join_keys + extra_required,
        environment=environment,
        g=g,
        depth=depth,
        history=history,
    )
    if not enrich_node:
        log_lambda(
            f"{str(type(base_node).__name__)} enrichment node unresolvable, returning just group node"
        )
        return base_node
    log_lambda(
        f"{str(type(base_node).__name__)} returning merge node with group node + enrichment node"
    )

    return MergeNode(
        input_concepts=unique(
            join_keys + extra_required + base_node.output_concepts, "address"
        ),
        output_concepts=unique(
            join_keys + extra_required + base_node.output_concepts, "address"
        ),
        environment=environment,
        g=g,
        parents=[enrich_node, base_node],
        node_joins=[
            NodeJoin(
                left_node=enrich_node,
                right_node=base_node,
                concepts=concept_to_relevant_joins(
                    [x for x in join_keys if x in enrich_node.output_lcl]
                ),
                filter_to_mutual=False,
                join_type=JoinType.LEFT_OUTER,
            )
        ],
    )


def resolve_join_order(joins: List[NodeJoin]) -> List[NodeJoin]:
    available_aliases: set[StrategyNode] = set()
    final_joins_pre = [*joins]
    final_joins = []
    while final_joins_pre:
        new_final_joins_pre: List[NodeJoin] = []
        for join in final_joins_pre:
            if not available_aliases:
                final_joins.append(join)
                available_aliases.add(join.left_node)
                available_aliases.add(join.right_node)
            elif join.left_node in available_aliases:
                # we don't need to join twice
                # so whatever join we found first, works
                if join.right_node in available_aliases:
                    continue
                final_joins.append(join)
                available_aliases.add(join.left_node)
                available_aliases.add(join.right_node)
            else:
                new_final_joins_pre.append(join)
        if len(new_final_joins_pre) == len(final_joins_pre):
            remaining = [join.left_node for join in new_final_joins_pre]
            remaining_right = [join.right_node for join in new_final_joins_pre]
            raise SyntaxError(
                f"did not find any new joins, available {available_aliases} remaining is {remaining + remaining_right} "
            )
        final_joins_pre = new_final_joins_pre
    return final_joins
