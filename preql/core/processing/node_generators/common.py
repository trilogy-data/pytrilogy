from typing import List, Tuple


from preql.core.enums import PurposeLineage, Purpose
from preql.core.models import (
    Concept,
    Function,
    AggregateWrapper,
    FilterItem,
    Environment,
)
from preql.utility import unique
from preql.core.processing.nodes.base_node import StrategyNode
from preql.core.processing.nodes.merge_node import MergeNode
from preql.core.enums import JoinType
from preql.core.processing.nodes import (
    NodeJoin,
)
from collections import defaultdict


def resolve_function_parent_concepts(concept: Concept) -> List[Concept]:
    if not isinstance(concept.lineage, (Function, AggregateWrapper)):
        raise ValueError(f"Concept {concept} lineage is not function or aggregate")
    if concept.derivation == PurposeLineage.AGGREGATE:
        if concept.grain:
            return unique(
                concept.lineage.concept_arguments + concept.grain.components_copy,
                "address",
            )
        return concept.lineage.concept_arguments
    # TODO: handle basic lineage chains?

    return unique(concept.lineage.concept_arguments, "address")


def resolve_filter_parent_concepts(concept: Concept) -> Tuple[Concept, List[Concept]]:
    if not isinstance(concept.lineage, FilterItem):
        raise ValueError
    base = [concept.lineage.content]
    base += concept.lineage.where.concept_arguments
    return concept.lineage.content, unique(base, "address")


def concept_to_relevant_joins(concepts: list[Concept]) -> List[Concept]:
    addresses = [x.address for x in concepts]
    sub_props = [
        x.address
        for x in concepts
        if x.keys and all([key.address in addresses for key in x.keys])
    ]
    final = [c for c in concepts if c.address not in sub_props]
    return final


def gen_property_enrichment_node(
    base_node: StrategyNode,
    extra_properties: list[Concept],
    environment: Environment,
    g,
    depth: int,
    source_concepts,
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
            + [environment.concepts[k] for k in required_keys],
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
):

    if set([x.address for x in local_optional]).issubset(
        set([y.address for y in base_node.output_concepts])
    ):
        log_lambda(
            f"{str(type(base_node).__name__)} has all optional {[x.address for x in base_node.output_concepts]}, skipping enrichmennt"
        )
        return base_node
    extra_required = [
        x
        for x in local_optional
        if x.address not in [y.address for y in base_node.output_concepts]
        or x.address in [y.address for y in base_node.partial_concepts]
    ]

    # property lookup optimization
    # this helps when evaluating a normalized star schema as you only want to lookup the missing properties based on the relevant keys
    if all([x.purpose == Purpose.PROPERTY for x in extra_required]):
        if all(
            x.keys
            and all(
                [
                    key.address in [y.address for y in base_node.output_concepts]
                    for key in x.keys
                ]
            )
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
            )

    enrich_node: StrategyNode = source_concepts(  # this fetches the parent + join keys
        # to then connect to the rest of the query
        mandatory_list=join_keys + extra_required,
        environment=environment,
        g=g,
        depth=depth,
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
                    [
                        x
                        for x in join_keys
                        if x.address in [y.address for y in enrich_node.output_concepts]
                    ]
                ),
                filter_to_mutual=False,
                join_type=JoinType.LEFT_OUTER,
            )
        ],
    )
