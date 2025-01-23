from collections import defaultdict
from typing import Callable, List, Tuple

from trilogy.core.enums import Derivation, Purpose
from trilogy.core.models.build import (
    BuildAggregateWrapper,
    BuildConcept,
    BuildFilterItem,
    BuildFunction,
    BuildWhereClause,
    LooseBuildConceptList,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.nodes import (
    History,
    NodeJoin,
)
from trilogy.core.processing.nodes.base_node import StrategyNode
from trilogy.core.processing.nodes.merge_node import MergeNode
from trilogy.utility import unique

AGGREGATE_TYPES = (BuildAggregateWrapper,)
FUNCTION_TYPES = (BuildFunction,)


def resolve_function_parent_concepts(
    concept: BuildConcept, environment: BuildEnvironment
) -> List[BuildConcept]:
    if not isinstance(concept.lineage, (*FUNCTION_TYPES, *AGGREGATE_TYPES)):
        raise ValueError(
            f"Concept {concept} lineage is not function or aggregate, is {type(concept.lineage)}"
        )
    if concept.derivation == Derivation.AGGREGATE:
        base: list[BuildConcept] = []
        if not concept.grain.abstract:
            base = concept.lineage.concept_arguments + [
                environment.concepts[c] for c in concept.grain.components
            ]
            # if the base concept being aggregated is a property with a key
            # keep the key as a parent
        else:
            base = concept.lineage.concept_arguments
        if isinstance(concept.lineage, AGGREGATE_TYPES):
            # for aggregate wrapper, don't include the by
            extra_property_grain = concept.lineage.function.concept_arguments
        else:
            extra_property_grain = concept.lineage.concept_arguments
        for x in extra_property_grain:
            if isinstance(x, BuildConcept) and x.purpose == Purpose.PROPERTY and x.keys:
                base += [environment.concepts[c] for c in x.keys]
        return unique(base, "address")
    # TODO: handle basic lineage chains?
    return unique(concept.lineage.concept_arguments, "address")


def resolve_condition_parent_concepts(
    condition: BuildWhereClause,
) -> Tuple[List[BuildConcept], List[Tuple[BuildConcept, ...]]]:
    base_existence = []
    base_rows: list[BuildConcept] = []
    base_rows += condition.row_arguments
    for ctuple in condition.existence_arguments:
        base_existence.append(ctuple)
    return unique(base_rows, "address"), base_existence


def resolve_filter_parent_concepts(
    concept: BuildConcept,
    environment: BuildEnvironment,
) -> Tuple[BuildConcept, List[BuildConcept], List[Tuple[BuildConcept, ...]]]:
    if not isinstance(concept.lineage, (BuildFilterItem,)):
        raise ValueError(
            f"Concept {concept} lineage is not filter item, is {type(concept.lineage)}"
        )
    direct_parent = concept.lineage.content
    base_existence = []
    base_rows = [direct_parent]
    condition_rows, condition_existence = resolve_condition_parent_concepts(
        concept.lineage.where
    )
    base_rows += condition_rows
    base_existence += condition_existence
    # this is required so that
    if (
        isinstance(direct_parent, BuildConcept)
        and direct_parent.purpose in (Purpose.PROPERTY, Purpose.METRIC)
        and direct_parent.keys
    ):
        base_rows += [environment.concepts[c] for c in direct_parent.keys]

    if concept.lineage.where.existence_arguments:
        return (
            concept.lineage.content,
            unique(base_rows, "address"),
            base_existence,
        )
    return concept.lineage.content, unique(base_rows, "address"), []


def gen_property_enrichment_node(
    base_node: StrategyNode,
    extra_properties: list[BuildConcept],
    history: History,
    environment: BuildEnvironment,
    g,
    depth: int,
    source_concepts,
    log_lambda: Callable,
    conditions: BuildWhereClause | None = None,
):
    required_keys: dict[str, set[str]] = defaultdict(set)
    for x in extra_properties:
        if not x.keys:
            raise SyntaxError(f"Property {x.address} missing keys in lookup")
        keys = "-".join([y for y in x.keys])
        required_keys[keys].add(x.address)
    final_nodes = []
    for _k, vs in required_keys.items():
        log_lambda(f"Generating enrichment node for {_k} with {vs}")
        ks = _k.split("-")
        enrich_node: StrategyNode = source_concepts(
            mandatory_list=[environment.concepts[k] for k in ks]
            + [environment.concepts[v] for v in vs],
            environment=environment,
            g=g,
            depth=depth + 1,
            history=history,
            conditions=conditions,
        )
        final_nodes.append(enrich_node)
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
        parents=[
            base_node,
        ]
        + final_nodes,
        preexisting_conditions=conditions.conditional if conditions else None,
    )


def gen_enrichment_node(
    base_node: StrategyNode,
    join_keys: List[BuildConcept],
    local_optional: list[BuildConcept],
    environment: BuildEnvironment,
    g,
    depth: int,
    source_concepts,
    log_lambda,
    history: History,
    conditions: BuildWhereClause | None = None,
):
    local_opts = LooseBuildConceptList(concepts=local_optional)

    extra_required = [
        x
        for x in local_opts
        if x not in base_node.output_lcl or x in base_node.partial_lcl
    ]

    # property lookup optimization
    # this helps create ergonomic merge nodes when evaluating a normalized star schema
    # as we only want to lookup the missing properties based on the relevant keys
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
                environment=environment,
                g=g,
                depth=depth,
                source_concepts=source_concepts,
                history=history,
                conditions=conditions,
                log_lambda=log_lambda,
            )
    log_lambda(
        f"{str(type(base_node).__name__)} searching for join keys {LooseBuildConceptList(concepts=join_keys)} and extra required {local_opts}"
    )
    enrich_node: StrategyNode = source_concepts(  # this fetches the parent + join keys
        # to then connect to the rest of the query
        mandatory_list=join_keys + extra_required,
        environment=environment,
        g=g,
        depth=depth + 1,
        history=history,
        conditions=conditions,
    )
    if not enrich_node:
        log_lambda(
            f"{str(type(base_node).__name__)} enrichment node unresolvable, returning just group node"
        )
        return base_node
    log_lambda(
        f"{str(type(base_node).__name__)} returning merge node with group node + enrichment node"
    )
    non_hidden = [
        x
        for x in base_node.output_concepts
        if x.address not in base_node.hidden_concepts
    ]
    return MergeNode(
        input_concepts=unique(join_keys + extra_required + non_hidden, "address"),
        output_concepts=unique(join_keys + extra_required + non_hidden, "address"),
        environment=environment,
        parents=[enrich_node, base_node],
        force_group=False,
        preexisting_conditions=conditions.conditional if conditions else None,
    )


def resolve_join_order(joins: List[NodeJoin]) -> List[NodeJoin]:
    if not joins:
        return []
    available_aliases: set[StrategyNode] = set()
    final_joins_pre = [*joins]
    final_joins = []
    left = set()
    right = set()
    for join in joins:
        left.add(join.left_node)
        right.add(join.right_node)

    potential_basis = left.difference(right)
    base_candidates = [x for x in final_joins_pre if x.left_node in potential_basis]
    if not base_candidates:
        raise SyntaxError(
            f"Unresolvable join dependencies, left requires {left} and right requires {right}"
        )
    base = base_candidates[0]
    final_joins.append(base)
    available_aliases.add(base.left_node)
    available_aliases.add(base.right_node)
    while final_joins_pre:
        new_final_joins_pre: List[NodeJoin] = []
        for join in final_joins_pre:
            if join.left_node in available_aliases:
                # we don't need to join twice
                # so whatever join we found first, works
                if join.right_node in available_aliases:
                    continue
                final_joins.append(join)
                available_aliases.add(join.left_node)
                available_aliases.add(join.right_node)
            else:
                new_final_joins_pre.append(join)
        final_joins_pre = new_final_joins_pre
    return final_joins
