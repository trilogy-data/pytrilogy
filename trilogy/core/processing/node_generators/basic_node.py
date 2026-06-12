from typing import List

from trilogy.constants import logger
from trilogy.core.enums import FunctionClass, FunctionType, SourceType
from trilogy.core.models.build import (
    BuildConcept,
    BuildConceptArgs,
    BuildFunction,
    BuildWhereClause,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.discovery_utility import get_upstream_concepts
from trilogy.core.processing.node_generators.common import (
    resolve_function_parent_concepts,
)
from trilogy.core.processing.nodes import ConstantNode, History, StrategyNode
from trilogy.utility import unique

LOGGER_PREFIX = "[GEN_BASIC_NODE]"


def is_equivalent_basic_function_lineage(
    x: BuildConcept,
    y: BuildConcept,
):
    if not isinstance(x.lineage, BuildFunction) or not isinstance(
        y.lineage, BuildFunction
    ):
        return False
    if x.lineage.operator == y.lineage.operator == FunctionType.ATTR_ACCESS:
        return x.lineage.concept_arguments == y.lineage.concept_arguments
    if x.lineage.operator == y.lineage.operator:
        return True
    if (
        y.lineage.operator in FunctionClass.AGGREGATE_FUNCTIONS.value
        or y.lineage.operator in FunctionClass.ONE_TO_MANY.value
    ):
        return False
    return True


def gen_basic_node(
    concept: BuildConcept,
    local_optional: List[BuildConcept],
    environment: BuildEnvironment,
    g,
    depth: int,
    source_concepts,
    history: History | None = None,
    conditions: BuildWhereClause | None = None,
):
    depth_prefix = "\t" * depth
    # A BASIC concept that is *also* directly bound to a datasource column
    # has two valid source paths: decompose the lineage, or source the
    # column directly. Prefer the column when the binding exists — the bound
    # value can diverge from the formula (e.g. TPC-DS CS_EXT_SALES_PRICE is
    # stored even on rows where CS_SALES_PRICE or CS_QUANTITY is NULL, so
    # sum(formula) drops those rows while sum(binding) doesn't). Replacing
    # the formula parents with a ROOT clone of the concept reroutes the
    # recursive source_concepts call through the ROOT handler, which picks
    # up the binding.
    if concept.canonical_address in environment.materialized_canonical_concepts:
        parent_concepts = [concept.with_materialized_source()]
    else:
        parent_concepts = resolve_function_parent_concepts(
            concept, environment=environment
        )

    # A membership comparison (`a in b`) over otherwise-unconnected models
    # carries existence semantics: the RHS is checked via an existence
    # subquery, not joined. resolve_function_parent_concepts returns both
    # sides as row parents, which would (wrongly) demand a join. When such a
    # comparison is itself a derived concept (auto / projected), split the
    # existence side out and wire it as an existence parent, mirroring the
    # inline-WHERE path in append_existence_check.
    existence_concepts: list[BuildConcept] = []
    if isinstance(concept.lineage, BuildConceptArgs):
        existence_concepts = unique(
            [c for tup in concept.lineage.existence_arguments for c in tup],
            "address",
        )
    if existence_concepts:
        existence_addresses = {c.address for c in existence_concepts}
        parent_concepts = [
            p for p in parent_concepts if p.address not in existence_addresses
        ]

    logger.info(
        f"{depth_prefix}{LOGGER_PREFIX} basic node for {concept} with lineage {concept.lineage} has parents {[x for x in parent_concepts]}"
    )
    synonyms: list[BuildConcept] = []
    ignored_optional: set[str] = set()

    # when we are getting an attribute, if there is anything else
    # that is an attribute of the same struct in local optional
    # select that value for discovery as well
    if (
        isinstance(concept.lineage, BuildFunction)
        and concept.lineage.operator == FunctionType.ATTR_ACCESS
    ):
        logger.info(
            f"{depth_prefix}{LOGGER_PREFIX} checking for synonyms for attribute access"
        )
        for x in local_optional:
            found = False
            for z in x.pseudonyms:
                # gate to ensure we don't match to multiple synonyms
                if found:
                    continue
                if z in environment.concepts:
                    s_concept = environment.concepts[z]
                else:
                    s_concept = environment.alias_origin_lookup[z]
                if is_equivalent_basic_function_lineage(concept, s_concept):
                    found = True
                    synonyms.append(s_concept)
                    ignored_optional.add(x.address)
    equivalent_candidates = [
        x
        for x in local_optional
        if is_equivalent_basic_function_lineage(concept, x)
        and x.address != concept.address
    ] + synonyms

    # Expanding an equivalent optional to its parents drops the optional's
    # own identity. If another requested concept depends on the optional
    # (e.g. it's in an aggregate's `by` clause), that downstream consumer
    # will then re-source the optional via its parents and force a regroup.
    # Keep the optional intact in that case so it flows through directly.
    comparison_set = [concept] + [
        x for x in local_optional if x.address != concept.address
    ]
    equivalent_optional: list[BuildConcept] = []
    for eo in equivalent_candidates:
        if any(
            eo.address in get_upstream_concepts(c)
            for c in comparison_set
            if c.address != eo.address
        ):
            logger.info(
                f"{depth_prefix}{LOGGER_PREFIX} not expanding equivalent optional {eo.address} for {concept.address}; it is upstream of another requested concept"
            )
            continue
        equivalent_optional.append(eo)

    if equivalent_optional:
        logger.info(
            f"{depth_prefix}{LOGGER_PREFIX} basic node for {concept} has equivalent optional {[x.address for x in equivalent_optional]}"
        )
    for eo in equivalent_optional:
        new_parents = resolve_function_parent_concepts(eo, environment=environment)
        logger.info(
            f"{depth_prefix}{LOGGER_PREFIX} equivalent optional {eo.address} has parents {[x.address for x in new_parents]}"
        )
        parent_concepts += new_parents
    non_equivalent_optional = [
        x
        for x in local_optional
        if x not in equivalent_optional
        and not any(x.address in y.pseudonyms for y in equivalent_optional)
        and x.address not in ignored_optional
    ]
    logger.info(
        f"{depth_prefix}{LOGGER_PREFIX} basic node for {concept} has non-equivalent optional {[x.address for x in non_equivalent_optional]}"
    )
    all_parents: list[BuildConcept] = unique(
        parent_concepts + non_equivalent_optional, "address"
    )
    logger.info(
        f"{depth_prefix}{LOGGER_PREFIX} Fetching parents {[x.address for x in all_parents]} with conditions {conditions}"
    )
    if all_parents:
        parent_node: StrategyNode | None = source_concepts(
            mandatory_list=all_parents,
            environment=environment,
            g=g,
            depth=depth + 1,
            history=history,
            conditions=conditions,
        )

        if not parent_node:
            logger.info(
                f"{depth_prefix}{LOGGER_PREFIX} No basic node could be generated for {concept}"
            )
            return None
    else:
        return ConstantNode(
            input_concepts=[],
            output_concepts=[concept],
            environment=environment,
            depth=depth,
        )

    if existence_concepts:
        existence_node = source_concepts(
            mandatory_list=existence_concepts,
            environment=environment,
            g=g,
            depth=depth + 1,
            history=history,
        )
        if not existence_node:
            logger.info(
                f"{depth_prefix}{LOGGER_PREFIX} could not resolve existence inputs {[x.address for x in existence_concepts]} for {concept}"
            )
            return None
        parent_node.add_parents([existence_node])
        parent_node.add_existence_concepts(existence_concepts)

    parent_node.add_output_concept(concept)
    for x in equivalent_optional:
        parent_node.add_output_concept(x)

    logger.info(
        f"{depth_prefix}{LOGGER_PREFIX} Returning basic select for {concept}: output {[x.address for x in parent_node.output_concepts]}"
    )
    # if it's a constant, don't prune outputs
    if parent_node.source_type == SourceType.CONSTANT:
        return parent_node
    targets = [concept] + local_optional + equivalent_optional
    targets = [
        s
        for s in parent_node.output_concepts
        if any(s.address in y.pseudonyms for y in targets)
    ] + targets
    hidden = [x for x in parent_node.output_concepts if x.address not in targets]
    parent_node.hide_output_concepts(hidden)

    logger.info(
        f"{depth_prefix}{LOGGER_PREFIX} Returning basic select for {concept}: input: {[x.address for x in parent_node.input_concepts]} output {[x.address for x in parent_node.output_concepts]} hidden {[x for x in parent_node.hidden_concepts]}"
    )

    return parent_node
