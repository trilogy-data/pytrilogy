from typing import List

from trilogy.constants import logger
from trilogy.core.enums import FunctionClass, FunctionType, SourceType
from trilogy.core.models.build import BuildConcept, BuildFunction, BuildWhereClause
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.node_generators.common import (
    resolve_function_parent_concepts,
)
from trilogy.core.processing.nodes import History, StrategyNode
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
    parent_concepts = resolve_function_parent_concepts(concept, environment=environment)

    logger.info(
        f"{depth_prefix}{LOGGER_PREFIX} basic node for {concept} with lineage {concept.lineage} has parents {[x for x in parent_concepts]}"
    )
    synonyms: list[BuildConcept] = []
    ignored_optional: set[str] = set()
    assert isinstance(concept.lineage, BuildFunction)
    # when we are getting an attribute, if there is anything else
    # that is an attribute of the same struct in local optional
    # select that value for discovery as well
    if concept.lineage.operator == FunctionType.ATTR_ACCESS:
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
    equivalent_optional = [
        x
        for x in local_optional
        if is_equivalent_basic_function_lineage(concept, x)
        and x.address != concept.address
    ] + synonyms

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
        f"{depth_prefix}{LOGGER_PREFIX} Fetching parents {[x.address for x in all_parents]}"
    )
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

    parent_node.source_type = SourceType.BASIC
    parent_node.add_output_concept(concept)
    for x in equivalent_optional:
        parent_node.add_output_concept(x)

    logger.info(
        f"{depth_prefix}{LOGGER_PREFIX} Returning basic select for {concept}: output {[x.address for x in parent_node.output_concepts]}"
    )
    targets = [concept] + local_optional + equivalent_optional
    should_hide = [
        x
        for x in parent_node.output_concepts
        if (
            x.address not in targets
            and not any(x.address in y.pseudonyms for y in targets)
        )
    ]
    parent_node.hide_output_concepts(should_hide)
    should_not_hide = [
        x
        for x in parent_node.output_concepts
        if x.address in targets or any(x.address in y.pseudonyms for y in targets)
    ]
    parent_node.unhide_output_concepts(should_not_hide)

    logger.info(
        f"{depth_prefix}{LOGGER_PREFIX} Returning basic select for {concept}: output {[x.address for x in parent_node.output_concepts]} hidden {[x for x in parent_node.hidden_concepts]}"
    )
    return parent_node
