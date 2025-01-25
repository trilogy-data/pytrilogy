import itertools
from collections import defaultdict
from typing import List

from trilogy.constants import logger
from trilogy.core.enums import FunctionType
from trilogy.core.models.build import BuildConcept, BuildFunction, BuildWhereClause
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.nodes import History, StrategyNode
from trilogy.core.processing.utility import padding

LOGGER_PREFIX = "[GEN_SYNONYM_NODE]"


def is_union(c: BuildConcept):
    return (
        isinstance(c.lineage, BuildFunction)
        and c.lineage.operator == FunctionType.UNION
    )


def gen_synonym_node(
    all_concepts: List[BuildConcept],
    environment: BuildEnvironment,
    g,
    depth: int,
    source_concepts,
    history: History | None = None,
    conditions: BuildWhereClause | None = None,
    accept_partial: bool = False,
) -> StrategyNode | None:
    local_prefix = f"[GEN_SYNONYM_NODE] {padding(depth)}"
    base_fingerprint = tuple([x.address for x in all_concepts])
    synonyms = defaultdict(list)
    synonym_count = 0
    for x in all_concepts:
        synonyms[x.address] = [x]
        for y in x.pseudonyms:

            if y in environment.alias_origin_lookup:
                synonyms[x.address].append(environment.alias_origin_lookup[y])
                synonym_count += 1
            elif y in environment.concepts:
                synonyms[x.address].append(environment.concepts[y])
                synonym_count += 1
    if synonym_count == 0:
        return None

    logger.info(f"{local_prefix} Generating Synonym Node with {len(synonyms)} synonyms")

    combinations = itertools.product(*(synonyms[obj] for obj in synonyms.keys()))
    for combo in combinations:
        fingerprint = tuple([x.address for x in combo])
        if fingerprint == base_fingerprint:
            continue
        attempt: StrategyNode | None = source_concepts(
            combo,
            history=history,
            environment=environment,
            depth=depth,
            conditions=conditions,
            g=g,
            accept_partial=accept_partial,
        )
        if attempt:
            logger.info(f"{local_prefix} found inputs with {combo}")
            return attempt
    return None
