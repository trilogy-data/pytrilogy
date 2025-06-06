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
    local_prefix = f"{padding(depth)}[GEN_SYNONYM_NODE]"
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
    for address in synonyms:
        synonyms[address].sort(key=lambda obj: obj.address)
    if synonym_count == 0:
        return None

    logger.info(f"{local_prefix} Generating Synonym Node with {len(synonyms)} synonyms")
    sorted_keys = sorted(synonyms.keys())
    combinations_list = list(itertools.product(*(synonyms[obj] for obj in sorted_keys)))

    def similarity_sort_key(combo):
        addresses = [x.address for x in combo]

        # Calculate similarity score - count how many pairs share prefixes
        similarity_score = 0
        for i in range(len(addresses)):
            for j in range(i + 1, len(addresses)):
                # Find common prefix length
                addr1_parts = addresses[i].split(".")
                addr2_parts = addresses[j].split(".")
                common_prefix_len = 0
                for k in range(min(len(addr1_parts), len(addr2_parts))):
                    if addr1_parts[k] == addr2_parts[k]:
                        common_prefix_len += 1
                    else:
                        break
                similarity_score += common_prefix_len

        # Sort by similarity (descending), then by addresses (ascending) for ties
        return (-similarity_score, addresses)

    combinations_list.sort(key=similarity_sort_key)
    logger.info(combinations_list)
    for combo in combinations_list:
        fingerprint = tuple([x.address for x in combo])
        if fingerprint == base_fingerprint:
            continue
        logger.info(
            f"{local_prefix} checking combination {fingerprint} with {len(combo)} concepts"
        )
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
