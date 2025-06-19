import itertools
from collections import defaultdict
from typing import List

from trilogy.constants import logger
from trilogy.core.enums import Derivation
from trilogy.core.models.build import BuildConcept, BuildWhereClause
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.nodes import History, StrategyNode
from trilogy.core.processing.utility import padding

LOGGER_PREFIX = "[GEN_SYNONYM_NODE]"


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
    base_fingerprint = tuple(sorted([x.address for x in all_concepts]))
    synonyms = defaultdict(list)
    has_synonyms = False
    for x in all_concepts:
        synonyms[x.address] = [x]
        if x.address in environment.alias_origin_lookup:
            parent = environment.concepts[x.address]
            if parent.address != x.address:
                synonyms[x.address].append(parent)
                has_synonyms = True
        for y in x.pseudonyms:
            if y in environment.alias_origin_lookup:
                synonyms[x.address].append(environment.alias_origin_lookup[y])
                has_synonyms = True
            elif y in environment.concepts:
                synonyms[x.address].append(environment.concepts[y])
                has_synonyms = True
    for address in synonyms:
        synonyms[address].sort(key=lambda obj: obj.address)
    if not has_synonyms:
        return None

    logger.info(f"{local_prefix} Generating Synonym Node with {len(synonyms)} synonyms")
    sorted_keys = sorted(synonyms.keys())
    combinations_list: list[tuple[BuildConcept, ...]] = list(
        itertools.product(*(synonyms[obj] for obj in sorted_keys))
    )

    def similarity_sort_key(combo: tuple[BuildConcept, ...]):
        addresses = [x.address for x in combo]

        # Calculate similarity score - count how many pairs share prefixes
        similarity_score = 0
        roots = sum(
            [1 for x in combo if x.derivation in (Derivation.ROOT, Derivation.CONSTANT)]
        )
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

        # Sort by roots, similarity (descending), then by addresses (ascending) for ties
        return (-roots, -similarity_score, addresses)

    combinations_list.sort(key=similarity_sort_key)
    for combo in combinations_list:
        fingerprint = tuple(sorted([x.address for x in combo]))
        if fingerprint == base_fingerprint:
            continue
        logger.info(
            f"{local_prefix} checking combination {fingerprint} with {len(combo)} concepts"
        )
        attempt: StrategyNode | None = source_concepts(
            list(combo),
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
