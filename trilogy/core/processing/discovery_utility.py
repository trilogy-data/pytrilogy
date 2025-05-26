from typing import List

from trilogy.constants import logger
from trilogy.core.enums import Derivation, Granularity
from trilogy.core.models.build import (
    BuildConcept,
    BuildRowsetItem,
)


def depth_to_prefix(depth: int) -> str:
    return "\t" * depth


LOGGER_PREFIX = "[DISCOVERY LOOP]"


def get_upstream_concepts(base: BuildConcept, nested: bool = False) -> set[str]:
    upstream = set()
    if nested:
        upstream.add(base.address)
    if not base.lineage:
        return upstream
    for x in base.lineage.concept_arguments:
        # if it's derived from any value in a rowset, ALL rowset items are upstream
        if x.derivation == Derivation.ROWSET:
            assert isinstance(x.lineage, BuildRowsetItem), type(x.lineage)
            for y in x.lineage.rowset.select.output_components:
                upstream.add(f"{x.lineage.rowset.name}.{y.address}")
                # upstream = upstream.union(get_upstream_concepts(y, nested=True))
        upstream = upstream.union(get_upstream_concepts(x, nested=True))
    return upstream


def get_priority_concept(
    all_concepts: List[BuildConcept],
    attempted_addresses: set[str],
    found_concepts: set[str],
    depth: int,
) -> BuildConcept:
    # optimized search for missing concepts
    pass_one = sorted(
        [
            c
            for c in all_concepts
            if c.address not in attempted_addresses and c.address not in found_concepts
        ],
        key=lambda x: x.address,
    )
    # sometimes we need to scan intermediate concepts to get merge keys or filter keys,
    # so do an exhaustive search
    # pass_two = [c for c in all_concepts+filter_only if c.address not in attempted_addresses]
    for remaining_concept in (pass_one,):
        priority = (
            # find anything that needs no joins first, so we can exit early
            [
                c
                for c in remaining_concept
                if c.derivation == Derivation.CONSTANT
                and c.granularity == Granularity.SINGLE_ROW
            ]
            +
            # then multiselects to remove them from scope
            [c for c in remaining_concept if c.derivation == Derivation.MULTISELECT]
            +
            # then rowsets to remove them from scope, as they cannot get partials
            [c for c in remaining_concept if c.derivation == Derivation.ROWSET]
            +
            # then rowsets to remove them from scope, as they cannot get partials
            [c for c in remaining_concept if c.derivation == Derivation.UNION]
            # we should be home-free here
            +
            # then aggregates to remove them from scope, as they cannot get partials
            [c for c in remaining_concept if c.derivation == Derivation.AGGREGATE]
            # then windows to remove them from scope, as they cannot get partials
            + [c for c in remaining_concept if c.derivation == Derivation.WINDOW]
            # then filters to remove them from scope, also cannot get partials
            + [c for c in remaining_concept if c.derivation == Derivation.FILTER]
            # unnests are weird?
            + [c for c in remaining_concept if c.derivation == Derivation.UNNEST]
            + [c for c in remaining_concept if c.derivation == Derivation.RECURSIVE]
            + [c for c in remaining_concept if c.derivation == Derivation.BASIC]
            + [c for c in remaining_concept if c.derivation == Derivation.GROUP_TO]
            # finally our plain selects
            + [
                c for c in remaining_concept if c.derivation == Derivation.ROOT
            ]  # and any non-single row constants
            + [c for c in remaining_concept if c.derivation == Derivation.CONSTANT]
        )

        priority += [
            c
            for c in remaining_concept
            if c.address not in [x.address for x in priority]
        ]
        final = []
        # if any thing is derived from another concept
        # get the derived copy first
        # as this will usually resolve cleaner
        for x in priority:
            if any(
                [
                    x.address
                    in get_upstream_concepts(
                        c,
                    )
                    for c in priority
                ]
            ):
                logger.info(
                    f"{depth_to_prefix(depth)}{LOGGER_PREFIX} delaying fetch of {x.address} as parent of another concept"
                )
                continue
            final.append(x)
        # then append anything we didn't get
        for x2 in priority:
            if x2 not in final:
                final.append(x2)
        if final:
            return final[0]
    raise ValueError(
        f"Cannot resolve query. No remaining priority concepts, have attempted {attempted_addresses}"
    )
