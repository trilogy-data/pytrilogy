from collections import defaultdict
from itertools import product
from typing import List

from trilogy.core.enums import (
    AddressType,
    BooleanOperator,
    ComparisonOperator,
)
from trilogy.core.models.build import (
    BuildComparison,
    BuildConcept,
    BuildConditional,
    BuildDatasource,
    BuildParenthetical,
)
from trilogy.core.models.core import EnumType
from trilogy.core.models.datasource import Address
from trilogy.core.processing.utility import simplify_conditions


def _datasource_score(ds: BuildDatasource) -> int:
    """Score by materialization level: 2=table, 1=static file (parquet/csv), 0=script/query."""
    if not isinstance(ds.address, Address):
        return 2
    if ds.address.is_query:
        return 0
    if ds.address.type == AddressType.PYTHON_SCRIPT:
        return 0
    if ds.address.is_file:
        return 1
    return 2


def _extract_enum_value_for_key(
    conditional: BuildComparison | BuildConditional | BuildParenthetical,
    key_address: str,
) -> object | None:
    """Extract the literal value for a specific concept key from a (compound) condition."""
    if isinstance(conditional, BuildComparison):
        if conditional.operator not in (ComparisonOperator.EQ, ComparisonOperator.IS):
            return None
        if (
            isinstance(conditional.left, BuildConcept)
            and conditional.left.address == key_address
            and not isinstance(conditional.right, BuildConcept)
        ):
            return conditional.right
        if (
            isinstance(conditional.right, BuildConcept)
            and conditional.right.address == key_address
            and not isinstance(conditional.left, BuildConcept)
        ):
            return conditional.left
        return None
    elif isinstance(conditional, BuildConditional):
        if conditional.operator == BooleanOperator.OR:
            return None
        _cond_types = (BuildComparison, BuildConditional, BuildParenthetical)
        if isinstance(conditional.left, _cond_types):
            left_val = _extract_enum_value_for_key(conditional.left, key_address)
            if left_val is not None:
                return left_val
        if isinstance(conditional.right, _cond_types):
            return _extract_enum_value_for_key(conditional.right, key_address)
    elif isinstance(conditional, BuildParenthetical):
        if isinstance(
            conditional.content, (BuildComparison, BuildConditional, BuildParenthetical)
        ):
            return _extract_enum_value_for_key(conditional.content, key_address)
    return None


def _best_enum_union(
    dses: list[BuildDatasource],
    enum_type: EnumType,
    merge_key: BuildConcept,
) -> list[BuildDatasource] | None:
    """Find the best minimal covering combination for an enum-partitioned key.

    Groups by covered enum value, then picks the highest-scoring combination
    (one source per value) that has field overlap beyond the merge key itself.
    Materialized table sources score higher than script/query sources.
    """
    by_value: dict[object, list[BuildDatasource]] = defaultdict(list)
    for ds in dses:
        if not ds.non_partial_for:
            continue
        val = _extract_enum_value_for_key(
            ds.non_partial_for.conditional, merge_key.address
        )
        if val is None:
            continue
        by_value[val].append(ds)

    # All enum values must have at least one candidate source
    if set(str(v) for v in by_value.keys()) < set(enum_type.values):
        return None

    values = list(by_value.keys())
    merge_key_addr = {merge_key.address}

    best: list[BuildDatasource] | None = None
    best_score = -1

    for combo in product(*[by_value[v] for v in values]):
        combo_list = list(combo)

        # A union requires at least 2 distinct sources; a single source is not a union
        if len(combo_list) < 2:
            continue

        # Require at least one shared concept beyond the merge key
        overlap = set(c.address for c in combo_list[0].output_concepts)
        for ds in combo_list[1:]:
            overlap &= {c.address for c in ds.output_concepts}
        if not (overlap - merge_key_addr):
            continue

        score = sum(_datasource_score(ds) for ds in combo_list)
        if score > best_score:
            best_score = score
            best = combo_list

    return best


def get_union_sources(
    datasources: list[BuildDatasource], concepts: list[BuildConcept]
) -> List[list[BuildDatasource]]:
    candidates: list[BuildDatasource] = []

    for x in datasources:
        if any([c.address in x.output_concepts for c in concepts]):
            if (
                any([c.address in x.partial_concepts for c in concepts])
                and x.non_partial_for
            ):
                candidates.append(x)
    assocs: dict[str, list[BuildDatasource]] = defaultdict(list[BuildDatasource])
    for x in candidates:
        if not x.non_partial_for:
            continue
        ca = x.non_partial_for.concept_arguments
        if len(ca) == 1:
            assocs[ca[0].address].append(x)
        else:
            # Multi-concept: register under each enum concept so _best_enum_union
            # can determine which one is the discriminating merge key.
            for c in ca:
                if isinstance(c.datatype, EnumType):
                    assocs[c.address].append(x)
    final: list[list[BuildDatasource]] = []
    for merge_key_addr, dses in assocs.items():
        if not dses or not dses[0].non_partial_for:
            continue
        merge_key = next(
            (
                c
                for c in dses[0].non_partial_for.concept_arguments
                if c.address == merge_key_addr
            ),
            dses[0].non_partial_for.concept_arguments[0],
        )
        if isinstance(merge_key.datatype, EnumType):
            result = _best_enum_union(dses, merge_key.datatype, merge_key)
            if result:
                final.append(result)
        else:
            conditions = [
                c.non_partial_for.conditional for c in dses if c.non_partial_for
            ]
            if simplify_conditions(conditions):
                final.append(dses)
    return final
