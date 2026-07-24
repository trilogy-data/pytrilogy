from collections import defaultdict
from itertools import product

from trilogy.core.enums import (
    AddressType,
    BooleanOperator,
    ComparisonOperator,
    Modifier,
)
from trilogy.core.models.build import (
    BoolExpr,
    BuildComparison,
    BuildConcept,
    BuildConditional,
    BuildDatasource,
    BuildParenthetical,
)
from trilogy.core.models.core import EnumType
from trilogy.core.models.datasource import Address
from trilogy.core.processing.condition_utility import simplify_conditions


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
    conditional: BoolExpr,
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
        if isinstance(conditional.left, BoolExpr):
            left_val = _extract_enum_value_for_key(conditional.left, key_address)
            if left_val is not None:
                return left_val
        if isinstance(conditional.right, BoolExpr):
            return _extract_enum_value_for_key(conditional.right, key_address)
    elif isinstance(conditional, BuildParenthetical):
        if isinstance(conditional.content, BoolExpr):
            return _extract_enum_value_for_key(conditional.content, key_address)
    return None


def _best_enum_union(
    dses: list[BuildDatasource],
    enum_type: EnumType,
    merge_key: BuildConcept,
) -> list[list[BuildDatasource]] | None:
    """Find the best minimal covering combinations for an enum-partitioned key.

    Groups by covered enum value, enumerates one-source-per-value combos,
    computes the combo's actual concept overlap (minus the merge key), and
    keeps the highest-scoring combo per distinct overlap signature. Returning
    one combo per overlap lets parallel partitionings (e.g., sales vs.
    returns vs. dim, all keyed by the same channel enum) each contribute
    their own union datasource instead of collapsing into the single best.
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
    if set(str(v) for v in by_value) < set(enum_type.values):
        return None

    values = list(by_value.keys())
    merge_key_addr = {merge_key.address}

    best_per_overlap: dict[frozenset[str], tuple[list[BuildDatasource], int]] = {}

    for combo in product(*[by_value[v] for v in values]):
        combo_list = list(combo)

        # A union requires at least 2 distinct sources; a single source is not a union
        if len(combo_list) < 2:
            continue

        # Require at least one shared concept beyond the merge key
        overlap = {col.concept.address for col in combo_list[0].columns}
        for ds in combo_list[1:]:
            overlap &= {col.concept.address for col in ds.columns}
        signature = frozenset(overlap - merge_key_addr)
        if not signature:
            continue
        # Members MAY disagree on intrinsic (~) partiality of a shared column
        # (a "mixed-family" combo, e.g. web_sales + catalog/store_returns).
        # Such a union is a legitimate per-channel provider of columns it binds
        # complete (q05: a web return's return-site lives on web_sales), and
        # union partial propagation keeps its ~-partial keys from ever outranking
        # a pure family that binds them complete (q14) — so don't reject it here.

        score = sum(_datasource_score(ds) for ds in combo_list)
        existing = best_per_overlap.get(signature)
        if existing is None or score > existing[1]:
            best_per_overlap[signature] = (combo_list, score)

    if not best_per_overlap:
        return None
    # Keep only maximal overlap signatures: drop a signature whose concept set
    # is a strict subset of another's. This filters out "mixed" combos (e.g.,
    # 2 sales + 1 dim) whose overlap is a strict subset of a pure-grouping
    # combo (e.g., 3 sales). Pure parallel partitionings (sales/returns/dim)
    # remain incomparable and all survive.
    sigs = list(best_per_overlap.keys())
    maximal = [s for s in sigs if not any(s < other for other in sigs)]
    return [best_per_overlap[s][0] for s in maximal]


def get_union_sources(
    datasources: list[BuildDatasource], concepts: list[BuildConcept]
) -> list[list[BuildDatasource]]:
    concept_addrs = {c.address for c in concepts}
    candidates: list[BuildDatasource] = []
    _PARTIAL = Modifier.PARTIAL

    # A candidate needs a non_partial_for clause and at least one partial column
    # whose concept matches the request. A matching partial column is also a
    # matching output column, so we don't need the separate output-overlap check
    # the original did.
    for x in datasources:
        if not x.non_partial_for:
            continue
        for col in x.columns:
            if _PARTIAL in col.modifiers and col.concept.address in concept_addrs:
                candidates.append(x)
                break

    assocs: dict[str, list[BuildDatasource]] = defaultdict(list[BuildDatasource])
    for x in candidates:
        ca = x.non_partial_for.concept_arguments  # type: ignore[union-attr]
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
                final.extend(result)
        else:
            conditions = [
                c.non_partial_for.conditional for c in dses if c.non_partial_for
            ]
            if simplify_conditions(conditions):
                final.append(dses)
    return final
