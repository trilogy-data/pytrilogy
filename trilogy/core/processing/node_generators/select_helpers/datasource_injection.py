from collections import defaultdict

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

    Groups by covered enum value, then searches one-source-per-value combos via
    a dynamic program over values whose state is the combo's concept overlap so
    far (minus the merge key), keeping the highest-scoring combo per distinct
    overlap signature. The score is separable (a per-source sum) and the
    overlap is the only coupling between values, so this is exhaustive over
    achievable signatures without enumerating the k^V combo product. Returning
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
    if {str(v) for v in by_value} < set(enum_type.values):
        return None

    values = list(by_value.keys())
    # A union requires at least 2 distinct sources; a single source is not a union
    if len(values) < 2:
        return None

    cols: dict[int, frozenset[str]] = {}
    scores: dict[int, int] = {}
    for candidates in by_value.values():
        for ds in candidates:
            cols[id(ds)] = frozenset(col.concept.address for col in ds.columns)
            scores[id(ds)] = _datasource_score(ds)

    # Members MAY disagree on intrinsic (~) partiality of a shared column
    # (a "mixed-family" combo, e.g. web_sales + catalog/store_returns).
    # Such a union is a legitimate per-channel provider of columns it binds
    # complete (q05: a web return's return-site lives on web_sales), and
    # union partial propagation keeps its ~-partial keys from ever outranking
    # a pure family that binds them complete (q14) — so don't reject it here.
    # An empty overlap beyond the merge key IS rejected, at the first step it
    # appears: intersection only shrinks, so no continuation can revive it.
    # Ties reproduce the old product enumeration exactly: per signature the
    # winner is the first max-scoring combo in product order (`combo_key` =
    # candidate index tuple, lexicographic = product order), and signatures
    # order by their first-achieving combo (`min_key`, tracked over ALL combos
    # reaching a signature, not just the best-scoring one).
    merge_key_addr = merge_key.address
    # signature -> (score, combo, combo_key, min_key)
    states: dict[
        frozenset[str],
        tuple[int, list[BuildDatasource], tuple[int, ...], tuple[int, ...]],
    ] = {}
    for idx, ds in enumerate(by_value[values[0]]):
        sig = cols[id(ds)] - {merge_key_addr}
        if not sig:
            continue
        existing = states.get(sig)
        if existing is None:
            states[sig] = (scores[id(ds)], [ds], (idx,), (idx,))
        elif scores[id(ds)] > existing[0]:
            states[sig] = (scores[id(ds)], [ds], (idx,), existing[3])

    for v in values[1:]:
        next_states: dict[
            frozenset[str],
            tuple[int, list[BuildDatasource], tuple[int, ...], tuple[int, ...]],
        ] = {}
        for sig, (score, combo, combo_key, min_key) in states.items():
            for idx, ds in enumerate(by_value[v]):
                new_sig = sig & cols[id(ds)]
                if not new_sig:
                    continue
                new_score = score + scores[id(ds)]
                new_key = combo_key + (idx,)
                existing = next_states.get(new_sig)
                if existing is None:
                    next_states[new_sig] = (
                        new_score,
                        combo + [ds],
                        new_key,
                        min_key + (idx,),
                    )
                    continue
                new_min_key = min(existing[3], min_key + (idx,))
                if new_score > existing[0] or (
                    new_score == existing[0] and new_key < existing[2]
                ):
                    next_states[new_sig] = (
                        new_score,
                        combo + [ds],
                        new_key,
                        new_min_key,
                    )
                else:
                    next_states[new_sig] = (
                        existing[0],
                        existing[1],
                        existing[2],
                        new_min_key,
                    )
        states = next_states
        if not states:
            return None

    best_per_overlap: dict[frozenset[str], tuple[list[BuildDatasource], int]] = {
        sig: (state[1], state[0])
        for sig, state in sorted(states.items(), key=lambda kv: kv[1][3])
    }
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


def _partition_families(
    datasources: list[BuildDatasource], concepts: list[BuildConcept]
) -> dict[str, list[BuildDatasource]]:
    """Discriminator address -> the `complete where` arms partitioned on it.

    A candidate needs a non_partial_for clause and at least one partial column
    whose concept matches the request. A matching partial column is also a
    matching output column, so we don't need a separate output-overlap check.
    """
    concept_addrs = {c.address for c in concepts}
    _PARTIAL = Modifier.PARTIAL
    candidates: list[BuildDatasource] = []
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
    return assocs


def _merge_key(dses: list[BuildDatasource], merge_key_addr: str) -> BuildConcept | None:
    if not dses or not dses[0].non_partial_for:
        return None
    args = dses[0].non_partial_for.concept_arguments  # type: ignore[union-attr]
    return next((c for c in args if c.address == merge_key_addr), args[0])


def get_union_sources(
    datasources: list[BuildDatasource], concepts: list[BuildConcept]
) -> list[list[BuildDatasource]]:
    final: list[list[BuildDatasource]] = []
    for merge_key_addr, dses in _partition_families(datasources, concepts).items():
        merge_key = _merge_key(dses, merge_key_addr)
        if merge_key is None:
            continue
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


def describe_incomplete_partitions(
    datasources: list[BuildDatasource], concepts: list[BuildConcept]
) -> str | None:
    """Why a `complete where` family failed to union into a complete source.

    ``get_union_sources`` only unions arms whose predicates provably exhaust the
    discriminator's domain — a family that misses a value would silently drop
    those rows. A plain `string` discriminator has no enumerable domain, so no
    set of equality predicates over it can ever be proven complete. Without this
    the query just reports "no complete sources", which reads as a planner
    failure rather than a modeling one.
    """
    reasons: list[str] = []
    for merge_key_addr, dses in _partition_families(datasources, concepts).items():
        if len(dses) < 2:
            continue
        merge_key = _merge_key(dses, merge_key_addr)
        if merge_key is None:
            continue
        if isinstance(merge_key.datatype, EnumType):
            if _best_enum_union(dses, merge_key.datatype, merge_key):
                continue
        elif simplify_conditions(
            [c.non_partial_for.conditional for c in dses if c.non_partial_for]
        ):
            continue
        names = ", ".join(sorted(d.name for d in dses))
        reasons.append(
            f"partial sources ({names}) partition on `{merge_key_addr}`"
            f" ({merge_key.datatype}), but their `complete where` clauses are not"
            " provably exhaustive over that type, so they cannot be unioned into a"
            " complete source"
        )
    if not reasons:
        return None
    return (
        "; ".join(reasons)
        + ". Declare the discriminator with an exhaustible type (e.g."
        " `enum<string>['a', 'b']`) so the partitioning can be proven complete,"
        " or filter the query to one partition."
    )
