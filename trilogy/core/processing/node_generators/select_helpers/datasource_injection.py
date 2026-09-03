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
from trilogy.core.processing.condition_utility import (
    ExcludedEnumValues,
    effective_enum_domain,
    simplify_conditions,
)


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
    excluded: ExcludedEnumValues | None = None,
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
    Coverage is judged over the effective domain: values the statement's row
    gate rules out (``excluded``) need no arm, and an arm for such a value
    cannot contribute.
    """
    required = {
        str(v) for v in effective_enum_domain(enum_type, merge_key, excluded).values
    }
    by_value: dict[object, list[BuildDatasource]] = defaultdict(list)
    for ds in dses:
        if not ds.non_partial_for:
            continue
        val = _extract_enum_value_for_key(
            ds.non_partial_for.conditional, merge_key.address
        )
        if val is None or str(val) not in required:
            continue
        by_value[val].append(ds)

    # Every value that can still occur must have at least one candidate source
    if not required <= {str(v) for v in by_value}:
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

    # Members MAY disagree on intrinsic (~) partiality of a shared column (a
    # mixed-family combo): such a union is a legitimate provider of the columns
    # it binds complete, and union partial propagation keeps its ~-partial keys
    # from outranking a pure family, so it is not rejected here. An empty
    # overlap beyond the merge key IS rejected at the first step it appears:
    # intersection only shrinks. Ties are deterministic: per signature the
    # winner is the first max-scoring combo in product order (`combo_key` =
    # candidate index tuple), and signatures order by their first-achieving
    # combo (`min_key`, tracked over ALL combos reaching a signature).
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
    # Keep only maximal overlap signatures: a mixed combo whose overlap is a
    # strict subset of a pure-family combo's is dropped, while parallel
    # partitionings remain incomparable and all survive.
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
    datasources: list[BuildDatasource],
    concepts: list[BuildConcept],
    excluded: ExcludedEnumValues | None = None,
) -> list[list[BuildDatasource]]:
    final: list[list[BuildDatasource]] = []
    for merge_key_addr, dses in _partition_families(datasources, concepts).items():
        merge_key = _merge_key(dses, merge_key_addr)
        if merge_key is None:
            continue
        if isinstance(merge_key.datatype, EnumType):
            result = _best_enum_union(dses, merge_key.datatype, merge_key, excluded)
            if result:
                final.extend(result)
        else:
            conditions = [
                c.non_partial_for.conditional for c in dses if c.non_partial_for
            ]
            if simplify_conditions(conditions, excluded):
                final.append(dses)
    return final


def describe_incomplete_partitions(
    datasources: list[BuildDatasource],
    concepts: list[BuildConcept],
    excluded: ExcludedEnumValues | None = None,
) -> str | None:
    """Why a `complete where` family failed to union into a complete source.

    ``get_union_sources`` only unions arms whose predicates provably exhaust the
    discriminator's domain. A plain `string` discriminator has no enumerable
    domain, so no set of equality predicates over it can be proven complete;
    this names that modeling gap instead of a generic "no complete sources".
    """
    reasons: list[str] = []
    for merge_key_addr, dses in _partition_families(datasources, concepts).items():
        if len(dses) < 2:
            continue
        merge_key = _merge_key(dses, merge_key_addr)
        if merge_key is None:
            continue
        if isinstance(merge_key.datatype, EnumType):
            if _best_enum_union(dses, merge_key.datatype, merge_key, excluded):
                continue
        elif simplify_conditions(
            [c.non_partial_for.conditional for c in dses if c.non_partial_for],
            excluded,
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
