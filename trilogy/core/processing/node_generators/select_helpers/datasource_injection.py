from collections import defaultdict
from dataclasses import dataclass

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
    BuildFunction,
    BuildParenthetical,
)
from trilogy.core.models.core import EnumType
from trilogy.core.models.datasource import Address
from trilogy.core.processing.condition_utility import (
    ExcludedEnumValues,
    decompose_condition,
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


@dataclass(frozen=True)
class _CoverUnit:
    """A set of arms that jointly cover one discriminator value.

    ``score`` sums the members' materialization scores and ``width`` counts
    them, so among equally materialized covers the one with fewer scans wins.
    ``cols`` is the column overlap every member binds."""

    members: tuple[BuildDatasource, ...]
    cols: frozenset[str]
    score: int
    width: int


_Claim = dict[str, BuildComparison]
_Arm = tuple[BuildDatasource, _Claim]


def _claim_atoms(conditional: BoolExpr) -> _Claim | None:
    """A `complete where` as ``{concept address: its equality atom}``.

    Only a pure conjunction of ``concept = literal`` atoms is a partition
    claim the cover proof can reason about exactly; anything else (an OR, a
    range, a concept-to-concept comparison, a repeated concept) returns None
    and the arm stays out of enum-family election."""
    out: _Claim = {}
    for chunk in decompose_condition(conditional):
        atom: object = chunk
        while isinstance(atom, BuildParenthetical):
            atom = atom.content
        if isinstance(atom, BuildConditional):
            # `decompose_condition` splits every AND it can; a conditional it
            # handed back is an OR (or an AND over non-conditions). Only one
            # freshly unwrapped from parentheses still has an AND to split.
            if atom is chunk or atom.operator != BooleanOperator.AND:
                return None
            nested = _claim_atoms(atom)
            if nested is None or set(nested) & set(out):
                return None
            out.update(nested)
            continue
        if not isinstance(atom, BuildComparison) or atom.operator not in (
            ComparisonOperator.EQ,
            ComparisonOperator.IS,
        ):
            return None
        if isinstance(atom.left, BuildConcept) and not isinstance(
            atom.right, BuildConcept
        ):
            concept = atom.left
        elif isinstance(atom.right, BuildConcept) and not isinstance(
            atom.left, BuildConcept
        ):
            concept = atom.right
        else:
            return None
        if concept.address in out:
            return None
        out[concept.address] = atom
    return out


def _claimed_value(atom: BuildComparison) -> str:
    literal: object = atom.right if isinstance(atom.left, BuildConcept) else atom.left
    if isinstance(literal, BuildFunction) and literal.arguments:
        literal = literal.arguments[0]
    return str(literal)


def _claimed_concept(atom: BuildComparison) -> BuildConcept:
    concept = atom.left if isinstance(atom.left, BuildConcept) else atom.right
    assert isinstance(concept, BuildConcept)
    return concept


def _columns(ds: BuildDatasource) -> frozenset[str]:
    return frozenset(col.concept.address for col in ds.columns)


def _cover_units(
    arms: list[_Arm],
    excluded: ExcludedEnumValues | None,
) -> list[_CoverUnit]:
    """The arm sets that jointly cover the population these arms were asked
    to cover, best per column overlap.

    An arm whose residual claim is empty covers by itself. Arms that still
    claim other discriminators cover only jointly: they are partitioned on
    one of those discriminators and every value of it must be covered in
    turn, recursively. A claim on ``(city, source)`` therefore never covers
    ``city`` by itself; it covers it together with the arms for the other
    ``source`` values."""
    units = [
        _CoverUnit(
            members=(ds,), cols=_columns(ds), score=_datasource_score(ds), width=1
        )
        for ds, claim in arms
        if not claim
    ]
    constrained = [(ds, claim) for ds, claim in arms if claim]
    if not constrained:
        return units
    counts: dict[str, int] = defaultdict(int)
    for _, claim in constrained:
        for address in claim:
            counts[address] += 1
    key_address = max(sorted(counts), key=lambda a: counts[a])
    key = next(
        _claimed_concept(claim[key_address])
        for _, claim in constrained
        if key_address in claim
    )
    if isinstance(key.datatype, EnumType):
        units.extend(_enum_cover_units(constrained, key, excluded))
        return units
    # A non-enum residual is provable only as a range/boolean cover, one atom
    # per arm.
    if all(len(claim) == 1 for _, claim in constrained) and simplify_conditions(
        [claim[key_address] for _, claim in constrained if key_address in claim],
        excluded,
    ):
        members = tuple(ds for ds, _ in constrained)
        units.append(
            _CoverUnit(
                members=members,
                cols=frozenset.intersection(*(_columns(ds) for ds in members)),
                score=sum(_datasource_score(ds) for ds in members),
                width=len(members),
            )
        )
    return units


_State = tuple[int, int, tuple[BuildDatasource, ...], tuple[int, ...], tuple[int, ...]]


def _enum_cover_units(
    arms: list[_Arm],
    key: BuildConcept,
    excluded: ExcludedEnumValues | None,
) -> list[_CoverUnit]:
    """Best covering combinations over an enum discriminator, one per column
    overlap signature.

    Each value of the effective domain (values the statement's row gate rules
    out need no arm) must have a cover: the arms claiming that value with the
    claim stripped, plus arms silent on this discriminator (their claim applies
    to every value), reduced by `_cover_units`. A dynamic program over values
    whose state is the combo's column overlap so far (minus the discriminator)
    keeps the highest-scoring combo per signature; the score is separable and
    the overlap is the only coupling, so this is exhaustive over achievable
    signatures without enumerating the product. One combo per overlap lets
    parallel partitionings (sales vs. returns vs. dim, all keyed by one channel
    enum) each contribute their own union. Ties break to the first
    max-scoring combo in product order; signatures order by their first
    achieving combo."""
    domain = effective_enum_domain(key.datatype, key, excluded)
    assert isinstance(domain, EnumType)
    required = [str(v) for v in domain.values]
    silent = [(ds, claim) for ds, claim in arms if key.address not in claim]
    by_value: dict[str, list[_Arm]] = defaultdict(list)
    for ds, claim in arms:
        atom = claim.get(key.address)
        if atom is None:
            continue
        value = _claimed_value(atom)
        if value in required:
            residual = {a: c for a, c in claim.items() if a != key.address}
            by_value[value].append((ds, residual))
    per_value: list[list[_CoverUnit]] = []
    for value in required:
        candidates = by_value.get(value, []) + silent
        units = _cover_units(candidates, excluded) if candidates else []
        if not units:
            return []
        per_value.append(units)

    states: dict[frozenset[str], _State] = {}
    for idx, unit in enumerate(per_value[0]):
        sig = unit.cols - {key.address}
        if not sig:
            continue
        existing = states.get(sig)
        if existing is None:
            states[sig] = (unit.score, unit.width, unit.members, (idx,), (idx,))
        elif (unit.score, -unit.width) > (existing[0], -existing[1]):
            states[sig] = (unit.score, unit.width, unit.members, (idx,), existing[4])
    for units in per_value[1:]:
        next_states: dict[frozenset[str], _State] = {}
        for sig, (score, width, members, combo_key, min_key) in states.items():
            for idx, unit in enumerate(units):
                new_sig = sig & unit.cols
                if not new_sig:
                    continue
                new: _State = (
                    score + unit.score,
                    width + unit.width,
                    _dedupe_members(members + unit.members),
                    combo_key + (idx,),
                    min_key + (idx,),
                )
                existing = next_states.get(new_sig)
                if existing is None:
                    next_states[new_sig] = new
                    continue
                new_min_key = min(existing[4], new[4])
                if (new[0], -new[1]) > (existing[0], -existing[1]) or (
                    (new[0], new[1]) == (existing[0], existing[1])
                    and new[3] < existing[3]
                ):
                    next_states[new_sig] = (*new[:4], new_min_key)
                else:
                    next_states[new_sig] = (*existing[:4], new_min_key)
        states = next_states
        if not states:
            return []
    return [
        _CoverUnit(members=members, cols=sig, score=score, width=width)
        for sig, (score, width, members, _, _) in sorted(
            states.items(), key=lambda kv: kv[1][4]
        )
    ]


def _dedupe_members(
    members: tuple[BuildDatasource, ...],
) -> tuple[BuildDatasource, ...]:
    seen: set[str] = set()
    out: list[BuildDatasource] = []
    for ds in members:
        if ds.identifier not in seen:
            seen.add(ds.identifier)
            out.append(ds)
    return tuple(out)


def _best_enum_union(
    dses: list[BuildDatasource],
    enum_type: EnumType,
    merge_key: BuildConcept,
    excluded: ExcludedEnumValues | None = None,
) -> list[list[BuildDatasource]] | None:
    """The minimal covering unions for an enum-partitioned key: every value of
    the effective domain covered, with a multi-discriminator claim covering its
    value only jointly with the arms for the other discriminators' values
    (`_cover_units`). One union per maximal column overlap; None when no
    complete cover exists or the only cover is a single source (not a union)."""
    arms: list[_Arm] = []
    for ds in dses:
        if not ds.non_partial_for:
            continue
        claim = _claim_atoms(ds.non_partial_for.conditional)
        if claim is not None and merge_key.address in claim:
            arms.append((ds, claim))
    units = [
        unit
        for unit in _enum_cover_units(arms, merge_key, excluded)
        if len(unit.members) >= 2
    ]
    if not units:
        return None
    # Keep only maximal overlap signatures: a mixed combo whose overlap is a
    # strict subset of a pure-family combo's is dropped, while parallel
    # partitionings remain incomparable and all survive.
    sigs = [unit.cols for unit in units]
    return [
        list(unit.members)
        for unit in units
        if not any(unit.cols < other for other in sigs)
    ]


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
