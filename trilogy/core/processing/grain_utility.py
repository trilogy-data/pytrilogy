from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field, replace

from trilogy.constants import MagicConstants
from trilogy.core.enums import (
    ComparisonOperator,
    Derivation,
    FunctionType,
    JoinType,
    Purpose,
    SourceType,
)
from trilogy.core.models.build import (
    BoolExpr,
    BuildComparison,
    BuildConcept,
    BuildConceptArgs,
    BuildDatasource,
    BuildFilterItem,
    BuildFunction,
    BuildGrain,
    BuildParenthetical,
    BuildRowsetItem,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.models.execute import (
    CTE,
    ConceptPair,
    Join,
    QueryDatasource,
    SourceJoin,
    UnionCTE,
    UnnestJoin,
    coalesced_key_groups,
    pair_matches_nulls,
)
from trilogy.core.processing.condition_utility import (
    NULL_PROPAGATING_OPS,
    concepts_implied_non_null,
    decompose_condition,
    gather_non_null_proofs,
    gather_or_groups,
    is_scalar_condition,
    membership_proves_non_null,
    opaque_binding_addresses,
)
from trilogy.core.processing.join_resolution import (
    complete_key_domain,
    deep_extent_free_carried,
    deep_extent_free_spans,
    held_region_spans,
    preserved_sources,
)
from trilogy.core.processing.utility import (
    applied_atoms,
    collect_applied_conditions,
    equivalent_addresses,
    join_left_sources,
    joined_right_ids,
    left_deep_joins,
    padded_by,
    unpadded_join_type,
)

GrainSource = QueryDatasource | BuildDatasource


def _null_tested_addresses(condition: BoolExpr | None) -> set[str]:
    if condition is None:
        return set()
    output: set[str] = set()
    for atom in decompose_condition(condition):
        if not isinstance(atom, BuildComparison):
            continue
        if atom.operator != ComparisonOperator.IS:
            continue
        if isinstance(atom.left, BuildConcept) and atom.right in (
            None,
            MagicConstants.NULL,
        ):
            output.add(atom.left.address)
        elif isinstance(atom.right, BuildConcept) and atom.left in (
            None,
            MagicConstants.NULL,
        ):
            output.add(atom.right.address)
    return output


def anti_join_preserved_grain(
    final_datasets: list[GrainSource],
    joins: list[SourceJoin | UnnestJoin],
    condition: BoolExpr | None,
) -> BuildGrain | None:
    """Grain of the preserved side of a proven two-source anti-join."""
    if len(final_datasets) != 2 or len(joins) != 1:
        return None
    join = joins[0]
    if not isinstance(join, Join):
        return None
    if join.join_type not in (JoinType.LEFT_OUTER, JoinType.RIGHT_OUTER):
        return None
    null_tests = _null_tested_addresses(condition)
    if not null_tests:
        return None
    right = join.right
    left = next((source for source in final_datasets if source is not right), None)
    if left is None:
        return None
    nullable_side, preserved_side = (
        (right, left) if join.join_type == JoinType.LEFT_OUTER else (left, right)
    )
    nullable_addresses = {
        concept.address for concept in nullable_side.nullable_concepts
    }
    intrinsic_outputs = {
        concept.address for concept in nullable_side.output_concepts
    } - nullable_addresses
    if not (null_tests & intrinsic_outputs):
        return None
    return preserved_side.effective_grain


def non_null_proofs(
    condition: BoolExpr,
) -> set[str]:
    """Concept addresses that this condition forces non-null in surviving rows.

    Logical-stage analysis: only descends through null-propagating operators,
    not ``IS NOT NULL``. The merge-stage caller can't see how merged join keys
    will materialize as ``COALESCE(left.k, right.k)`` at SQL time, so honoring
    ``IS NOT NULL`` here would claim a shared key non-null on either side
    individually. The post-CTE ``DowngradeFullJoinOnGuards`` pass operates on
    materialized SQL and can safely honor the fuller form.
    """
    proofs: set[str] = set()
    for atom in decompose_condition(condition):
        if isinstance(atom, BuildParenthetical):
            if isinstance(
                atom.content,
                BoolExpr,
            ):
                proofs |= non_null_proofs(atom.content)
            continue
        if not isinstance(atom, BuildComparison):
            continue
        if atom.operator in NULL_PROPAGATING_OPS:
            proofs |= concepts_implied_non_null(atom.left)
            proofs |= concepts_implied_non_null(atom.right)
        else:
            proofs |= membership_proves_non_null(atom)
    return proofs


def _source_concept_for_address(
    source: GrainSource,
    address: str,
) -> BuildConcept | None:
    return next(
        (
            concept
            for concept in source.output_concepts
            if address in concept.equivalent_addresses
        ),
        None,
    )


def _concept_coverage_addresses(
    concept: BuildConcept, include_aggregate_by_keys: bool = True
) -> set[str]:
    addresses = set(concept.equivalent_addresses)
    # Aggregate by-keys are one-way: rows at the by-grain can be ROLLED UP to
    # produce the aggregate, but the aggregate's grain is coarser. Include them
    # when checking materialization paths; exclude when asking "is upstream
    # already at this grain": a value of its keys does not determine them
    # (`SelectGrain.by_keys_split_tuple`). A rowset handle over an aggregate
    # does anchor its rows, at the handle's own grain (`bought.amt` at
    # `{bought.city, bought.ticket, customer.sk}`).
    if (
        (
            (include_aggregate_by_keys and concept.is_aggregate)
            or (
                isinstance(concept.lineage, BuildRowsetItem)
                and concept.lineage.content.is_aggregate
            )
        )
        and concept.grain
        and not concept.grain.abstract
    ):
        addresses.update(concept.grain.components)
    if concept.derivation == Derivation.MULTISELECT and concept.keys:
        addresses.update(concept.keys)
    return addresses


def concept_source_address(concept: BuildConcept) -> str:
    if concept.derivation == Derivation.ROWSET and isinstance(
        concept.lineage, BuildRowsetItem
    ):
        return concept.lineage.content.address
    if concept.derivation == Derivation.FILTER and isinstance(
        concept.lineage, BuildFilterItem
    ):
        content = concept.lineage.content
        if isinstance(content, BuildConcept):
            return content.address
    # A pure rename is a 1:1 relabel, so for grain purposes it lives at the
    # source concept's grain. Recurse, since the renamed source may itself be
    # a rowset/filter output; otherwise a renamed projection of union outputs
    # looks like extra grain and forces a GROUP BY that drops UNION ALL rows.
    if (
        concept.derivation == Derivation.BASIC
        and isinstance(concept.lineage, BuildFunction)
        and concept.lineage.operator == FunctionType.ALIAS
        and len(concept.lineage.concept_arguments) == 1
    ):
        return concept_source_address(concept.lineage.concept_arguments[0])
    return concept.address


def rowset_source_grain(
    grain: BuildGrain,
    environment: BuildEnvironment,
    padded: frozenset[str] = frozenset(),
) -> BuildGrain:
    concepts = [
        (
            address
            if address in padded
            else concept_source_address(environment.concepts[address])
        )
        for address in grain.components
    ]
    return BuildGrain.from_concepts(concepts, environment=environment)


def _applied_atoms(source: GrainSource) -> set[str]:
    condition = getattr(source, "condition", None)
    return (
        {str(atom) for atom in decompose_condition(condition)} if condition else set()
    )


def padded_filters(
    grain: BuildGrain,
    parents: Iterable[GrainSource],
    environment: BuildEnvironment,
) -> frozenset[str]:
    """The filtered values of `grain` whose condition no parent applied to its
    rows: NULL on every row the condition rejects, so they name none of them
    (`Filters.rejected_rows_share_one_null`). `tree_id ? c` beside `city` is
    one NULL row per city, however many trees `c` rejects."""
    applied = [_applied_atoms(parent) for parent in parents]
    padded = set()
    for address in grain.components:
        concept = environment.concepts[address]
        if concept.derivation != Derivation.FILTER or not isinstance(
            concept.lineage, BuildFilterItem
        ):
            continue
        atoms = {
            str(atom) for atom in decompose_condition(concept.lineage.where.conditional)
        }
        if not any(atoms <= held for held in applied):
            padded.add(address)
    return frozenset(padded)


def _grain_coverage_addresses(
    grain: BuildGrain,
    environment: BuildEnvironment,
    include_aggregate_by_keys: bool = True,
    padded: frozenset[str] = frozenset(),
) -> set[str]:
    addresses: set[str] = set()
    for candidate in (grain, rowset_source_grain(grain, environment, padded)):
        for address in candidate.components:
            concept = environment.concepts[address]
            addresses.update(
                _concept_coverage_addresses(
                    concept, include_aggregate_by_keys=include_aggregate_by_keys
                )
            )
    # Follow each covered address to its pseudonyms: a MULTISELECT align alias
    # expands to its keys, which are often themselves aliases of the underlying
    # column. Without this second hop a pregrain carrying the source column
    # looks like extra grain and forces a spurious group.
    for address in list(addresses):
        equivalent = environment.concepts.get(address)
        if equivalent:
            addresses.update(equivalent.equivalent_addresses)
    return addresses


def _concept_covered_by_grain(
    concept: BuildConcept,
    grain: BuildGrain,
    environment: BuildEnvironment,
) -> bool:
    return bool(
        _concept_coverage_addresses(concept)
        & _grain_coverage_addresses(grain, environment)
    )


def _right_join_keys(join: Join) -> list[BuildConcept]:
    return [pair.right for pair in join.pairs] if join.pairs else join.concepts or []


def _join_right_preserves_cardinality(
    join: SourceJoin | UnnestJoin,
    environment: BuildEnvironment,
) -> bool:
    if not isinstance(join, Join):
        return False
    if join.join_type in (JoinType.FULL, JoinType.RIGHT_OUTER, JoinType.CROSS):
        return False
    if not join.pairs and not join.concepts:
        return False

    right_grain = join.right.effective_grain
    if not right_grain.components:
        return True
    materialized_keys = [
        _source_concept_for_address(join.right, key.address) or key
        for key in _right_join_keys(join)
    ]
    coverage: set[str] = set()
    for key in materialized_keys:
        coverage.update(_concept_coverage_addresses(key))
    if right_grain.components.issubset(coverage):
        return True
    # FD closure (docs/domain_graph_design.md step 4): grain components the
    # join keys functionally determine admit at most one right row per key
    # tuple, so cardinality is preserved even though the components are not
    # among the keys.
    graph = environment.domain_graph
    if not graph.fd_edges:
        return False
    determinants = coverage | {key.address for key in materialized_keys}
    return all(
        graph.determines(determinants, component)
        for component in right_grain.components - coverage
    )


def _join_left_keys_covered_by_grain(
    join: SourceJoin | UnnestJoin,
    grain: BuildGrain,
    environment: BuildEnvironment,
) -> bool:
    if not isinstance(join, Join):
        return False
    if join.pairs:
        left_keys = [
            _source_concept_for_address(pair.node, pair.left.address) or pair.left
            for pair in join.pairs
        ]
    elif join.concepts:
        left_keys = [
            (
                _source_concept_for_address(join.left, concept.address)
                if join.left
                else None
            )
            or concept
            for concept in join.concepts
        ]
    else:
        return False
    return all(
        _concept_covered_by_grain(concept, grain, environment) for concept in left_keys
    )


def _join_right_grain_can_be_omitted(
    join: SourceJoin | UnnestJoin,
    grain: BuildGrain,
    environment: BuildEnvironment,
) -> bool:
    return _join_right_preserves_cardinality(
        join, environment
    ) and _join_left_keys_covered_by_grain(join, grain, environment)


def _datasource_addresses(source: GrainSource) -> set[str]:
    return {concept.address for concept in source.output_concepts}


def _left_join_sources(
    join: SourceJoin,
    final_datasets: list[GrainSource],
) -> list[GrainSource]:
    sources = join_left_sources(join)
    if sources:
        return sources
    return [
        source
        for source in final_datasets
        if source.identifier != join.right.identifier
    ]


def _left_join_addresses(
    join: SourceJoin,
    final_datasets: list[GrainSource],
) -> set[str]:
    return {
        address
        for source in _left_join_sources(join, final_datasets)
        for address in _datasource_addresses(source)
    }


def _partial_addresses(source: GrainSource) -> set[str]:
    return {concept.address for concept in source.partial_concepts}


def _partial_lookup_keys(sources: list[GrainSource], keys: set[str]) -> set[str]:
    """The `~` keys of a side that is a lookup keyed by them (notes by
    customer): the members it lacks are the other side's own rows, not
    extension rows, so a non-null key proves no match there."""
    out: set[str] = set()
    for source in sources:
        grain = set(source.grain.components)
        partial = keys & _partial_addresses(source)
        if partial and grain and grain <= keys:
            out |= partial
    return out


def _unprovable_addresses(sources: list[GrainSource]) -> set[str]:
    """Addresses a non-null proof cannot force a side through: partial
    bindings (the value may genuinely be absent from this side) and opaque
    bindings (a CASE/raw column that is non-null even on padded rows)."""
    out: set[str] = set()
    for source in sources:
        out |= _partial_addresses(source)
        out |= opaque_binding_addresses(source)
    return out


@dataclass(frozen=True)
class JoinProofs:
    """What the merge knows, before its joins are typed, about which sides
    every surviving row must match.

    ``proofs`` (this node's own WHERE, null-propagating atoms only) and
    ``branch_proofs`` (filters applied inside its branches) are the
    conservative harvest a FULL needs: a FULL's merged key renders as a
    cross-side COALESCE, so ``IS NOT NULL`` on it proves neither side.
    ``side_proofs`` and ``or_groups`` are the full harvest (``IS NOT NULL``,
    ``BETWEEN``, OR-of-ANDs) for a LEFT/RIGHT, whose padded side is one
    materialized relation. ``filtered_ids`` are the sources that applied an
    atom of the request WHERE this merge does not re-render, so each final
    row must have a match there; ``coalescing_keys`` are authored union/full
    relation keys whose preserving typing is row intent and stands."""

    proofs: set[str] = field(default_factory=set)
    branch_proofs: set[str] = field(default_factory=set)
    side_proofs: set[str] = field(default_factory=set)
    or_groups: list[list[set[str]]] = field(default_factory=list)
    filtered_ids: set[str] = field(default_factory=set)
    coalescing_keys: set[str] = field(default_factory=set)


@dataclass(frozen=True)
class _PartnerFacts:
    """What one side of a join knows that decides whether the other side is
    the WHERE's population."""

    partial: frozenset[str] = frozenset()
    regions: frozenset[str] = frozenset()
    filtered_regions: frozenset[str] = frozenset()
    nullable: frozenset[str] = frozenset()
    identifiers: frozenset[str] = frozenset()
    # nullable keys of a side that applied the WHERE: its NULL-keyed rows passed
    filtered_nullable: frozenset[str] = frozenset()
    # filter atoms every source of the side applied
    applied: tuple[BoolExpr, ...] = ()


def _partner_facts(
    sources: Iterable[GrainSource], filtered_ids: set[str]
) -> _PartnerFacts:
    partial: set[str] = set()
    regions: set[str] = set()
    filtered_regions: set[str] = set()
    nullable: set[str] = set()
    identifiers: set[str] = set()
    filtered_nullable: set[str] = set()
    applied: list[BoolExpr] | None = None
    for source in sources:
        identifiers.add(source.identifier)
        atoms = applied_atoms(source)
        applied = (
            atoms
            if applied is None
            else [a for a in applied if any(a == b for b in atoms)]
        )
        partial |= _partial_addresses(source)
        nullable |= _nullable_addresses(source)
        spans = held_region_spans(source)
        regions |= spans
        if source.identifier in filtered_ids:
            filtered_regions |= spans
            filtered_nullable |= _nullable_addresses(source)
    return _PartnerFacts(
        frozenset(partial),
        frozenset(regions),
        frozenset(filtered_regions),
        frozenset(nullable),
        frozenset(identifiers),
        frozenset(filtered_nullable),
        tuple(applied or ()),
    )


def _applies_all_of(source: GrainSource, partner: _PartnerFacts) -> bool:
    """The partner applied every filter `source` did, so its rows passed
    whatever narrowed `source`'s."""
    atoms = applied_atoms(source)
    return bool(atoms) and all(any(a == b for b in partner.applied) for a in atoms)


def _reads_partner(source: GrainSource, partner: _PartnerFacts) -> bool:
    """This side filters the partner's own row stream: it holds every member
    the partner has, a NULL-keyed one included, less what the WHERE rejected,
    and the null-safe pairing finds each of them."""
    return isinstance(source, QueryDatasource) and any(
        parent.identifier in partner.identifiers for parent in source.datasources
    )


def _is_filter_population(
    identifier: str,
    by_id: dict[str, GrainSource],
    filtered_ids: set[str],
    join_addresses: set[str],
    partner: _PartnerFacts,
) -> bool:
    """Whether this side's row set IS the request WHERE's population.

    It has to have applied the WHERE, and it must not owe its narrowness to
    anything else. A side joined on a region's span to a partner holding that
    region's rows covers only the members its facts bound: a row missing there
    is a member nobody referenced, not a row the WHERE rejected, and the
    partner stays preserved. The same for an extent-free branch
    (docs/extent_ownership.md) and what the span's region domain carries (the
    names of customers WITH an order), which only matters when the other side
    binds the axis complete; a partner partial on it carries no extension
    member to preserve. A partner that applied the WHERE itself and holds a
    region's rows this side lacks keeps them on any key: they passed the
    WHERE there, and are NULL on a key absent on the region (`count(customer_id)
    by status where name = 'cat'` beside `sum(amount) by status`). A partner
    whose key is NULL on some row holds a member this side never has, whatever
    made it NULL (a `?` binding, or a join below that padded it): no key pairs
    with it, so its row passed the WHERE unless what this side applied rejects
    the all-NULL row it pads (`where customer_id is null` keeps the order with
    no customer, `where state = 'GA'` drops it), unless this side filtered the
    partner's own rows and so has that member too (`_reads_partner`). Hence
    the raw `nullable_concepts`, not their provenance."""
    if identifier not in filtered_ids:
        return False
    source = by_id.get(identifier)
    if source is None:
        return True
    if (
        join_addresses & partner.filtered_nullable
        and not _rejects_padding(source)
        and not _reads_partner(source, partner)
    ):
        return False
    # A `~` side lacks members whatever the WHERE says: a WHERE on the key
    # alone (`cust_id != 10`), or one the partner applied too (`city = 'A'`
    # on the trees beside the trees' unnested lists), filters both sides
    # alike, so its narrowness is the `~`, not the WHERE.
    partial = _partial_addresses(source)
    if (join_addresses & partial) - partner.partial and (
        _filters_only_keys(source, join_addresses) or _applies_all_of(source, partner)
    ):
        return False
    held = held_region_spans(source)
    if join_addresses & partner.regions and not join_addresses & held:
        return False
    if partner.filtered_regions - held:
        return False
    suppressed = partial & (
        deep_extent_free_spans(source) | deep_extent_free_carried(source)
    )
    if not (join_addresses & suppressed):
        return True
    return bool(join_addresses & partner.partial)


def _join_key_addresses(join: SourceJoin) -> tuple[set[str], set[str]]:
    if join.pairs:
        return (
            {pair.left.address for pair in join.pairs},
            {pair.right.address for pair in join.pairs},
        )
    keys = {concept.address for concept in join.concepts or []}
    return keys, set(keys)


def _side_forced(
    proofs: set[str],
    or_groups: list[list[set[str]]],
    side_only: set[str],
    keys: set[str],
    unprovable: set[str],
) -> bool:
    """A side is forced present when a proof names a column NULL exactly on
    its padded rows: a side-only address (under every disjunct of an OR, for
    a side-level proof), or the side's whole key tuple (complete on it, so
    every non-null key value has a match there)."""
    provable = side_only - unprovable
    if proofs & provable:
        return True
    if any(all(disjunct & provable for disjunct in group) for group in or_groups):
        return True
    return bool(keys) and keys <= proofs and not keys & unprovable


def _exclusive_addresses(
    sources: list[GrainSource], final_datasets: list[GrainSource]
) -> set[str]:
    """Addresses only ``sources`` hold among the merge's datasets: each renders
    off this side alone, never a coalesce with a later join's side."""
    ids = {source.identifier for source in sources}
    others = {
        address
        for source in final_datasets
        if source.identifier not in ids
        for address in _datasource_addresses(source)
    }
    return {
        address for source in sources for address in _datasource_addresses(source)
    } - others


def _stream_sources(
    join: SourceJoin, final_datasets: list[GrainSource], stream: frozenset[str]
) -> list[GrainSource]:
    """Every source joined before ``join``: a padded left row is NULL on all
    of them, not only on the ones its ON clause reads."""
    named = {source.identifier for source in join_left_sources(join)}
    return [
        source
        for source in final_datasets
        if source.identifier in stream or source.identifier in named
    ]


def downgrade_join_for_proofs(
    join: SourceJoin | UnnestJoin,
    proofs: set[str],
    final_datasets: list[GrainSource],
    side_proofs: set[str] | None = None,
    or_groups: list[list[set[str]]] | None = None,
    stream: frozenset[str] = frozenset(),
) -> None:
    """Narrow a FULL when ``proofs`` (concepts forced non-null in every
    surviving row) rule out the padded rows it preserves: only the side
    whose proof holds is kept, both forced is INNER. A side-only column
    bound partially or opaquely proves nothing (``_unprovable_addresses``):
    a value the region domain carries is partial on the solid stream, and a
    WHERE over it keeps the domain's rows. The key tuple still forces a
    side through a `~` key: a span the plan does not extend has no
    extension row to keep. A `~` lookup keyed by the span is not forced:
    the rows it lacks are the other side's (`Rewrite.key_proof_keeps_lookup_gap`).

    ``side_proofs`` and ``or_groups`` are the full harvest (``IS NOT NULL``
    included). They prove a side only through a column no other dataset of
    the merge holds: such a column renders off that side alone, so no
    coalesce masks its padded NULL."""
    if not isinstance(join, Join):
        return
    side_proofs = side_proofs or set()
    or_groups = or_groups or []
    if join.join_type != JoinType.FULL or not (proofs or side_proofs or or_groups):
        return
    left_keys, right_keys = _join_key_addresses(join)
    left_all = _left_join_addresses(join, final_datasets)
    right_all = _datasource_addresses(join.right)
    left_sources = _left_join_sources(join, final_datasets)
    left_unprovable = _unprovable_addresses(left_sources)
    right_unprovable = _unprovable_addresses([join.right])
    left_only = (left_all - right_all) - left_unprovable
    right_only = (right_all - left_all) - right_unprovable
    streamed = _stream_sources(join, final_datasets, stream)
    left_exclusive = _exclusive_addresses(streamed, final_datasets)
    right_exclusive = _exclusive_addresses([join.right], final_datasets)
    left_forced = _side_forced(
        proofs, [], left_only, left_keys, _partial_lookup_keys(left_sources, left_keys)
    ) or _side_forced(
        side_proofs | proofs,
        or_groups,
        left_exclusive,
        set(),
        _unprovable_addresses(streamed),
    )
    right_forced = _side_forced(
        proofs,
        [],
        right_only,
        right_keys,
        _partial_lookup_keys([join.right], right_keys),
    ) or _side_forced(
        side_proofs | proofs, or_groups, right_exclusive, set(), right_unprovable
    )
    join.join_type = unpadded_join_type(join.join_type, left_forced, right_forced)


def downgrade_directional_join_for_proofs(
    join: SourceJoin | UnnestJoin,
    proofs: set[str],
    or_groups: list[list[set[str]]],
    final_datasets: list[GrainSource],
    stream: frozenset[str] = frozenset(),
) -> None:
    """A LEFT/RIGHT whose padded side is forced present by ``proofs`` keeps
    no padded row, so it is INNER. Partial and opaque bindings on the padded
    side prove nothing (``_unprovable_addresses``)."""
    if not isinstance(join, Join):
        return
    if join.join_type not in (JoinType.LEFT_OUTER, JoinType.RIGHT_OUTER):
        return
    if not proofs and not or_groups:
        return
    left_keys, right_keys = _join_key_addresses(join)
    left_all = _left_join_addresses(join, final_datasets)
    right_all = _datasource_addresses(join.right)
    padded_only, padded_keys, padded_sources = (
        (right_all - left_all, right_keys, [join.right])
        if join.join_type == JoinType.LEFT_OUTER
        else (left_all - right_all, left_keys, _left_join_sources(join, final_datasets))
    )
    if join.join_type == JoinType.RIGHT_OUTER:
        padded_sources = _stream_sources(join, final_datasets, stream)
        padded_only |= _exclusive_addresses(padded_sources, final_datasets)
    if _side_forced(
        proofs,
        or_groups,
        padded_only,
        padded_keys,
        _unprovable_addresses(padded_sources),
    ):
        join.join_type = JoinType.INNER


def _nullable_addresses(source: GrainSource) -> frozenset[str]:
    return frozenset(c.address for c in source.nullable_concepts)


def _rejects_padding(source: GrainSource) -> bool:
    """Whether a condition applied in `source` fails on a row where all of its
    columns are NULL."""
    return any(
        gather_non_null_proofs(condition) or gather_or_groups(condition)
        for condition in collect_applied_conditions(source)
    )


def _filters_only_keys(source: GrainSource, keys: set[str]) -> bool:
    """Whether every condition applied in `source` reads only the join `keys`,
    which the other side holds too: it rejects none of `source`'s own rows."""
    conditions = collect_applied_conditions(source)
    return bool(conditions) and all(
        isinstance(condition, BuildConceptArgs)
        and {c.address for c in condition.row_arguments} <= keys
        for condition in conditions
    )


def _joins_a_coalescing_relation(join: SourceJoin, coalescing_keys: set[str]) -> bool:
    """The join is an authored union/full relation's own pairing: it crosses
    the relation's two endpoints, or names a member with no pairing to read.
    Two sources of ONE endpoint (a filtered scan beside a value keyed on the
    same key) are not the relation, nor are two sides that each already hold
    both members (a filtered copy of the paired stream joined back to it):
    the relation was paired below them."""
    pairs = join.pairs or []
    if not pairs:
        return bool({c.address for c in join.concepts or []} & coalescing_keys)
    held = [
        _datasource_addresses(source)
        for source in (*join_left_sources(join), join.right)
    ]
    return any(
        pair.left.address != pair.right.address
        and {pair.left.address, pair.right.address} & coalescing_keys
        and not all({pair.left.address, pair.right.address} <= h for h in held)
        for pair in pairs
    )


def tighten_join_for_filtered_branch(
    join: SourceJoin | UnnestJoin,
    filtered_ids: set[str],
    coalescing_keys: set[str],
    by_id: dict[str, GrainSource],
    stream: frozenset[str] = frozenset(),
) -> None:
    """A side that IS the request WHERE's population must match every final
    row, so a join that null-extends it resurrects rows the WHERE rejected.
    Authored coalescing relations keep their preserving typing. `stream` is
    everything joined before: a region an earlier join brought in holds rows
    the right side never has (a bucket no order references, beside a total
    per customer), whichever side the keys are read off."""
    if not isinstance(join, Join) or not filtered_ids:
        return
    join_addresses = {
        address
        for pair in join.pairs or []
        for address in (pair.left.address, pair.right.address)
    } | {concept.address for concept in join.concepts or []}
    if _joins_a_coalescing_relation(join, coalescing_keys):
        return
    left_ids = {source.identifier for source in join_left_sources(join)}
    left = _partner_facts(
        (by_id[identifier] for identifier in left_ids if identifier in by_id),
        filtered_ids,
    )
    held = _partner_facts(
        (by_id[identifier] for identifier in stream if identifier in by_id),
        filtered_ids,
    )
    left = replace(
        left,
        regions=left.regions | held.regions,
        filtered_regions=left.filtered_regions | held.filtered_regions,
    )
    right = join.right
    right_filtered = _is_filter_population(
        right.identifier, by_id, filtered_ids, join_addresses, left
    )
    right_facts = _partner_facts([right], filtered_ids)
    left_filtered = any(
        _is_filter_population(
            identifier, by_id, filtered_ids, join_addresses, right_facts
        )
        for identifier in left_ids
    )
    join.join_type = unpadded_join_type(join.join_type, left_filtered, right_filtered)


def _binds_subset(source: GrainSource, address: str) -> bool:
    """`source` binds `address` as a `~` subset with no NULL key."""
    return address in _partial_addresses(source) - _nullable_addresses(source)


def narrow_subset_full_join(
    join: SourceJoin | UnnestJoin,
    alive: set[str],
    padded: set[str],
    coalescing_keys: set[str],
    prior: Sequence[SourceJoin | UnnestJoin] = (),
) -> None:
    """A FULL pads only one side when the other is `~` on every key and each
    of its rows has a partner holding the key's whole domain
    (`complete_key_domain`). A `~` right pairs with a left source whose every
    row survives (`alive`); a `~` left source pairs on every stream row only
    when no earlier join padded it. So the narrowing does not depend on which
    side the walk joined first. A NULL key (`~?`) has no partner, and an
    authored union/full relation is row intent: both stay FULL.

    A complete right beside a `~` left is not padded either when a surviving
    left equated with that `~` one holds the key's whole domain: the FULL was
    typed off the `~` partner, walked before the complete left
    (`_complete_anchor`). When that left is on every stream row, non-NULL,
    every stream row has a partner too, and the join is INNER."""
    if not isinstance(join, Join) or join.join_type != JoinType.FULL:
        return
    if not join.pairs or _joins_a_coalescing_relation(join, coalescing_keys):
        return
    anchor = _complete_anchor(join, alive, prior)
    if anchor is not None and _on_every_row(anchor, padded):
        join.join_type = JoinType.INNER
    elif anchor is not None or all(
        _binds_subset(join.right, pair.right.address)
        and pair.node.identifier in alive
        and complete_key_domain(pair.node, pair.left.address)
        for pair in join.pairs
    ):
        join.join_type = JoinType.LEFT_OUTER
    elif all(
        _binds_subset(pair.node, pair.left.address)
        and pair.node.identifier not in padded
        and complete_key_domain(join.right, pair.right.address)
        for pair in join.pairs
    ):
        join.join_type = JoinType.RIGHT_OUTER


def _complete_anchor(
    join: Join, alive: set[str], prior: Sequence[SourceJoin | UnnestJoin]
) -> ConceptPair | None:
    """For a join on one key, from a complete right onto a `~` left, the
    surviving complete left an earlier join equated with every other left.
    Both complete sides hold the key's whole domain, so every right row has
    a partner, and the ON reads the coalesce of the lefts, which on any
    stream row is the complete left's value or NULL beside it."""
    if len({pair.right.address for pair in join.pairs}) != 1:
        return None
    address = join.pairs[0].right.address
    if not complete_key_domain(join.right, address) or address in (
        _nullable_addresses(join.right)
    ):
        return None
    if not any(
        pair.left.address in _partial_addresses(pair.node) for pair in join.pairs
    ):
        return None
    return next(
        (
            anchor
            for anchor in join.pairs
            if anchor.node.identifier in alive
            and complete_key_domain(anchor.node, anchor.left.address)
            and all(
                other is anchor or _equated_by(prior, anchor, other)
                for other in join.pairs
            )
        ),
        None,
    )


def _on_every_row(pair: ConceptPair, padded: set[str]) -> bool:
    """No earlier join padded the left, and its key is never NULL."""
    return pair.node.identifier not in padded and pair.left.address not in (
        _nullable_addresses(pair.node)
    )


def _equated_by(
    prior: Sequence[SourceJoin | UnnestJoin], a: ConceptPair, b: ConceptPair
) -> bool:
    """Whether an earlier join paired the two lefts' reads directly."""
    reads = {(a.node.identifier, a.left.address), (b.node.identifier, b.left.address)}
    return any(
        {
            (pair.node.identifier, pair.left.address),
            (j.right.identifier, pair.right.address),
        }
        == reads
        for j in prior
        if isinstance(j, Join)
        for pair in j.pairs or []
    )


def narrow_join_types(
    joins: list[SourceJoin | UnnestJoin],
    proofs: JoinProofs,
    final_datasets: list[GrainSource],
) -> None:
    """The planner's one narrowing decision over freshly typed joins: the
    preserving form ``get_join_type`` chose is kept only where no surviving
    row is already proven to match the preserved side."""
    by_id = {source.identifier: source for source in final_datasets}
    streams = {id(join): left for join, left in left_deep_joins(joins)}
    padded: set[str] = set()
    for index, join in enumerate(joins):
        stream = streams.get(id(join), frozenset())
        narrow_subset_full_join(
            join,
            preserved_sources(list(final_datasets), joins[:index]),
            padded,
            proofs.coalescing_keys,
            joins[:index],
        )
        downgrade_join_for_proofs(
            join,
            proofs.proofs,
            final_datasets,
            proofs.side_proofs,
            proofs.or_groups,
            stream,
        )
        downgrade_join_for_proofs(join, proofs.branch_proofs, final_datasets)
        tighten_join_for_filtered_branch(
            join,
            proofs.filtered_ids,
            proofs.coalescing_keys,
            by_id,
            stream,
        )
        if isinstance(join, Join):
            padded |= padded_by(join, stream)
    narrow_joins_read_by_inner(joins)


def narrow_directional_join_types(
    joins: list[SourceJoin | UnnestJoin],
    proofs: JoinProofs,
    final_datasets: list[GrainSource],
) -> None:
    """Second narrowing step, after the directional joins' key pairs were
    pruned to their preserved source: a LEFT/RIGHT whose padded side the
    full harvest forces present is INNER."""
    streams = {id(join): left for join, left in left_deep_joins(joins)}
    for join in joins:
        downgrade_directional_join_for_proofs(
            join,
            proofs.side_proofs,
            proofs.or_groups,
            final_datasets,
            streams.get(id(join), frozenset()),
        )
    narrow_joins_read_by_inner(joins, inner_only=True)


def _pair_source_id(pair: ConceptPair) -> str:
    return pair.node.identifier


def _inner_rejected_sources(join: SourceJoin) -> set[str]:
    """Sources a plain INNER equality of ``join`` reads a key column of: a
    NULL never matches ``=``, so a row padded on that source is dropped. A
    coalesced left (several sources of one key) rejects none of them alone,
    and an opaque binding renders non-null on padded rows."""
    if join.join_type != JoinType.INNER:
        return set()
    out: set[str] = set()
    for pairs in coalesced_key_groups(join.pairs or [], _pair_source_id):
        if any(pair_matches_nulls(pair, join.modifiers) for pair in pairs):
            continue
        out.add(join.right.identifier)
        if len({pair.node.identifier for pair in pairs}) > 1:
            continue
        pair = pairs[0]
        if pair.left.address not in opaque_binding_addresses(pair.node):
            out.add(pair.node.identifier)
    return out


def narrow_joins_read_by_inner(
    joins: list[SourceJoin | UnnestJoin], inner_only: bool = False
) -> None:
    """An outer join whose padded side a later INNER equality reads keeps no
    padded row: the NULL key matches nothing there. Narrowing one join to
    INNER can prove an earlier one, so this runs to a fixpoint. Once the
    merge has ordered its sources by padding, ``inner_only`` keeps a FULL
    from changing which side it preserves."""
    changed = True
    while changed:
        changed = False
        rejected: set[str] = set()
        for join, stream in reversed(left_deep_joins(joins)):
            new = unpadded_join_type(
                join.join_type,
                bool(stream & rejected),
                join.right.identifier in rejected,
            )
            if inner_only and new != JoinType.INNER:
                new = join.join_type
            if new != join.join_type:
                join.join_type = new
                changed = True
            rejected |= _inner_rejected_sources(join)


# Outputs whose value is computed by the grouping itself: a grouping that
# emits one of them is never a pure identity over its parent rows.
GROUP_COMPUTED_DERIVATIONS = frozenset(
    {
        Derivation.AGGREGATE,
        Derivation.WINDOW,
        Derivation.UNNEST,
        Derivation.RECURSIVE,
    }
)


def join_preserves_left_rows(join: Join) -> bool:
    """A lookup join: INNER/LEFT onto a side whose whole grain the join keys
    cover adds no rows to the left stream."""
    join_type = join.join_type
    right = join.right.source if isinstance(join.right, (CTE, UnionCTE)) else join.right
    if join_type not in (JoinType.INNER, JoinType.LEFT_OUTER):
        return False
    right_grain = set(right.grain.components)
    if not right_grain:
        return True
    return right_grain <= equivalent_addresses(_right_join_keys(join))


def _only_lookup_joins(joins: Iterable[SourceJoin | UnnestJoin]) -> bool:
    return all(
        isinstance(join, Join) and join_preserves_left_rows(join) for join in joins
    )


def unique_at_declared_grain(source: GrainSource) -> bool:
    """Whether ``source`` emits at most one row per its own declared grain.

    A datasource's ``grain(...)`` is a uniqueness contract, and a GROUP
    re-establishes uniqueness. Everything else passes its inputs' rows
    through, so it only holds the contract when every row-feeding input is
    itself unique at a grain the declaration already covers: a projection
    over a finer-grained input declares the grain it is heading for, not one
    it has reached, and a UNION stack concatenates its arms so a key in two
    arms arrives twice. Lookup joins neither add rows nor need their grain
    covered."""
    if isinstance(source, BuildDatasource):
        return True
    if source.source_type == SourceType.GROUP:
        return True
    if source.source_type == SourceType.UNION:
        return False
    if not _only_lookup_joins(source.joins):
        return False
    looked_up = joined_right_ids(source.joins)
    declared = set(source.grain.components)
    return all(
        unique_at_declared_grain(sub) and set(sub.grain.components) <= declared
        for sub in source.datasources
        if sub.identifier not in looked_up
    )


def stacks_duplicate_rows(source: GrainSource) -> bool:
    """Whether ``source`` can emit repeated rows at its own declared grain: a
    UNION stack does (its grouping establishes the grain rather than
    restating it), a GROUP never does, anything else passes its inputs'
    duplicates through."""
    if not isinstance(source, QueryDatasource):
        return False
    if source.source_type == SourceType.UNION:
        return True
    if source.source_type == SourceType.GROUP:
        return False
    return any(stacks_duplicate_rows(sub) for sub in source.datasources)


def is_identity_group(
    datasets: list[GrainSource],
    joins: list[SourceJoin | UnnestJoin],
    target_grain: BuildGrain,
    condition: BoolExpr | None,
    output_concepts: list[BuildConcept],
    rollup_concepts: list[BuildConcept],
) -> bool:
    """Grouping these joined sources to ``target_grain`` would keep every
    row: one row-feeding root, already unique at a grain the target covers,
    joined only to lookups, computing no aggregate of its own, under at most
    a scalar WHERE."""
    if rollup_concepts:
        return False
    right_ids = joined_right_ids(joins)
    roots = [source for source in datasets if source.identifier not in right_ids]
    if len(roots) != 1:
        return False
    root = roots[0]
    if not unique_at_declared_grain(root):
        return False
    if not set(root.grain.components) <= set(target_grain.components):
        return False
    if not _only_lookup_joins(joins):
        return False
    supplied = {
        concept.address for source in datasets for concept in source.output_concepts
    }
    if any(
        concept.derivation in GROUP_COMPUTED_DERIVATIONS
        and concept.address not in supplied
        for concept in output_concepts
    ):
        return False
    return condition is None or is_scalar_condition(condition, materialized=supplied)


def calculate_joined_pregrain(
    final_datasets: list[GrainSource],
    joins: list[SourceJoin | UnnestJoin],
    grain: BuildGrain,
    environment: BuildEnvironment,
) -> BuildGrain:
    cardinality_preserved = {
        join.right.identifier
        for join in joins
        if isinstance(join, Join)
        and _join_right_grain_can_be_omitted(join, grain, environment)
    }
    output = BuildGrain()
    for source in final_datasets:
        if source.identifier in cardinality_preserved:
            continue
        output += source.effective_grain
    return output


def grain_satisfied_by_pregrain(
    pregrain: BuildGrain,
    grain: BuildGrain,
    environment: BuildEnvironment,
) -> bool:
    # The grain, its rowset source grain and their pseudonyms: a MULTISELECT
    # align identity covers its source keys, else a pregrain carrying them
    # looks like extra grain.
    coverage = _grain_coverage_addresses(grain, environment)
    if pregrain.components.issubset(coverage):
        return True
    # FD closure: a pregrain component the grain functionally determines is
    # constant within each group, so grouping by {grain, component} reduces
    # to {grain} and the pregrain is satisfied without regrouping.
    graph = environment.domain_graph
    if not graph.fd_edges:
        return False
    return all(
        graph.determines(coverage, component)
        for component in pregrain.components - coverage
    )


def nullable_spellings(nullable: Iterable[BuildConcept]) -> set[str]:
    """A key NULL on some row is NULL there under every one of its spellings."""
    return equivalent_addresses(nullable)


def determined_past_nulls(
    environment: BuildEnvironment,
    coverage: set[str],
    nullable: set[str],
    components: Iterable[str],
) -> bool:
    """Every component is covered, or determined by covered columns non-null
    on every row (`nullable` as `nullable_spellings`)."""
    from trilogy.core.processing.v4_helper.functional_dependency import (
        strict_fd_determines,
    )

    determinants = coverage - nullable
    return all(
        component in coverage
        or strict_fd_determines(environment, determinants, component)
        for component in components
    )


def rows_unique_at_outputs(
    joined: BuildGrain,
    outputs: list[BuildConcept],
    nullable: Iterable[BuildConcept],
    environment: BuildEnvironment,
) -> bool:
    """The joined rows are already one per output row: every row identity the
    stream carries is projected, or determined by projected columns non-null
    on every row. `joined` is the UNFOLDED union of the sources' grains: the
    key-hierarchy fold (`user.id` under `id`) holds only where the key is
    present, and after `users LEFT items FULL products` it is padded."""
    return determined_past_nulls(
        environment,
        equivalent_addresses(outputs),
        nullable_spellings(nullable),
        joined.components,
    )


def condition_key_grain(
    condition: BoolExpr | None,
    environment: BuildEnvironment,
) -> BuildGrain:
    if condition is None:
        return BuildGrain()
    return BuildGrain(
        components={
            address
            for address in non_null_proofs(condition)
            if environment.concepts[address].purpose == Purpose.KEY
        }
    )


def has_condition_key_outside_grain(
    condition: BoolExpr | None,
    grain: BuildGrain,
    environment: BuildEnvironment,
) -> bool:
    """A WHERE on a key the grain neither contains nor functionally determines
    reaches that key through a finer relation, so the merge must regroup. A key
    the grain determines (`city` under a `tree_id` grain) is constant per
    output row: the filter only removes rows and adds no grain."""
    condition_grain = condition_key_grain(condition, environment)
    if not condition_grain.components:
        return False
    return not grain_satisfied_by_pregrain(condition_grain, grain, environment)
