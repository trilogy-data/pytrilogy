from __future__ import annotations

from dataclasses import dataclass, field

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
    BuildDatasource,
    BuildFilterItem,
    BuildFunction,
    BuildGrain,
    BuildParenthetical,
    BuildRowsetItem,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.models.execute import BaseJoin, QueryDatasource, UnnestJoin
from trilogy.core.processing.condition_utility import (
    NULL_PROPAGATING_OPS,
    concepts_implied_non_null,
    decompose_condition,
    is_scalar_condition,
    opaque_binding_addresses,
)
from trilogy.core.processing.join_resolution import deep_extent_free_spans

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
    joins: list[BaseJoin | UnnestJoin],
    condition: BoolExpr | None,
) -> BuildGrain | None:
    """Grain of the preserved side of a proven two-source anti-join."""
    if len(final_datasets) != 2 or len(joins) != 1:
        return None
    join = joins[0]
    if not isinstance(join, BaseJoin):
        return None
    if join.join_type not in (JoinType.LEFT_OUTER, JoinType.RIGHT_OUTER):
        return None
    null_tests = _null_tested_addresses(condition)
    if not null_tests:
        return None
    right = join.right_datasource
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


def _concept_covers_grain(concept: BuildConcept, grain: BuildGrain) -> bool:
    if grain.components & concept.equivalent_addresses:
        return True
    return bool(
        concept.derivation == Derivation.MULTISELECT
        and concept.keys
        and grain.components.issubset(concept.keys)
    )


def _concept_coverage_addresses(
    concept: BuildConcept, include_aggregate_by_keys: bool = True
) -> set[str]:
    addresses = set(concept.equivalent_addresses)
    # Aggregate by-keys are one-way: rows at the by-grain can be ROLLED UP to
    # produce the aggregate, but the aggregate's grain is coarser. Include them
    # when checking materialization paths; exclude when asking "is upstream
    # already at this grain" (a regroup is still required).
    if (
        include_aggregate_by_keys
        and concept.is_aggregate
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
) -> BuildGrain:
    concepts = [
        concept_source_address(environment.concepts[address])
        for address in grain.components
    ]
    return BuildGrain.from_concepts(concepts, environment=environment)


def _grain_coverage_addresses(
    grain: BuildGrain,
    environment: BuildEnvironment,
    include_aggregate_by_keys: bool = True,
) -> set[str]:
    addresses: set[str] = set()
    for candidate in (grain, rowset_source_grain(grain, environment)):
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


def _join_right_preserves_cardinality(
    join: BaseJoin | UnnestJoin,
    environment: BuildEnvironment,
) -> bool:
    if not isinstance(join, BaseJoin):
        return False
    if join.join_type in (JoinType.FULL, JoinType.RIGHT_OUTER, JoinType.CROSS):
        return False
    if not join.concept_pairs and not join.concepts:
        return False

    right_grain = join.right_datasource.effective_grain
    if not right_grain.components:
        return True
    right_keys = (
        [pair.right for pair in join.concept_pairs]
        if join.concept_pairs
        else join.concepts or []
    )
    materialized_keys = [
        _source_concept_for_address(join.right_datasource, key.address) or key
        for key in right_keys
    ]
    coverage: set[str] = set()
    for key in materialized_keys:
        coverage.update(_concept_coverage_addresses(key))
    if right_grain.components.issubset(coverage) or any(
        _concept_covers_grain(key, right_grain) for key in materialized_keys
    ):
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
    join: BaseJoin | UnnestJoin,
    grain: BuildGrain,
    environment: BuildEnvironment,
) -> bool:
    if not isinstance(join, BaseJoin):
        return False
    if join.concept_pairs:
        left_keys = [
            _source_concept_for_address(pair.existing_datasource, pair.left.address)
            or pair.left
            for pair in join.concept_pairs
        ]
    elif join.concepts:
        left_keys = [
            (
                _source_concept_for_address(join.left_datasource, concept.address)
                if join.left_datasource
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
    join: BaseJoin | UnnestJoin,
    grain: BuildGrain,
    environment: BuildEnvironment,
) -> bool:
    return _join_right_preserves_cardinality(
        join, environment
    ) and _join_left_keys_covered_by_grain(join, grain, environment)


def _datasource_addresses(source: GrainSource) -> set[str]:
    return {concept.address for concept in source.output_concepts}


def _left_join_sources(
    join: BaseJoin,
    final_datasets: list[GrainSource],
) -> list[GrainSource]:
    if join.left_datasource is not None:
        return [join.left_datasource]
    if not join.concept_pairs:
        return [
            source
            for source in final_datasets
            if source.identifier != join.right_datasource.identifier
        ]
    sources: dict[str, GrainSource] = {}
    for pair in join.concept_pairs:
        sources.setdefault(
            pair.existing_datasource.identifier, pair.existing_datasource
        )
    return list(sources.values())


def _left_join_addresses(
    join: BaseJoin,
    final_datasets: list[GrainSource],
) -> set[str]:
    return {
        address
        for source in _left_join_sources(join, final_datasets)
        for address in _datasource_addresses(source)
    }


def _unprovable_addresses(sources: list[GrainSource]) -> set[str]:
    """Addresses a non-null proof cannot force a side through: partial
    bindings (the value may genuinely be absent from this side) and opaque
    bindings (a CASE/raw column that is non-null even on padded rows)."""
    out: set[str] = set()
    for source in sources:
        out |= {c.address for c in source.partial_concepts}
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


def collect_applied_conditions(source: GrainSource) -> list[BoolExpr]:
    """All filter conditions applied anywhere within a parent branch's tree."""
    out: list[BoolExpr] = []
    if isinstance(source, QueryDatasource):
        if source.condition is not None:
            out.append(source.condition)
        for parent in source.datasources:
            out.extend(collect_applied_conditions(parent))
    return out


def _is_filter_population(
    identifier: str,
    by_id: dict[str, GrainSource],
    filtered_ids: set[str],
    join_addresses: set[str],
) -> bool:
    """Whether this side's row set IS the request WHERE's population.

    It has to have applied the WHERE, and it must not owe its narrowness to
    anything else. An extent-free branch covers only the span members its facts
    bound (docs/extent_ownership.md), so a row missing there is a member nobody
    referenced, not a row the WHERE rejected, and the other side stays
    preserved."""
    if identifier not in filtered_ids:
        return False
    source = by_id.get(identifier)
    if source is None:
        return True
    suppressed = {c.address for c in source.partial_concepts} & deep_extent_free_spans(
        source
    )
    return not (join_addresses & suppressed)


def _join_key_addresses(join: BaseJoin) -> tuple[set[str], set[str]]:
    if join.concept_pairs:
        return (
            {pair.left.address for pair in join.concept_pairs},
            {pair.right.address for pair in join.concept_pairs},
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


def downgrade_join_for_proofs(
    join: BaseJoin | UnnestJoin,
    proofs: set[str],
    final_datasets: list[GrainSource],
) -> None:
    """Narrow a FULL when ``proofs`` (concepts forced non-null in every
    surviving row) rule out the padded rows it preserves: only the side
    whose proof holds is kept, both forced is INNER."""
    if not isinstance(join, BaseJoin):
        return
    if join.join_type != JoinType.FULL or not proofs:
        return
    left_keys, right_keys = _join_key_addresses(join)
    left_all = _left_join_addresses(join, final_datasets)
    right_all = _datasource_addresses(join.right_datasource)
    left_forced = _side_forced(proofs, [], left_all - right_all, left_keys, set())
    right_forced = _side_forced(proofs, [], right_all - left_all, right_keys, set())
    if left_forced and right_forced:
        join.join_type = JoinType.INNER
    elif left_forced:
        join.join_type = JoinType.LEFT_OUTER
    elif right_forced:
        join.join_type = JoinType.RIGHT_OUTER


def downgrade_directional_join_for_proofs(
    join: BaseJoin | UnnestJoin,
    proofs: set[str],
    or_groups: list[list[set[str]]],
    final_datasets: list[GrainSource],
) -> None:
    """A LEFT/RIGHT whose padded side is forced present by ``proofs`` keeps
    no padded row, so it is INNER. Partial and opaque bindings on the padded
    side prove nothing (``_unprovable_addresses``)."""
    if not isinstance(join, BaseJoin):
        return
    if join.join_type not in (JoinType.LEFT_OUTER, JoinType.RIGHT_OUTER):
        return
    if not proofs and not or_groups:
        return
    left_keys, right_keys = _join_key_addresses(join)
    left_all = _left_join_addresses(join, final_datasets)
    right_all = _datasource_addresses(join.right_datasource)
    if join.join_type == JoinType.LEFT_OUTER:
        forced = _side_forced(
            proofs,
            or_groups,
            right_all - left_all,
            right_keys,
            _unprovable_addresses([join.right_datasource]),
        )
    else:
        forced = _side_forced(
            proofs,
            or_groups,
            left_all - right_all,
            left_keys,
            _unprovable_addresses(_left_join_sources(join, final_datasets)),
        )
    if forced:
        join.join_type = JoinType.INNER


def tighten_join_for_filtered_branch(
    join: BaseJoin | UnnestJoin,
    filtered_ids: set[str],
    coalescing_keys: set[str],
    by_id: dict[str, GrainSource],
) -> None:
    """A side that IS the request WHERE's population must match every final
    row, so a join that null-extends it resurrects rows the WHERE rejected.
    Authored coalescing relations keep their preserving typing."""
    if not isinstance(join, BaseJoin) or not filtered_ids:
        return
    join_addresses = {
        address
        for pair in join.concept_pairs or []
        for address in (pair.left.address, pair.right.address)
    } | {concept.address for concept in join.concepts or []}
    if join_addresses & coalescing_keys:
        return
    left_ids: set[str] = set()
    if join.left_datasource is not None:
        left_ids.add(join.left_datasource.identifier)
    for pair in join.concept_pairs or []:
        left_ids.add(pair.existing_datasource.identifier)
    right_filtered = _is_filter_population(
        join.right_datasource.identifier, by_id, filtered_ids, join_addresses
    )
    left_filtered = any(
        _is_filter_population(identifier, by_id, filtered_ids, join_addresses)
        for identifier in left_ids
    )
    if join.join_type == JoinType.FULL:
        if right_filtered and left_filtered:
            join.join_type = JoinType.INNER
        elif right_filtered:
            join.join_type = JoinType.RIGHT_OUTER
        elif left_filtered:
            join.join_type = JoinType.LEFT_OUTER
    elif (
        join.join_type == JoinType.LEFT_OUTER
        and right_filtered
        or join.join_type == JoinType.RIGHT_OUTER
        and left_filtered
    ):
        join.join_type = JoinType.INNER


def narrow_join_types(
    joins: list[BaseJoin | UnnestJoin],
    proofs: JoinProofs,
    final_datasets: list[GrainSource],
) -> None:
    """The planner's one narrowing decision over freshly typed joins: the
    preserving form ``get_join_type`` chose is kept only where no surviving
    row is already proven to match the preserved side."""
    by_id = {source.identifier: source for source in final_datasets}
    for join in joins:
        downgrade_join_for_proofs(join, proofs.proofs, final_datasets)
        downgrade_join_for_proofs(join, proofs.branch_proofs, final_datasets)
        tighten_join_for_filtered_branch(
            join, proofs.filtered_ids, proofs.coalescing_keys, by_id
        )


def narrow_directional_join_types(
    joins: list[BaseJoin | UnnestJoin],
    proofs: JoinProofs,
    final_datasets: list[GrainSource],
) -> None:
    """Second narrowing step, after the directional joins' key pairs were
    pruned to their preserved source: a LEFT/RIGHT whose padded side the
    full harvest forces present is INNER."""
    for join in joins:
        downgrade_directional_join_for_proofs(
            join, proofs.side_proofs, proofs.or_groups, final_datasets
        )


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


def join_preserves_left_rows(join: BaseJoin) -> bool:
    """A lookup join: INNER/LEFT onto a side whose whole grain the join keys
    cover adds no rows to the left stream."""
    if join.join_type not in (JoinType.INNER, JoinType.LEFT_OUTER):
        return False
    right_grain = set(join.right_datasource.grain.components)
    if not right_grain:
        return True
    right_keys = (
        [pair.right for pair in join.concept_pairs]
        if join.concept_pairs
        else join.concepts or []
    )
    coverage = {address for key in right_keys for address in key.equivalent_addresses}
    return right_grain <= coverage


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
    if any(
        not isinstance(join, BaseJoin) or not join_preserves_left_rows(join)
        for join in source.joins
    ):
        return False
    looked_up = {
        join.right_datasource.identifier
        for join in source.joins
        if isinstance(join, BaseJoin)
    }
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
    joins: list[BaseJoin | UnnestJoin],
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
    right_ids = {
        join.right_datasource.identifier for join in joins if isinstance(join, BaseJoin)
    }
    roots = [source for source in datasets if source.identifier not in right_ids]
    if len(roots) != 1:
        return False
    root = roots[0]
    if not unique_at_declared_grain(root):
        return False
    if not set(root.grain.components) <= set(target_grain.components):
        return False
    if any(
        not isinstance(join, BaseJoin) or not join_preserves_left_rows(join)
        for join in joins
    ):
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
    joins: list[BaseJoin | UnnestJoin],
    grain: BuildGrain,
    environment: BuildEnvironment,
) -> BuildGrain:
    cardinality_preserved = {
        join.right_datasource.identifier
        for join in joins
        if isinstance(join, BaseJoin)
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
    if pregrain.issubset(grain):
        return True
    if pregrain.issubset(rowset_source_grain(grain, environment)):
        return True
    # Expand grain via coverage so a MULTISELECT align identity covers its
    # source keys; otherwise a pregrain carrying them looks like extra grain.
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
    condition_grain = condition_key_grain(condition, environment)
    if not condition_grain.components:
        return False
    return not condition_grain.issubset(rowset_source_grain(grain, environment))
