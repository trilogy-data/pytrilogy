"""Unit coverage for the private helpers in ``grain_utility``.

These exercise the type-narrowing guards and edge branches that aren't
naturally hit by the higher-level merge-node tests, so we get full coverage
without contriving end-to-end queries.
"""

from trilogy.core.enums import Derivation, FunctionType, JoinType, Purpose
from trilogy.core.models.build import (
    BuildColumnAssignment,
    BuildConcept,
    BuildDatasource,
    BuildFunction,
    BuildGrain,
)
from trilogy.core.models.core import DataType
from trilogy.core.models.execute import BaseJoin, UnnestJoin
from trilogy.core.processing.grain_utility import (
    _concept_covers_grain,
    _join_left_keys_covered_by_grain,
    _join_right_preserves_cardinality,
    _left_join_addresses,
    downgrade_join_for_condition,
)


def _concept(
    name: str,
    *,
    purpose: Purpose = Purpose.KEY,
    derivation: Derivation = Derivation.ROOT,
    keys: set[str] | None = None,
) -> BuildConcept:
    return BuildConcept(
        name=name,
        canonical_name=name,
        datatype=DataType.STRING,
        purpose=purpose,
        build_is_aggregate=False,
        derivation=derivation,
        namespace="test",
        grain=BuildGrain(),
        keys=keys,
        pseudonyms=set(),
    )


def _datasource(
    name: str,
    concepts: list[BuildConcept],
    grain: BuildGrain | None = None,
) -> BuildDatasource:
    return BuildDatasource(
        name=name,
        columns=[BuildColumnAssignment(alias=c.name, concept=c) for c in concepts],
        address=name,
        namespace="test",
        grain=grain if grain is not None else BuildGrain(),
    )


def _unnest_join() -> UnnestJoin:
    return UnnestJoin(
        concepts=[],
        parent=BuildFunction(
            operator=FunctionType.UNNEST,
            arguments=[],
            output_data_type=DataType.STRING,
            output_purpose=Purpose.PROPERTY,
            arg_count=0,
        ),
        alias="unnest_test",
    )


def test_concept_covers_grain_multiselect_keys_branch():
    """A multiselect concept whose ``keys`` are a superset of the grain components
    covers the grain even when the concept's own address isn't in the grain."""
    multiselect_concept = _concept(
        "ms",
        derivation=Derivation.MULTISELECT,
        keys={"test.a", "test.b"},
    )
    grain = BuildGrain(components={"test.a"})
    assert _concept_covers_grain(multiselect_concept, grain)

    no_overlap = _concept(
        "ms_other",
        derivation=Derivation.MULTISELECT,
        keys={"test.x"},
    )
    assert not _concept_covers_grain(no_overlap, BuildGrain(components={"test.a"}))


def test_join_right_preserves_cardinality_unnest_join_returns_false():
    """Type-narrowing guard: UnnestJoin can't preserve right cardinality."""
    assert _join_right_preserves_cardinality(_unnest_join()) is False


def test_join_right_preserves_cardinality_abstract_right_grain():
    """When right_datasource has no grain components and no key outputs, the
    abstract-grain branch returns True without inspecting join keys."""
    k = _concept(
        "k", purpose=Purpose.PROPERTY
    )  # PROPERTY so effective_grain stays empty
    right = _datasource("r", [k])
    join = BaseJoin(
        right_datasource=right,
        join_type=JoinType.LEFT_OUTER,
        concepts=[k],
    )
    assert _join_right_preserves_cardinality(join) is True


def test_join_left_keys_covered_by_grain_unnest_join_returns_false():
    """Mirror guard for the left-keys helper."""
    assert (
        _join_left_keys_covered_by_grain(
            _unnest_join(),
            BuildGrain(),
            environment=None,  # type: ignore[arg-type]
        )
        is False
    )


def test_join_left_keys_covered_by_grain_no_pairs_no_concepts():
    """Defensive else-branch: when both concept_pairs and concepts end up empty,
    fall through to False."""
    k = _concept("k", purpose=Purpose.PROPERTY)
    right = _datasource("r", [k])
    join = BaseJoin(
        right_datasource=right,
        join_type=JoinType.LEFT_OUTER,
        concepts=[],
    )
    join.concept_pairs = None
    join.concepts = None
    assert (
        _join_left_keys_covered_by_grain(
            join,
            BuildGrain(),
            environment=None,  # type: ignore[arg-type]
        )
        is False
    )


def test_left_join_addresses_no_pairs_no_left_datasource():
    """When left_datasource is None and no concept_pairs, fall back to all
    addresses across non-right final_datasets."""
    a = _concept("a", purpose=Purpose.PROPERTY)
    b = _concept("b", purpose=Purpose.PROPERTY)
    left = _datasource("left", [a])
    right = _datasource("right", [b])

    join = BaseJoin(
        right_datasource=right,
        join_type=JoinType.LEFT_OUTER,
        concepts=[],
    )
    join.concept_pairs = None
    addresses = _left_join_addresses(join, [left, right])
    assert addresses == {a.address}


def test_downgrade_join_for_condition_unnest_join_no_op():
    """Calling downgrade with an UnnestJoin must short-circuit cleanly."""
    assert downgrade_join_for_condition(_unnest_join(), None, []) is None
