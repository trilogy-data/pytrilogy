"""Unit tests for _resolve_condition_disposition in discovery_utility.

Covers the key state dimensions:
- evaluate_loop_condition_pushdown returns conditions vs None
- force_conditions true/false
- force_pushdown_to_complex_input true/false
- Conditions contain routing atoms (complete_where-matching) vs not
- Remaining concepts: all materialized roots vs mixed derivations
"""

from trilogy.core.enums import (
    ComparisonOperator,
    Derivation,
    Granularity,
    Purpose,
)
from trilogy.core.models.build import (
    BuildColumnAssignment,
    BuildComparison,
    BuildConcept,
    BuildDatasource,
    BuildGrain,
    BuildWhereClause,
)
from trilogy.core.models.build_environment import (
    BuildEnvironment,
    BuildEnvironmentDatasourceDict,
)
from trilogy.core.models.core import DataType
from trilogy.core.processing.discovery_utility import _resolve_condition_disposition


def _concept(
    name: str,
    derivation: Derivation = Derivation.ROOT,
    granularity: Granularity = Granularity.MULTI_ROW,
) -> BuildConcept:
    return BuildConcept(
        name=name,
        canonical_name=name,
        datatype=DataType.STRING,
        purpose=Purpose.KEY,
        build_is_aggregate=False,
        derivation=derivation,
        granularity=granularity,
        grain=BuildGrain(),
    )


def _where(concept: BuildConcept, value: str = "x") -> BuildWhereClause:
    return BuildWhereClause(
        conditional=BuildComparison(
            left=concept,
            right=value,
            operator=ComparisonOperator.EQ,
        )
    )


def _env_with_partial_ds(
    concept: BuildConcept, where: BuildWhereClause
) -> BuildEnvironment:
    ds = BuildDatasource(
        name="partial_ds",
        columns=[BuildColumnAssignment(alias=concept.name, concept=concept)],
        address="partial_ds",
        non_partial_for=where,
    )
    datasources = BuildEnvironmentDatasourceDict()
    datasources["partial_ds"] = ds
    return BuildEnvironment(datasources=datasources)


# --- Group 1: conditions=None after evaluate_loop_condition_pushdown ---


class TestConditionsConsumed:
    """Cases where evaluate_loop_condition_pushdown returned None."""

    def test_no_conditions_no_original(self):
        """Case 1: No conditions at all -> no injection, None."""
        inject, routing = _resolve_condition_disposition(
            conditions=None,
            original_conditions=None,
            remaining=[_concept("a")],
            materialized_canonical=set(),
            force_conditions=False,
            force_pushdown_to_complex_input=False,
            environment=None,
            depth=0,
        )
        assert not inject
        assert routing is None

    def test_consumed_with_routing_atoms(self):
        """Case 2: Conditions consumed, but original has routing atoms -> recovered."""
        city = _concept("city")
        city_where = _where(city, "USSFO")
        env = _env_with_partial_ds(city, city_where)

        inject, routing = _resolve_condition_disposition(
            conditions=None,
            original_conditions=city_where,
            remaining=[_concept("a")],
            materialized_canonical=set(),
            force_conditions=False,
            force_pushdown_to_complex_input=False,
            environment=env,
            depth=0,
        )
        assert not inject
        assert routing is not None

    def test_consumed_no_routing_atoms(self):
        """Conditions consumed, original has no matching datasource -> stays None."""
        city = _concept("city")
        city_where = _where(city, "USSFO")
        env = BuildEnvironment()  # no datasources

        inject, routing = _resolve_condition_disposition(
            conditions=None,
            original_conditions=city_where,
            remaining=[_concept("a")],
            materialized_canonical=set(),
            force_conditions=False,
            force_pushdown_to_complex_input=False,
            environment=env,
            depth=0,
        )
        assert not inject
        assert routing is None

    def test_consumed_no_environment(self):
        """Case 14: Conditions consumed, environment=None -> no recovery."""
        city = _concept("city")

        inject, routing = _resolve_condition_disposition(
            conditions=None,
            original_conditions=_where(city),
            remaining=[_concept("a")],
            materialized_canonical=set(),
            force_conditions=False,
            force_pushdown_to_complex_input=False,
            environment=None,
            depth=0,
        )
        assert not inject
        assert routing is None


# --- Group 2: conditions survived, all materialized roots ---


class TestAllMaterializedRoots:
    """Cases where remaining concepts are all materialized roots."""

    def test_materialized_roots_with_routing_atoms(self):
        """Case 7: All materialized roots + routing atoms -> inject, routing atoms."""
        city = _concept("city")
        city_where = _where(city, "USSFO")
        env = _env_with_partial_ds(city, city_where)
        root_a = _concept("a")

        inject, routing = _resolve_condition_disposition(
            conditions=city_where,
            original_conditions=city_where,
            remaining=[root_a],
            materialized_canonical={root_a.canonical_address},
            force_conditions=False,
            force_pushdown_to_complex_input=False,
            environment=env,
            depth=0,
        )
        assert inject
        assert routing is not None

    def test_materialized_roots_no_routing_atoms(self):
        """Case 6: All materialized roots, no matching datasource -> inject, None."""
        species = _concept("species")
        species_where = _where(species, "Oak")
        env = BuildEnvironment()  # no partial datasources
        root_a = _concept("a")

        inject, routing = _resolve_condition_disposition(
            conditions=species_where,
            original_conditions=species_where,
            remaining=[root_a],
            materialized_canonical={root_a.canonical_address},
            force_conditions=False,
            force_pushdown_to_complex_input=False,
            environment=env,
            depth=0,
        )
        assert inject
        assert routing is None

    def test_materialized_roots_no_environment(self):
        """Materialized roots, environment=None -> inject, conditions=None."""
        species = _concept("species")
        species_where = _where(species, "Oak")
        root_a = _concept("a")

        inject, routing = _resolve_condition_disposition(
            conditions=species_where,
            original_conditions=species_where,
            remaining=[root_a],
            materialized_canonical={root_a.canonical_address},
            force_conditions=False,
            force_pushdown_to_complex_input=False,
            environment=None,
            depth=0,
        )
        assert inject
        assert routing is None

    def test_materialized_roots_takes_precedence_over_force_conditions(self):
        """Case 11: Both materialized roots and force_conditions -> materialized wins."""
        city = _concept("city")
        city_where = _where(city, "USSFO")
        env = _env_with_partial_ds(city, city_where)
        root_a = _concept("a")

        inject, routing = _resolve_condition_disposition(
            conditions=city_where,
            original_conditions=city_where,
            remaining=[root_a],
            materialized_canonical={root_a.canonical_address},
            force_conditions=True,
            force_pushdown_to_complex_input=True,
            environment=env,
            depth=0,
        )
        assert inject
        # materialized-roots path extracts routing atoms, not full conditions
        assert routing is not None

    def test_single_row_not_treated_as_materialized_root(self):
        """Case 13: SINGLE_ROW granularity fails the materialized-root check."""
        city = _concept("city")
        city_where = _where(city, "USSFO")
        single = _concept("count_all", granularity=Granularity.SINGLE_ROW)

        inject, routing = _resolve_condition_disposition(
            conditions=city_where,
            original_conditions=city_where,
            remaining=[single],
            materialized_canonical={single.canonical_address},
            force_conditions=False,
            force_pushdown_to_complex_input=False,
            environment=None,
            depth=0,
        )
        # Not materialized roots -> passthrough
        assert not inject
        assert routing is city_where

    def test_empty_remaining_not_materialized_roots(self):
        """Empty remaining list should not trigger materialized-roots path."""
        city = _concept("city")
        city_where = _where(city, "USSFO")

        inject, routing = _resolve_condition_disposition(
            conditions=city_where,
            original_conditions=city_where,
            remaining=[],
            materialized_canonical=set(),
            force_conditions=False,
            force_pushdown_to_complex_input=False,
            environment=None,
            depth=0,
        )
        assert not inject
        assert routing is city_where


# --- Group 3: force_conditions path ---


class TestForceConditions:
    """Cases where force_conditions=True and not all materialized roots."""

    def test_force_conditions_with_pushdown(self):
        """Case 10: force_conditions + force_pushdown -> inject, keep conditions."""
        city = _concept("city")
        city_where = _where(city, "USSFO")
        derived = _concept("option_value", derivation=Derivation.BASIC)

        inject, routing = _resolve_condition_disposition(
            conditions=city_where,
            original_conditions=city_where,
            remaining=[derived],
            materialized_canonical=set(),
            force_conditions=True,
            force_pushdown_to_complex_input=True,
            environment=None,
            depth=0,
        )
        assert inject
        assert routing is city_where

    def test_force_conditions_no_pushdown(self):
        """Case 9: force_conditions, no pushdown -> inject, conditions=None."""
        city = _concept("city")
        city_where = _where(city, "USSFO")
        derived = _concept("option_value", derivation=Derivation.BASIC)

        inject, routing = _resolve_condition_disposition(
            conditions=city_where,
            original_conditions=city_where,
            remaining=[derived],
            materialized_canonical=set(),
            force_conditions=True,
            force_pushdown_to_complex_input=False,
            environment=None,
            depth=0,
        )
        assert inject
        assert routing is None

    def test_force_conditions_no_pushdown_recovers_routing_atoms(self):
        """Case 4: force_conditions clears conditions, but routing atoms recovered."""
        city = _concept("city")
        city_where = _where(city, "USSFO")
        env = _env_with_partial_ds(city, city_where)
        derived = _concept("option_value", derivation=Derivation.BASIC)

        inject, routing = _resolve_condition_disposition(
            conditions=city_where,
            original_conditions=city_where,
            remaining=[derived],
            materialized_canonical=set(),
            force_conditions=True,
            force_pushdown_to_complex_input=False,
            environment=env,
            depth=0,
        )
        assert inject
        assert routing is not None


# --- Group 4: passthrough (no materialized roots, no force_conditions) ---


class TestPassthrough:
    """Cases where conditions pass through unchanged."""

    def test_conditions_pass_through(self):
        """Case 8: Mixed derivations, no force -> passthrough."""
        city = _concept("city")
        city_where = _where(city, "USSFO")
        derived = _concept("option_value", derivation=Derivation.BASIC)

        inject, routing = _resolve_condition_disposition(
            conditions=city_where,
            original_conditions=city_where,
            remaining=[derived],
            materialized_canonical=set(),
            force_conditions=False,
            force_pushdown_to_complex_input=False,
            environment=None,
            depth=0,
        )
        assert not inject
        assert routing is city_where

    def test_none_conditions_pass_through(self):
        """Case 12: No conditions -> no injection, None."""
        inject, routing = _resolve_condition_disposition(
            conditions=None,
            original_conditions=None,
            remaining=[_concept("a")],
            materialized_canonical=set(),
            force_conditions=False,
            force_pushdown_to_complex_input=False,
            environment=None,
            depth=0,
        )
        assert not inject
        assert routing is None
