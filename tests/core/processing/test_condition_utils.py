from trilogy.core.enums import ComparisonOperator, Purpose
from trilogy.core.models.build import (
    BuildComparison,
    BuildConcept,
    BuildGrain,
    BuildWhereClause,
)
from trilogy.core.models.core import DataType
from trilogy.core.processing.utility import (
    condition_implies,
    conditions_mutually_exclusive,
    filter_union_children,
    strip_condition_atoms,
)


def _concept(name: str) -> BuildConcept:
    return BuildConcept(
        name=name,
        canonical_name=name,
        datatype=DataType.STRING,
        purpose=Purpose.KEY,
        build_is_aggregate=False,
        namespace="test",
        grain=BuildGrain(),
        pseudonyms=set(),
    )


def _eq(concept: BuildConcept, value: str) -> BuildComparison:
    return BuildComparison(left=concept, right=value, operator=ComparisonOperator.EQ)


def _where(comp: BuildComparison) -> BuildWhereClause:
    return BuildWhereClause(conditional=comp)


city = _concept("city")
dbh = _concept("dbh")

cond_bos = _eq(city, "USBOS")
cond_sfo = _eq(city, "USSFO")
cond_nyc = _eq(city, "USNYC")
cond_dbh = _eq(dbh, "48")
cond_bos_and_dbh = cond_bos + cond_dbh  # city='USBOS' AND dbh='48'


class TestConditionImplies:
    def test_exact_match(self):
        assert condition_implies(cond_bos, cond_bos)

    def test_superset_implies_subset(self):
        assert condition_implies(cond_bos_and_dbh, cond_bos)

    def test_subset_does_not_imply_superset(self):
        assert not condition_implies(cond_bos, cond_bos_and_dbh)

    def test_different_value_not_implied(self):
        assert not condition_implies(cond_bos, cond_sfo)


class TestConditionsMutuallyExclusive:
    def test_same_concept_different_eq_values(self):
        assert conditions_mutually_exclusive(cond_bos, cond_sfo)
        assert conditions_mutually_exclusive(cond_bos, cond_nyc)

    def test_same_value_not_exclusive(self):
        assert not conditions_mutually_exclusive(cond_bos, cond_bos)

    def test_different_concepts_not_exclusive(self):
        assert not conditions_mutually_exclusive(cond_bos, cond_dbh)

    def test_compound_with_conflicting_atom(self):
        assert conditions_mutually_exclusive(cond_bos_and_dbh, cond_sfo)

    def test_bool_exclusive(self):
        is_home = _concept("is_home")
        home_true = BuildComparison(
            left=is_home, right=True, operator=ComparisonOperator.EQ
        )
        home_false = BuildComparison(
            left=is_home, right=False, operator=ComparisonOperator.EQ
        )
        assert conditions_mutually_exclusive(home_true, home_false)
        assert not conditions_mutually_exclusive(home_true, home_true)


class TestStripConditionAtoms:
    def test_strip_one_atom(self):
        stripped = strip_condition_atoms(cond_bos_and_dbh, cond_bos)
        assert stripped == cond_dbh

    def test_strip_all_returns_none(self):
        assert strip_condition_atoms(cond_bos, cond_bos) is None

    def test_strip_non_present_atom_unchanged(self):
        stripped = strip_condition_atoms(cond_bos, cond_sfo)
        assert stripped == cond_bos


class TestFilterUnionChildren:
    def setup_method(self):
        self.non_partial_map = {
            "boston": _where(cond_bos),
            "sf": _where(cond_sfo),
            "nyc": _where(cond_nyc),
        }

    def test_city_filter_drops_other_cities(self):
        # query: city='USBOS' AND dbh='48' → only boston kept
        kept = filter_union_children(self.non_partial_map, cond_bos_and_dbh)
        assert set(kept) == {"boston"}

    def test_kept_child_has_stripped_condition(self):
        kept = filter_union_children(self.non_partial_map, cond_bos_and_dbh)
        # city='USBOS' is stripped; only dbh remains
        assert kept["boston"] == cond_dbh

    def test_no_conditions_match_fallback_to_all(self):
        # If all would be dropped, fall back to full set
        unrelated = _eq(_concept("other"), "x")
        kept = filter_union_children(self.non_partial_map, unrelated)
        assert set(kept) == {"boston", "sf", "nyc"}

    def test_child_without_non_partial_always_kept(self):
        non_partial_map = {"boston": _where(cond_bos), "all": None}
        kept = filter_union_children(non_partial_map, cond_bos_and_dbh)
        assert "all" in kept
        assert kept["all"] == cond_bos_and_dbh

    def test_implied_not_mutually_exclusive_kept(self):
        # query exactly matches non_partial_for → kept, condition stripped to None
        kept = filter_union_children({"boston": _where(cond_bos)}, cond_bos)
        assert "boston" in kept
        assert kept["boston"] is None
