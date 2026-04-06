from trilogy.core.enums import BooleanOperator, ComparisonOperator, Purpose
from trilogy.core.models.build import (
    BuildComparison,
    BuildConcept,
    BuildConditional,
    BuildGrain,
    BuildParenthetical,
    BuildWhereClause,
)
from trilogy.core.models.core import DataType, EnumType
from trilogy.core.processing.condition_utility import (
    condition_implies,
    condition_implies_with_extras,
    conditions_mutually_exclusive,
    decompose_condition,
    drop_covered_conditions,
    filter_union_children,
    flatten_conditions,
    merge_conditions,
    strip_condition_atoms,
)


def _enum_concept(name: str, values: list[str]) -> BuildConcept:
    return BuildConcept(
        name=name,
        canonical_name=name,
        datatype=EnumType(type=DataType.STRING, values=values),
        purpose=Purpose.KEY,
        build_is_aggregate=False,
        namespace="test",
        grain=BuildGrain(),
        pseudonyms=set(),
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


class TestConditionImpliesWithExtras:
    def test_exact_match_no_extras(self):
        implied, extras = condition_implies_with_extras(cond_bos, cond_bos)
        assert implied
        assert extras == []

    def test_superset_returns_extras(self):
        implied, extras = condition_implies_with_extras(cond_bos_and_dbh, cond_bos)
        assert implied
        assert extras == [cond_dbh]

    def test_subset_not_implied(self):
        implied, extras = condition_implies_with_extras(cond_bos, cond_bos_and_dbh)
        assert not implied
        assert extras == []

    def test_disjoint_not_implied(self):
        implied, extras = condition_implies_with_extras(cond_bos, cond_sfo)
        assert not implied
        assert extras == []

    def test_three_atoms_two_extras(self):
        extra = _concept("extra")
        cond_extra = _eq(extra, "z")
        three = cond_bos + cond_dbh + cond_extra  # type: ignore[operator]
        implied, extras = condition_implies_with_extras(three, cond_bos)
        assert implied
        assert cond_dbh in extras
        assert cond_extra in extras
        assert cond_bos not in extras


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

    def test_reversed_operand_order(self):
        # Covers lines 941-944: value on left, concept on right.
        cond_bos_reversed = BuildComparison(
            left="USBOS", right=city, operator=ComparisonOperator.EQ
        )
        cond_sfo_reversed = BuildComparison(
            left="USSFO", right=city, operator=ComparisonOperator.EQ
        )
        assert conditions_mutually_exclusive(cond_bos_reversed, cond_sfo_reversed)
        assert not conditions_mutually_exclusive(cond_bos_reversed, cond_bos_reversed)

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

    def test_strip_one_of_three_atoms_joins_remainder(self):
        # Covers the loop on line 963: remaining has 2 atoms needing recombination.
        extra = _concept("extra")
        cond_extra = _eq(extra, "z")
        three = cond_bos + cond_dbh + cond_extra  # type: ignore[operator]
        stripped = strip_condition_atoms(three, cond_bos)
        assert stripped is not None
        from trilogy.core.processing.condition_utility import decompose_condition

        atoms = decompose_condition(stripped)
        assert cond_dbh in atoms
        assert cond_extra in atoms
        assert cond_bos not in atoms


class TestDropCoveredConditions:
    def test_more_specific_dropped_when_general_present(self):
        # cond_bos_and_dbh implies cond_bos → cond_bos_and_dbh is redundant
        result = drop_covered_conditions([cond_bos_and_dbh, cond_bos])
        assert cond_bos in result
        assert cond_bos_and_dbh not in result

    def test_general_not_dropped(self):
        result = drop_covered_conditions([cond_bos_and_dbh, cond_bos])
        assert cond_bos in result

    def test_disjoint_conditions_both_kept(self):
        result = drop_covered_conditions([cond_bos, cond_sfo])
        assert cond_bos in result
        assert cond_sfo in result

    def test_single_condition_kept(self):
        assert drop_covered_conditions([cond_bos]) == [cond_bos]

    def test_empty_list(self):
        assert drop_covered_conditions([]) == []

    def test_exact_duplicates_deduplicated(self):
        result = drop_covered_conditions([cond_bos, cond_bos])
        assert result == [cond_bos]

    def test_three_way_keeps_most_general(self):
        extra = _concept("extra")
        cond_extra = _eq(extra, "z")
        most_specific = cond_bos + cond_dbh + cond_extra  # type: ignore[operator]
        mid = cond_bos + cond_dbh  # type: ignore[operator]
        result = drop_covered_conditions([most_specific, mid, cond_bos])
        assert cond_bos in result
        assert mid not in result
        assert most_specific not in result


class TestMergeConditions:
    def setup_method(self):
        self.status = _enum_concept("status", ["a", "b"])
        self.cond_a = _eq(self.status, "a")
        self.cond_b = _eq(self.status, "b")

    def test_enum_complete_drops_enum_keeps_common(self):
        c1 = cond_bos + self.cond_a  # type: ignore[operator]
        c2 = cond_bos + self.cond_b  # type: ignore[operator]
        result = merge_conditions([c1, c2])
        assert result == cond_bos

    def test_enum_complete_no_common_returns_none(self):
        result = merge_conditions([self.cond_a, self.cond_b])
        assert result is None

    def test_enum_incomplete_returns_first_unchanged(self):
        # Only one value covered — can't prove complete
        c1 = cond_bos + self.cond_a  # type: ignore[operator]
        c2 = cond_bos + self.cond_a  # type: ignore[operator]
        result = merge_conditions([c1, c2])
        assert result == c1

    def test_non_enum_varying_returns_first_unchanged(self):
        # dbh is not an enum — can't prove complete
        c1 = cond_bos + cond_dbh  # type: ignore[operator]
        c2 = cond_sfo + cond_dbh  # type: ignore[operator]
        result = merge_conditions([c1, c2])
        assert result == c1

    def test_identical_conditions_returns_single(self):
        result = merge_conditions([cond_bos, cond_bos])
        assert result == cond_bos

    def test_single_condition_returned_as_is(self):
        assert merge_conditions([cond_bos]) == cond_bos

    def test_empty_returns_none(self):
        assert merge_conditions([]) is None

    def test_three_value_enum_complete(self):
        tri = _enum_concept("tri", ["x", "y", "z"])
        cx = _eq(tri, "x")
        cy = _eq(tri, "y")
        cz = _eq(tri, "z")
        result = merge_conditions(
            [
                cond_bos + cx,  # type: ignore[operator]
                cond_bos + cy,  # type: ignore[operator]
                cond_bos + cz,  # type: ignore[operator]
            ]
        )
        assert result == cond_bos


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

    def test_all_exclusive_fallback_to_all(self):
        # All children mutually exclusive → fallback returns every child. Covers line 993.
        other_city = _eq(city, "USOTH")
        kept = filter_union_children(self.non_partial_map, other_city)
        assert set(kept) == {"boston", "sf", "nyc"}
        assert all(v == other_city for v in kept.values())

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


# --- helpers for flatten tests ---

_tautology = BuildComparison(left=True, right=True, operator=ComparisonOperator.IS)


class TestFlattenConditions:
    def test_unwraps_parenthetical_around_comparison(self):
        wrapped = BuildParenthetical(content=cond_bos)
        assert flatten_conditions(wrapped) == cond_bos

    def test_unwraps_nested_parentheticals(self):
        double = BuildParenthetical(content=BuildParenthetical(content=cond_sfo))
        assert flatten_conditions(double) == cond_sfo

    def test_preserves_parenthetical_around_or_conditional(self):
        """Parens around OR enforce precedence and must not be stripped."""
        or_cond = BuildConditional(
            left=cond_bos, right=cond_sfo, operator=BooleanOperator.OR
        )
        wrapped = BuildParenthetical(content=or_cond)
        result = flatten_conditions(wrapped)
        assert isinstance(result, BuildParenthetical)

    def test_flattens_both_sides_of_and(self):
        cond = BuildConditional(
            left=BuildParenthetical(content=cond_bos),
            right=BuildParenthetical(content=cond_dbh),
            operator=BooleanOperator.AND,
        )
        result = flatten_conditions(cond)
        assert isinstance(result, BuildConditional)
        assert result.left == cond_bos
        assert result.right == cond_dbh

    def test_flattened_atoms_match_for_condition_implies(self):
        """The whole point: after flattening, condition_implies can see through
        what were Parenthetical wrappers."""
        wrapped_cond = BuildConditional(
            left=BuildParenthetical(content=cond_bos),
            right=BuildParenthetical(content=cond_dbh),
            operator=BooleanOperator.AND,
        )
        flattened = flatten_conditions(wrapped_cond)
        assert condition_implies(flattened, cond_bos)

    def test_decompose_after_flatten(self):
        wrapped_cond = BuildConditional(
            left=BuildParenthetical(content=cond_bos),
            right=BuildParenthetical(content=cond_dbh),
            operator=BooleanOperator.AND,
        )
        atoms = decompose_condition(flatten_conditions(wrapped_cond))
        assert cond_bos in atoms
        assert cond_dbh in atoms

    def test_drops_tautology_from_and_right(self):
        cond = BuildConditional(
            left=cond_bos, right=_tautology, operator=BooleanOperator.AND
        )
        assert flatten_conditions(cond) == cond_bos

    def test_drops_tautology_from_and_left(self):
        cond = BuildConditional(
            left=_tautology, right=cond_sfo, operator=BooleanOperator.AND
        )
        assert flatten_conditions(cond) == cond_sfo

    def test_both_tautologies_returns_tautology(self):
        cond = BuildConditional(
            left=_tautology, right=_tautology, operator=BooleanOperator.AND
        )
        result = flatten_conditions(cond)
        assert _is_tautology(result)

    def test_tautology_in_or_preserved(self):
        """Tautology removal only applies to AND chains."""
        cond = BuildConditional(
            left=cond_bos, right=_tautology, operator=BooleanOperator.OR
        )
        result = flatten_conditions(cond)
        assert isinstance(result, BuildConditional)

    def test_drops_parenthetical_tautology(self):
        """Tautology wrapped in parens should be unwrapped then dropped."""
        cond = BuildConditional(
            left=cond_bos,
            right=BuildParenthetical(content=_tautology),
            operator=BooleanOperator.AND,
        )
        assert flatten_conditions(cond) == cond_bos

    def test_plain_comparison_unchanged(self):
        assert flatten_conditions(cond_bos) == cond_bos

    def test_unwraps_condition_compared_to_true(self):
        cond = BuildComparison(
            left=BuildParenthetical(content=cond_bos),
            right=True,
            operator=ComparisonOperator.EQ,
        )

        assert flatten_conditions(cond) == cond_bos

    def test_condition_compared_to_true_implies_partition_condition(self):
        cond = BuildComparison(
            left=BuildParenthetical(content=cond_bos),
            right=True,
            operator=ComparisonOperator.EQ,
        )

        assert condition_implies(flatten_conditions(cond), cond_bos)


def _is_tautology(node) -> bool:
    from trilogy.core.processing.condition_utility import (
        _is_tautology as _is_taut_impl,
    )

    return isinstance(node, BuildComparison) and _is_taut_impl(node)
