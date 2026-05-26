import pytest

from trilogy.core.enums import (
    Boolean,
    BooleanOperator,
    ChartPlaceKind,
    ChartType,
    ComparisonOperator,
    DatePart,
    IOType,
    Modifier,
    Purpose,
    ValidationScope,
)


def test_boolean_operator():
    assert BooleanOperator("AND") == BooleanOperator.AND
    assert BooleanOperator("and") == BooleanOperator.AND


def test_boolean():
    assert Boolean("TRUE") == Boolean.TRUE
    assert Boolean("true") == Boolean.TRUE
    assert Boolean(True) == Boolean.TRUE
    assert Boolean(False) == Boolean.FALSE


def test_purpose_constant_alias():
    assert Purpose("constant") == Purpose.CONSTANT


def test_purpose_param_alias():
    assert Purpose("param") == Purpose.PARAMETER


def test_modifier_partial_squiggle_alias():
    assert Modifier("~") == Modifier.PARTIAL


def test_modifier_nullable_question_alias():
    assert Modifier("?") == Modifier.NULLABLE


def test_modifier_partial_direct_value_works():
    assert Modifier("Partial") == Modifier.PARTIAL


def test_comparison_operator_not_in_string():
    assert ComparisonOperator("not in") == ComparisonOperator.NOT_IN


def test_comparison_operator_not_in_list():
    assert ComparisonOperator(["not", "in"]) == ComparisonOperator.NOT_IN


def test_comparison_operator_is_not_list():
    assert ComparisonOperator(["is", "not"]) == ComparisonOperator.IS_NOT


def test_comparison_operator_in_single_element_list():
    assert ComparisonOperator(["in"]) == ComparisonOperator.IN


def test_comparison_operator_case_insensitive_uppercase():
    assert ComparisonOperator("IN") == ComparisonOperator.IN


def test_comparison_operator_string_equality_against_str():
    assert ComparisonOperator.EQ == "="
    assert ComparisonOperator.NE == "!="
    assert (ComparisonOperator.EQ == 7) is False


def test_date_part_case_insensitive():
    assert DatePart("YEAR") == DatePart.YEAR


def test_iotype_case_insensitive_and_chart_predicate():
    assert IOType("PNG") == IOType.PNG
    assert IOType.PNG.is_chart_format is True
    assert IOType.CSV.is_chart_format is False


def test_validation_scope_case_insensitive():
    assert ValidationScope("ALL") == ValidationScope.ALL
    assert ValidationScope("Concepts") == ValidationScope.CONCEPTS


def test_chart_type_case_insensitive():
    assert ChartType("LINE") == ChartType.LINE
    assert ChartType("Bar") == ChartType.BAR


def test_chart_place_kind_case_insensitive():
    assert ChartPlaceKind("HLINE") == ChartPlaceKind.HLINE
    assert ChartPlaceKind("Vline") == ChartPlaceKind.VLINE


def test_unknown_purpose_falls_to_default_missing():
    with pytest.raises(ValueError):
        Purpose("not-a-purpose")
