from trilogy.constants import MagicConstants
from trilogy.core.enums import (
    BooleanOperator,
    ComparisonOperator,
    FunctionType,
    Purpose,
)
from trilogy.core.models.build import (
    BuildComparison,
    BuildConcept,
    BuildConditional,
    BuildFunction,
    BuildGrain,
    BuildParenthetical,
)
from trilogy.core.models.core import DataType, ListWrapper, TupleWrapper
from trilogy.core.processing.condition_utility import (
    _atom_proves_non_null,
    _coalesce_primary_proves_non_null,
    _eval_literal_comparison,
    _flip_op,
    _literal_value,
    _not_null_concept,
    conditions_cover_domain,
)


def _concept(name: str, datatype: DataType = DataType.INTEGER) -> BuildConcept:
    return BuildConcept(
        name=name,
        canonical_name=name,
        datatype=datatype,
        purpose=Purpose.PROPERTY,
        build_is_aggregate=False,
        namespace="test",
        grain=BuildGrain(),
        pseudonyms=set(),
    )


def _is_not_null(concept: BuildConcept) -> BuildComparison:
    return BuildComparison(
        left=concept, right=MagicConstants.NULL, operator=ComparisonOperator.IS_NOT
    )


def _coalesce(*args) -> BuildFunction:
    return BuildFunction(
        operator=FunctionType.COALESCE,
        arguments=list(args),
        output_data_type=DataType.INTEGER,
        output_purpose=Purpose.PROPERTY,
        arg_count=len(args),
    )


x = _concept("x")
y = _concept("y")


class TestNotNullConcept:
    def test_concept_is_not_null_left(self):
        assert _not_null_concept(_is_not_null(x)) is x

    def test_concept_is_not_null_reversed_operands(self):
        reversed_atom = BuildComparison(
            left=MagicConstants.NULL, right=x, operator=ComparisonOperator.IS_NOT
        )
        assert _not_null_concept(reversed_atom) is x

    def test_concept_vs_concept_is_not_returns_none(self):
        both_concepts = BuildComparison(
            left=x, right=y, operator=ComparisonOperator.IS_NOT
        )
        assert _not_null_concept(both_concepts) is None

    def test_wrong_operator_returns_none(self):
        eq = BuildComparison(left=x, right=5, operator=ComparisonOperator.EQ)
        assert _not_null_concept(eq) is None

    def test_non_comparison_returns_none(self):
        cond = BuildConditional(
            left=_is_not_null(x), right=_is_not_null(y), operator=BooleanOperator.AND
        )
        assert _not_null_concept(cond) is None


class TestFlipOp:
    def test_flips(self):
        assert _flip_op(ComparisonOperator.LT) == ComparisonOperator.GT
        assert _flip_op(ComparisonOperator.GT) == ComparisonOperator.LT
        assert _flip_op(ComparisonOperator.LTE) == ComparisonOperator.GTE
        assert _flip_op(ComparisonOperator.GTE) == ComparisonOperator.LTE

    def test_symmetric_flip_to_self(self):
        assert _flip_op(ComparisonOperator.EQ) == ComparisonOperator.EQ
        assert _flip_op(ComparisonOperator.NE) == ComparisonOperator.NE

    def test_unflippable_returns_none(self):
        assert _flip_op(ComparisonOperator.IN) is None
        assert _flip_op(ComparisonOperator.LIKE) is None


class TestLiteralValue:
    def test_plain_literals(self):
        assert _literal_value(5) == 5
        assert _literal_value(2.5) == 2.5
        assert _literal_value("abc") == "abc"
        assert _literal_value(True) is True

    def test_unwraps_parenthetical(self):
        assert _literal_value(BuildParenthetical(content=7)) == 7

    def test_null_literal_returns_none(self):
        assert _literal_value(MagicConstants.NULL) is None
        assert _literal_value(None) is None

    def test_list_wrapper_folds_to_tuple(self):
        assert _literal_value(ListWrapper([1, 2, 3], type=DataType.INTEGER)) == (
            1,
            2,
            3,
        )

    def test_tuple_wrapper_folds_to_tuple(self):
        assert _literal_value(TupleWrapper([4, 5], type=DataType.INTEGER)) == (4, 5)

    def test_list_with_non_literal_member_returns_none(self):
        assert _literal_value(ListWrapper([1, x], type=DataType.INTEGER)) is None

    def test_non_literal_returns_none(self):
        assert _literal_value(x) is None


class TestEvalLiteralComparison:
    def test_equality_operators(self):
        assert _eval_literal_comparison(1, ComparisonOperator.EQ, 1) is True
        assert _eval_literal_comparison(1, ComparisonOperator.IS, 2) is False
        assert _eval_literal_comparison(1, ComparisonOperator.NE, 2) is True

    def test_ordering_operators(self):
        assert _eval_literal_comparison(1, ComparisonOperator.LT, 2) is True
        assert _eval_literal_comparison(3, ComparisonOperator.GT, 2) is True
        assert _eval_literal_comparison(2, ComparisonOperator.LTE, 2) is True
        assert _eval_literal_comparison(2, ComparisonOperator.GTE, 5) is False

    def test_membership_operators(self):
        assert _eval_literal_comparison(1, ComparisonOperator.IN, (1, 2)) is True
        assert _eval_literal_comparison(9, ComparisonOperator.NOT_IN, (1, 2)) is True

    def test_membership_against_non_tuple_returns_none(self):
        assert _eval_literal_comparison(1, ComparisonOperator.IN, 1) is None
        assert _eval_literal_comparison(1, ComparisonOperator.NOT_IN, 1) is None

    def test_incomparable_types_return_none(self):
        assert _eval_literal_comparison("abc", ComparisonOperator.LT, 5) is None

    def test_unfoldable_operator_returns_none(self):
        assert _eval_literal_comparison(1, ComparisonOperator.LIKE, "a%") is None


class TestCoalescePrimaryProvesNonNull:
    def test_non_function_lhs(self):
        assert _coalesce_primary_proves_non_null(5, ComparisonOperator.GT, 0) == set()

    def test_non_coalesce_function(self):
        fn = BuildFunction(
            operator=FunctionType.ABS,
            arguments=[x],
            output_data_type=DataType.INTEGER,
            output_purpose=Purpose.PROPERTY,
            arg_count=1,
        )
        assert _coalesce_primary_proves_non_null(fn, ComparisonOperator.GT, 0) == set()

    def test_coalesce_without_default(self):
        assert (
            _coalesce_primary_proves_non_null(_coalesce(x), ComparisonOperator.GT, 0)
            == set()
        )

    def test_non_literal_rhs(self):
        assert (
            _coalesce_primary_proves_non_null(_coalesce(x, 0), ComparisonOperator.GT, y)
            == set()
        )

    def test_non_literal_default(self):
        assert (
            _coalesce_primary_proves_non_null(_coalesce(x, y), ComparisonOperator.GT, 0)
            == set()
        )

    def test_default_survives_comparison(self):
        assert (
            _coalesce_primary_proves_non_null(
                _coalesce(x, 100), ComparisonOperator.GT, 0
            )
            == set()
        )

    def test_default_fails_comparison_proves_primary(self):
        assert _coalesce_primary_proves_non_null(
            _coalesce(x, 0), ComparisonOperator.GT, 0
        ) == {x.address}

    def test_parenthetical_wrapped_coalesce(self):
        wrapped = BuildParenthetical(content=_coalesce(x, 0))
        assert _coalesce_primary_proves_non_null(wrapped, ComparisonOperator.GT, 0) == {
            x.address
        }


def test_conditions_cover_domain_empty_is_false():
    assert conditions_cover_domain({}) is False


def test_atom_proves_non_null_and_branch():
    """An AND ``BuildConditional`` passed straight to ``_atom_proves_non_null``
    (the shape ``decompose_condition`` yields when a child is opaque) unions
    both sides' proofs."""
    cond = BuildConditional(
        left=_is_not_null(x), right=_is_not_null(y), operator=BooleanOperator.AND
    )
    assert _atom_proves_non_null(cond) == {x.address, y.address}
