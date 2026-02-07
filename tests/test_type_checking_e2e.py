"""
End-to-end tests for type checking effectiveness.

These tests validate that the parse layer catches incorrect type comparisons,
assignments, outputs, and function arguments. The goal is comprehensive coverage
of type validation in the Trilogy language.
"""

import pytest

from trilogy import Dialects
from trilogy.core.exceptions import InvalidSyntaxException
from trilogy.core.models.core import DataType
from trilogy.parsing.exceptions import ParseError
from trilogy.parsing.parse_engine import parse_text

# =============================================================================
# COMPARISON TYPE CHECKING
# =============================================================================


class TestComparisonTypeMismatch:
    """Tests for catching type mismatches in comparison operations."""

    def test_integer_vs_string_equality(self):
        """Integer cannot be compared with string using equality."""
        with pytest.raises(
            InvalidSyntaxException, match="Cannot compare INTEGER.*STRING"
        ):
            parse_text("""
                const x <- 1;
                const y <- 'hello';
                select x where x = y;
                """)

    def test_integer_vs_string_less_than(self):
        """Integer cannot be compared with string using less than."""
        with pytest.raises(
            InvalidSyntaxException, match="Cannot compare INTEGER.*STRING"
        ):
            parse_text("""
                const x <- 1;
                const y <- 'hello';
                select x where x < y;
                """)

    def test_integer_vs_string_greater_than(self):
        """Integer cannot be compared with string using greater than."""
        with pytest.raises(
            InvalidSyntaxException, match="Cannot compare INTEGER.*STRING"
        ):
            parse_text("""
                const x <- 1;
                const y <- 'hello';
                select x where x > y;
                """)

    def test_boolean_vs_string(self):
        """Boolean cannot be compared with string."""
        with pytest.raises(InvalidSyntaxException, match="Cannot compare BOOL.*STRING"):
            parse_text("""
                const x <- true;
                const y <- 'hello';
                select x where x = y;
                """)

    def test_boolean_vs_integer(self):
        """Boolean cannot be compared with integer."""
        with pytest.raises(InvalidSyntaxException, match="Cannot compare"):
            parse_text("""
                const x <- true;
                const y <- 1;
                select x where x = y;
                """)

    def test_date_vs_string(self):
        """Date cannot be compared with string directly."""
        with pytest.raises(InvalidSyntaxException, match="Cannot compare"):
            parse_text("""
                const x <- cast('2023-01-01' as date);
                const y <- 'hello';
                select x where x = y;
                """)

    def test_date_vs_integer(self):
        """Date cannot be compared with integer."""
        with pytest.raises(InvalidSyntaxException, match="Cannot compare"):
            parse_text("""
                const x <- cast('2023-01-01' as date);
                const y <- 20230101;
                select x where x = y;
                """)

    def test_float_vs_string(self):
        """Float cannot be compared with string."""
        with pytest.raises(
            InvalidSyntaxException, match="Cannot compare FLOAT.*STRING"
        ):
            parse_text("""
                const x <- 1.5;
                const y <- 'hello';
                select x where x = y;
                """)

    def test_array_vs_integer(self):
        """Array cannot be compared with integer using equality."""
        with pytest.raises(InvalidSyntaxException, match="Cannot compare"):
            parse_text("""
                const x <- [1, 2, 3];
                const y <- 1;
                select y where x = y;
                """)


class TestComparisonTypeCompatibility:
    """Tests for valid type compatibility in comparisons."""

    def test_integer_vs_integer(self):
        """Integer can be compared with integer."""
        env, _ = parse_text("""
            const x <- 1;
            const y <- 2;
            select x where x = y;
            """)
        assert env.concepts["x"].datatype == DataType.INTEGER

    def test_integer_vs_float(self):
        """Integer can be compared with float (numeric compatibility)."""
        env, _ = parse_text("""
            const x <- 1;
            const y <- 2.5;
            select x where x = y;
            """)

    def test_float_vs_numeric(self):
        """Float can be compared with numeric."""
        env, _ = parse_text("""
            const x <- 1.5;
            const y numeric(10,2);
            select x where x = y;
            """)

    def test_string_vs_string(self):
        """String can be compared with string."""
        env, _ = parse_text("""
            const x <- 'hello';
            const y <- 'world';
            select x where x = y;
            """)


# =============================================================================
# IS/IS NOT OPERATOR TYPE CHECKING
# =============================================================================


class TestIsOperatorTypeChecking:
    """Tests for IS/IS NOT operator which requires null or boolean values."""

    def test_is_with_array_fails(self):
        """IS operator cannot be used with array literal."""
        with pytest.raises(
            InvalidSyntaxException, match="Cannot use is with non-null or boolean"
        ):
            parse_text("""
                const x <- 1;
                select x where x is [1, 2];
                """)

    def test_is_with_integer_fails(self):
        """IS operator cannot be used with integer literal."""
        with pytest.raises(
            InvalidSyntaxException, match="Cannot use is with non-null or boolean"
        ):
            parse_text("""
                const x <- 1;
                select x where x is 1;
                """)

    def test_is_with_string_fails(self):
        """IS operator cannot be used with string literal."""
        with pytest.raises(
            InvalidSyntaxException, match="Cannot use is with non-null or boolean"
        ):
            parse_text("""
                const x <- 'hello';
                select x where x is 'hello';
                """)

    def test_is_null_succeeds(self):
        """IS operator can be used with null."""
        env, _ = parse_text("""
            const x <- 1;
            select x where x is null;
            """)

    def test_is_not_null_succeeds(self):
        """IS NOT operator can be used with null."""
        env, _ = parse_text("""
            const x <- 1;
            select x where x is not null;
            """)

    def test_is_true_succeeds(self):
        """IS operator can be used with boolean true."""
        env, _ = parse_text("""
            const x <- true;
            select x where x is true;
            """)

    def test_is_false_succeeds(self):
        """IS operator can be used with boolean false."""
        env, _ = parse_text("""
            const x <- true;
            select x where x is false;
            """)


# =============================================================================
# IN OPERATOR TYPE CHECKING
# =============================================================================


class TestInOperatorTypeChecking:
    """Tests for IN operator type validation against array element types."""

    def test_integer_in_string_array_fails(self):
        """Integer cannot be checked against array of strings."""
        with pytest.raises(
            (InvalidSyntaxException, SyntaxError), match="Cannot compare"
        ):
            parse_text("""
                const x <- 1;
                select x where x in ['a', 'b', 'c'];
                """)

    def test_string_in_integer_array_fails(self):
        """String cannot be checked against array of integers."""
        with pytest.raises(
            (InvalidSyntaxException, SyntaxError), match="Cannot compare"
        ):
            parse_text("""
                const x <- 'hello';
                select x where x in [1, 2, 3];
                """)

    def test_integer_in_integer_array_succeeds(self):
        """Integer can be checked against array of integers."""
        env, _ = parse_text("""
            const x <- 1;
            select x where x in [1, 2, 3];
            """)

    def test_string_in_string_array_succeeds(self):
        """String can be checked against array of strings."""
        env, _ = parse_text("""
            const x <- 'hello';
            select x where x in ['hello', 'world'];
            """)

    def test_float_in_integer_array_succeeds(self):
        """Float can be checked against array of integers (numeric compatibility)."""
        env, _ = parse_text("""
            const x <- 1.5;
            select x where x in [1, 2, 3];
            """)


# =============================================================================
# FUNCTION ARGUMENT TYPE CHECKING - STRING FUNCTIONS
# =============================================================================


class TestStringFunctionArgumentTypes:
    """Tests for string function argument type validation."""

    def test_len_with_integer_fails(self):
        """LEN function requires string or array, not integer."""
        with pytest.raises(InvalidSyntaxException, match="Invalid argument type"):
            parse_text("""
                const x <- 123;
                auto y <- len(x);
                select y;
                """)

    def test_len_with_string_succeeds(self):
        """LEN function works with string."""
        env, _ = parse_text("""
            const x <- 'hello';
            auto y <- len(x);
            select y;
            """)
        assert env.concepts["y"].datatype == DataType.INTEGER

    def test_len_with_array_succeeds(self):
        """LEN function works with array."""
        env, _ = parse_text("""
            const x <- [1, 2, 3];
            auto y <- len(x);
            select y;
            """)
        assert env.concepts["y"].datatype == DataType.INTEGER


    def test_upper_with_integer_fails(self):
        """UPPER function requires string, not integer."""
        with pytest.raises(InvalidSyntaxException, match="Invalid argument type"):
            parse_text("""
                const x <- 123;
                auto y <- upper(x);
                select y;
                """)

    def test_upper_with_string_succeeds(self):
        """UPPER function works with string."""
        env, _ = parse_text("""
            const x <- 'hello';
            auto y <- upper(x);
            select y;
            """)
        assert env.concepts["y"].datatype == DataType.STRING


    def test_lower_with_integer_fails(self):
        """LOWER function requires string, not integer."""
        with pytest.raises(InvalidSyntaxException, match="Invalid argument type"):
            parse_text("""
                const x <- 123;
                auto y <- lower(x);
                select y;
                """)


    def test_trim_with_integer_fails(self):
        """TRIM function requires string, not integer."""
        with pytest.raises(InvalidSyntaxException, match="Invalid argument type"):
            parse_text("""
                const x <- 123;
                auto y <- trim(x);
                select y;
                """)

    def test_substring_with_integer_fails(self):
        """SUBSTRING function requires string as first argument."""
        with pytest.raises(InvalidSyntaxException, match="Invalid argument type"):
            parse_text("""
                const x <- 123;
                auto y <- substring(x, 1, 2);
                select y;
                """)

    def test_contains_with_integer_fails(self):
        """CONTAINS function requires strings, not integers."""
        with pytest.raises(InvalidSyntaxException, match="Invalid argument type"):
            parse_text("""
                const x <- 123;
                auto y <- contains(x, 'test');
                select y;
                """)

    def test_replace_with_integer_pattern_fails(self):
        """REPLACE function requires strings."""
        with pytest.raises(InvalidSyntaxException, match="Invalid argument type"):
            parse_text("""
                const x <- 'hello';
                const y <- 123;
                auto z <- replace(x, y, 'test');
                select z;
                """)

    def test_strpos_with_integer_fails(self):
        """STRPOS with integer arguments should fail."""
        with pytest.raises((InvalidSyntaxException)):
            parse_text("""
                const x <- 123;
                auto y <- strpos(x, 'test');
                select y;
                """)

    def test_regexp_contains_integer_pattern_fails(self):
        """REGEXP_CONTAINS with integer pattern should fail."""
        with pytest.raises((InvalidSyntaxException)):
            parse_text("""
                const x <- 'hello';
                const y <- 123;
                auto z <- regexp_contains(x, y);
                select z;
                """)


# =============================================================================
# FUNCTION ARGUMENT TYPE CHECKING - NUMERIC FUNCTIONS
# =============================================================================


class TestNumericFunctionArgumentTypes:
    """Tests for numeric function argument type validation."""

    def test_sum_with_string_fails(self):
        """SUM function requires numeric types, not string."""
        with pytest.raises(InvalidSyntaxException, match="Invalid argument type"):
            parse_text("""
                const x <- 'hello';
                auto y <- sum(x);
                select y;
                """)

    def test_avg_with_string_fails(self):
        """AVG function requires numeric types, not string."""
        with pytest.raises(InvalidSyntaxException, match="Invalid argument type"):
            parse_text("""
                const x <- 'hello';
                auto y <- avg(x);
                select y;
                """)

    def test_sqrt_with_string_fails(self):
        """SQRT function requires numeric type, not string."""
        with pytest.raises(InvalidSyntaxException, match="Invalid argument type"):
            parse_text("""
                const x <- 'hello';
                auto y <- sqrt(x);
                select y;
                """)

    def test_abs_with_string_fails(self):
        """ABS function requires numeric type, not string."""
        with pytest.raises(InvalidSyntaxException, match="Invalid argument type"):
            parse_text("""
                const x <- 'hello';
                auto y <- abs(x);
                select y;
                """)

    def test_round_with_string_fails(self):
        """ROUND function requires numeric type, not string."""
        with pytest.raises(InvalidSyntaxException, match="Invalid argument type"):
            parse_text("""
                const x <- 'hello';
                auto y <- round(x, 2);
                select y;
                """)

    def test_floor_with_string_fails(self):
        """FLOOR function requires numeric type, not string."""
        with pytest.raises(InvalidSyntaxException, match="Invalid argument type"):
            parse_text("""
                const x <- 'hello';
                auto y <- floor(x);
                select y;
                """)

    def test_ceil_with_string_fails(self):
        """CEIL function requires numeric type, not string."""
        with pytest.raises(InvalidSyntaxException, match="Invalid argument type"):
            parse_text("""
                const x <- 'hello';
                auto y <- ceil(x);
                select y;
                """)

    def test_sqrt_with_integer_succeeds(self):
        """SQRT function works with integer."""
        env, _ = parse_text("""
            const x <- 16;
            auto y <- sqrt(x);
            select y;
            """)

    def test_abs_with_integer_succeeds(self):
        """ABS function works with integer."""
        env, _ = parse_text("""
            const x <- -5;
            auto y <- abs(x);
            select y;
            """)


# =============================================================================
# FUNCTION ARGUMENT TYPE CHECKING - DATE FUNCTIONS
# =============================================================================


class TestDateFunctionArgumentTypes:
    """Tests for date/time function argument type validation."""

    def test_date_part_with_string_fails(self):
        """DATE_PART function requires date/datetime, not string."""
        with pytest.raises(InvalidSyntaxException, match="Invalid argument type"):
            parse_text("""
                const x <- 'hello';
                auto y <- year(x);
                select y;
                """)

    def test_date_part_with_integer_fails(self):
        """DATE_PART function requires date/datetime, not integer."""
        with pytest.raises(InvalidSyntaxException, match="Invalid argument type"):
            parse_text("""
                const x <- 123;
                auto y <- year(x);
                select y;
                """)

    def test_date_truncate_with_string_fails(self):
        """DATE_TRUNCATE function requires date/datetime, not string."""
        with pytest.raises(InvalidSyntaxException, match="Invalid argument type"):
            parse_text("""
                const x <- 'hello';
                auto y <- date_trunc(x, month);
                select y;
                """)

    def test_year_with_date_succeeds(self):
        """YEAR function works with date."""
        env, _ = parse_text("""
            const x <- cast('2023-01-15' as date);
            auto y <- year(x);
            select y;
            """)
        assert env.concepts["y"].datatype.data_type == DataType.INTEGER


# =============================================================================
# FUNCTION ARGUMENT TYPE CHECKING - ARRAY FUNCTIONS
# =============================================================================


class TestArrayFunctionArgumentTypes:
    """Tests for array function argument type validation."""

    def test_unnest_with_integer_fails(self):
        """UNNEST function requires array type, not integer."""
        with pytest.raises(InvalidSyntaxException, match="Invalid argument type"):
            parse_text("""
                const x <- 123;
                auto y <- unnest(x);
                select y;
                """)

    def test_unnest_with_string_fails(self):
        """UNNEST function requires array type, not string."""
        with pytest.raises(InvalidSyntaxException, match="Invalid argument type"):
            parse_text("""
                const x <- 'hello';
                auto y <- unnest(x);
                select y;
                """)

    def test_unnest_with_array_succeeds(self):
        """UNNEST function works with array."""
        env, _ = parse_text("""
            const x <- [1, 2, 3];
            auto y <- unnest(x);
            select y;
            """)
        assert env.concepts["y"].datatype == DataType.INTEGER

    def test_array_to_string_with_integer_array_fails(self):
        """ARRAY_TO_STRING requires string array, not integer array."""
        with pytest.raises(InvalidSyntaxException, match="Invalid argument type"):
            parse_text("""
                const x <- [1, 2, 3];
                auto y <- array_to_string(x, ',');
                select y;
                """)

    def test_map_keys_with_non_map_fails(self):
        """MAP_KEYS requires map type, not integer."""
        with pytest.raises(InvalidSyntaxException, match="Invalid argument type"):
            parse_text("""
                const x <- 123;
                auto y <- map_keys(x);
                select y;
                """)

    def test_map_values_with_non_map_fails(self):
        """MAP_VALUES requires map type, not integer."""
        with pytest.raises(InvalidSyntaxException, match="Invalid argument type"):
            parse_text("""
                const x <- 123;
                auto y <- map_values(x);
                select y;
                """)

    def test_array_sum_string_array_fails(self):
        """ARRAY_SUM with string array should fail."""
        with pytest.raises(InvalidSyntaxException):
            parse_text("""
                const x <- ['a', 'b', 'c'];
                auto y <- array_sum(x);
                select y;
                """)

    def test_generate_array_with_string_fails(self):
        """GENERATE_ARRAY with string bounds should fail."""
        with pytest.raises((TypeError, InvalidSyntaxException)):
            parse_text("""
                const s <- 'a';
                const e <- 'z';
                auto x <- generate_array(s, e, 1);
                select x;
                """)


# =============================================================================
# COALESCE TYPE CHECKING
# =============================================================================


class TestCoalesceTypeChecking:
    """Tests for COALESCE function type uniformity requirement."""

    def test_coalesce_mixed_types_fails(self):
        """COALESCE requires all arguments to be the same type."""
        with pytest.raises(InvalidSyntaxException, match="same type"):
            parse_text("""
                const x <- 1;
                const y <- 'hello';
                auto z <- coalesce(x, y);
                select z;
                """)

    def test_coalesce_integer_and_string_fails(self):
        """COALESCE with integer and string fails."""
        with pytest.raises(InvalidSyntaxException, match="same type"):
            parse_text("""
                auto z <- coalesce(1, 'fallback');
                select z;
                """)

    def test_coalesce_same_types_succeeds(self):
        """COALESCE with same types succeeds."""
        env, _ = parse_text("""
            const x <- 1;
            const y <- 2;
            auto z <- coalesce(x, y);
            select z;
            """)
        assert env.concepts["z"].datatype == DataType.INTEGER

    def test_coalesce_with_null_succeeds(self):
        """COALESCE with null and typed value succeeds."""
        env, _ = parse_text("""
            const x <- 1;
            auto z <- coalesce(null, x);
            select z;
            """)
        assert env.concepts["z"].datatype == DataType.INTEGER



# =============================================================================
# CASE EXPRESSION TYPE CHECKING
# =============================================================================


class TestCaseExpressionTypeChecking:
    """Tests for CASE expression output type uniformity."""

    def test_case_mixed_output_types_fails(self):
        """CASE expression branches must have consistent output types."""
        with pytest.raises(InvalidSyntaxException, match="same output datatype"):
            parse_text("""
                const x <- 1;
                auto y <- case when x = 1 then 'one' when x = 2 then 2 else 'other' end;
                select y;
                """)

    def test_case_string_and_integer_fails(self):
        """CASE with string and integer outputs fails."""
        with pytest.raises(InvalidSyntaxException, match="same output datatype"):
            parse_text("""
                const x <- 1;
                auto y <- case when x = 1 then 'string' else 123 end;
                select y;
                """)

    def test_case_consistent_string_succeeds(self):
        """CASE expression with consistent string outputs succeeds."""
        env, _ = parse_text("""
            const x <- 1;
            auto y <- case when x = 1 then 'one' else 'other' end;
            select y;
            """)
        assert env.concepts["y"].datatype == DataType.STRING

    def test_case_consistent_integer_succeeds(self):
        """CASE expression with consistent integer outputs succeeds."""
        env, _ = parse_text("""
            const x <- 1;
            auto y <- case when x = 1 then 10 else 20 end;
            select y;
            """)
        assert env.concepts["y"].datatype == DataType.INTEGER

    def test_case_with_null_branch_succeeds(self):
        """CASE expression with null branch is allowed."""
        env, _ = parse_text("""
            const x <- 1;
            auto y <- case when x = 1 then 10 else null end;
            select y;
            """)


# =============================================================================
# ARITHMETIC OPERATION TYPE CHECKING
# =============================================================================


class TestArithmeticOperationTypes:
    """Tests for arithmetic operation type validation."""

    def test_add_string_and_integer_fails(self):
        """Cannot add string and integer."""
        with pytest.raises((TypeError, InvalidSyntaxException)):
            parse_text("""
                const x <- 'hello';
                const y <- 1;
                auto z <- x + y;
                select z;
                """)

    def test_subtract_string_and_integer_fails(self):
        """Cannot subtract string from integer."""
        with pytest.raises((TypeError, InvalidSyntaxException)):
            parse_text("""
                const x <- 'hello';
                const y <- 1;
                auto z <- y - x;
                select z;
                """)

    def test_multiply_string_and_integer_fails(self):
        """Cannot multiply string and integer."""
        with pytest.raises((TypeError, InvalidSyntaxException)):
            parse_text("""
                const x <- 'hello';
                const y <- 2;
                auto z <- x * y;
                select z;
                """)

    def test_divide_string_by_integer_fails(self):
        """Cannot divide string by integer."""
        with pytest.raises((TypeError, InvalidSyntaxException)):
            parse_text("""
                const x <- 'hello';
                const y <- 2;
                auto z <- x / y;
                select z;
                """)

    def test_mod_string_and_integer_fails(self):
        """MOD operation between string and integer should fail."""
        with pytest.raises((TypeError, InvalidSyntaxException)):
            parse_text("""
                const x <- 'hello';
                const y <- 2;
                auto z <- mod(x, y);
                select z;
                """)

    def test_add_integers_succeeds(self):
        """Can add two integers."""
        env, _ = parse_text("""
            const x <- 1;
            const y <- 2;
            auto z <- x + y;
            select z;
            """)
        assert env.concepts["z"].datatype == DataType.INTEGER

    def test_add_integer_and_float_succeeds(self):
        """Can add integer and float."""
        env, _ = parse_text("""
            const x <- 1;
            const y <- 2.5;
            auto z <- x + y;
            select z;
            """)
        assert env.concepts["z"].datatype == DataType.FLOAT

    def test_divide_integers_produces_float(self):
        """Division of integers produces float."""
        env, _ = parse_text("""
            const x <- 10;
            const y <- 3;
            auto z <- x / y;
            select z;
            """)


# =============================================================================
# STRING OPERATION TYPE CHECKING
# =============================================================================


class TestStringOperationTypes:
    """Tests for string operation type validation."""

    def test_concat_strings_succeeds(self):
        """Can concatenate strings."""
        env, _ = parse_text("""
            const x <- 'hello';
            const y <- 'world';
            auto z <- concat(x, y);
            select z;
            """)
        assert env.concepts["z"].datatype == DataType.STRING

    def test_like_with_non_string_fails(self):
        """LIKE operator requires string operands."""
        with pytest.raises((TypeError, InvalidSyntaxException)):
            parse_text("""
                const x <- 123;
                select x where like(x, '%test%');
                """)

    def test_like_with_string_succeeds(self):
        """LIKE operator works with string operands."""
        env, _ = parse_text("""
            const x <- 'hello';
            select x where like(x, '%ell%');
            """)

    def test_concat_integer_directly_fails(self):
        """Direct integer concatenation should require explicit cast."""
        with pytest.raises((TypeError, InvalidSyntaxException)):
            parse_text("""
                const x <- 123;
                auto y <- concat(x, 'suffix');
                select y;
                """)


# =============================================================================
# CUSTOM TYPE / TRAIT VALIDATION
# =============================================================================


class TestCustomTypeTraitValidation:
    """Tests for custom type and trait validation in functions."""

    def test_custom_function_missing_trait_fails(self):
        """Custom function expecting trait fails when trait not present."""
        env, _ = parse_text("""
            type positive int;
            key field int::positive;
            def add_positive(x: int::positive, y: int::positive) -> x + y;

            datasource test (field) grain (field) query '''select 1 as field''';
            """)
        dialects = Dialects.DUCK_DB.default_executor(environment=env, hooks=[])
        with pytest.raises(InvalidSyntaxException, match="expected traits"):
            dialects.parse_text("""
                select @add_positive(1, 2) as result;
                """)

    def test_custom_function_with_trait_succeeds(self):
        """Custom function with correct trait succeeds."""
        env, _ = parse_text("""
            type positive int;
            key field int::positive;
            def add_positive(x: int::positive, y: int::positive) -> x + y;

            datasource test (field) grain (field) query '''select 1 as field''';
            select @add_positive(field, 2::int::positive) as result;
            """)


# =============================================================================
# AGGREGATE FUNCTION TYPE CHECKING
# =============================================================================


class TestAggregateFunctionTypes:
    """Tests for aggregate function type validation."""

    def test_count_accepts_any_type(self):
        """COUNT accepts any type."""
        env, _ = parse_text("""
            const x <- 'hello';
            auto y <- count(x);
            select y;
            """)
        assert env.concepts["y"].datatype == DataType.INTEGER

    def test_count_distinct_accepts_any_type(self):
        """COUNT DISTINCT accepts any type."""
        env, _ = parse_text("""
            const x <- 'hello';
            auto y <- count_distinct(x);
            select y;
            """)
        assert env.concepts["y"].datatype == DataType.INTEGER

    def test_max_with_numeric_succeeds(self):
        """MAX works with numeric types."""
        env, _ = parse_text("""
            const x <- 1;
            auto y <- max(x);
            select y;
            """)

    def test_min_with_numeric_succeeds(self):
        """MIN works with numeric types."""
        env, _ = parse_text("""
            const x <- 1;
            auto y <- min(x);
            select y;
            """)

    def test_bool_and_with_non_bool_fails(self):
        """BOOL_AND requires boolean argument."""
        with pytest.raises(InvalidSyntaxException, match="Invalid argument type"):
            parse_text("""
                const x <- 1;
                auto y <- bool_and(x);
                select y;
                """)

    def test_bool_or_with_non_bool_fails(self):
        """BOOL_OR requires boolean argument."""
        with pytest.raises(InvalidSyntaxException, match="Invalid argument type"):
            parse_text("""
                const x <- 1;
                auto y <- bool_or(x);
                select y;
                """)

    def test_bool_and_with_bool_succeeds(self):
        """BOOL_AND works with boolean."""
        env, _ = parse_text("""
            const x <- true;
            auto y <- bool_and(x);
            select y;
            """)

    def test_bool_or_with_bool_succeeds(self):
        """BOOL_OR works with boolean."""
        env, _ = parse_text("""
            const x <- true;
            auto y <- bool_or(x);
            select y;
            """)


# =============================================================================
# CAST TYPE CHECKING
# =============================================================================


class TestCastTypeChecking:
    """Tests for CAST operation type validation."""

    def test_cast_integer_to_string(self):
        """Can cast integer to string."""
        env, _ = parse_text("""
            const x <- 123;
            auto y <- cast(x as string);
            select y;
            """)
        assert env.concepts["y"].datatype == DataType.STRING

    def test_cast_string_to_integer(self):
        """Can cast string to integer."""
        env, _ = parse_text("""
            const x <- '123';
            auto y <- cast(x as int);
            select y;
            """)
        assert env.concepts["y"].datatype == DataType.INTEGER

    def test_cast_shorthand_syntax(self):
        """Can use shorthand :: cast syntax."""
        env, _ = parse_text("""
            const x <- 123;
            auto y <- x::string;
            select y;
            """)
        assert env.concepts["y"].datatype == DataType.STRING


# =============================================================================
# NULL HANDLING TYPE CHECKING
# =============================================================================


class TestNullHandlingTypes:
    """Tests for NULL value type handling."""

    def test_nullif_succeeds(self):
        """NULLIF comparing same types works."""
        env, _ = parse_text("""
            const x <- 1;
            auto y <- nullif(x, 0);
            select y;
            """)

    def test_is_null_with_any_type(self):
        """IS NULL syntax works with any type."""
        env, _ = parse_text("""
            const x <- 'hello';
            auto y <- x is null;
            select y;
            """)
        assert env.concepts["y"].datatype == DataType.BOOL


# =============================================================================
# DATE/DATETIME TYPE COMPATIBILITY
# =============================================================================


class TestDateTimeTypeCompatibility:
    """Tests for date/datetime type compatibility."""

    def test_date_vs_datetime_comparison_fails(self):
        """Date and datetime comparison should be type-safe."""
        with pytest.raises((InvalidSyntaxException, SyntaxError)):
            parse_text("""
                const x <- cast('2023-01-01' as date);
                const y <- cast('2023-01-01 12:00:00' as datetime);
                select x where x = y;
                """)

    def test_date_vs_date_comparison_succeeds(self):
        """Date can be compared with date."""
        env, _ = parse_text("""
            const x <- cast('2023-01-01' as date);
            const y <- cast('2023-01-02' as date);
            select x where x < y;
            """)


# =============================================================================
# ARRAY TYPE CHECKING
# =============================================================================


class TestArrayTypeChecking:
    """Tests for array type checking."""

    def test_typed_array_literal(self):
        """Typed array literal works."""
        env, _ = parse_text("""
            const x <- [1, 2, 3]::array<int>;
            select x;
            """)

    def test_mixed_array_literal_fails(self):
        """Array literal with mixed types should fail or have specific behavior."""
        with pytest.raises((TypeError, InvalidSyntaxException, ParseError)):
            parse_text("""
                const x <- [1, 'two', 3];
                select x;
                """)

    def test_index_access_on_string_fails(self):
        """Index access on string (not array/map) should fail or behave predictably."""
        with pytest.raises((TypeError, InvalidSyntaxException)):
            parse_text("""
                const x <- 'hello';
                auto y <- x[0];
                select y;
                """)

    def test_index_access_on_array_succeeds(self):
        """Index access on array works."""
        env, _ = parse_text("""
            const x <- [1, 2, 3];
            auto y <- x[0];
            select y;
            """)


# =============================================================================
# INTEGRATION TESTS WITH EXECUTOR
# =============================================================================


class TestTypeCheckingWithExecutor:
    """Integration tests that run through the full executor pipeline."""

    def test_select_type_mismatch_in_where(self):
        """Type mismatch in WHERE clause is caught."""
        executor = Dialects.DUCK_DB.default_executor()
        with pytest.raises(InvalidSyntaxException, match="Cannot compare"):
            executor.parse_text("""
                key id int;
                property id.name string;

                datasource test (
                    id: id,
                    name: name
                )
                grain (id)
                query '''select 1 as id, 'test' as name''';

                select id where id = name;
                """)

    def test_aggregate_on_wrong_type(self):
        """Aggregate function on wrong type is caught."""
        executor = Dialects.DUCK_DB.default_executor()
        with pytest.raises(InvalidSyntaxException, match="Invalid argument type"):
            executor.parse_text("""
                key id int;
                property id.name string;

                datasource test (
                    id: id,
                    name: name
                )
                grain (id)
                query '''select 1 as id, 'test' as name''';

                select sum(name) as total;
                """)

    def test_valid_query_succeeds(self):
        """Valid query with proper types succeeds."""
        executor = Dialects.DUCK_DB.default_executor()
        executor.parse_text("""
            key id int;
            property id.name string;
            property id.value float;

            datasource test (
                id: id,
                name: name,
                value: value
            )
            grain (id)
            query '''select 1 as id, 'test' as name, 1.5 as value''';

            select
                id,
                name,
                sum(value) as total_value
            where id > 0 and like(name, '%test%');
            """)
