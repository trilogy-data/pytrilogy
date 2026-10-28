import pytest

from trilogy import Dialects
from trilogy.core.enums import FunctionType
from trilogy.dialect.base import BaseDialect
from trilogy.dialect.bigquery import BigqueryDialect
from trilogy.dialect.clickhouse import ClickhouseDialect
from trilogy.dialect.postgres import PostgresDialect
from trilogy.dialect.presto import PrestoDialect
from trilogy.dialect.snowflake import SnowflakeDialect


def _value(text: str):
    executor = Dialects.DUCK_DB.default_executor()
    return executor.execute_text(text)[-1].fetchall()[0][0]


@pytest.mark.parametrize(
    "expr, expected",
    [
        ("bit_and(12, 10)", 8),
        ("bit_or(12, 10)", 14),
        ("bit_xor(12, 10)", 6),
        ("bit_shift_left(3, 4)", 48),
        ("bit_shift_right(48, 4)", 3),
        ("bit_shift_left(1::bigint, 40)", 1 << 40),
    ],
)
def test_bitwise(expr: str, expected: int) -> None:
    assert _value(f"select {expr} -> v;") == expected


def test_array_reduce_folds_with_a_def() -> None:
    assert (
        _value(
            "def weighted(acc, x) -> acc * 10 + x;\n"
            "select array_reduce(generate_array(1, 4, 1), @weighted, 0) -> v;"
        )
        == 1234
    )


def test_array_reduce_state_takes_the_initial_type() -> None:
    assert (
        _value(
            "def shift_in(acc, x) -> bit_or(bit_shift_left(acc, 8), x);\n"
            "select array_reduce(generate_array(1, 5, 1), @shift_in, 0::bigint) -> v;"
        )
        == 0x0102030405
    )


def test_array_reduce_needs_two_arguments() -> None:
    with pytest.raises(Exception, match="exactly 2 arguments"):
        _value(
            "def one(x) -> x;\n"
            "select array_reduce(generate_array(1, 4, 1), @one, 0) -> v;"
        )


def test_array_reduce_needs_an_array() -> None:
    with pytest.raises(Exception, match="must be applied to an array"):
        _value("def add(acc, x) -> acc + x;\nselect array_reduce(5, @add, 0) -> v;")


@pytest.mark.parametrize(
    "dialect, xor",
    [
        (BaseDialect, "(a ^ b)"),
        (BigqueryDialect, "(a ^ b)"),
        (PostgresDialect, "(a # b)"),
        (SnowflakeDialect, "BITXOR(a, b)"),
        (PrestoDialect, "bitwise_xor(a, b)"),
        (ClickhouseDialect, "bitXor(a, b)"),
    ],
)
def test_xor_renders_per_dialect(dialect: type[BaseDialect], xor: str) -> None:
    assert dialect.FUNCTION_MAP[FunctionType.BIT_XOR](["a", "b"], []) == xor


def test_array_reduce_is_refused_where_there_is_no_fold() -> None:
    with pytest.raises(NotImplementedError, match="array_reduce"):
        BigqueryDialect.FUNCTION_MAP[FunctionType.ARRAY_REDUCE](
            ["arr", "acc", "x", "body", "0"], []
        )
