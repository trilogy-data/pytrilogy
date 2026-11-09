"""Renderings behind formal/Trilogy/Readings.lean on MySQL, which has no
engine in CI and read a numeric prefix ('abc' AS SIGNED is 0);
tests/core/test_text_read_laws.py runs the laws on DuckDB, SQLite and
ClickHouse. The other dialects with no engine in CI raise on a string with no
reading and are left as they were."""

import pytest

from tests.dialect.test_string_function_renderings import _select
from trilogy import Dialects
from trilogy.core.casts import DATE_PATTERN, NUMBER_PATTERN

TEXT = "REGEXP_REPLACE(`s`.`v`, '^[[:space:]]+|[[:space:]]+$', '')"
EXACT = f"CAST({TEXT} AS DECIMAL(65, 30))"
INT_RANGE = f" AND {EXACT} > -9223372036854775808.5 AND {EXACT} < 9223372036854775807.5"


@pytest.mark.parametrize(
    "expr, guard, read",
    [
        ("v::int", INT_RANGE, f"CAST({EXACT} AS SIGNED)"),
        (
            "v::numeric(12,2)",
            f" AND ABS(ROUND({EXACT}, 2)) < 1e10",
            f"CAST({TEXT} AS DECIMAL(12,2))",
        ),
        ("v::float", "", f"CAST({TEXT} AS DOUBLE)"),
    ],
)
def test_mysql_reads_numbers_in_range(expr: str, guard: str, read: str) -> None:
    """CAST saturates past the target's range (Readings.lean,
    `saturation_is_not_the_value`)."""
    assert _select(Dialects.MYSQL, expr) == (
        f"CASE WHEN `s`.`v` REGEXP '{NUMBER_PATTERN}'{guard} THEN {read} END"
    )


@pytest.mark.parametrize(
    "expr, read",
    [
        (
            "v::date",
            (
                "STR_TO_DATE(REPLACE(SUBSTRING_INDEX(SUBSTRING_INDEX("
                f"{TEXT}, ' ', 1), 'T', 1), '/', '-'), '%Y-%c-%e')"
            ),
        ),
        ("v::datetime", f"CAST(REPLACE({TEXT}, '/', '-') AS DATETIME(6))"),
        ("datetime(v)", f"CAST(REPLACE({TEXT}, '/', '-') AS DATETIME(6))"),
    ],
)
def test_mysql_reads_the_reference_shape(expr: str, read: str) -> None:
    """A zone is left unread here (Instants.lean): CAST reads only `+HH:MM`,
    into the session's zone."""
    assert _select(Dialects.MYSQL, expr) == (
        f"CASE WHEN `s`.`v` REGEXP '{DATE_PATTERN}' THEN {read} END"
    )


def test_mysql_keeps_other_casts() -> None:
    assert _select(Dialects.MYSQL, "i::int") == "CAST(`s`.`i` AS SIGNED)"
