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


@pytest.mark.parametrize(
    "expr, pattern, read",
    [
        ("v::int", NUMBER_PATTERN, f"CAST(CAST({TEXT} AS DECIMAL(65, 30)) AS SIGNED)"),
        ("v::numeric(12,2)", NUMBER_PATTERN, f"CAST({TEXT} AS DECIMAL(12,2))"),
        ("v::float", NUMBER_PATTERN, f"CAST({TEXT} AS DOUBLE)"),
        (
            "v::date",
            DATE_PATTERN,
            "STR_TO_DATE(REPLACE(SUBSTRING_INDEX(SUBSTRING_INDEX("
            f"{TEXT}, ' ', 1), 'T', 1), '/', '-'), '%Y-%c-%e')",
        ),
    ],
)
def test_mysql_reads_the_reference_shape(expr: str, pattern: str, read: str) -> None:
    assert _select(Dialects.MYSQL, expr) == (
        f"CASE WHEN `s`.`v` REGEXP '{pattern}' THEN {read} END"
    )


def test_mysql_keeps_other_casts() -> None:
    assert _select(Dialects.MYSQL, "i::int") == "CAST(`s`.`i` AS SIGNED)"
