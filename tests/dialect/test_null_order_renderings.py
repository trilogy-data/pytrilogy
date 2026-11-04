"""ORDER BY renderings behind `formal/Trilogy/Windows.lean` (`render_places`)
on every dialect; tests/windows/test_window_laws.py runs them on DuckDB and
SQLite."""

import pytest

from trilogy.core.enums import NullSort, Ordering
from trilogy.dialect.base import BaseDialect
from trilogy.dialect.bigquery import BigqueryDialect
from trilogy.dialect.clickhouse import ClickhouseDialect
from trilogy.dialect.duckdb import DuckDBDialect
from trilogy.dialect.mysql import MySQLDialect
from trilogy.dialect.postgres import PostgresDialect
from trilogy.dialect.presto import PrestoDialect
from trilogy.dialect.snowflake import SnowflakeDialect
from trilogy.dialect.sql_server import SqlServerDialect
from trilogy.dialect.sqlite import SQLiteDialect

O = Ordering
FLAG = "CASE WHEN x IS NULL THEN 1 ELSE 0 END"

RENDERINGS: dict[type[BaseDialect], dict[Ordering, str]] = {
    DuckDBDialect: {
        O.ASCENDING: "x asc",
        O.DESCENDING: "x desc",
        O.ASC_NULLS_FIRST: "x asc nulls first",
        O.DESC_NULLS_LAST: "x desc nulls last",
    },
    PrestoDialect: {O.ASCENDING: "x asc", O.DESCENDING: "x desc"},
    ClickhouseDialect: {O.ASCENDING: "x asc", O.DESCENDING: "x desc"},
    SQLiteDialect: {
        O.ASCENDING: "x asc nulls last",
        O.DESCENDING: "x desc",
        O.ASC_NULLS_FIRST: "x asc nulls first",
    },
    BigqueryDialect: {O.ASCENDING: "x asc nulls last", O.DESCENDING: "x desc"},
    PostgresDialect: {
        O.ASCENDING: "x asc",
        O.DESCENDING: "x desc nulls last",
        O.DESC_NULLS_FIRST: "x desc nulls first",
    },
    SnowflakeDialect: {O.ASCENDING: "x asc", O.DESCENDING: "x desc nulls last"},
    SqlServerDialect: {
        O.ASCENDING: f"{FLAG} asc, x asc",
        O.DESCENDING: "x desc",
        O.ASC_NULLS_FIRST: "x asc",
        O.DESC_NULLS_FIRST: f"{FLAG} desc, x desc",
    },
    MySQLDialect: {
        O.ASCENDING: "(x) IS NULL asc, x asc",
        O.DESC_NULLS_FIRST: "(x) IS NULL desc, x desc",
    },
}


@pytest.mark.parametrize(
    "dialect, order, expected",
    [(d, o, e) for d, cases in RENDERINGS.items() for o, e in cases.items()],
    ids=repr,
)
def test_ordering_rendering(
    dialect: type[BaseDialect], order: Ordering, expected: str
) -> None:
    assert dialect().render_ordering("x", order) == expected


@pytest.mark.parametrize("dialect", list(RENDERINGS), ids=lambda d: d.__name__)
@pytest.mark.parametrize("order", [O.ASC_NULLS_AUTO, O.DESC_NULLS_AUTO])
def test_nulls_auto_renders_the_direction(
    dialect: type[BaseDialect], order: Ordering
) -> None:
    assert dialect().render_ordering("x", order) == f"x {order.direction}"


def _placement(dialect: type[BaseDialect], rendered: str, descending: bool) -> bool:
    """Whether the engine running `rendered` puts NULLs first."""
    if rendered.startswith(("CASE", "(x) IS NULL")):
        return rendered.split(",")[0].endswith("desc")
    if "nulls" in rendered:
        return rendered.endswith("first")
    return dialect.NULL_SORT.nulls_first(descending)


@pytest.mark.parametrize("dialect", list(RENDERINGS), ids=lambda d: d.__name__)
@pytest.mark.parametrize(
    "order", [o for o in Ordering if o.nulls_first is not None], ids=lambda o: o.value
)
def test_rendering_places_nulls_as_asked(
    dialect: type[BaseDialect], order: Ordering
) -> None:
    rendered = dialect().render_ordering("x", order)
    assert _placement(dialect, rendered, order.descending) == order.nulls_first


@pytest.mark.parametrize(
    "sort, asc_first, desc_first",
    [
        (NullSort.LAST, False, False),
        (NullSort.SMALLEST, True, False),
        (NullSort.LARGEST, False, True),
    ],
)
def test_engine_defaults(sort: NullSort, asc_first: bool, desc_first: bool) -> None:
    assert (sort.nulls_first(False), sort.nulls_first(True)) == (asc_first, desc_first)
