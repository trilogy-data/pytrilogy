"""Every aggregate function must have a grain-match formula.

When a node already sits at an aggregate's grain (one row per group) the
renderer emits the aggregate via ``FUNCTION_GRAIN_MATCH_MAP`` with no GROUP BY.
An aggregate missing an explicit entry there falls through to its real
``FUNCTION_MAP`` rendering (e.g. ``stddev_samp(x)``) and produces an invalid
ungrouped aggregate -- the q17 composite-union-join binder error. This guard
fails loudly if a new aggregate is added without a single-row collapse formula.
"""

import pytest

from trilogy.core.enums import FunctionClass, FunctionType
from trilogy.dialect.base import BaseDialect
from trilogy.dialect.bigquery import BigqueryDialect
from trilogy.dialect.clickhouse import ClickhouseDialect
from trilogy.dialect.duckdb import DuckDBDialect
from trilogy.dialect.postgres import PostgresDialect
from trilogy.dialect.presto import PrestoDialect, TrinoDialect
from trilogy.dialect.snowflake import SnowflakeDialect
from trilogy.dialect.sql_server import SqlServerDialect
from trilogy.dialect.sqlite import SQLiteDialect

DIALECTS = [
    BaseDialect,
    BigqueryDialect,
    ClickhouseDialect,
    DuckDBDialect,
    PostgresDialect,
    PrestoDialect,
    TrinoDialect,
    SnowflakeDialect,
    SqlServerDialect,
    SQLiteDialect,
]

AGGREGATES = FunctionClass.AGGREGATE_FUNCTIONS.value


@pytest.mark.parametrize("dialect", DIALECTS, ids=lambda d: d.__name__)
def test_all_aggregates_have_grain_match_formula(dialect):
    grain_match = dialect.FUNCTION_GRAIN_MATCH_MAP
    function_map = dialect.FUNCTION_MAP
    missing = [
        op
        for op in AGGREGATES
        # An aggregate with no explicit grain-match override inherits the real
        # aggregate rendering from FUNCTION_MAP (same callable object), which is
        # invalid without a GROUP BY.
        if grain_match.get(op) is None or grain_match[op] is function_map.get(op)
    ]
    assert not missing, (
        f"{dialect.__name__} aggregates missing a single-row grain-match "
        f"formula (fall through to an invalid ungrouped aggregate): "
        f"{[op.value for op in missing]}"
    )


def test_grain_match_collapses_stddev_and_variance():
    # The specific q17 regression: two-pass aggregates collapse to NULL over a
    # single-row group rather than rendering the real aggregate.
    gm = BaseDialect.FUNCTION_GRAIN_MATCH_MAP
    assert gm[FunctionType.STDDEV](["x"], []) == "NULL"
    assert gm[FunctionType.VARIANCE](["x"], []) == "NULL"
