"""`formal/Trilogy/Lookup.lean`: a WHERE on a dimension's column read off a
fact through a `?~` key filters the fact's rows and adds none of the
dimension's (`where_on_lookup_is_inner`, `full_lookup_adds_dimension_rows`,
tpc-ds q83); a count only tested against zero renders without DISTINCT, which
cannot change its answer (`zero_test_ignores_repeats`), and one tested against
another value keeps it (`exact_test_reads_repeats`)."""

import pytest

from trilogy import Dialects
from trilogy.core.enums import ComparisonOperator
from trilogy.core.models.build import BuildComparison, Factory
from trilogy.dialect.base import zero_tested_count

LOOKUP_MODEL = """
key day_sk int;
property day_sk.week int;
key order_id int;
property order_id.qty int;
key item int;

datasource days (sk: day_sk, wk: week)
grain (day_sk)
query '''select 1 sk, 10 wk union all select 2, 10 union all select 3, 99''';

datasource returns (oid: order_id, it: item, d: ?~day_sk, q: qty)
grain (order_id)
query '''select 1 oid, 7 it, 1 d, 5 q union all select 2, 7, null, 3
union all select 3, 8, 2, 4''';
"""

COUNT_MODEL = """
key order_id int;
key item_sk int;
property item_sk.item_id string;
key channel string;
property <order_id, item_sk, channel>.qty int;

datasource items (sk: item_sk, id: item_id)
grain (item_sk)
query '''select 1 sk, 'x' id union all select 2, 'x' union all select 3, 'y' ''';

datasource lines (oid: order_id, sk: item_sk, ch: channel, q: qty)
grain (order_id, item_sk, channel)
query '''select 1 oid, 1 sk, 'S' ch, 5 q union all select 1, 2, 'S', 2
union all select 2, 3, 'W', 4 union all select 3, 3, 'S', 1''';

auto present <- count(order_id ? channel = 'S') by item_id;
auto total <- sum(qty ? channel = 'S') by item_id;
"""


def _run(model: str, query: str) -> tuple[str, list]:
    executor = Dialects.DUCK_DB.default_executor()
    sql = executor.generate_sql(model + query)[-1]
    rows = executor.execute_text(model + query)[-1].fetchall()
    return sql, sorted(rows, key=lambda r: tuple((v is None, v) for v in r))


def test_where_on_dimension_adds_no_dimension_row():
    sql, rows = _run(
        LOOKUP_MODEL,
        "auto weeks <- week ? week in (10, 99);\n"
        "where week in weeks select item, sum(qty) as q;",
    )
    assert rows == [(7, 5), (8, 4)]
    assert "OUTER" not in sql and "FULL" not in sql


def test_lookup_without_where_keeps_null_keyed_rows():
    _, rows = _run(LOOKUP_MODEL, "select item, sum(qty) as q;")
    assert rows == [(7, 8), (8, 4)]


@pytest.mark.parametrize(
    "having,expected",
    [
        ("present > 0", [("x", 7), ("y", 1)]),
        ("0 < present", [("x", 7), ("y", 1)]),
        ("present >= 1", [("x", 7), ("y", 1)]),
        ("present = 0", []),
        ("present != 0", [("x", 7), ("y", 1)]),
    ],
)
def test_zero_tested_count_renders_plain(having: str, expected: list):
    sql, rows = _run(COUNT_MODEL, f"select item_id, total having {having};")
    assert rows == expected
    assert "count(distinct" not in sql


def test_count_against_one_keeps_distinct():
    sql, rows = _run(COUNT_MODEL, "select item_id, total having present = 1;")
    assert rows == [("x", 7), ("y", 1)]
    assert "count(distinct" in sql


@pytest.mark.parametrize(
    "operator,value,tested",
    [
        (ComparisonOperator.GT, 0, True),
        (ComparisonOperator.GTE, 1, True),
        (ComparisonOperator.LT, 1, True),
        (ComparisonOperator.LTE, 0, True),
        (ComparisonOperator.EQ, 0, True),
        (ComparisonOperator.NE, 0, True),
        (ComparisonOperator.GT, -3, True),
        (ComparisonOperator.GT, 1, False),
        (ComparisonOperator.GTE, 2, False),
        (ComparisonOperator.EQ, 1, False),
        (ComparisonOperator.LT, 2, False),
    ],
)
def test_zero_tested_count(operator: ComparisonOperator, value: int, tested: bool):
    executor = Dialects.DUCK_DB.default_executor()
    executor.parse_text(COUNT_MODEL)
    present = executor.environment.concepts["present"]
    built = Factory(environment=executor.environment).build(present)
    comparison = BuildComparison(left=built, right=value, operator=operator)
    assert (zero_tested_count(comparison) == built.address) is tested
