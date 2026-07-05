"""`by rollup/cube` over passthrough (pre-aggregated rowset) measures — the
q05 family.

The grouping spec travels on aggregate wrappers; a select that only passes
through rowset measures had no carrier, so the clause was silently discarded
(plain GROUP BY, zero subtotal rows). Additive (sum/count-derived)
passthroughs now re-aggregate with an implicit sum; non-additive passthroughs
raise instead of dropping the clause or emitting invalid SQL.
"""

import pytest

from trilogy import Dialects
from trilogy.core.exceptions import InvalidSyntaxException

FIXTURE = """
key sale_id int;
property sale_id.channel string;
property sale_id.eid int;
property sale_id.amount float;

datasource sales (
    sale_id: sale_id,
    channel: channel,
    eid: eid,
    amount: amount,
)
grain (sale_id)
query '''
select 1 as sale_id, 'A' as channel, 1 as eid, 10.0 as amount union all
select 2, 'A', 1, 20.0 union all
select 3, 'A', 2, 5.0 union all
select 4, 'B', 1, 7.0
''';

rowset agg <-
select channel as ch, eid as e, sum(amount) as ext, count(sale_id) as cnt;
"""

EXPECTED = [
    (None, None, 42.0),
    ("A", None, 35.0),
    ("A", 1, 30.0),
    ("A", 2, 5.0),
    ("B", None, 7.0),
    ("B", 1, 7.0),
]


def _norm(rows):
    return sorted(
        (
            tuple(round(float(v), 2) if isinstance(v, (int, float)) else v for v in row)
            for row in rows
        ),
        key=lambda t: tuple((x is not None, x) for x in t),
    )


@pytest.fixture
def executor():
    exec = Dialects.DUCK_DB.default_executor()
    exec.execute_text(FIXTURE)
    return exec


def test_passthrough_rollup_emits_rollup_and_subtotals(executor):
    sql = executor.generate_sql("""
select agg.ch, agg.e, agg.ext as total
by rollup (agg.ch, agg.e);
""")[-1]
    assert "ROLLUP" in sql.upper()
    rows = _norm(executor.execute_raw_sql(sql).fetchall())
    assert rows == EXPECTED


def test_passthrough_rollup_matches_fresh_aggregate_form(executor):
    passthrough = _norm(executor.execute_text("""
select agg.ch, agg.e, agg.ext as total
by rollup (agg.ch, agg.e);
""")[0].fetchall())
    fresh = _norm(executor.execute_text("""
select agg.ch, agg.e, sum(agg.ext) as total
by rollup (agg.ch, agg.e);
""")[0].fetchall())
    assert passthrough == fresh == EXPECTED


def test_mixed_passthrough_and_fresh_aggregate(executor):
    rows = _norm(executor.execute_text("""
select agg.ch, agg.e, agg.ext as total, sum(agg.cnt) as total_cnt
by rollup (agg.ch, agg.e);
""")[0].fetchall())
    assert [r[:3] for r in rows] == EXPECTED
    assert rows[0] == (None, None, 42.0, 4)


def test_passthrough_cube_emits_subtotals(executor):
    rows = executor.execute_text("""
select agg.ch, agg.e, agg.ext as total
by cube (agg.ch, agg.e);
""")[0].fetchall()
    assert (None, None, 42.0) in {
        tuple(round(float(v), 2) if isinstance(v, (int, float)) else v for v in r)
        for r in rows
    }
    # cube adds the eid-only grouping set rollup lacks
    assert any(r[0] is None and r[1] is not None for r in rows)


def test_non_additive_passthrough_rollup_raises(executor):
    executor.execute_text(
        "rowset avg_agg <- select channel as ch, eid as e, avg(amount) as aext;"
    )
    with pytest.raises(InvalidSyntaxException, match="cannot re-aggregate"):
        executor.execute_text("""
select avg_agg.ch, avg_agg.e, avg_agg.aext as total
by rollup (avg_agg.ch, avg_agg.e);
""")


def test_keys_only_rollup_raises(executor):
    with pytest.raises(InvalidSyntaxException, match="at least one aggregate"):
        executor.execute_text("""
select agg.ch, agg.e
by rollup (agg.ch, agg.e);
""")
