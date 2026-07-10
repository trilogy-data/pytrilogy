"""Multi-key coalescing scoped join (`subset join`) onto a rowset under
`by rollup` — the q14 family.

Three defects, all fixed together:
1. The rollup CTE emitted the join-key group's canonical addresses as bare,
   un-grouped columns next to `GROUP BY ROLLUP(...)` (BinderException).
2. Post-rollup enrichment joined the mixed-grain rollup output back on its
   dims, silently dropping every subtotal/grand-total row.
3. Reduced shapes (2-key join without/with partial rollup) failed to build
   (DisconnectedConcepts) because the WHERE row-args were force-sourced at the
   top level instead of below the grouping.
"""

import pytest

from trilogy import Dialects

FIXTURE = """
key sale_id int;
property sale_id.channel string;
property sale_id.brand int;
property sale_id.class_id int;
property sale_id.category int;
property sale_id.qty int;
property sale_id.price float;

datasource sales (
    sale_id: sale_id,
    channel: channel,
    brand: brand,
    class_id: class_id,
    category: category,
    qty: qty,
    price: price,
)
grain (sale_id)
query '''
select 1 as sale_id, 'A' as channel, 1 as brand, 1 as class_id, 1 as category, 2 as qty, 10.0 as price union all
select 2, 'A', 1, 1, 1, 2, 10.0 union all
select 3, 'B', 1, 1, 1, 3, 10.0 union all
select 4, 'C', 1, 1, 1, 1, 30.0 union all
select 5, 'A', 2, 1, 1, 1, 5.0 union all
select 6, 'B', 2, 1, 1, 1, 5.0 union all
select 7, 'C', 3, 2, 2, 1, 7.0
''';

rowset all_ch <-
select brand as b, class_id as c, category as g
having count_distinct(channel) = 3;

rowset stats <-
select avg(qty * price) as overall_avg;

rowset per_ch <-
select channel as ch, brand as b, class_id as c, category as g,
    sum(qty * price) as ts, count(sale_id) as cnt;
"""

SUBSET_JOIN_ROLLUP = """
select per_ch.ch,
    case when grouping(per_ch.b) = 1 then null else per_ch.b end as out_b,
    case when grouping(per_ch.c) = 1 then null else per_ch.c end as out_c,
    case when grouping(per_ch.g) = 1 then null else per_ch.g end as out_g,
    sum(per_ch.ts) as total_sales,
    sum(per_ch.cnt) as total_count
subset join per_ch.b = all_ch.b
    and per_ch.c = all_ch.c
    and per_ch.g = all_ch.g
where all_ch.b is not null and all_ch.c is not null and all_ch.g is not null
by rollup (per_ch.ch, per_ch.b, per_ch.c, per_ch.g)
having sum(per_ch.ts) > stats.overall_avg;
"""

# same semantics via the canonical concat-tuple-key membership idiom
CANONICAL_TUPLE_KEY = """
auto tuple_key <- concat(brand::string, '|', class_id::string, '|', category::string);
rowset cross_tuples <-
select tuple_key as ck
having count_distinct(channel) = 3;
rowset per_ch2 <-
where tuple_key in cross_tuples.ck
select channel as ch, brand as b, class_id as c, category as g,
    sum(qty * price) as ts, count(sale_id) as cnt;
select per_ch2.ch,
    case when grouping(per_ch2.b) = 1 then null else per_ch2.b end as out_b,
    case when grouping(per_ch2.c) = 1 then null else per_ch2.c end as out_c,
    case when grouping(per_ch2.g) = 1 then null else per_ch2.g end as out_g,
    sum(per_ch2.ts) as total_sales,
    sum(per_ch2.cnt) as total_count
by rollup (per_ch2.ch, per_ch2.b, per_ch2.c, per_ch2.g)
having sum(per_ch2.ts) > stats.overall_avg;
"""


def _norm(rows):
    return sorted(
        (
            tuple(round(float(v), 2) if isinstance(v, (int, float)) else v for v in row)
            for row in rows
        ),
        key=lambda t: tuple((x is None, x) for x in t),
    )


@pytest.fixture
def executor():
    exec = Dialects.DUCK_DB.default_executor()
    exec.execute_text(FIXTURE)
    return exec


def test_three_key_subset_join_rollup_executes_and_matches_canonical(executor):
    subset_rows = _norm(executor.execute_text(SUBSET_JOIN_ROLLUP)[0].fetchall())
    canonical_rows = _norm(executor.execute_text(CANONICAL_TUPLE_KEY)[0].fetchall())
    assert subset_rows == canonical_rows
    # only tuple (1,1,1) is present in all three channels
    assert not any(row[1] in (2, 3) for row in subset_rows)
    # 3 leaves + 3 levels of subtotals per channel + one grand total
    assert len(subset_rows) == 13
    grand_totals = [r for r in subset_rows if r[0] is None]
    assert grand_totals == [(None, None, None, None, 100.0, 4)]
    channel_subtotals = {
        r[0]: r[4] for r in subset_rows if r[0] is not None and r[1] is None
    }
    assert channel_subtotals == {"A": 40.0, "B": 30.0, "C": 30.0}


def test_two_key_subset_join_no_rollup_builds(executor):
    rows = executor.execute_text("""
select per_ch.ch, per_ch.b, sum(per_ch.ts) as total_sales
subset join per_ch.b = all_ch.b and per_ch.c = all_ch.c
where all_ch.b is not null and all_ch.c is not null
order by per_ch.ch asc, per_ch.b asc;
""")[0].fetchall()
    assert rows
    assert not any(row[1] == 3 for row in rows)


def test_two_key_subset_join_partial_rollup_builds(executor):
    rows = executor.execute_text("""
select per_ch.ch, sum(per_ch.ts) as total_sales
subset join per_ch.b = all_ch.b and per_ch.c = all_ch.c
where all_ch.b is not null and all_ch.c is not null
by rollup (per_ch.ch);
""")[0].fetchall()
    channels = {row[0] for row in rows}
    assert None in channels, "rollup grand-total row missing"
