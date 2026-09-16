"""A ROLLUP's subtotal rows survive the join-narrowing optimization.

`UpgradeOuterFromKeySetEquivalence` turns an OUTER join into INNER when both
sides hold the same conceptual value set. A ROLLUP/CUBE/GROUPING SETS side
breaks that premise in a way the value-set tests cannot see: the grouping
itself mints a NULL grouping key on every subtotal row, downstream of the
sources being compared. Narrowing there deletes precisely the subtotal and
grand-total rows the query asked for.

Found by the differential fuzzer on a randomized dataset, not by the fixed
corpus, because the fixed seeds happen to carry a NULL subset key that the
null-safe join pairs the grand total against. Pinned here deterministically:
three fact rows, two kept keys, no NULLs anywhere in the data.
"""

from trilogy import Dialects

MODEL = """
key left_id int;
property left_id.left_key int?;
property left_id.left_value int;
datasource left_facts (id: left_id, k: left_key, value: left_value)
grain (left_id)
query '''select 1 as id, 10 as k, 100 as value
union all select 2, 10, 200
union all select 3, 20, 400''';

key subset_id int;
property subset_id.subset_key int?;
property subset_id.subset_value int;
datasource subset_facts (id: subset_id, k: subset_key, value: subset_value)
grain (subset_id)
query '''select 1 as id, 10 as k, 50 as value
union all select 2, 20, 60''';
"""

QUERY = """
rowset left_rows <- select left_id as row_id, left_key as k, left_value as v;
rowset kept <- where subset_value >= 20 select subset_key as k;

select left_rows.k, sum(left_rows.v) as total
subset join left_rows.k = kept.k
where kept.k is not null
by rollup (left_rows.k)
order by left_rows.k asc nulls last;
"""


def test_rollup_grand_total_survives_a_subset_join_back():
    executor = Dialects.DUCK_DB.default_executor()
    executor.parse_text(MODEL)
    sql = executor.generate_sql(QUERY)[-1]
    rows = [tuple(r) for r in executor.execute_raw_sql(sql).fetchall()]
    # Per-key totals plus the grand total, which is their sum.
    assert rows == [(10, 300), (20, 400), (None, 700)], sql
