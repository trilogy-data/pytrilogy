"""Nullability of union-join (FULL JOIN) payload columns must survive the
MergeNode boundary (q51).

A `union join` rowset null-extends each side's payload columns. When two
window functions over *different* nullable payloads split into separate plan
branches and re-merge, the join must treat those payloads null-safely — a
plain `=` on a null-extended column silently deletes the whole extended side.
"""

from trilogy import Dialects
from trilogy.core.models.environment import Environment

MODEL = """
key web_id int;
property web_id.w_item int;
property web_id.w_date int;
property web_id.w_amount int;
key store_id int;
property store_id.s_item int;
property store_id.s_date int;
property store_id.s_amount int;

datasource web_sales (id: web_id, item: w_item, dt: w_date, amt: w_amount)
grain (web_id)
query '''select * from (values (1,1,1,10),(2,1,2,20)) as t(id,item,dt,amt)''';

datasource store_sales (id: store_id, item: s_item, dt: s_date, amt: s_amount)
grain (store_id)
query '''select * from (values (1,1,2,5),(2,1,3,7),(3,2,1,3)) as t(id,item,dt,amt)''';
"""
# union-join key space: (1,1) web-only, (1,2) both, (1,3) store-only,
# (2,1) store-only -> FULL-JOIN cardinality is 4 rows.

COMBINED = """with wd as
select w_item as ik, w_date as dt, sum(w_amount) as wt;
with sd as
select s_item as ik, s_date as dt, sum(s_amount) as st;
with combined as
select coalesce(wd.ik, sd.ik) as ik,
       coalesce(wd.dt, sd.dt) as dt,
       wd.wt as wt, sd.st as st
union join wd.ik = sd.ik
union join wd.dt = sd.dt;
"""


def _rows(body: str):
    e = Dialects.DUCK_DB.default_executor(environment=Environment())
    e.parse_text(MODEL)
    return [tuple(r) for r in e.execute_query(body).fetchall()]


def test_two_windows_over_different_nullable_payloads_keep_all_rows():
    # The q51 trigger: two running sums over the two null-extended payloads.
    # The single-sided rows (web-only (1,1), store-only (1,3)/(2,1)) must
    # survive, and each running sum must skip the NULL side.
    rows = _rows(COMBINED + """select combined.ik, combined.dt,
  sum(combined.wt) over (partition by combined.ik order by combined.dt asc) as wrun,
  sum(combined.st) over (partition by combined.ik order by combined.dt asc) as srun
order by combined.ik asc, combined.dt asc;""")
    assert rows == [
        (1, 1, 10, None),
        (1, 2, 30, 5),
        (1, 3, 30, 12),
        (2, 1, None, 3),
    ]


def test_one_window_keeps_all_rows():
    rows = _rows(COMBINED + """select combined.ik, combined.dt, combined.st,
  sum(combined.wt) over (partition by combined.ik order by combined.dt asc) as wrun
order by combined.ik asc, combined.dt asc;""")
    assert rows == [
        (1, 1, None, 10),
        (1, 2, 5, 30),
        (1, 3, 7, 30),
        (2, 1, 3, None),
    ]


def test_plain_projection_keeps_all_rows():
    rows = _rows(
        COMBINED + "select combined.ik, combined.dt, combined.wt, combined.st "
        "order by combined.ik asc, combined.dt asc;"
    )
    assert rows == [
        (1, 1, 10, None),
        (1, 2, 20, 5),
        (1, 3, None, 7),
        (2, 1, None, 3),
    ]


def test_window_over_each_payload_matches_single_window_cardinality():
    # Cardinality must be identical however many windows are requested.
    two = _rows(COMBINED + """select combined.ik, combined.dt,
  sum(combined.wt) over (partition by combined.ik order by combined.dt asc) as wrun,
  sum(combined.st) over (partition by combined.ik order by combined.dt asc) as srun;""")
    one = _rows(COMBINED + """select combined.ik, combined.dt,
  sum(combined.wt) over (partition by combined.ik order by combined.dt asc) as wrun;""")
    zero = _rows(COMBINED + "select combined.ik, combined.dt;")
    assert len(two) == len(one) == len(zero) == 4
