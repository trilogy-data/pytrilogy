"""A derived concept may name a full boolean predicate (`and`/`or`, `between`,
`is null`) — the same grammar `?`/`where` accept — and use it as a reusable
row predicate. Regression for bug_compound_boolean_in_derived_concept.md, which
covers both the parse layer and end-to-end planning/rendering.
"""

from trilogy import Dialects

MODEL = """
key id int;
property id.d date;
property id.flag int;
property id.v int;

datasource t (
    id: id,
    d: d,
    flag: flag,
    v: v,
)
grain (id)
query '''select 1 as id, date '2020-06-01' as d, 1 as flag, 10 as v
   union all select 2, date '2021-01-01', 1, 20
   union all select 3, date '2020-07-01', 0, 30''';

auto in_window <- (d between '2020-01-01'::date and '2020-12-31'::date) and flag = 1;
"""


def _executor():
    x = Dialects.DUCK_DB.default_executor()
    x.parse_text(MODEL)
    return x


def test_named_predicate_evaluates_per_row() -> None:
    # id=1 in window AND flag=1 -> True; id=2 out of window -> False;
    # id=3 in window but flag=0 -> False.
    x = _executor()
    rows = x.execute_query("select id, in_window order by id asc;").fetchall()
    assert [(r.id, r.in_window) for r in rows] == [(1, True), (2, False), (3, False)]


def test_named_predicate_usable_in_filtered_aggregate() -> None:
    # Only id=1 satisfies the predicate, so sum(v) over it is 10.
    x = _executor()
    row = x.execute_query("select sum(v ? in_window) as s;").fetchall()[0]
    assert row.s == 10
