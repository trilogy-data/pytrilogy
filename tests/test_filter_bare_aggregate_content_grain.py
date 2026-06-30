"""A bare aggregate (no `by` clause) inside a filter's `? <condition>` must group by
the FILTERED CONTENT's grain — the rows being filtered — not the outer consuming
grain. This mirrors how a SELECT's WHERE co-grains its aggregates to the select
grain (HAVING semantics).

Regression for TPC-DS q23. The agent wrote a "frequent prefix" filter
`auto frequent_prefix <- substring(item.desc,1,30) ? (count_distinct(...) > 4)`
and used it as a membership RHS in another model. Two ways the inherited-consuming-
grain bug bit:

* Consumed at its OWN grain (`select frequent_prefix`), the bare count's grouping
  became the filter concept itself -> a self-referential grain -> `RecursionError`.
* Used as a membership RHS from a SECOND model (`substring(catalog.item.desc) in
  frequent_prefix`), the store-derived count was grouped by the *catalog* item key,
  a disconnected grain -> `UnresolvableQueryException: Could not resolve connections`.

Both now resolve, grouping the condition's aggregate by the filtered content, and
produce the same result an explicit `by <content>` would.
"""

from pathlib import Path

import pytest

from trilogy import Dialects
from trilogy.core.models.environment import Environment
from trilogy.executor import Executor

# s_item 1,2 share prefix 'AA' (5 distinct (item,date) pairs -> frequent); 3 is 'BB' (1).
# c_item 10 is 'AA' (matches), 11 is 'BB' (does not). Separate item dimensions, so a
# store count grouped by the catalog key would be a disconnected (unresolvable) grain.
MODEL = """
key s_item int;
property s_item.s_descr string;
key c_item int;
property c_item.c_descr string;
key s_line int;
property s_line.s_item int;
property s_line.s_dt int;
key c_line int;
property c_line.c_item int;
property c_line.c_amt float;
datasource sitems (i: s_item, d: s_descr) grain (s_item)
query '''select 1 i,'AA x' d union all select 2 i,'AA y' d union all select 3 i,'BB z' d''';
datasource citems (i: c_item, d: c_descr) grain (c_item)
query '''select 10 i,'AA p' d union all select 11 i,'BB q' d''';
datasource sales (l: s_line, it: s_item, d: s_dt) grain (s_line)
query '''
select 1 l,1 it,1 d union all select 2 l,1 it,2 d union all select 3 l,1 it,3 d union all
select 4 l,2 it,1 d union all select 5 l,2 it,2 d union all select 6 l,3 it,1 d''';
datasource catalog (l: c_line, it: c_item, a: c_amt) grain (c_line)
query '''select 100 l,10 it,5.0 a union all select 101 l,11 it,9.0 a''';
"""

COND = "count_distinct(concat(s_item::string, '-', s_dt::string))"
PREFIX_DECL = "auto sp <- substring(s_descr, 1, 2);\n"

# bare aggregate filter, consumed at its own grain
BARE_ALONE = (
    MODEL
    + PREFIX_DECL
    + f"auto fp <- sp ? ({COND} > 4);\nselect fp where fp is not null order by fp;"
)
# bare aggregate filter, used as a cross-dimension membership RHS (the reported bug)
BARE_MEMBERSHIP = (
    MODEL
    + PREFIX_DECL
    + (
        f"auto fp <- sp ? ({COND} > 4);\n"
        "select c_item, sum(c_amt) as total where substring(c_descr,1,2) in fp order by c_item;"
    )
)
# explicit `by <content>` — must give the same answer as the bare form
BY_CONTENT_MEMBERSHIP = (
    MODEL
    + PREFIX_DECL
    + (
        f"auto fp <- sp ? ({COND} by sp > 4);\n"
        "select c_item, sum(c_amt) as total where substring(c_descr,1,2) in fp order by c_item;"
    )
)


@pytest.fixture
def executor(tmp_path: Path) -> Executor:
    return Dialects.DUCK_DB.default_executor(environment=Environment())


def test_bare_aggregate_filter_selected_alone_resolves(executor: Executor):
    rows = [tuple(r) for r in executor.execute_text(BARE_ALONE)[0].fetchall()]
    assert rows == [("AA",)]


def test_bare_aggregate_filter_membership_rhs_resolves(executor: Executor):
    sql = executor.generate_sql(BARE_MEMBERSHIP)[-1]
    assert "INVALID_REFERENCE_BUG" not in sql
    rows = [tuple(r) for r in executor.execute_text(BARE_MEMBERSHIP)[0].fetchall()]
    assert rows == [(10, 5.0)]


def test_bare_matches_explicit_by_content(executor: Executor):
    bare = [tuple(r) for r in executor.execute_text(BARE_MEMBERSHIP)[0].fetchall()]
    explicit = [
        tuple(r) for r in executor.execute_text(BY_CONTENT_MEMBERSHIP)[0].fetchall()
    ]
    assert bare == explicit == [(10, 5.0)]
