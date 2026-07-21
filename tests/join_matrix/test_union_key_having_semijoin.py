"""Union-join coalesced output key + HAVING on a non-output concept (the
TPC-DS q78 shape,
evals/tpcds_agent/bug_q78_union_key_coalesce_references_having_semijoin_cte.md).

HAVING referencing a concept outside the SELECT outputs builds a
post-aggregation semijoin CTE wired in only through `exists (select 1 ...)`.
Pre-fix, `safe_get_cte_value` rendered that EXISTS-only parent as a real
source from two branches:

- the outer-join-key coalesce branch emitted it as a table reference in the
  outer SELECT (DuckDB Binder: "Referenced table ... not found");
- the plain multi-source coalesce branch, for a fused key outside the final
  CTE's own join-key class, leaked it into the semijoin's correlation
  operand — inside the EXISTS the reference is *valid* SQL but compares the
  probe against its own column, a tautology that drops that key from the
  correlation. Exposed by the NULL-quantity store group (item 20): its
  probe row (ratio NULL, other 2) then also admits the store-less
  (item 20, cust 500) group.

Needs all three trigger ingredients: a coalesced union-join output key, the
non-output HAVING semijoin, and a cross-arm output (`ratio`) forcing the
semijoin CTE to also carry the key.

Data as (yr, item, cust, qty), qty None = NULL. Store presence differs by
HAVING spelling: `is_store is not null` admits the NULL-quantity store group,
`store_qty is not null` does not.
"""

from pathlib import Path

import pytest

from tests.join_matrix.harness import sort_rows
from trilogy import Dialects, Executor
from trilogy.core.models.environment import Environment

STORE = [
    (2000, 10, 100, 5),
    (2000, 10, 200, 3),
    (2000, 11, 300, 7),
    (2001, 10, 100, 6),
    (2000, 20, 100, None),
]
CAT = [
    (2000, 10, 100, 2),
    (2000, 12, 400, 9),
    (2001, 12, 400, 1),
    (2000, 20, 100, 2),
]
WEB = [(2000, 10, 200, 4), (2000, 20, 500, 2)]


def _rows_sql(rows: list[tuple[int, int, int, int | None]]) -> str:
    return " union all ".join(
        f"select {r} r, {y} y, {i} i, {c} c,"
        f" {'cast(null as int)' if q is None else q} q"
        for r, (y, i, c, q) in enumerate(rows, start=1)
    )


MODEL = f"""
key sid int;
property sid.s_yr int;
property sid.s_item int;
property sid.s_cust int;
property sid.s_qty int;
datasource store_fact (r: sid, y: s_yr, i: s_item, c: s_cust, q: ?s_qty) grain (sid)
query '''{_rows_sql(STORE)}''';

key cid int;
property cid.c_yr int;
property cid.c_item int;
property cid.c_cust int;
property cid.c_qty int;
datasource cat_fact (r: cid, y: c_yr, i: c_item, c: c_cust, q: c_qty) grain (cid)
query '''{_rows_sql(CAT)}''';

key wid int;
property wid.w_yr int;
property wid.w_item int;
property wid.w_cust int;
property wid.w_qty int;
datasource web_fact (r: wid, y: w_yr, i: w_item, c: w_cust, q: w_qty) grain (wid)
query '''{_rows_sql(WEB)}''';
"""

TWO_KEY_ROWSETS = """
with store_g as select s_item as item_sk, s_cust as cust_sk, sum(s_qty) as store_qty, 1 as is_store;
with cat_g as select c_item as item_sk, c_cust as cust_sk, sum(c_qty) as cat_qty;
with web_g as select w_item as item_sk, w_cust as cust_sk, sum(w_qty) as web_qty;
"""

THREE_KEY_ROWSETS = """
with store_g as select s_yr as yr, s_item as item_sk, s_cust as cust_sk, sum(s_qty) as store_qty, 1 as is_store;
with cat_g as select c_yr as yr, c_item as item_sk, c_cust as cust_sk, sum(c_qty) as cat_qty;
with web_g as select w_yr as yr, w_item as item_sk, w_cust as cust_sk, sum(w_qty) as web_qty;
"""

TWO_KEY_JOINS = """
union join store_g.item_sk = cat_g.item_sk
union join store_g.cust_sk = cat_g.cust_sk
union join store_g.item_sk = web_g.item_sk
union join store_g.cust_sk = web_g.cust_sk
"""

THREE_KEY_JOINS = """
union join store_g.yr = cat_g.yr
union join store_g.item_sk = cat_g.item_sk
union join store_g.cust_sk = cat_g.cust_sk
union join store_g.yr = web_g.yr
union join store_g.item_sk = web_g.item_sk
union join store_g.cust_sk = web_g.cust_sk
"""

QUERY_BODY = """
select
    {year_output}
    store_g.item_sk as item_sk,
    store_g.cust_sk as customer_sk,
    store_g.store_qty as store_qty,
    coalesce(cat_g.cat_qty, 0) + coalesce(web_g.web_qty, 0) as other_qty,
    round(store_g.store_qty::numeric
          / nullif(coalesce(cat_g.cat_qty,0)+coalesce(web_g.web_qty,0),0)::numeric, 2) as ratio
having
    coalesce(cat_g.cat_qty, 0) + coalesce(web_g.web_qty, 0) > 0
    and {store_present};
"""

# predicate spelling -> (trilogy HAVING conjunct, store presence requires a
# non-NULL aggregate). The first references a NON-output concept (semijoin
# path under test); the second an output (no semijoin, sanity baseline).
HAVING_FORMS = {
    "non_output_semijoin": ("store_g.is_store is not null", False),
    "output_reference": ("store_g.store_qty is not null", True),
}


def _oracle(with_year: bool, require_qty: bool) -> list[tuple]:
    def agg(rows: list[tuple[int, int, int, int | None]]) -> dict[tuple, int | None]:
        grouped: dict[tuple, list[int | None]] = {}
        for yr, item, cust, qty in rows:
            key = (yr, item, cust) if with_year else (item, cust)
            grouped.setdefault(key, []).append(qty)
        return {
            k: (
                None
                if all(q is None for q in v)
                else sum(q for q in v if q is not None)
            )
            for k, v in grouped.items()
        }

    store, cat, web = agg(STORE), agg(CAT), agg(WEB)
    expected = []
    for key in sorted(set(store) | set(cat) | set(web)):
        if key not in store:
            continue
        qty = store[key]
        if require_qty and qty is None:
            continue
        other = (cat.get(key) or 0) + (web.get(key) or 0)
        if other == 0:
            continue
        ratio = None if qty is None else round(qty / other, 2)
        expected.append((*key, qty, other, ratio))
    return sort_rows(expected)


def _run(tmp_path: Path, query: str) -> list[tuple]:
    engine: Executor = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=tmp_path)
    )
    statements = engine.parse_text(MODEL + query)
    sql = engine.generate_sql(statements[-1])[-1]
    assert "INVALID_REFERENCE_BUG" not in sql, sql
    rows = [tuple(r) for r in engine.execute_raw_sql(sql).fetchall()]
    return sort_rows([(*r[:-1], None if r[-1] is None else float(r[-1])) for r in rows])


@pytest.mark.parametrize("having", HAVING_FORMS)
def test_two_key_union(tmp_path: Path, having: str):
    predicate, require_qty = HAVING_FORMS[having]
    query = (
        TWO_KEY_ROWSETS
        + TWO_KEY_JOINS
        + QUERY_BODY.format(year_output="", store_present=predicate)
    )
    assert _run(tmp_path, query) == _oracle(with_year=False, require_qty=require_qty)


@pytest.mark.parametrize("having", HAVING_FORMS)
def test_three_key_union(tmp_path: Path, having: str):
    predicate, require_qty = HAVING_FORMS[having]
    query = (
        THREE_KEY_ROWSETS
        + THREE_KEY_JOINS
        + QUERY_BODY.format(year_output="store_g.yr as year,", store_present=predicate)
    )
    assert _run(tmp_path, query) == _oracle(with_year=True, require_qty=require_qty)
