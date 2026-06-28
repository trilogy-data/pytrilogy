"""Systemic guardrail for rowset node generation.

Most rowset bugs in this codebase share one root cause: a collapsed/pruned
operand loses provenance and the consumer can no longer source it. It surfaces
two ways, both of which this matrix is designed to catch:

  1. A literal ``INVALID_REFERENCE_BUG`` sentinel in the rendered SQL (caught at
     ``generate_sql`` time in strict mode).
  2. A dangling-but-plausible CTE/column reference -- valid-looking SQL that only
     DuckDB rejects at execute time (a ``BinderException`` / ``CatalogException``).

The contract every rowset shape must satisfy: it resolves to *correct rows*, or
it raises a *clean trilogy exception* (an actionable author-facing error). It
must never leak a raw DuckDB error or a sentinel ValueError. This sweep covers
the cross product of rowset shape (single / cross-rowset / nested / union) and
usage (direct select / enrich / membership / WHERE comparison / HAVING), so a
refactor of ``gen_rowset_node`` can gate on it.
"""

import pytest

from trilogy import Dialects
from trilogy.core.exceptions import (
    DisconnectedConceptsException,
    InvalidSyntaxException,
    UndefinedConceptException,
    UnresolvableQueryException,
)
from trilogy.executor import Executor

# Clean, author-facing trilogy errors. A rowset shape that cannot be expressed
# must raise one of these -- never a raw DuckDB error or the sentinel ValueError.
CLEAN_ERRORS: tuple[type[Exception], ...] = (
    UnresolvableQueryException,
    DisconnectedConceptsException,
    InvalidSyntaxException,
    UndefinedConceptException,
)

SALES_MODEL = """
key item_id int;
property item_id.item_name string;
datasource items (id: item_id, nm: item_name) grain (item_id)
query '''select 1 id, 'apple' nm union all select 2 id, 'banana' nm union all select 3 id, 'cherry' nm''';

key sale_id int;
property sale_id.s_qty int;
property sale_id.s_year int;
datasource sales (id: sale_id, it: item_id, q: s_qty, y: s_year) grain (sale_id)
query '''
select 1 id, 1 it, 10 q, 1999 y union all
select 2 id, 1 it, 5 q, 2000 y union all
select 3 id, 2 it, 7 q, 1999 y union all
select 4 id, 2 it, 3 q, 2000 y union all
select 5 id, 3 it, 9 q, 2000 y
''';

key ret_id int;
property ret_id.r_qty int;
property ret_id.r_year int;
datasource returns (id: ret_id, it: item_id, q: r_qty, y: r_year) grain (ret_id)
query '''
select 1 id, 1 it, 2 q, 1999 y union all
select 2 id, 2 it, 1 q, 2000 y union all
select 3 id, 4 it, 4 q, 2000 y
''';
"""

# A second model with two facts that DON'T share a base key, so an inner join
# between two rowsets over them is a genuine (collapse-unexpressible) intersect.
INTERSECT_MODEL = """
key store_sale_id int;
property store_sale_id.s_last_name string;
property store_sale_id.s_yr int;
datasource store_sales (id: store_sale_id, ln: s_last_name, y: s_yr) grain (store_sale_id)
query '''select 1 id, 'Smith' ln, 2000 y union all select 2 id, 'Jones' ln, 2000 y''';

key cat_sale_id int;
property cat_sale_id.c_last_name string;
property cat_sale_id.c_yr int;
datasource cat_sales (id: cat_sale_id, ln: c_last_name, y: c_yr) grain (cat_sale_id)
query '''select 1 id, 'Smith' ln, 2000 y union all select 2 id, 'Brown' ln, 2000 y''';
"""

# (id, full query, expected) -- expected is a list of row tuples (success) or a
# clean exception type (must raise it; anything else, incl. a leaked DuckDB error
# or sentinel ValueError, fails the test).
_MATRIX: list[tuple[str, str, object]] = [
    # --- single rowset ---
    (
        "single_direct_select",
        SALES_MODEL
        + "rowset rs <- select item_id, sum(s_qty) as sq;"
        + "select rs.item_id, rs.sq order by rs.item_id;",
        [(1, 15), (2, 10), (3, 9)],
    ),
    (
        "single_enrich_via_explicit_join",
        SALES_MODEL
        + "rowset rs <- select item_id as k, sum(s_qty) as sq;"
        + "select item_name, rs.sq,\ninner join rs.k = item_id\norder by item_name;",
        [("apple", 15), ("banana", 10), ("cherry", 9)],
    ),
    (
        "single_having_post_aggregate",
        SALES_MODEL
        + "rowset rs <- select item_id as k, sum(s_qty) as sq having sum(s_qty) > 9;"
        + "select rs.k, rs.sq order by rs.k;",
        [(1, 15), (2, 10)],
    ),
    # --- membership: outer keys filtered against a rowset's key set ---
    (
        "membership_in_rowset_keyset",
        SALES_MODEL
        + "rowset returned <- where r_qty > 0 select item_id as ri;"
        + "where item_id in returned.ri\nselect item_id, sum(s_qty) as sq order by item_id;",
        [(1, 15), (2, 10)],
    ),
    # --- cross-rowset WHERE/JOIN comparison (year-over-year) ---
    (
        "cross_rowset_yoy_join",
        SALES_MODEL
        + "rowset y99 <- where s_year = 1999 select item_id as k, sum(s_qty) as s99;"
        + "rowset y00 <- where s_year = 2000 select item_id as k2, sum(s_qty) as s00;"
        + "inner join y99.k = y00.k2\nselect y99.k, y99.s99, y00.s00 order by y99.k;",
        [(1, 10, 5), (2, 7, 3)],
    ),
    # --- nested rowset (rowset aggregates another rowset's output) ---
    (
        "nested_rowset_global_aggregate",
        SALES_MODEL
        + "rowset per_item <- select item_id as k, sum(s_qty) as sq;"
        + "rowset total <- select sum(per_item.sq) as grand;"
        + "select total.grand;",
        [(34,)],
    ),
    # --- union(...) TVF stacked then re-aggregated ---
    (
        "union_rowset_restacked_aggregate",
        SALES_MODEL
        + "with combined as union("
        + "(select item_id as k, sum(s_qty) as v),"
        + "(select item_id as k, sum(r_qty) as v)"
        + ") -> (k, v);"
        + "select combined.k, sum(combined.v) as total order by combined.k;",
        [(1, 17), (2, 11), (3, 9), (4, 4)],
    ),
    # --- clean-error contracts ---
    (
        # q38: inner join of two rowsets over non-shared bases is an intersect
        # the collapse model can't express -> clean error pointing at union().
        "cross_rowset_inner_intersect_clean_error",
        INTERSECT_MODEL
        + "rowset store_combos <- where s_yr = 2000 select s_last_name as last_name;"
        + "rowset catalog_combos <- where c_yr = 2000 select c_last_name as last_name;"
        + "rowset sc <- select store_combos.last_name as last_name"
        + " inner join store_combos.last_name = catalog_combos.last_name;"
        + "select count(sc.last_name) as c;",
        UnresolvableQueryException,
    ),
    (
        # Single rowset with no join: a base-keyed property (item_name) must NOT
        # auto-source off the rowset's key -- that would invent a join path the
        # author never declared. By design this is a clean disconnect (declare
        # `join rs.k = item_id` to resolve it), never a sentinel or silent join.
        "single_rowset_islanded_property_clean_error",
        SALES_MODEL
        + "rowset rs <- select item_id as k, sum(s_qty) as sq;"
        + "select item_name, rs.sq order by item_name;",
        DisconnectedConceptsException,
    ),
]


def _params():
    return [pytest.param(q, exp, id=cid) for cid, q, exp in _MATRIX]


@pytest.mark.parametrize("query,expected", _params())
def test_rowset_generation_matrix(query: str, expected: object):
    executor: Executor = Dialects.DUCK_DB.default_executor()

    if isinstance(expected, type):
        with pytest.raises(CLEAN_ERRORS) as excinfo:
            executor.execute_text(query)
        assert isinstance(
            excinfo.value, expected
        ), f"expected {expected.__name__}, got {type(excinfo.value).__name__}"
        return

    # Success cell: SQL must render without a sentinel and execute to exact rows.
    sql = executor.generate_sql(query)[-1]
    assert "INVALID_REFERENCE_BUG" not in sql, f"sentinel leaked into SQL:\n{sql}"
    rows = [tuple(r) for r in executor.execute_text(query)[-1].fetchall()]
    assert rows == expected
