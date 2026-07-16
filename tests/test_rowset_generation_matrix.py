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

# A second model with two facts that DON'T share a base key, so relating two
# rowsets over them is a genuine cross-base intersect (expressed via membership).
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
        + "where item_name is not null select item_name, rs.sq,\nsubset join item_id = rs.k\norder by item_name;",
        [("apple", 15), ("banana", 10), ("cherry", 9)],
    ),
    # --- HAVING (post-aggregate filtering inside the rowset body) ---
    (
        "single_having_post_aggregate",
        SALES_MODEL
        + "rowset rs <- select item_id as k, sum(s_qty) as sq having sum(s_qty) > 9;"
        + "select rs.k, rs.sq order by rs.k;",
        [(1, 15), (2, 10)],
    ),
    (
        # HAVING references count(sale_id), which is NOT projected by the rowset
        # -- it must be promoted to a hidden output and the filter applied.
        # counts: item1=2, item2=2, item3=1 -> count>1 keeps items 1,2.
        "having_on_nonprojected_aggregate",
        SALES_MODEL
        + "rowset rs <- select item_id as k, sum(s_qty) as sq having count(sale_id) > 1;"
        + "select rs.k, rs.sq order by rs.k;",
        [(1, 15), (2, 10)],
    ),
    (
        # HAVING comparing two aggregates: sum > count*5.
        # item1 15>10 yes; item2 10>10 no; item3 9>5 yes -> items 1,3.
        "having_compare_two_aggregates",
        SALES_MODEL
        + "rowset rs <- select item_id as k, sum(s_qty) as sq"
        + " having sum(s_qty) > count(sale_id) * 5;"
        + "select rs.k, rs.sq order by rs.k;",
        [(1, 15), (3, 9)],
    ),
    (
        # HAVING inside the rowset, then enrich a base-keyed property via join.
        # HAVING sum>9 keeps items 1,2 -> apple, banana. Joins are
        # row-preserving: items outside the rowset survive NULL-padded, so the
        # restriction to rowset rows is the explicit `rs.sq is not null`.
        "having_then_enrich_property",
        SALES_MODEL
        + "rowset rs <- select item_id as k, sum(s_qty) as sq having sum(s_qty) > 9;"
        + "where item_name is not null and rs.sq is not null select item_name, rs.sq,\nsubset join item_id = rs.k\norder by item_name;",
        [("apple", 15), ("banana", 10)],
    ),
    (
        # ... and without the explicit filter the enrichment preserves:
        # cherry (item 3, fails HAVING) survives with a NULL measure.
        "having_then_enrich_property_preserving",
        SALES_MODEL
        + "rowset rs <- select item_id as k, sum(s_qty) as sq having sum(s_qty) > 9;"
        + "where item_name is not null select item_name, rs.sq,\nsubset join item_id = rs.k\norder by item_name;",
        [("apple", 15), ("banana", 10), ("cherry", None)],
    ),
    (
        # HAVING (inside rowset) AND an outer membership filter (over a name-derived
        # keyset) must BOTH apply. HAVING sum>9 keeps {1,2}; keepers (name!='banana')
        # = {1,3}; AND -> {1}. item3 passes the membership but fails HAVING, proving
        # HAVING isn't dropped; item2 passes HAVING but fails membership, proving the
        # membership isn't dropped.
        "having_and_outer_membership_compose",
        SALES_MODEL
        + "rowset rs <- select item_id as k, sum(s_qty) as sq having sum(s_qty) > 9;"
        + "rowset keepers <- where item_name != 'banana' select item_id as ni;"
        + "where rs.k in keepers.ni select rs.k, rs.sq order by rs.k;",
        [(1, 15)],
    ),
    (
        # Membership into a HAVING-filtered rowset: outer keys kept only if in the
        # rowset's post-HAVING key set. big.k (sum>9) = {1,2}; count sales there.
        "membership_in_having_filtered_rowset",
        SALES_MODEL
        + "rowset big <- select item_id as k, sum(s_qty) as sq having sum(s_qty) > 9;"
        + "where item_id in big.k\nselect item_id, count(sale_id) as cnt order by item_id;",
        [(1, 2), (2, 2)],
    ),
    (
        # Outer membership filter over a name-derived keyset must filter the rowset
        # even though its own key/measure are the only SELECT outputs. Only item 1
        # is 'apple', so rs is filtered to {1}.
        "outer_membership_on_derived_keyset_filters",
        SALES_MODEL
        + "rowset rs <- select item_id as k, sum(s_qty) as sq;"
        + "rowset ap <- where item_name = 'apple' select item_id as ai;"
        + "where rs.k in ap.ai select rs.k, rs.sq order by rs.k;",
        [(1, 15)],
    ),
    # --- membership: outer keys filtered against a rowset's key set ---
    (
        "membership_in_rowset_keyset",
        SALES_MODEL
        + "rowset returned <- where r_qty > 0 select item_id as ri;"
        + "where item_id in returned.ri\nselect item_id, sum(s_qty) as sq order by item_id;",
        [(1, 15), (2, 10)],
    ),
    # --- cross-rowset WHERE/JOIN comparison (year-over-year), BOTH join types ---
    # LEFT is row-preserving; the intersection is the explicit both-sides
    # `is not null` idiom (one-sided not-null keeps the other side's exclusive
    # rows). UNION is coalescing: the two slices meet at one fused key, so a row
    # present in only one year survives with the other side NULL (item 3 has only
    # a 2000 row). These two cells are the same shape over `left` vs `union` --
    # the join-type axis this matrix must exercise, since most bugs at this
    # boundary are specific to the coalescing (key-collapsing) side.
    (
        "cross_rowset_yoy_left",
        SALES_MODEL
        + "rowset y99 <- where s_year = 1999 select item_id as k, sum(s_qty) as s99;"
        + "rowset y00 <- where s_year = 2000 select item_id as k2, sum(s_qty) as s00;"
        + "where y99.s99 is not null and y00.s00 is not null select y99.k, y99.s99, y00.s00\nsubset join y00.k2 = y99.k order by y99.k;",
        [(1, 10, 5), (2, 7, 3)],
    ),
    (
        "cross_rowset_yoy_union",
        SALES_MODEL
        + "rowset y99 <- where s_year = 1999 select item_id as k, sum(s_qty) as s99;"
        + "rowset y00 <- where s_year = 2000 select item_id as k2, sum(s_qty) as s00;"
        + "where y00.s00 is not null select y99.k, y99.s99, y00.s00\nunion join y99.k = y00.k2 order by y99.k;",
        [(1, 10, 5), (2, 7, 3), (3, None, 9)],
    ),
    (
        # q64 shape: reference the COALESCED union-join key downstream. The join
        # collapses y99.k onto the fused key and the body only projects the
        # collapsed side; the collapsed key column must still be projected so the
        # outer select can source it (regression for the q64 render sentinel /
        # 23-join timeout). Both slices group by the same key.
        "coalesced_key_referenced_downstream_union",
        SALES_MODEL
        + "rowset y99 <- where s_year = 1999 select item_id as k, sum(s_qty) as s99;"
        + "rowset y00 <- where s_year = 2000 select item_id as k, sum(s_qty) as s00;"
        + "select y99.k, y99.s99, y00.s00\nunion join y99.k = y00.k order by y99.k;",
        [(1, 10, 5), (2, 7, 3), (3, None, 9)],
    ),
    (
        # q02 shape: a third rowset scoped-joins two sibling rowsets, then a
        # FILTERED aggregate over its measure is grouped by one of its dims. The
        # filter mints a `_virt_filter` over a rowset measure; islanding must not
        # orphan that consumer from the rowset's component (it is a legitimate
        # downstream use of a declared output, not navigation into the rowset's
        # base concepts). diff = y99 - y00: item1 10-5=5, item2 7-3=4 (item3 has no
        # 1999 row, so its y00 side is null and the where-not-null drops it). The
        # agg passes each diff through.
        "filtered_aggregate_over_scoped_join_rowset",
        SALES_MODEL
        + "rowset y99 <- where s_year = 1999 select item_id as k, sum(s_qty) as s99;"
        + "rowset y00 <- where s_year = 2000 select item_id as k2, sum(s_qty) as s00;"
        + "rowset diff <- where y00.s00 is not null select y99.k as k, y99.s99 - y00.s00 as d"
        + " subset join y00.k2 = y99.k;"
        + "def only(x) -> sum(diff.d ? diff.k = x);"
        + "select @only(1) as d1, @only(2) as d2;",
        [(5, 4)],
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
    # --- cross-rowset intersection over non-shared bases (membership idiom) ---
    (
        # q38 shape: intersect two rowsets whose bases don't share a key. The pure
        # key-domain intersection is expressed with a membership filter (the idiom
        # that replaces the removed scoped `inner join`). store 2000 last names =
        # {Smith, Jones}; catalog 2000 = {Smith, Brown}; intersection = {Smith} -> 1.
        "cross_rowset_intersect_via_membership",
        INTERSECT_MODEL
        + "rowset store_combos <- where s_yr = 2000 select s_last_name as last_name;"
        + "rowset catalog_combos <- where c_yr = 2000 select c_last_name as last_name;"
        + "rowset sc <- where store_combos.last_name in catalog_combos.last_name"
        + " select store_combos.last_name as last_name;"
        + "select count(sc.last_name) as c;",
        [(1,)],
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
    (
        # union join of two DIFFERENT-fact aggregates whose SOLE grouping column
        # is the join key, keeping just the key + a measure and filtering on the
        # other side's measure. Formerly emitted a completion-merge condition
        # referencing a column the grouped CTE never projects (dangling merge
        # column -> raw DuckDB BinderException); the HAVING-filter side, carrying
        # the coalescing key member, was wrongly promoted to a row-join source.
        # Item 4 (returns-only) survives with a NULL sales measure: the coalescing
        # RIGHT_OUTER must not narrow to INNER via a spurious same-column subset
        # "proof" over the two independent item populations.
        "union_join_keyonly_grain",
        SALES_MODEL
        + "rowset ci <- select item_id as k, sum(r_qty) as amount;"
        + "rowset sa <- select item_id as k, sum(s_qty) as sq;"
        + "rowset filtered <- select sa.k, sa.sq"
        + " union join sa.k = ci.k having ci.amount is not null;"
        + "select filtered.k, filtered.sq order by filtered.k;",
        [(1, 15), (2, 10), (4, None)],
    ),
]


def _params():
    return [pytest.param(q, exp, id=cid) for cid, q, exp in _MATRIX]


def _assert_matrix_cell(query: str, expected: object) -> None:
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


@pytest.mark.parametrize("query,expected", _params())
def test_rowset_generation_matrix(query: str, expected: object):
    _assert_matrix_cell(query, expected)
