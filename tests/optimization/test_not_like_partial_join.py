"""Regression: an aggregate-`?` filter using the first-class `not like`/`not ilike`
operator over a PARTIAL foreign key must preserve dimension entities with zero
matching fact rows (count 0) — exactly as the logically-equivalent `not (x like y)`
form does.

Setup: three customers (1,2,3); orders only for 1 and 2 (orders is partial for
customer). `count(order ? comment not like '%special%requests%') by customer`
should yield one row per customer, with customer 3 (no orders) at count 0 — the
TPC-H q13 shape (reference: customer LEFT JOIN orders ON ... AND comment NOT LIKE).

The first-class `not like` was treated as a plain null-propagating predicate, so
the disjoint-pushdown lifted it into a WHERE above the partial-FK OUTER join (and
UpgradeJoinOnGuards downgraded LEFT→INNER), dropping customer 3. The fix suppresses
that pushdown when the group key is only partially covered by the filter content's
source, falling back to the null-safe CASE-WHEN aggregate form that preserves
zero-match entities — matching the equivalent `not (x like y)` form.
"""

from trilogy import Dialects

_MODEL = """
key customer_id int;
property customer_id.cname string;
datasource customers (cid: customer_id, cname: cname) grain (customer_id)
query '''SELECT 'a' cname, 1 cid UNION ALL SELECT 'b',2 UNION ALL SELECT 'c',3''';

key order_id int;
property order_id.ocomment string;
datasource orders (oid: order_id, ocomment: ocomment, ocust: ~customer_id) grain (order_id)
query '''SELECT 10 oid,'hello' ocomment,1 ocust UNION ALL SELECT 11,'special requests',1 UNION ALL SELECT 12,'world',2''';
"""

_QUERY = "select customer_id, count(order_id ? {cond}) as c order by customer_id asc;"


def _rows(cond: str) -> set[tuple]:
    ex = Dialects.DUCK_DB.default_executor()
    ex.execute_text(_MODEL)
    parsed = ex.parse_text(_QUERY.format(cond=cond))[-1]
    return {tuple(r) for r in ex.execute_query(parsed).fetchall()}


_EXPECTED = {(1, 1), (2, 1), (3, 0)}  # customer 3 has no orders -> count 0


def test_negated_like_preserves_zero_match_entities():
    """Baseline that already works: `not (comment like ...)` keeps customer 3."""
    assert _rows("not (ocomment like '%special%requests%')") == _EXPECTED


def test_not_like_preserves_zero_match_entities():
    assert _rows("ocomment not like '%special%requests%'") == _EXPECTED


def test_equality_preserves_zero_match_entities():
    assert _rows("ocomment = 'hello'") == {(1, 1), (2, 0), (3, 0)}


def test_inequality_preserves_zero_match_entities():
    assert _rows("ocomment != 'world'") == {(1, 2), (2, 0), (3, 0)}


def test_is_not_null_preserves_zero_match_entities():
    assert _rows("ocomment is not null") == {(1, 2), (2, 1), (3, 0)}
