"""Anchor-preserving scoped OUTER join from one rowset to one or more others,
where the join keys are FK references to *shared* dimensions.

Reduced from enriched TPC-DS q78 ("keep store rows, add summed other-channel
quantity"). Each channel (store/catalog/web) is its own fact, but all three
``import`` the SAME ``item`` and ``customer`` dimension models — so the rowset
keys (``ss.item.id`` / ``cs.item.id`` / ``ws.item.id``) are pseudonyms of one
shared ``item.id``, and ``customer.id`` is an optional FK. That shared-dimension
structure is the trigger: simpler models whose keys are plain per-fact properties
resolve fine, so they do NOT reproduce this bug.

BUG (see evals/tpcds_agent/q78_three_source_outer_join_bug.md): a scoped subset join
from an anchor rowset to one or more others over these shared-dimension keys either
fails to resolve (DisconnectedConceptsException, unable to source the other
channels' shared keys) or silently returns NULL anchor identity columns by keying
the anchor re-join on a SUM measure instead of the grain key. INNER is unaffected.

These tests assert the anchor is preserved (its identity columns are never NULL,
its measure is correct) AND that no aggregate measure appears in a join ON clause.
A measure should never be a join key — even when redundant alongside the real keys
it is a plan smell (tracked separately); the corrupt key-replacing form shows up
directly as wrong rows. They go green once q78 is fixed.
"""

import re
from pathlib import Path

import pytest

from trilogy import Dialects
from trilogy.core.models.environment import Environment

_ITEM = "key id int;\n"
_CUSTOMER = "key id int;\n"


def _fact(name: str, grain_key: str, rows: str) -> str:
    return f"""
import item as item;
import customer as customer;
key {grain_key} int;
properties <{grain_key}, item.id> (
    quantity int,
);
datasource {name} (
    GK: {grain_key},
    ITEM: item.id,
    CUST: ?customer.id,
    QTY: quantity,
)
grain ({grain_key}, item.id)
query '''
{rows}
''';
"""


# channel a (anchor): (item1,cust1)=10, (item2,cust2)=20
# channel b         : (item1,cust1)=5
# channel c         : (item1,cust1)=3,  (item2,cust2)=7
_STORE = _fact(
    "store_sales",
    "ticket",
    "select 1 GK, 1 ITEM, 1 CUST, 10 QTY\nunion all select 2 GK, 2 ITEM, 2 CUST, 20 QTY",
)
_CATALOG = _fact("catalog_sales", "order_number", "select 1 GK, 1 ITEM, 1 CUST, 5 QTY")
_WEB = _fact(
    "web_sales",
    "web_order",
    "select 1 GK, 1 ITEM, 1 CUST, 3 QTY\nunion all select 2 GK, 2 ITEM, 2 CUST, 7 QTY",
)

_HEAD = """
import store_sales as ss;
import catalog_sales as cs;
import web_sales as ws;
with store_agg as
where ss.customer.id is not null
select ss.item.id as item_id, ss.customer.id as cust_id, sum(ss.quantity) as store_qty;
with catalog_agg as
where cs.customer.id is not null
select cs.item.id as item_id, cs.customer.id as cust_id, sum(cs.quantity) as cat_qty;
with web_agg as
where ws.customer.id is not null
select ws.item.id as item_id, ws.customer.id as cust_id, sum(ws.quantity) as web_qty;
"""

TWO_SOURCE = _HEAD + """
select
    store_agg.item_id, store_agg.cust_id, store_agg.store_qty,
    coalesce(catalog_agg.cat_qty, 0) as other_qty
subset join catalog_agg.item_id = store_agg.item_id
subset join catalog_agg.cust_id = store_agg.cust_id
having coalesce(catalog_agg.cat_qty, 0) > 0
order by store_agg.item_id asc;
"""

THREE_SOURCE_CHAINED = _HEAD + """
select
    store_agg.item_id, store_agg.cust_id, store_agg.store_qty,
    coalesce(catalog_agg.cat_qty, 0) + coalesce(web_agg.web_qty, 0) as other_qty
subset join web_agg.item_id = catalog_agg.item_id = store_agg.item_id
subset join web_agg.cust_id = catalog_agg.cust_id = store_agg.cust_id
having coalesce(catalog_agg.cat_qty, 0) + coalesce(web_agg.web_qty, 0) > 0
order by store_agg.item_id asc;
"""

THREE_SOURCE_STAR = _HEAD + """
select
    store_agg.item_id, store_agg.cust_id, store_agg.store_qty,
    coalesce(catalog_agg.cat_qty, 0) + coalesce(web_agg.web_qty, 0) as other_qty
subset join catalog_agg.item_id = store_agg.item_id
subset join catalog_agg.cust_id = store_agg.cust_id
subset join web_agg.item_id = store_agg.item_id
subset join web_agg.cust_id = store_agg.cust_id
having coalesce(catalog_agg.cat_qty, 0) + coalesce(web_agg.web_qty, 0) > 0
order by store_agg.item_id asc;
"""

# Aggregate output column (e.g. ``store_agg_store_qty``) on either side of a join
# equality. A measure must never be a join key — neither the corrupt key-replacing
# form nor a redundant extra condition alongside the real keys (tracked separately).
_MEASURE_IN_JOIN = re.compile(r'_(store|cat|web)_qty"\s*=|=\s*"[^"]+"\."[^"]*_qty"')


@pytest.fixture
def models(tmp_path: Path) -> Path:
    (tmp_path / "item.preql").write_text(_ITEM)
    (tmp_path / "customer.preql").write_text(_CUSTOMER)
    (tmp_path / "store_sales.preql").write_text(_STORE)
    (tmp_path / "catalog_sales.preql").write_text(_CATALOG)
    (tmp_path / "web_sales.preql").write_text(_WEB)
    return tmp_path


def _run(models: Path, body: str) -> tuple[str, list[tuple]]:
    eng = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=models)
    )
    sql = eng.generate_sql(body)[-1]
    rows = [tuple(r) for r in eng.execute_raw_sql(sql).fetchall()]
    # normalize numeric (DuckDB may return Decimal / HugeInt) for comparison
    norm = [(r[0], r[1], None if r[2] is None else int(r[2]), int(r[3])) for r in rows]
    return sql, norm


def test_two_source_outer_join_anchor_preserved(models: Path):
    sql, rows = _run(models, TWO_SOURCE)
    assert "INVALID_REFERENCE_BUG" not in sql
    assert not _MEASURE_IN_JOIN.search(sql), f"measure used as join key:\n{sql}"
    # (item1,cust1): other=5>0, anchor qty=10. (item2,cust2): other=0, filtered.
    assert rows == [(1, 1, 10, 5)], rows


def test_three_source_chained_outer_join_anchor_preserved(models: Path):
    sql, rows = _run(models, THREE_SOURCE_CHAINED)
    assert "INVALID_REFERENCE_BUG" not in sql
    assert not _MEASURE_IN_JOIN.search(sql), f"measure used as join key:\n{sql}"
    # both anchor rows survive (other>0); identity + anchor measure must be intact.
    assert rows == [(1, 1, 10, 8), (2, 2, 20, 7)], rows


def test_three_source_star_outer_join_anchor_preserved(models: Path):
    sql, rows = _run(models, THREE_SOURCE_STAR)
    assert "INVALID_REFERENCE_BUG" not in sql
    assert not _MEASURE_IN_JOIN.search(sql), f"measure used as join key:\n{sql}"
    assert rows == [(1, 1, 10, 8), (2, 2, 20, 7)], rows
