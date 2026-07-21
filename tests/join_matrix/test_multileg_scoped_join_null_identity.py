"""Multi-leg scoped joins off a shared anchor honor authored keys, including
NULL-matches-NULL identity — the TPC-DS q25 verdict pin.

Shape: two `union join` legs off one anchor fact (`ss ⋈ sr` on
ticket/item/customer, `ss ⋈ cs` on customer/item), measures from all three
facts, output grain (item, store) COARSER than the correlation keys. The
planner decomposes this into per-measure branches recombined by a MergeNode;
the suspicion (bug_q25_multileg_scoped_join_correlation_key_drop.md) was that
branch recombination on the projected dims alone drops the authored
correlation keys. DISPROVEN 2026-07-20: on TPC-DS the engine's rows are
value-exact with a row-level correlated flat oracle (chain of null-safe
LEFT/FULL joins on exactly the authored keys) — the divergence from the SQL
reference is NULL-matches-NULL identity on the nullable customer key, which
plain SQL `=` silently rejects. SQL parity is an explicit author predicate
(`where anchor.customer is not null`), per the unified membership identity
semantics.

Expected rows below are the hand-computed flat null-safe oracle: join rows
(t1: ss100/sr5/—), (t2: ss200/—/cs50), (t3: ss300/—/cs70),
(t4: ss400/sr9/cs90 — the NULL-customer row pairing by identity on BOTH legs).
"""

from pathlib import Path

from tests.join_matrix.harness import sort_rows
from trilogy import Dialects, Executor
from trilogy.core.models.environment import Environment

ITEM = """
key item_sk int;
datasource items (i: item_sk) grain (item_sk)
query '''
select 10 i union all select 20 i
''';
"""

CUSTOMER = """
key customer_sk int;
datasource customers (c: customer_sk) grain (customer_sk)
query '''
select 1 c union all select 2 c union all select 3 c
''';
"""

STORE_SALES = """
import item as item;
import customer as customer;
key ticket int;
properties <ticket, item.item_sk> (profit int?, store string?);
datasource store_sales (t: ticket, i: item.item_sk, c: ?customer.customer_sk, s: store, p: profit) grain (ticket, item.item_sk)
query '''
select 1 t, 10 i, 1 c, 's1' s, 100 p
union all select 2 t, 10 i, 2 c, 's1' s, 200 p
union all select 3 t, 20 i, 3 c, 's1' s, 300 p
union all select 4 t, 20 i, cast(null as int) c, 's1' s, 400 p
''';
"""

STORE_RETURNS = """
import item as item;
import customer as customer;
key ticket int;
properties <ticket, item.item_sk> (loss int?);
datasource store_returns (t: ticket, i: item.item_sk, c: ?customer.customer_sk, l: loss) grain (ticket, item.item_sk)
query '''
select 1 t, 10 i, 1 c, 5 l
union all select 4 t, 20 i, cast(null as int) c, 9 l
''';
"""

CATALOG_SALES = """
import item as item;
import customer as customer;
key order_id int;
properties <order_id, item.item_sk> (cprofit int?);
datasource catalog_sales (o: order_id, i: item.item_sk, c: ?customer.customer_sk, p: cprofit) grain (order_id, item.item_sk)
query '''
select 9 o, 10 i, 2 c, 50 p
union all select 8 o, 20 i, 3 c, 70 p
union all select 7 o, 20 i, cast(null as int) c, 90 p
''';
"""

HEADER = (
    "import store_sales as ss;\n"
    "import store_returns as sr;\n"
    "import catalog_sales as cs;\n"
)
JOINS = (
    "union join ss.ticket = sr.ticket and ss.item.item_sk = sr.item.item_sk"
    " and ss.customer.customer_sk = sr.customer.customer_sk\n"
    "union join ss.customer.customer_sk = cs.customer.customer_sk"
    " and ss.item.item_sk = cs.item.item_sk\n"
)
MEASURES = (
    "  sum(ss.profit) as ss_profit,\n"
    "  sum(sr.loss) as sr_loss,\n"
    "  sum(cs.cprofit) as cs_profit,\n"
)
HAVING = "having sr_loss is not null and cs_profit is not null\n"


def _run(tmp_path: Path, query: str) -> list[tuple]:
    (tmp_path / "item.preql").write_text(ITEM)
    (tmp_path / "customer.preql").write_text(CUSTOMER)
    (tmp_path / "store_sales.preql").write_text(STORE_SALES)
    (tmp_path / "store_returns.preql").write_text(STORE_RETURNS)
    (tmp_path / "catalog_sales.preql").write_text(CATALOG_SALES)
    engine: Executor = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=tmp_path)
    )
    statements = engine.parse_text(HEADER + query)
    sql = engine.generate_sql(statements[-1])[-1]
    assert "INVALID_REFERENCE_BUG" not in sql, sql
    return sort_rows([tuple(r) for r in engine.execute_raw_sql(sql).fetchall()])


def test_multileg_null_identity_holds(tmp_path: Path):
    # Item 20 is admitted only through the NULL-customer row's identity match
    # on both legs (sr loss 9, cs profit 90+70); its measures include the
    # correlated NULL-row contributions.
    rows = _run(
        tmp_path,
        "select\n  ss.item.item_sk,\n  ss.store,\n" + MEASURES + JOINS + HAVING + ";",
    )
    assert rows == sort_rows([(10, "s1", 300, 5, 50), (20, "s1", 700, 9, 160)])


def test_multileg_sql_parity_via_not_null(tmp_path: Path):
    # Excluding NULL-key anchor rows restores plain-SQL `=` behavior: item 20
    # loses its only return match and the HAVING guard drops it.
    rows = _run(
        tmp_path,
        "where ss.customer.customer_sk is not null\n"
        "select\n  ss.item.item_sk,\n  ss.store,\n" + MEASURES + JOINS + HAVING + ";",
    )
    assert rows == sort_rows([(10, "s1", 300, 5, 50)])


def test_multileg_projected_keys_correlate_per_customer(tmp_path: Path):
    # At the finer (item, store, customer) grain the HAVING guards enforce
    # per-customer correlation; only the NULL-identity row satisfies both legs.
    rows = _run(
        tmp_path,
        "select\n  ss.item.item_sk,\n  ss.store,\n  ss.customer.customer_sk,\n"
        + MEASURES
        + JOINS
        + HAVING
        + ";",
    )
    assert rows == sort_rows([(20, "s1", None, 400, 9, 90)])
