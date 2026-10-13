"""Return-only projections over a two-fact unified model must not scan the
sales anchor.

The `all_sales` shape (q05): a `returns` fact whose grain keys are `~` (intrinsic
partial) shares a grain with a `sales` fact whose keys are complete. A query that
touches only return-side concepts otherwise keeps `sales` purely to anchor the
grain and joins the whole fact. When the WHERE proves membership in `returns`
(a returns-only column proven non-null), the `~` keys cover the whole result
population, so the anchor is dead weight and must not be scanned.

The pin-heal (`partial_bridging.decide_heal`) carries that proof by dropping
the `~` from the binding before discovery; these guard the two surfaces it
feeds (source selection + the emitted scan's partiality).
"""

from trilogy import Dialects

MODEL = """
key channel string;
key order_id int;
key item_id int;
key rdim_id int;
key rdate_id int;
property rdim_id.rdim_name string;
property rdate_id.rdate_val date;
properties <order_id, item_id, channel> (
    ext_sales_price float?,
    return_amount float?,
);
partial datasource sales (
    raw(''' 'STORE' '''): channel,
    o: order_id, i: item_id, esp: ext_sales_price,
) grain (order_id, item_id, channel) complete where channel = 'STORE' address sales;
partial datasource returns (
    raw(''' 'STORE' '''): channel,
    o: ~order_id, i: ~item_id, rd: ?rdim_id, dt: ?rdate_id, ra: return_amount,
) grain (order_id, item_id, channel) complete where channel = 'STORE' address returns;
partial datasource rdim (
    raw(''' 'STORE' '''): channel, rd: rdim_id, rn: rdim_name,
) grain (rdim_id, channel) complete where channel = 'STORE' address rdim;
datasource datedim (dt: rdate_id, dd: rdate_val) grain (rdate_id) address datedim;
"""


def _setup():
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_raw_sql("""
        CREATE TABLE sales AS
            SELECT 1 o,10 i,100.0 esp UNION ALL SELECT 1,11,50.0
            UNION ALL SELECT 2,10,70.0 UNION ALL SELECT 3,12,20.0;
        CREATE TABLE returns AS
            SELECT 1 o,10 i,5 rd,900 dt,30.0 ra UNION ALL SELECT 2,10,5,900,70.0;
        CREATE TABLE rdim AS SELECT 5 rd,'D5' rn;
        CREATE TABLE datedim AS
            SELECT 900 dt, DATE '2000-01-01' dd UNION ALL SELECT 901, DATE '2001-01-01';
        """)
    executor.execute_text(MODEL)
    return executor


def _sql(executor, text: str) -> str:
    return executor.generate_sql(executor.parse_text(text)[-1])[0]


def _rows(executor, text: str) -> set[tuple]:
    return {tuple(r) for r in executor.execute_text(text)[-1].fetchall()}


def test_return_only_dim_aggregate_drops_sales_anchor():
    """The q05 arm: aggregate return-side measures by a return dimension, filtered
    to return rows. The sales fact contributes nothing and must not be scanned."""
    ex = _setup()
    text = (
        "where channel='STORE' "
        "and rdate_val = '2000-01-01'::date and rdim_id is not null "
        "select rdim_name, sum(return_amount) as r;"
    )
    sql = _sql(ex, text)
    assert '"sales"' not in sql, sql
    assert _rows(ex, text) == {("D5", 100.0)}


def test_return_only_bare_key_single_scan():
    """Grouping return measures by the grain key itself: the key comes back
    complete from `returns`, so it is not re-sourced from `sales` and there is no
    stitch join — a single fact scan."""
    ex = _setup()
    text = (
        "where channel='STORE' and rdim_id is not null "
        "select order_id, sum(return_amount) as r;"
    )
    sql = _sql(ex, text)
    assert '"sales"' not in sql, sql
    assert not any(
        j in sql for j in ("FULL JOIN", "LEFT OUTER JOIN", "RIGHT OUTER JOIN")
    ), sql
    assert _rows(ex, text) == {(1, 30.0), (2, 70.0)}


def test_sales_measure_referenced_keeps_anchor():
    """A referenced sales measure means `sales` is no longer a subset of `returns`
    — the anchor is legitimately needed and must stay."""
    ex = _setup()
    text = (
        "where channel='STORE' and rdim_id is not null "
        "select order_id, sum(return_amount) as r, sum(ext_sales_price) as s;"
    )
    sql = _sql(ex, text)
    assert '"sales"' in sql, sql
    assert _rows(ex, text) == {(1, 30.0, 100.0), (2, 70.0, 70.0)}
