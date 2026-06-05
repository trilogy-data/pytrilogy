from trilogy import Dialects

MODEL = """
key order_id int;
property order_id.sale_amount float?;
property order_id.return_amount float?;
property order_id.return_flag int?;

datasource sales_src (
    order_id: order_id,
    sale_amount: sale_amount,
)
grain (order_id)
query '''select 1 as order_id, 100.0 as sale_amount
   union all select 2, 200.0
   union all select 3, 300.0''';

datasource returns_src (
    order_id: ~order_id,
    return_amount: return_amount,
    return_flag: return_flag,
)
grain (order_id)
query '''select 1 as order_id, 10.0 as return_amount, 1 as return_flag''';
"""


def _run(select: str) -> tuple[float, float]:
    x = Dialects.DUCK_DB.default_executor()
    x.parse_text(MODEL)
    row = x.execute_query(select).fetchall()[0]
    return row.sales, row.returns


def test_filter_on_secondary_measure_keeps_outer_join() -> None:
    # 3 sales (100/200/300), 1 return (order 1 = 10). Filtering the return
    # measure must not drop the unmatched sales rows (orders 2 & 3): the
    # secondary datasource join stays LEFT OUTER, not INNER.
    assert _run(
        "select sum(sale_amount) as sales, "
        "sum(return_amount ? return_flag = 1) as returns;"
    ) == (600.0, 10.0)


def test_filter_on_both_measures_keeps_outer_join() -> None:
    assert _run(
        "select sum(sale_amount ? sale_amount > 0) as sales, "
        "sum(return_amount ? return_flag = 1) as returns;"
    ) == (600.0, 10.0)


def test_unfiltered_measures_baseline() -> None:
    assert _run("select sum(sale_amount) as sales, sum(return_amount) as returns;") == (
        600.0,
        10.0,
    )
