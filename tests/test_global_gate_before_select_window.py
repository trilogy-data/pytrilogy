from trilogy import Dialects
from trilogy.core.models.environment import Environment

MODEL = """
key item int;
key ticket int;
property <ticket, item>.np float;
datasource sales (item: item, ticket: ticket, np: np)
grain (ticket, item)
query '''select * from (values (1,1,1.0),(1,2,3.0),(2,1,10.0),(3,1,0.5),(3,2,0.5),(4,1,20.0)) as t(item,ticket,np)''';
"""

GATE = "where avg(np) > (avg(np) by *)"


def _rows(body: str):
    e = Dialects.DUCK_DB.default_executor(environment=Environment())
    e.parse_text(MODEL)
    return [tuple(r) for r in e.execute_query(body).fetchall()]


def test_global_gate_filters_before_select_window():
    rows = _rows(
        f"{GATE} select item, rank(item) over (order by avg(np) asc) as rk "
        "order by item asc;"
    )
    assert rows == [(2, 1), (4, 2)]


def test_rowset_having_over_window_with_global_gate():
    rows = _rows(
        f"rowset bp <- {GATE} select item as item_sk, avg(np) as anp, "
        "rank(item) over (order by avg(np) asc) as rk having rk <= 1;\n"
        "select bp.item_sk, bp.anp, bp.rk;"
    )
    assert rows == [(2, 10.0, 1)]


def test_rowset_having_over_window_with_filtered_global_gate():
    rows = _rows(
        "rowset bp <- where avg(np) > (avg(np ? ticket = 1) by *) "
        "select item as item_sk, rank(item) over (order by avg(np) asc) as rk "
        "having rk <= 10;\n"
        "select bp.item_sk, bp.rk order by bp.rk asc;"
    )
    assert rows == [(2, 1), (4, 2)]


def test_global_gate_filters_finer_grained_aggregate():
    rows = _rows(
        "where avg(np) by item > (avg(np) by *) "
        "select ticket, sum(np) as s order by ticket asc;"
    )
    assert rows == [(1, 30.0)]
