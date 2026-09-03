"""A scoped join on expression keys between two rowsets where the FINAL
contributors are projection wrappers over the rowset boundaries (the
expression is also projected, or a raw member is), so the boundary that
exposes the join virtual is not the merge parent."""

import pytest

from trilogy import Dialects

FIXTURE = """
key sale_id int;
property sale_id.s_channel string;
property sale_id.amount float;
key ret_id int;
property ret_id.r_channel string;
property ret_id.loss float;

datasource sale_rows (sale_id: sale_id, channel: s_channel, amount: amount)
grain (sale_id)
query '''
select 1 as sale_id, 'store' as channel, 10.0 as amount union all
select 2, 'store', 20.0 union all
select 3, 'catalog', 30.0
''';

datasource return_rows (ret_id: ret_id, channel: r_channel, loss: loss)
grain (ret_id)
query '''
select 1 as ret_id, 'STORE' as channel, 1.0 as loss union all
select 2, 'CATALOG', 3.0 union all
select 3, 'WEB', 5.0
''';

with s as select s_channel as channel, sum(amount) as sales;
with r as select r_channel as channel, sum(loss) as returns;
"""


@pytest.fixture
def executor():
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(FIXTURE)
    return executor


def _rows(executor, query):
    return sorted(
        (tuple(r) for r in executor.execute_text(query)[0].fetchall()), key=str
    )


def test_left_expression_projected(executor):
    rows = _rows(
        executor,
        """select upper(s.channel) as ch, s.sales, r.returns
        union join upper(s.channel) = upper(r.channel);""",
    )
    assert rows == [("CATALOG", 30.0, 3.0), ("STORE", 30.0, 1.0), (None, None, 5.0)]


def test_right_expression_projected(executor):
    rows = _rows(
        executor,
        """select upper(r.channel) as ch, s.sales, r.returns
        union join upper(s.channel) = upper(r.channel);""",
    )
    assert rows == [("CATALOG", 30.0, 3.0), ("STORE", 30.0, 1.0), ("WEB", None, 5.0)]


def test_raw_member_projected(executor):
    rows = _rows(
        executor,
        """select s.channel as ch, s.sales, r.returns
        union join upper(s.channel) = upper(r.channel);""",
    )
    assert rows == [("catalog", 30.0, 3.0), ("store", 30.0, 1.0), (None, None, 5.0)]


def test_both_expressions_projected(executor):
    rows = _rows(
        executor,
        """select upper(s.channel) as ch, upper(r.channel) as rch, s.sales, r.returns
        union join upper(s.channel) = upper(r.channel);""",
    )
    assert rows == [
        ("CATALOG", "CATALOG", 30.0, 3.0),
        ("STORE", "STORE", 30.0, 1.0),
        (None, "WEB", None, 5.0),
    ]


def test_subset_join_nothing_projected(executor):
    rows = _rows(
        executor,
        """select s.sales, r.returns
        subset join upper(r.channel) = upper(s.channel);""",
    )
    assert rows == [(30.0, 1.0), (30.0, 3.0)]


def test_subset_join_left_expression_projected(executor):
    rows = _rows(
        executor,
        """select upper(s.channel) as ch, s.sales, r.returns
        subset join upper(r.channel) = upper(s.channel);""",
    )
    assert rows == [("CATALOG", 30.0, 3.0), ("STORE", 30.0, 1.0)]


def test_subset_join_reversed_keeps_unmatched_anchor_rows(executor):
    rows = _rows(
        executor,
        """select upper(s.channel) as ch, s.sales, r.returns
        subset join upper(s.channel) = upper(r.channel);""",
    )
    assert rows == [("CATALOG", 30.0, 3.0), ("STORE", 30.0, 1.0), (None, None, 5.0)]
