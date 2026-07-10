"""q84 handoff (handoff_q84_union_join_rowset_grain_collapse) — REFUTED.

The reported asymmetry ("a scoped `union join` onto a rowset drops the rowset's
grain from the final GROUP BY, while a direct datasource join preserves it") was
a misreading: the 'passing' agent query authored ``--sr.ticket_number as ticket``
select items, and ``--`` is the select-item HIDE modifier, not a comment. The
hidden outputs put the returns grain into the select grain, which is what
preserved the fan-out. With identical authored outputs the two forms agree in
both directions:

- no grain outputs   -> dedup to the select grain (rowset == direct)
- hidden grain outputs -> fan-out preserved (rowset == direct)

This file pins that parity so a real asymmetry regression shows up as a cell
split. Data shape mirrors q84: one customer (alice) whose demographic matches
two store returns; those returns share a ticket and differ only by item, so the
hidden-grain cells also pin that pruning the item column out of intermediate
CTEs never introduces an early dedup.
"""

import pytest

from trilogy import Dialects

MODEL = """
key c_id int;
property c_id.c_name string;
key c_demo int;
datasource customers (id: c_id, name: c_name, demo: c_demo) grain (c_id)
query '''select 1 as id, 'alice' as name, 10 as demo
union all select 2 as id, 'bob' as name, 20 as demo
union all select 3 as id, 'carol' as name, 30 as demo''';

key r_ticket int;
key r_item int;
key r_demo int;
datasource store_returns (ticket: r_ticket, item: r_item, demo: r_demo) grain (r_ticket, r_item)
query '''select 100 as ticket, 1 as item, 10 as demo
union all select 100 as ticket, 2 as item, 10 as demo
union all select 300 as ticket, 3 as item, 20 as demo''';

with return_demos as
select r_ticket, r_item, r_demo as demo_id
where r_demo is not null;
"""

DEDUPED = ["alice", "bob"]
FANNED_OUT = ["alice", "alice", "bob"]


@pytest.fixture
def executor():
    exec = Dialects.DUCK_DB.default_executor()
    exec.execute_text(MODEL)
    return exec


def _names(executor, query):
    return [r[0] for r in executor.execute_text(query)[0].fetchall()]


def test_rowset_join_no_grain_outputs_dedups(executor):
    assert (
        _names(
            executor,
            """
select c_name
union join return_demos.demo_id = c_demo
where return_demos.r_ticket is not null
order by c_name asc;
""",
        )
        == DEDUPED
    )


def test_direct_join_no_grain_outputs_dedups(executor):
    assert (
        _names(
            executor,
            """
select c_name
union join r_demo = c_demo
where r_ticket is not null
order by c_name asc;
""",
        )
        == DEDUPED
    )


def test_rowset_join_hidden_grain_outputs_preserve_fanout(executor):
    assert (
        _names(
            executor,
            """
select c_name,
  --return_demos.r_ticket,
  --return_demos.r_item
union join return_demos.demo_id = c_demo
where return_demos.r_ticket is not null
order by c_name asc;
""",
        )
        == FANNED_OUT
    )


def test_direct_join_hidden_grain_outputs_preserve_fanout(executor):
    assert (
        _names(
            executor,
            """
select c_name,
  --r_ticket,
  --r_item
union join r_demo = c_demo
where r_ticket is not null
order by c_name asc;
""",
        )
        == FANNED_OUT
    )


def test_rowset_join_visible_grain_outputs_preserve_fanout(executor):
    rows = executor.execute_text("""
select c_name, return_demos.r_ticket, return_demos.r_item
union join return_demos.demo_id = c_demo
where return_demos.r_ticket is not null
order by c_name asc, return_demos.r_item asc;
""")[0].fetchall()
    assert [tuple(r) for r in rows] == [
        ("alice", 100, 1),
        ("alice", 100, 2),
        ("bob", 300, 3),
    ]
