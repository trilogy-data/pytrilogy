"""`PushFilteredCountIntoJoin` moves a filtered COUNT's predicate onto its LEFT
JOIN's ON clause. The count is the same either way; every other column read from
the right side is not, because a row that fails the predicate stops matching and
the left row is NULL-extended instead. The rule is sound only when the aggregate
is the sole reader of that side — the TPC-H q13 shape.
"""

from tests.helpers.rows import executor_for, fetch_rows
from trilogy.core.enums import JoinType
from trilogy.core.models.execute import Join
from trilogy.executor import Executor

_MODEL = """
key line_id int;
key order_id int;
key user_id int;
property line_id.sale_price float;
property user_id.state string;

datasource order_items (lid: line_id, oid: order_id, uid: ~user_id, price: sale_price)
grain (line_id)
query '''
select 1 lid, 10 oid, 1 uid, 5.0 price union all
select 2 lid, 10 oid, 1 uid, 7.0 price union all
select 3 lid, 11 oid, 2 uid, 3.0 price
''';

datasource users (uid: user_id, st: state)
grain (user_id)
query '''
select 1 uid, 'ca' st union all select 2 uid, 'ny' st union all select 3 uid, 'wa' st
''';
"""


def _fired(executor: Executor, query: str) -> bool:
    """The predicate moved onto a LEFT JOIN's ON clause."""
    return any(
        isinstance(join, Join)
        and join.jointype == JoinType.LEFT_OUTER
        and join.condition is not None
        for cte in executor.parse_text(query)[-1].ctes
        for join in cte.joins
    )


_BY_ORDER = """select order_id, state, count(line_id ? sale_price > 4) as big_lines
order by order_id asc;"""

_BY_ORDER_NAMED = """auto big_line <- line_id ? sale_price > 4;
select order_id, state, count(big_line) as big_lines order by order_id asc;"""

_BY_USER = """select user_id, state, count(line_id ? sale_price > 4) as big_lines
order by user_id asc;"""


def test_grouping_key_off_the_right_keeps_its_value():
    executor = executor_for(_MODEL)
    assert fetch_rows(executor, _BY_ORDER) == [
        (10, "ca", 2),
        (11, "ny", 0),
        (None, "wa", 0),
    ]
    assert not _fired(executor, _BY_ORDER)


def test_named_filter_grouping_key_off_the_right_keeps_its_value():
    executor = executor_for(_MODEL)
    assert fetch_rows(executor, _BY_ORDER_NAMED) == [
        (10, "ca", 2),
        (11, "ny", 0),
        (None, "wa", 0),
    ]
    assert not _fired(executor, _BY_ORDER_NAMED)


def test_fires_when_the_count_is_the_sole_reader_of_the_right():
    executor = executor_for(_MODEL)
    assert fetch_rows(executor, _BY_USER) == [(1, "ca", 2), (2, "ny", 0), (3, "wa", 0)]
    assert _fired(executor, _BY_USER)
