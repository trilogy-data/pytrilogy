"""An inline `(select ...)` subquery body is a scalar that cross-joins; correlating
it back to the enclosing scope (`where cat_avg.category = category`) is not a shape
the planner supports.

The nested body gate islands `cat_avg`, so the correlation pairs a rowset
handle with a base concept through no declared join and refuses as a
disconnected select that says so, pointing at the supported spelling: read the
handle in the enclosing select and relate it there with a declared join. It
once reached the planner, where `_filter_arg_parents` picked a built group to
supply a FINAL-deferred filter's row arg, the rowset supplying the correlation
key hid it, and the merge leaked the internal "Invalid input concepts to
node!" assertion at the agent.
"""

import pytest

from trilogy import Dialects
from trilogy.core.exceptions import DisconnectedConceptsException
from trilogy.core.models.environment import Environment

_MODEL = """
key sk int;
property sk.category string;
property sk.price float;
datasource item (sk: sk, c: category, p: price) grain (sk)
query '''select 1 sk, 'a' c, 10.0 p union all select 2 sk, 'a' c, 30.0 p
    union all select 3 sk, 'b' c, 5.0 p''';

with cat_avg as
    where category is not null
    select category as category, avg(price) as avg_price;
"""

_CORRELATED = """
with qualifying as
    where category is not null
      and price > 1.2 * (select cat_avg.avg_price where cat_avg.category = category)
    select sk;
select count(qualifying.sk) as q;
"""

_DECLARED = """
with qualifying as
    where category is not null and price > 1.2 * cat_avg.avg_price
    select sk
    subset join cat_avg.category = category;
select count(qualifying.sk) as q;
"""

_UNCORRELATED = """
with qualifying as
    where price > 1.2 * (select cat_avg.avg_price where cat_avg.category = 'a')
    select sk;
select count(qualifying.sk) as q;
"""


def _engine():
    return Dialects.DUCK_DB.default_executor(environment=Environment())


def test_correlated_inline_subquery_refuses_without_leaking_node_invariant():
    with pytest.raises(DisconnectedConceptsException) as exc:
        _engine().generate_sql(_MODEL + _CORRELATED)
    message = str(exc.value)
    assert "correlated subquery is not supported" in message, message
    assert "`subset join cat_avg.category = category`" in message, message
    # the aggregate output has no name outside the body to join on
    assert "_cat_avg_avg_price" not in message, message


def test_the_declared_join_spelling_the_refusal_points_at_plans():
    rows = _engine().execute_text(_MODEL + _DECLARED)[0].fetchall()
    assert [tuple(row) for row in rows] == [(1,)]


def test_uncorrelated_inline_subquery_still_plans():
    rows = _engine().execute_text(_MODEL + _UNCORRELATED)[0].fetchall()
    assert [tuple(row) for row in rows] == [(1,)]
