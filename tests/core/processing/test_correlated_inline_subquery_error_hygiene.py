"""An inline `(select ...)` subquery body is a scalar that cross-joins; correlating
it back to the enclosing scope (`where cat_avg.category = category`) is not a shape
the planner supports.

It must still refuse at the planner's own guard rather than leaking the node-input
invariant. `_filter_arg_parents` picks a built group to supply a FINAL-deferred
filter's row arg, and the rowset supplying the correlation key hides it, so the
merge read the parent's usable outputs, found nothing, and raised the internal
"Invalid input concepts to node!" assertion at the agent.
"""

import pytest

from trilogy import Dialects
from trilogy.core.exceptions import UnresolvableQueryException
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

_UNCORRELATED = """
with qualifying as
    where price > 1.2 * (select cat_avg.avg_price where cat_avg.category = 'a')
    select sk;
select count(qualifying.sk) as q;
"""


def _engine():
    return Dialects.DUCK_DB.default_executor(environment=Environment())


def test_correlated_inline_subquery_refuses_without_leaking_node_invariant():
    with pytest.raises(UnresolvableQueryException) as exc:
        _engine().generate_sql(_MODEL + _CORRELATED)
    assert "Invalid input concepts to node" not in str(exc.value)


def test_uncorrelated_inline_subquery_still_plans():
    rows = _engine().execute_text(_MODEL + _UNCORRELATED)[0].fetchall()
    assert [tuple(row) for row in rows] == [(1,)]
