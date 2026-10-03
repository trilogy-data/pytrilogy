"""A region join is typed by what the stream joined against its holder
carries, so the planner hands the narrowing pass nothing it already knew."""

from tests.helpers.models import LINE_ITEMS
from tests.helpers.planning import recorded
from tests.helpers.rows import executor_for, fetch_rows

_QUERY = """select user_id, product_id, sum(sale_price) as revenue
order by user_id asc nulls first, product_id asc;"""


def _join_types(title: str) -> list[str]:
    trace = recorded(executor_for(LINE_ITEMS), _QUERY)
    step = next(s for s in trace.steps if s.title == title)
    return [j.type for c in step.data.ctes for j in c.joins or []]


def test_first_region_holder_is_left_at_plan_time():
    planned = _join_types("CTEs before optimization")
    assert planned == ["left outer", "full"]
    assert planned == _join_types("CTEs after optimization")


def test_two_family_rows_survive():
    assert fetch_rows(executor_for(LINE_ITEMS), _QUERY) == [
        (None, 3, None),
        (1, 1, 5.0),
        (1, 2, 7.0),
        (2, 1, 3.0),
        (3, None, None),
    ]
