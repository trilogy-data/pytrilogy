"""A region join is typed by what the stream joined against its holder
carries, so the planner hands the narrowing pass nothing it already knew."""

from tests.core.processing.test_v4_dim_peel_not_built import _MODEL
from trilogy import Dialects, Environment
from trilogy.core.processing import plan_trace

_QUERY = """select user_id, product_id, sum(sale_price) as revenue
order by user_id asc nulls first, product_id asc;"""


def _join_types(title: str) -> list[str]:
    env, _ = Environment().parse(_MODEL)
    executor = Dialects.DUCK_DB.default_executor(environment=env)
    with plan_trace.recording(_QUERY) as trace:
        executor.generate_sql(_QUERY)
    step = next(s for s in trace.steps if s.title == title)
    return [j.type for c in step.data.ctes for j in c.joins or []]


def test_first_region_holder_is_left_at_plan_time():
    planned = _join_types("CTEs before optimization")
    assert planned == ["left outer", "full"]
    assert planned == _join_types("CTEs after optimization")


def test_two_family_rows_survive():
    env, _ = Environment().parse(_MODEL)
    executor = Dialects.DUCK_DB.default_executor(environment=env)
    rows = [tuple(r) for r in executor.execute_text(_QUERY)[-1].fetchall()]
    assert rows == [
        (None, 3, None),
        (1, 1, 5.0),
        (1, 2, 7.0),
        (2, 1, 3.0),
        (3, None, None),
    ]
