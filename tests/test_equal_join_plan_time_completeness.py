from tests.helpers.planning import recorded
from trilogy import Dialects, Environment
from trilogy.core.processing import plan_trace
from trilogy.core.processing.plan_trace_model import JoinTypeStep

MODEL = """
key a_id int;
property a_id.a_val int;
key b_id int;
property b_id.b_val int;
property b_id.b_flag bool;
datasource a (a_id, a_val) grain (a_id)
query '''select 1 a_id, 10 a_val union all select 2, 20 union all select 3, 30''';
datasource b (b_id, b_val, b_flag) grain (b_id)
query '''select 1 b_id, 100 b_val, true b_flag union all select 2, 200, false
union all select 3, 300, true''';
merge b_id into a_id;
"""


def _join_steps(query: str) -> tuple[list[JoinTypeStep], list[tuple]]:
    env = Environment()
    env.parse(MODEL)
    executor = Dialects.DUCK_DB.default_executor(environment=env)
    trace: plan_trace.PlanTrace = recorded(executor, query)
    steps = [s.data for s in trace.steps if isinstance(s.data, JoinTypeStep)]
    return steps, executor.execute_text(query)[-1].fetchall()


def test_equal_key_between_complete_scans_is_typed_by_the_planner():
    steps, rows = _join_steps("select a_id, a_val, b_val order by a_id asc;")
    assert steps and all(s.rule != "full_join_keys" for s in steps), steps
    assert rows == [(1, 10, 100), (2, 20, 200), (3, 30, 300)]


def test_merge_where_over_complete_scans_keeps_its_rows():
    _, rows = _join_steps("where b_flag select a_id, a_val, b_val order by a_id asc;")
    assert rows == [(1, 10, 100), (3, 30, 300)]
