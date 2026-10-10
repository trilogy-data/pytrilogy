import copy
import sys
from pathlib import Path

from tests.core.processing.test_plan_trace import _trace

_DEBUGGER_DIR = Path(__file__).resolve().parents[3] / "local_scripts" / "plan_debugger"
if str(_DEBUGGER_DIR) not in sys.path:
    sys.path.insert(0, str(_DEBUGGER_DIR))

from plan_trace_diff import diff_traces, format_diff, keyed_steps, value_diff

QUERY = "select name, count(order_id) as order_count;"


def test_same_statement_traced_twice_is_the_same_plan():
    diff = diff_traces(_trace(QUERY), _trace(QUERY))
    assert diff.same_plan, format_diff(diff)
    assert "same plan" in format_diff(diff)
    assert "optimizer" in diff.phase_ms


def test_a_changed_join_type_is_reported_on_its_step():
    a = _trace(QUERY)
    b = copy.deepcopy(a)
    join = next(s for s in b["steps"] if s["phase"] == "join")
    join["data"]["type"] = "full"
    diff = diff_traces(a, b)
    ((key, lines),) = diff.changed.items()
    assert key[1] == "join"
    assert lines == [".type: 'left outer' -> 'full'"]
    assert not diff.same_plan


def test_steps_only_one_trace_has_are_listed_by_side():
    a = _trace(QUERY)
    b = copy.deepcopy(a)
    dropped = b["steps"].pop(1)
    b["steps"].append({**dropped, "title": "extra"})
    diff = diff_traces(a, b)
    assert [k[3] for k in diff.only_a] == [dropped["title"]]
    assert [k[3] for k in diff.only_b] == ["extra"]


def test_repeated_titles_align_by_occurrence():
    keys = list(keyed_steps(_trace(QUERY)))
    grouping = [k for k in keys if k[1] == "grouping"]
    assert len(set(grouping)) == len(grouping)


def test_value_diff_paths():
    assert value_diff({"a": [1, {"b": 2}]}, {"a": [1, {"b": 3}]}) == [".a[1].b: 2 -> 3"]
    assert value_diff([1], [1, 2]) == [".: [1] -> [1, 2]"]
