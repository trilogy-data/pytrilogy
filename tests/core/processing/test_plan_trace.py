import json
import time

from trilogy import Environment
from trilogy.core.processing import plan_trace
from trilogy.core.query_processor import process_query
from trilogy.core.statements.author import SelectStatement

MODEL = """
key customer_id int;
property customer_id.name string;
key order_id int;
property order_id.amount int;

datasource customers (customer_id: customer_id, name: name)
grain (customer_id)
address customers;

datasource orders (order_id: order_id, customer_id: ~customer_id, amount: amount)
grain (order_id)
address orders;

rowset by_customer <- select customer_id, name, count(order_id) as n;
"""


def _trace(query: str) -> dict:
    env, statements = Environment().parse(MODEL + query)
    select = [s for s in statements if isinstance(s, SelectStatement)][-1]
    with plan_trace.recording(query) as trace:
        process_query(env, select)
    assert not plan_trace.active()
    return json.loads(json.dumps(trace.to_dict()))


def test_inactive_recorder_is_a_no_op():
    assert not plan_trace.active()
    plan_trace.record("ignored", plan_trace.StrategyStep(None))
    with plan_trace.plan_scope("ignored", 0, [], []) as plan_id:
        assert plan_id is None
    assert plan_trace.current() is None


def test_phases_recorded_in_pipeline_order():
    trace = _trace("select name, count(order_id) as order_count;")
    phases = [s["phase"] for s in trace["steps"]]
    first = {p: phases.index(p) for p in dict.fromkeys(phases)}
    order = [p for p in plan_trace.PHASES if p in first]
    assert order == sorted(order, key=first.__getitem__)
    assert {"request", "concept_graph", "keyspace", "grouping", "group_graph"} <= set(
        phases
    )
    assert {"source", "node", "final", "strategy", "resolve", "ctes"} <= set(phases)
    assert phases.count("ctes") == 2
    assert len(trace["plans"]) == 1


def test_keyspace_step_carries_the_regions_and_the_domain_reaches_the_node():
    trace = _trace("select name, amount, count(order_id) as order_count;")
    keyspace = next(s for s in trace["steps"] if s["phase"] == "keyspace")["data"][
        "keyspace"
    ]
    spans = [r["spans"] for r in keyspace["regions"]]
    assert spans == [[], ["local.customer_id"]]
    assert keyspace["matrix"]["local.name"] == ["defined", "defined"]
    assert keyspace["matrix"]["local.amount"] == ["defined", "absent"]
    assert keyspace["output_demanded_spans"] == ["local.customer_id"]
    grouped = [s for s in trace["steps"] if s["phase"] == "group_graph"][-1]["data"]
    domains = [
        gid
        for gid, attrs in grouped["graph"]["nodes"].items()
        if attrs and attrs["extent_spans"]
    ]
    assert domains == ["grp:root:root:∅:extent:local.customer_id"]
    built = next(
        s
        for s in trace["steps"]
        if s["phase"] == "node" and s["data"]["group"] == domains[0]
    )
    assert built["data"]["node"]["datasource"] == "customers"
    resolved = next(s for s in trace["steps"] if s["phase"] == "resolve")["data"]
    assert resolved["datasource"]["region_spans"] == ["local.customer_id"]


def test_rowset_body_is_a_nested_plan_with_its_own_search():
    trace = _trace("select by_customer.name, by_customer.n;")
    plans = trace["plans"]
    assert [p["depth"] for p in plans] == [0, 1]
    assert plans[1]["parent"] == plans[0]["id"]
    assert plans[1]["parent_context"].startswith("grp:rowset:")
    rowset_built = [
        s for s in trace["steps"] if s["title"] == "built " + plans[1]["parent_context"]
    ]
    assert rowset_built and rowset_built[0]["plan"] == plans[0]["id"]
    inner = [s for s in trace["steps"] if s["plan"] == plans[1]["id"]]
    assert {"keyspace", "source", "final"} <= {s["phase"] for s in inner}
    searches = [s for s in inner if s["title"].startswith("network search")]
    assert searches and all(s["data"]["solution"] for s in searches)


def test_env_var_writes_a_trace_per_statement(tmp_path, monkeypatch):
    out = tmp_path / "plan.json"
    monkeypatch.setenv(plan_trace.TRACE_ENV, str(out))
    env, statements = Environment().parse(MODEL + "select name, count(order_id) as n;")
    process_query(env, [s for s in statements if isinstance(s, SelectStatement)][-1])
    written = json.loads(out.read_text(encoding="utf-8"))
    assert written["version"] == plan_trace.TRACE_VERSION
    assert any(s["phase"] == "sql" for s in written["steps"]) is False
    assert any(s["phase"] == "ctes" for s in written["steps"])
    assert not plan_trace.active()
    process_query(env, [s for s in statements if isinstance(s, SelectStatement)][-1])
    assert (tmp_path / "plan.2.json").exists()


def test_nested_recording_restores_the_outer_trace():
    with plan_trace.recording("outer") as outer:
        with plan_trace.recording("inner"):
            pass
        assert plan_trace.current() is outer
    assert not plan_trace.active()


def _tree_groups(node: dict | None, out: set[str] | None = None) -> set[str]:
    out = set() if out is None else out
    if not node or "ref" in node:
        return out
    if node.get("group"):
        out.add(node["group"])
    for parent in node["parents"]:
        _tree_groups(parent, out)
    return out


def test_final_tree_keeps_the_tag_of_a_root_published_through_its_scan():
    # the root's generator hands back a passthrough over its conditioned scan;
    # publishing collapses the passthrough, and the scan keeps the group's tag
    trace = _trace("select customer_id, count(order_id) as n where amount > 5;")
    built = {
        s["data"]["group"]
        for s in trace["steps"]
        if s["phase"] == "node" and s["data"]["node"] is not None
    }
    final = next(s for s in trace["steps"] if s["phase"] == "final")["data"]["node"]
    root = next(gid for gid in built if gid.startswith("grp:root:root:"))
    assert root in _tree_groups(final)


def test_steps_carry_planner_time_except_snapshot_only_steps():
    trace = _trace("select name, count(order_id) as order_count;")
    steps = trace["steps"]
    untimed = {s["phase"] for s in steps if s["ms"] is None}
    assert untimed == {"request", "strategy"}
    assert all(s["ms"] >= 0 for s in steps if s["ms"] is not None)
    at = [s["at_ms"] for s in steps]
    assert at == sorted(at)
    assert trace["total_ms"] >= at[-1]


@plan_trace.off_clock
def _slow_snapshot() -> None:
    time.sleep(0.05)


def test_off_clock_time_is_not_planner_time():
    with plan_trace.recording() as trace:
        plan_trace.record("before", plan_trace.ResolveStep(None, None))
        _slow_snapshot()
        plan_trace.record("after", plan_trace.ResolveStep(None, None))
    assert trace.steps[-1].ms is not None and trace.steps[-1].ms < 25
    assert trace.total_ms is not None and trace.total_ms < 25
