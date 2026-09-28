import json

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
    plan_trace.start(query)
    try:
        process_query(env, select)
    finally:
        trace = plan_trace.stop()
    assert trace is not None
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
