"""An aggregate grouped by the row grain key must not strand a sibling's axis.

`min(x ? p) by id` at output grain `id` is the most downstream group exposing
`id`, so the cover election hands it the key and the row-grain projection that
maps `id` onto a sibling aggregate's axis (`min(y ? q) by cell`) covers no
mandatory output and is dropped. The merge is then two grouped sides with no
column in common and the keyless-join guard raises. Both merge layers were
affected: the FINAL assembly when the aggregates are projected side by side,
and a consumer group's own parent merge when an expression reads them together.
"""

import networkx as nx
import pytest

from trilogy import Dialects
from trilogy.core.exceptions import UnresolvableQueryException
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.models.environment import Environment
from trilogy.core.processing.v4_helper import strategy_builder
from trilogy.core.processing.v4_helper.edges import EdgeMap
from trilogy.core.processing.v4_helper.join_plan import JoinPlan
from trilogy.core.processing.v4_helper.models import GroupAttrs
from trilogy.executor import Executor

MODEL = """
key id string;
key source string;
property id.zone string;

datasource rows (
  i: id,
  s: source,
  z: zone
)
grain (id)
query '''
select 'tem-1' i, 'municipal' s, 'c1' z union all
select 'tem-2' i, 'municipal' s, 'c2' z union all
select 'com-1' i, 'community' s, 'c2' z union all
select 'osm-1' i, 'osm' s, 'c1' z union all
select 'osm-2' i, 'osm' s, 'c3' z''';

auto cell <- concat(zone, '-grid');
auto anchor <- min(id ? source != 'osm') by cell;
auto self_if_municipal <- min(id ? source = 'municipal') by id;
auto cluster_id <- coalesce(self_if_municipal, anchor, id);
"""


@pytest.fixture
def executor() -> Executor:
    return Dialects.DUCK_DB.default_executor(environment=Environment())


def test_grain_key_aggregate_beside_foreign_axis_aggregate(executor: Executor):
    results = executor.execute_text(
        MODEL + "select id, self_if_municipal, anchor order by id asc;"
    )[-1].fetchall()
    assert results == [
        ("com-1", None, "com-1"),
        ("osm-1", None, "tem-1"),
        ("osm-2", None, None),
        ("tem-1", "tem-1", "tem-1"),
        ("tem-2", "tem-2", "com-1"),
    ]


def test_expression_over_both_aggregates(executor: Executor):
    results = executor.execute_text(MODEL + "select id, cluster_id order by id asc;")[
        -1
    ].fetchall()
    assert results == [
        ("com-1", "com-1"),
        ("osm-1", "tem-1"),
        ("osm-2", "osm-2"),
        ("tem-1", "tem-1"),
        ("tem-2", "tem-2"),
    ]


def test_row_gate_on_the_derived_cluster(executor: Executor):
    results = executor.execute_text(
        MODEL + "select id where id = cluster_id order by id asc;"
    )[-1].fetchall()
    assert results == [("com-1",), ("osm-2",), ("tem-1",), ("tem-2",)]


def _capture_merge_plans(monkeypatch) -> list[JoinPlan]:
    planned: list[JoinPlan] = []
    plan_merge = strategy_builder._merge_plan

    def capture(*args, **kwargs) -> JoinPlan | None:
        plan = plan_merge(*args, **kwargs)
        if plan is not None:
            planned.append(plan)
        return plan

    monkeypatch.setattr(strategy_builder, "_merge_plan", capture)
    return planned


def test_the_build_joins_the_bridges_recorded_before_it(
    executor: Executor, monkeypatch
):
    planned = _capture_merge_plans(monkeypatch)
    executor.execute_text(MODEL + "select id, cluster_id order by id asc;")
    assert any(plan.extras for plan in planned)


def test_a_bridge_left_unrecorded_is_never_elected_at_build(
    executor: Executor, monkeypatch
):
    planned = _capture_merge_plans(monkeypatch)
    plan_bridges = strategy_builder._plan_group_bridges

    def unrecorded(
        group_graph: nx.DiGraph,
        group_edges: EdgeMap,
        attrs: dict[str, GroupAttrs],
        order: list[str],
        environment: BuildEnvironment,
    ) -> None:
        plan_bridges(group_graph, group_edges, attrs, order, environment)
        for gid in order:
            attrs[gid].bridges = ()

    monkeypatch.setattr(strategy_builder, "_plan_group_bridges", unrecorded)
    with pytest.raises(UnresolvableQueryException, match="keyless join"):
        executor.execute_text(MODEL + "select id, cluster_id order by id asc;")
    assert planned and not any(plan.extras for plan in planned)
