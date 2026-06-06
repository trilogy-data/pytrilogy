from trilogy import Environment
from trilogy.core.env_processor import generate_graph
from trilogy.core.graph_models import SearchCriteria
from trilogy.core.models.build import BuildUnionDatasource
from trilogy.core.processing.concept_strategies_v4 import V4History
from trilogy.core.processing.nodes import UnionNode
from trilogy.core.processing.v4_helper.source_planning import (
    SourceRequest,
    _bridge_plan,
    _datasource_nodes_for_bridge,
    _inject_union_datasources,
)
from trilogy.core.processing.v4_helper.source_policy import (
    FALLBACK_SOURCE_POLICY,
    ROWSET_SOURCE_POLICY,
    STRICT_SOURCE_POLICY,
    SourceAttempt,
    SourcePolicy,
    source_policy_from_legacy_accept_partial,
)

PARTIAL_UNION_MODEL = """
key sales_channel enum<string>['WEB', 'CATALOG'];
key order_id int;
key item_id int;
key date_id int;

property date_id.week_seq int;
property date_id.sale_year int;
property <order_id, sales_channel, item_id>.ext_sales_price float?;

datasource date_dim (
    date_id: date_id,
    week_seq: week_seq,
    sale_year: sale_year,
)
grain (date_id)
query '''
select 1 date_id, 10 week_seq, 2001 sale_year
union all select 2, 10, 2001
''';

partial datasource web_sales (
    raw(''' 'WEB' '''): sales_channel,
    order_id: order_id,
    item_id: item_id,
    sold_date_id: ?date_id,
    ext_sales_price: ext_sales_price,
)
grain (order_id, sales_channel, item_id)
complete where sales_channel = 'WEB'
query '''
select 100 order_id, 1 item_id, 1 sold_date_id, 20.0 ext_sales_price
''';

partial datasource catalog_sales (
    raw(''' 'CATALOG' '''): sales_channel,
    order_id: order_id,
    item_id: item_id,
    sold_date_id: ?date_id,
    ext_sales_price: ext_sales_price,
)
grain (order_id, sales_channel, item_id)
complete where sales_channel = 'CATALOG'
query '''
select 200 order_id, 1 item_id, 2 sold_date_id, 10.0 ext_sales_price
''';
"""


def _build_partial_union():
    env = Environment()
    env.parse(PARTIAL_UNION_MODEL)
    return env, env.materialize_for_select()


def _source_outputs(benv):
    return [
        benv.concepts[address]
        for address in [
            "local.week_seq",
            "local.ext_sales_price",
            "local.item_id",
            "local.order_id",
            "local.sales_channel",
        ]
    ]


class TestSourcePolicy:
    def test_attempts_map_to_search_criteria(self):
        assert SourceAttempt.FULL.criteria == SearchCriteria.FULL_ONLY
        assert (
            SourceAttempt.PARTIAL_UNSCOPED.criteria == SearchCriteria.PARTIAL_UNSCOPED
        )
        assert (
            SourceAttempt.PARTIAL_SCOPED.criteria
            == SearchCriteria.PARTIAL_INCLUDING_SCOPED
        )

    def test_policy_cache_key_is_attempt_order(self):
        policy = SourcePolicy((SourceAttempt.FULL, SourceAttempt.PARTIAL_UNSCOPED))
        assert policy.cache_key == "full|partial_unscoped"
        assert policy.accepts_partial
        assert not STRICT_SOURCE_POLICY.accepts_partial

    def test_legacy_accept_partial_maps_to_default_fallback_policy(self):
        assert source_policy_from_legacy_accept_partial(False) is FALLBACK_SOURCE_POLICY
        assert source_policy_from_legacy_accept_partial(True) is FALLBACK_SOURCE_POLICY
        assert ROWSET_SOURCE_POLICY is FALLBACK_SOURCE_POLICY


class TestBridgeSourcePlanning:
    def test_inject_union_datasources_adds_enum_partition_union(self):
        _, benv = _build_partial_union()
        graph = generate_graph(benv)

        _inject_union_datasources(graph, _source_outputs(benv), benv)

        union = graph.datasources["ds~web_sales-catalog_sales"]
        assert isinstance(union, BuildUnionDatasource)
        assert [child.name for child in union.children] == [
            "web_sales",
            "catalog_sales",
        ]
        assert (
            "ds~web_sales-catalog_sales",
            "c~local.ext_sales_price@Grain<local.item_id,local.order_id,local.sales_channel>",
        ) in graph.edges()

    def test_bridge_plan_uses_union_source_for_partial_channel_sources(self):
        env, benv = _build_partial_union()
        request = SourceRequest(
            outputs=_source_outputs(benv),
            environment=benv,
            graph=generate_graph(benv),
            history=V4History(base_environment=env),
            source_policy=FALLBACK_SOURCE_POLICY,
        )

        plan = _bridge_plan(request, SourceAttempt.FULL)

        assert plan is not None
        assert "ds~web_sales-catalog_sales" in plan.graph.datasources

    def test_bridge_datasource_nodes_materialize_union_parent(self):
        env, benv = _build_partial_union()
        request = SourceRequest(
            outputs=_source_outputs(benv),
            environment=benv,
            graph=generate_graph(benv),
            history=V4History(base_environment=env),
            source_policy=FALLBACK_SOURCE_POLICY,
        )
        plan = _bridge_plan(request, SourceAttempt.FULL)
        assert plan is not None

        parents = _datasource_nodes_for_bridge(request, plan, SourceAttempt.FULL)

        assert parents is not None
        assert any(isinstance(parent, UnionNode) for parent in parents)
