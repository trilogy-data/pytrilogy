from trilogy import Environment
from trilogy.core.enums import ComparisonOperator
from trilogy.core.env_processor import generate_graph
from trilogy.core.graph_models import SearchCriteria
from trilogy.core.models.build import (
    BuildComparison,
    BuildUnionDatasource,
    BuildWhereClause,
)
from trilogy.core.processing.concept_strategies_v4 import V4History
from trilogy.core.processing.nodes import UnionNode
from trilogy.core.processing.v4_helper.source_planning import (
    SourceRequest,
    _bridge_plan,
    _datasource_grain_concept_nodes,
    _datasource_nodes_for_bridge,
    _inject_union_datasources,
    _original_datasource_concept_nodes,
    _search_concepts_for_bridge,
    plan_source,
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

GRAINED_BRIDGE_MODEL = """
key customer_id int;
key item_id int;
key order_number int;
key ticket_number int;
property <order_number, item_id>.catalog_quantity int;

datasource catalog_sales (
    customer_id: customer_id,
    item_id: item_id,
    order_number: order_number,
    catalog_quantity: catalog_quantity,
)
grain (order_number, item_id)
query '''select 1 customer_id, 10 item_id, 100 order_number, 5 catalog_quantity''';

datasource store_sales (
    customer_id: customer_id,
    item_id: item_id,
    ticket_number: ticket_number,
)
grain (ticket_number, item_id)
query '''select 1 customer_id, 10 item_id, 200 ticket_number''';
"""

CHANNEL_DIM_MODEL = """
key sales_channel enum<string>['WEB', 'CATALOG'];
key channel_dim_id int;
property <channel_dim_id, sales_channel>.channel_dim_text_id string;

partial datasource web_dim (
    raw(''' 'WEB' '''): sales_channel,
    web_site_sk: channel_dim_id,
    web_site_id: channel_dim_text_id,
)
grain (channel_dim_id, sales_channel)
complete where sales_channel = 'WEB'
query '''select 1 web_site_sk, 'web1' web_site_id''';

partial datasource catalog_dim (
    raw(''' 'CATALOG' '''): sales_channel,
    catalog_page_sk: channel_dim_id,
    catalog_page_id: channel_dim_text_id,
)
grain (channel_dim_id, sales_channel)
complete where sales_channel = 'CATALOG'
query '''select 1 catalog_page_sk, 'cat1' catalog_page_id''';
"""

PARTITIONED_CHANNEL_DIM_MODEL = """
key sales_channel enum<string>['WEB', 'CATALOG'];
key channel_dim_id int;
property channel_dim_id.channel_dim_text_id string;

partial datasource web_dim (
    raw(''' 'WEB' '''): sales_channel,
    web_site_sk: channel_dim_id,
    web_site_id: channel_dim_text_id,
)
grain (channel_dim_id, sales_channel)
complete where sales_channel = 'WEB'
query '''select 1 web_site_sk, 'web1' web_site_id''';

partial datasource catalog_dim (
    raw(''' 'CATALOG' '''): sales_channel,
    catalog_page_sk: channel_dim_id,
    catalog_page_id: channel_dim_text_id,
)
grain (channel_dim_id, sales_channel)
complete where sales_channel = 'CATALOG'
query '''select 1 catalog_page_sk, 'cat1' catalog_page_id''';
"""

CONDITION_ONLY_MODEL = """
key id int;
property id.month_seq int;
property id.value int;

datasource d (
    id: id,
    month_seq: month_seq,
    value: value,
)
grain (id)
query '''select 1 id, 1201 month_seq, 10 value''';
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


def _build_grained_bridge():
    env = Environment()
    env.parse(GRAINED_BRIDGE_MODEL)
    return env, env.materialize_for_select()


def _build_channel_dim():
    env = Environment()
    env.parse(CHANNEL_DIM_MODEL)
    return env, env.materialize_for_select()


def _build_partitioned_channel_dim():
    env = Environment()
    env.parse(PARTITIONED_CHANNEL_DIM_MODEL)
    return env, env.materialize_for_select()


def _build_condition_only():
    env = Environment()
    env.parse(CONDITION_ONLY_MODEL)
    return env, env.materialize_for_select()


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
    def test_original_datasource_nodes_restore_same_address_bridge_key(self):
        _, benv = _build_grained_bridge()
        source_graph = generate_graph(benv)
        bridge_graph = source_graph.copy()
        catalog_customer = next(
            node
            for node in source_graph.neighbors("ds~catalog_sales")
            if node.startswith("c~") and "customer_id" in node
        )
        bridge_graph.remove_node(catalog_customer)

        restored = _original_datasource_concept_nodes(
            source_graph,
            bridge_graph,
            "ds~catalog_sales",
            {"local.customer_id"},
            benv,
        )

        assert restored == [catalog_customer]
        assert catalog_customer in bridge_graph
        assert bridge_graph.has_edge("ds~catalog_sales", catalog_customer)

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

    def test_bridge_search_includes_requested_grain_keys(self):
        env, benv = _build_channel_dim()
        request = SourceRequest(
            outputs=[benv.concepts["local.channel_dim_text_id"]],
            environment=benv,
            graph=generate_graph(benv),
            history=V4History(base_environment=env),
            source_policy=FALLBACK_SOURCE_POLICY,
        )

        search = _search_concepts_for_bridge(request)

        assert {concept.address for concept in search} == {
            "local.channel_dim_id",
            "local.channel_dim_text_id",
            "local.sales_channel",
        }

    def test_bridge_plan_treats_grain_keys_as_injected_concepts(self):
        env, benv = _build_channel_dim()
        request = SourceRequest(
            outputs=[benv.concepts["local.channel_dim_text_id"]],
            environment=benv,
            graph=generate_graph(benv),
            history=V4History(base_environment=env),
            source_policy=FALLBACK_SOURCE_POLICY,
        )

        plan = _bridge_plan(request, SourceAttempt.FULL)

        assert plan is not None
        assert {concept.address for concept in plan.concepts} == {
            "local.channel_dim_id",
            "local.channel_dim_text_id",
            "local.sales_channel",
        }

    def test_bridge_component_source_exposes_selected_concept_grain_keys(self):
        env, benv = _build_channel_dim()
        request = SourceRequest(
            outputs=[benv.concepts["local.channel_dim_text_id"]],
            environment=benv,
            graph=generate_graph(benv),
            history=V4History(base_environment=env),
            source_policy=FALLBACK_SOURCE_POLICY,
        )
        plan = _bridge_plan(request, SourceAttempt.FULL)
        assert plan is not None

        parents = _datasource_nodes_for_bridge(request, plan, SourceAttempt.FULL)
        assert parents is not None
        union = next(parent for parent in parents if isinstance(parent, UnionNode))

        for branch in union.parents:
            assert {
                "local.channel_dim_id",
                "local.channel_dim_text_id",
                "local.sales_channel",
            } <= {concept.address for concept in branch.output_concepts}

    def test_component_source_exposes_selected_graph_node_grain_keys(self):
        _, benv = _build_partitioned_channel_dim()
        graph = generate_graph(benv)
        _inject_union_datasources(
            graph,
            [benv.concepts["local.channel_dim_text_id"]],
            benv,
        )
        ds_node = "ds~web_dim-catalog_dim"
        selected = [
            "c~local.channel_dim_text_id@Grain<local.channel_dim_id,local.sales_channel>"
        ]

        extra = _datasource_grain_concept_nodes(graph, ds_node, selected, benv)

        assert any("c~local.sales_channel@" in node for node in extra)

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

    def test_direct_source_includes_condition_only_row_args(self):
        env, benv = _build_condition_only()
        condition = BuildWhereClause(
            conditional=BuildComparison(
                left=benv.concepts["local.month_seq"],
                right=1200,
                operator=ComparisonOperator.GT,
            )
        )

        node = plan_source(
            SourceRequest(
                outputs=[benv.concepts["local.value"]],
                environment=benv,
                graph=generate_graph(benv),
                history=V4History(base_environment=env),
                conditions=condition,
            )
        )

        assert node is not None
        assert "local.value" in {concept.address for concept in node.output_concepts}
