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
from trilogy.core.processing.nodes import RecursiveNode, UnionNode
from trilogy.core.processing.v4_helper.source_planning import (
    SourceRequest,
    _bridge_plan,
    _concepts_in_graph,
    _datasource_grain_concept_nodes,
    _datasource_nodes_for_bridge,
    _inject_union_datasources,
    _original_datasource_concept_nodes,
    _requested_concepts,
    _resolve_bridge_graph,
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

# `code` lives complete on `carrier`; `flight` references it as a partial `~code`
# foreign key. `data_through` is a `<*>` single-row watermark constant. Sourcing
# {code, id, data_through} must keep `code` complete (carrier LEFT JOIN flight),
# not collapse onto the partial flight scan. (refresh brief 06)
PARTIAL_KEY_WITH_WATERMARK_MODEL = """
key code string;
key id int;
property <*>.data_through datetime;

datasource carrier (
    code: code,
)
grain (code)
query '''select 'AA' as code''';

datasource flight (
    carrier: ~code,
    id2: id,
)
grain (id)
query '''select 'AA' as carrier, 1 as id2''';

datasource flight_watermark (
    data_through: data_through,
)
grain (data_through)
query '''select TIMESTAMP '2026-04-24 10:07:49' as data_through''';
"""

# A recursive `recurse_edge` connector (`first_parent`, grain {id}) merged into a
# dimension key (`pid`) on a SEPARATE datasource. Sourcing {id, plabel} must
# materialize the recursion as a connector parent (it owns `id`) and join it to
# the dimension on `first_parent ≡ pid` — the bridge's derived connector lives in
# `alias_origin_lookup`, not as a `ds~` node. (brief 02)
RECURSIVE_CONNECTOR_MODEL = """
key id int;
property id.parent int;
key pid int;
property pid.plabel string;

datasource edges (
    id: id,
    parent: parent,
)
grain (id)
query '''select 1 as id, null as parent union all select 2, 1 union all select 3, 2''';

datasource pnodes (
    pid: pid,
    plabel: plabel,
)
grain (pid)
query '''select 1 as pid, 'A' as plabel union all select 2, 'B' ''';

auto first_parent <- recurse_edge(id, parent);
merge first_parent into pid;
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


def _build_partial_key_with_watermark():
    env = Environment()
    env.parse(PARTIAL_KEY_WITH_WATERMARK_MODEL)
    return env, env.materialize_for_select()


def _build_recursive_connector():
    env = Environment()
    env.parse(RECURSIVE_CONNECTOR_MODEL)
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

    def test_bridge_plan_ignores_single_row_subset_as_non_connector(self):
        """A bridged set that is a strict SUBSET of the request is not a
        connector — it just means a single-row/abstract concept (`data_through`)
        was excluded from the bridge search. `_bridge_plan` must return None so
        `_direct_source` builds the complete-key merge, rather than treating the
        subset as a connector and routing through one partial scan. (brief 06)"""
        env, benv = _build_partial_key_with_watermark()
        request = SourceRequest(
            outputs=[
                benv.concepts["local.code"],
                benv.concepts["local.id"],
                benv.concepts["local.data_through"],
            ],
            environment=benv,
            graph=generate_graph(benv),
            history=V4History(base_environment=env),
            source_policy=FALLBACK_SOURCE_POLICY,
        )

        # The bridge graph excludes the single-row `data_through`, so its concept
        # set is exactly the strict subset that used to misfire the old
        # `bridged != requested` gate.
        search = _search_concepts_for_bridge(request)
        bridge_graph = _resolve_bridge_graph(
            search,
            request,
            attempt=SourceAttempt.FULL,
            filter_downstream=False,
            search_conditions=None,
        )
        assert bridge_graph is not None
        bridged = {c.address for c in _concepts_in_graph(bridge_graph, benv)}
        requested = {c.address for c in _requested_concepts(request)}
        assert bridged < requested  # strict subset (data_through dropped)

        for attempt in (SourceAttempt.FULL, SourceAttempt.PARTIAL_UNSCOPED):
            assert _bridge_plan(request, attempt) is None

    def test_plan_source_keeps_partial_key_complete_with_single_row_output(self):
        """Sourcing a partial foreign key (`code`) alongside a single-row
        constant (`data_through`) must still merge the complete dimension source
        (`carrier`), leaving `code` non-partial. (brief 06)"""
        env, benv = _build_partial_key_with_watermark()
        node = plan_source(
            SourceRequest(
                outputs=[
                    benv.concepts["local.code"],
                    benv.concepts["local.id"],
                    benv.concepts["local.data_through"],
                ],
                environment=benv,
                graph=generate_graph(benv),
                history=V4History(base_environment=env),
                source_policy=FALLBACK_SOURCE_POLICY,
            )
        )

        assert node is not None
        assert {concept.address for concept in node.output_concepts} == {
            "local.code",
            "local.id",
            "local.data_through",
        }
        assert "local.code" not in {
            concept.address for concept in (node.partial_concepts or [])
        }

    def test_plan_source_materializes_recursive_bridge_connector(self):
        """A `recurse_edge` connector merged into a dimension key on another
        datasource is a derived bridge concept with no `ds~` node — its real
        lineage lives in `alias_origin_lookup`. `plan_source` must materialize
        the recursion as a parent (carrying both its key `id` and the join key
        `first_parent`) and join it to the dimension source, not drop it and
        render INVALID_REFERENCE. (brief 02)"""
        env, benv = _build_recursive_connector()
        node = plan_source(
            SourceRequest(
                outputs=[benv.concepts["local.id"], benv.concepts["local.plabel"]],
                environment=benv,
                graph=generate_graph(benv),
                history=V4History(base_environment=env),
                source_policy=FALLBACK_SOURCE_POLICY,
            )
        )

        assert node is not None
        assert {"local.id", "local.plabel"} <= {
            concept.address for concept in node.output_concepts
        }
        recursive_nodes: list[RecursiveNode] = []
        stack = list(node.parents)
        while stack:
            current = stack.pop()
            if isinstance(current, RecursiveNode):
                recursive_nodes.append(current)
            stack.extend(current.parents)
        assert recursive_nodes, "recursion connector was dropped from the bridge"
        connector_outputs = {
            c.address for n in recursive_nodes for c in n.output_concepts
        }
        assert {"local.id", "local.first_parent"} <= connector_outputs

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
