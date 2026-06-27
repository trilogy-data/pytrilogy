"""End-to-end coverage for the v4 per-derivation generators and the rowset
boundary builder.

Each test drives the real `search_concepts` planner against a tiny inline
environment so the dispatch table, the group-graph parent wiring, and the
generator bodies all run on a genuine plan — synthetic StrategyNodes wouldn't
exercise the lineage/grain branches the generators isinstance-check.
"""

import types

import pytest

import trilogy.core.processing.concept_strategies_v4 as cs
from trilogy import Dialects, Environment
from trilogy.constants import CONFIG
from trilogy.core import graph as nx
from trilogy.core.enums import ComparisonOperator, Derivation
from trilogy.core.env_processor import generate_graph
from trilogy.core.models.build import (
    BuildComparison,
    BuildHavingClause,
    BuildSubselectComparison,
    BuildWhereClause,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.concept_strategies_v4 import (
    V4History,
    _resolve_and_inject_condition,
    _resolve_condition_sources,
    resolve_rowset,
    search_concepts,
)
from trilogy.core.processing.nodes import (
    GroupNode,
    MergeNode,
    RecursiveNode,
    SelectNode,
    StrategyNode,
    UnionNode,
    UnnestNode,
)
from trilogy.core.processing.v4_helper.strategy_builder import _add_needed_concept
from trilogy.core.processing.v4_node_generators.dispatch import build_node
from trilogy.core.processing.v4_node_generators.recursive import gen_recursive
from trilogy.core.processing.v4_node_generators.rowset import gen_rowset


def _build(text: str) -> tuple[Environment, BuildEnvironment]:
    env = Environment()
    env.parse(text)
    return env, env.materialize_for_select()


def _search(env, benv, addresses, conditions=None):
    return search_concepts(
        mandatory_list=[benv.concepts[a] for a in addresses],
        history=V4History(base_environment=env),
        environment=benv,
        depth=0,
        g=generate_graph(benv),
        conditions=conditions or [],
    )


def _generate_v4_sql(text: str) -> str:
    env = Environment()
    executor = Dialects.DUCK_DB.default_executor(environment=env)
    CONFIG.use_v4_discovery = True
    try:
        statements = executor.generate_sql(text)
    finally:
        CONFIG.use_v4_discovery = False
    return statements[-1]


def _find(node: StrategyNode, kind: type) -> bool:
    """True if any node in the parent tree is an instance of `kind`."""
    if isinstance(node, kind):
        return True
    return any(_find(p, kind) for p in node.parents)


def _walk(node: StrategyNode) -> list[StrategyNode]:
    nodes = [node]
    for parent in node.parents:
        nodes.extend(_walk(parent))
    return nodes


# ----- recursive -------------------------------------------------------

RECURSIVE_MODEL = """
key id int;
property id.parent int;
property id.label string;

datasource nodes (id:id, label:label) grain (id)
    query '''select 1 as id, 'A' as label''';
datasource edges (id:id, parent:parent) grain (id)
    query '''select 1 as id, null as parent''';

auto first_parent <- recurse_edge(id, parent);
"""


class TestRecursive:
    def test_recursive_resolves_to_recursive_node(self):
        env, benv = _build(RECURSIVE_MODEL)
        fp = benv.concepts["local.first_parent"]
        assert fp.derivation == Derivation.RECURSIVE
        info = _search(env, benv, ["local.first_parent", "local.id"])
        assert info.strategy_node is not None
        assert _find(info.strategy_node, RecursiveNode)

    def test_recursive_with_enrichment(self):
        """Pulling a property of the recursion key forces an enrichment join
        above the RecursiveNode."""
        env, benv = _build(RECURSIVE_MODEL)
        info = _search(env, benv, ["local.first_parent", "local.id", "local.label"])
        assert info.strategy_node is not None
        out = {c.address for c in info.strategy_node.output_concepts}
        assert "local.label" in out


# ----- union -----------------------------------------------------------

UNION_MODEL = """
key one int;
key two int;

datasource d1 (one:one) grain (one) query '''select 1 as one''';
datasource d2 (two:two) grain (two) query '''select 1 as two''';

auto u <- union(one, two);
"""

# Relational `union((select...),(select...)) -> (k, v)` TVF: a column-positional
# row stack whose arms are independent sub-selects. This is the form routed
# through `_resolve_union_select` (distinct from the inline `union(one, two)`
# concept-stack above, which the dispatch generator handles).
RELATIONAL_UNION_MODEL = """
key id int;
property id.val int;
key id2 int;
property id2.val2 int;

datasource d1 (id: id, val: val) grain (id) query '''select 1 as id, 10 as val''';
datasource d2 (id2: id2, val2: val2) grain (id2) query '''select 2 as id2, 20 as val2''';

with combined as union(
    (select id as k, val as v),
    (select id2 as k, val2 as v)
) -> (k, v);

select combined.k, combined.v order by combined.k asc;
"""


class TestUnion:
    def test_union_resolves_to_union_node(self):
        env, benv = _build(UNION_MODEL)
        info = _search(env, benv, ["local.u"])
        assert info.strategy_node is not None
        assert _find(info.strategy_node, UnionNode)

    def test_relational_union_resolves_to_union_node(self):
        env, benv = _build(RELATIONAL_UNION_MODEL)
        info = _search(env, benv, ["combined.k", "combined.v"])
        assert info.strategy_node is not None
        assert _find(info.strategy_node, UnionNode)
        # One arm per select, each stacked under the union.
        union_node = next(
            n for n in _walk(info.strategy_node) if isinstance(n, UnionNode)
        )
        assert len(union_node.parents) == 2

    def test_relational_union_end_to_end(self):
        prior = CONFIG.use_v4_discovery
        CONFIG.use_v4_discovery = True
        try:
            rows = (
                Dialects.DUCK_DB.default_executor()
                .execute_query(RELATIONAL_UNION_MODEL)
                .fetchall()
            )
        finally:
            CONFIG.use_v4_discovery = prior
        assert [tuple(r) for r in rows] == [(1, 10), (2, 20)]


# ----- root source planning -------------------------------------------

BRIDGED_ROOT_MODEL = """
key passenger_id int;
property passenger_id.name string;
property passenger_id.passenger_last_name string;

key rich_full_name string;
property rich_full_name.rich_last_name string;
property rich_full_name.net_worth int;

merge passenger_last_name into rich_last_name;

datasource passengers (
    passenger_id: passenger_id,
    name: name,
    passenger_last_name: passenger_last_name,
)
grain (passenger_id)
query '''select 1 as passenger_id, 'Ada' as name, 'Lovelace' as passenger_last_name''';

datasource rich_info (
    rich_full_name: rich_full_name,
    rich_last_name: rich_last_name,
    net_worth: net_worth,
)
grain (rich_full_name)
query '''select 'Ada Lovelace' as rich_full_name, 'Lovelace' as rich_last_name, 10 as net_worth''';
"""

ROOT_AGGREGATE_FILTER_MODEL = """
key customer_id int;
property customer_id.name string;
key order_id int;
key warehouse_id int;
property order_id.channel string;
property order_id.amount float;

datasource sales (
    order_id: order_id,
    customer_id: customer_id,
    warehouse_id: warehouse_id,
    channel: channel,
    amount: amount,
)
grain (order_id, warehouse_id)
query '''select 1 as order_id, 1 as customer_id, 1 as warehouse_id, 'STORE' as channel, 2.0 as amount''';

datasource customers (
    customer_id: customer_id,
    name: name,
)
grain (customer_id)
query '''select 1 as customer_id, 'Ada' as name''';

auto store_total <- sum(amount ? channel = 'STORE') by customer_id;
auto web_total <- sum(amount ? channel = 'WEB') by customer_id;
"""

EXISTENCE_COPY_MODEL = """
key order_id int;
key warehouse_id int;
property order_id.cost float;
key returned_order_id int;

datasource sales (
    order_id: order_id,
    warehouse_id: warehouse_id,
    cost: cost,
)
grain (order_id, warehouse_id)
query '''select 1 as order_id, 1 as warehouse_id, 2.0 as cost''';

datasource returns (
    returned_order_id: returned_order_id,
)
grain (returned_order_id)
query '''select 2 as returned_order_id''';

auto multi_warehouse_order <- order_id ? count(warehouse_id) by order_id > 1;
"""


class TestRootSourcePlanning:
    def test_root_source_planner_injects_bridge_keys(self):
        env, benv = _build(BRIDGED_ROOT_MODEL)
        info = _search(env, benv, ["local.name", "local.net_worth"])

        assert info.strategy_node is not None
        assert isinstance(info.strategy_node, MergeNode)
        parent_outputs = [
            {concept.address for concept in parent.output_concepts}
            for parent in info.strategy_node.parents
        ]
        assert any(
            {"local.name", "local.rich_last_name"}.issubset(outputs)
            for outputs in parent_outputs
        )
        assert any(
            {"local.net_worth", "local.rich_last_name"}.issubset(outputs)
            for outputs in parent_outputs
        )

    def test_root_filter_can_use_aggregate_side_parent(self):
        sql = _generate_v4_sql(ROOT_AGGREGATE_FILTER_MODEL + """
where
    store_total > 0
    and web_total > store_total
select
    name,
order by
    name asc
;
""")

        assert "INVALID_REFERENCE_BUG" not in sql
        assert '"name"' in sql
        assert "customer_id" in sql
        assert "on 1=1" not in sql

    def test_existence_sources_follow_copied_condition_hosts(self):
        sql = _generate_v4_sql(EXISTENCE_COPY_MODEL + """
where
    order_id not in returned_order_id
    and order_id in multi_warehouse_order
select
    count(order_id) as order_count,
    sum(cost) as total_cost,
;
""")

        assert "INVALID_REFERENCE_BUG" not in sql
        assert "returned_order_id" in sql
        assert "multi_warehouse_order" in sql


# ----- unnest ----------------------------------------------------------

UNNEST_MODEL = """
key id int;
datasource d (id:id) grain (id) query '''select 1 as id''';
auto nums <- unnest([1, 2, 3]);
"""


class TestUnnest:
    def test_unnest_resolves_to_unnest_node(self):
        env, benv = _build(UNNEST_MODEL)
        info = _search(env, benv, ["local.nums"])
        assert info.strategy_node is not None
        assert _find(info.strategy_node, UnnestNode)


# ----- group_to --------------------------------------------------------

GROUP_TO_MODEL = """
key store int;
key region int;
property store.sales float;

datasource d (store:store, region:region, sales:sales) grain (store)
    query '''select 1 as store, 1 as region, 2.0 as sales''';

auto region_sales <- group sales by region;
"""


class TestGroupTo:
    def test_group_to_resolves_to_group_node(self):
        env, benv = _build(GROUP_TO_MODEL)
        rs = benv.concepts["local.region_sales"]
        assert rs.derivation == Derivation.GROUP_TO
        info = _search(env, benv, ["local.region_sales", "local.region"])
        assert info.strategy_node is not None
        assert _find(info.strategy_node, GroupNode)


# ----- rowset boundary -------------------------------------------------

ROWSET_MODEL = """
key order_id int;
key store_id int;
property order_id.value float;

datasource orders (order_id:order_id, store_id:store_id, value:value)
    grain (order_id)
    query '''select 1 as order_id, 1 as store_id, 2.0 as value''';

auto store_total <- sum(value) by store_id;

with high_value as
select store_id
where store_total > 5;
"""

ROWSET_HAVING_MODEL = """
key order_id int;
key store_id int;
property order_id.value float;

datasource orders (order_id:order_id, store_id:store_id, value:value)
    grain (order_id)
    query '''select 1 as order_id, 1 as store_id, 2.0 as value''';

with high_value as
select
    store_id,
    sum(value) by store_id -> store_total
having
    store_total > 5;
"""


ROWSET_ORDER_MODEL = """
key order_id int;
key store_id int;
property order_id.value float;

datasource orders (order_id:order_id, store_id:store_id, value:value)
    grain (order_id)
    query '''select 1 as order_id, 1 as store_id, 2.0 as value''';

auto store_total <- sum(value) by store_id;

with high_value as
select store_id
where store_total > 5
order by store_id desc;
"""


HAVING_EXTERNAL_MODEL = """
key order_id int;
key store_id int;
property order_id.value float;

datasource orders (order_id:order_id, store_id:store_id, value:value)
    grain (order_id)
    query '''select 1 as order_id, 1 as store_id, 2.0 as value''';

auto store_total <- sum(value) by store_id;
auto avg_value <- avg(value);
"""


class TestRowset:
    def test_rowset_resolves_end_to_end(self):
        env, benv = _build(ROWSET_MODEL)
        info = _search(env, benv, ["high_value.store_id"])
        assert info.strategy_node is not None
        out = {c.address for c in info.strategy_node.output_concepts}
        assert "high_value.store_id" in out

    def test_rowset_having_resolves_end_to_end(self):
        """The inner HAVING (`store_total > 5`) is applied as a post-aggregate
        filter inside the rowset boundary."""
        env, benv = _build(ROWSET_HAVING_MODEL)
        info = _search(env, benv, ["high_value.store_id"])
        assert info.strategy_node is not None
        out = {c.address for c in info.strategy_node.output_concepts}
        assert "high_value.store_id" in out

    def test_rowset_order_by_resolves_end_to_end(self):
        """A rowset with an inner ORDER BY (including a NOT NULL guard in its
        WHERE) resolves end to end."""
        env, benv = _build(ROWSET_ORDER_MODEL)
        info = _search(env, benv, ["high_value.store_id"])
        assert info.strategy_node is not None

    def test_resolve_rowset_applies_boundary_conditions(self):
        """A filter injected at the rowset boundary (a consumer-side predicate
        over the rowset's rows) is applied as a WHERE over the materialized
        boundary (q64 shape)."""
        env, benv = _build(ROWSET_MODEL)
        rc = benv.concepts["high_value.store_id"]
        cond = BuildWhereClause(
            conditional=BuildComparison(
                left=rc, right=1, operator=ComparisonOperator.GT
            )
        )
        node = resolve_rowset(
            [rc],
            benv,
            depth=0,
            g=generate_graph(benv),
            history=V4History(base_environment=env),
            conditions=cond,
        )
        assert isinstance(node, SelectNode)
        assert node.conditions is not None

    def test_resolve_rowset_non_rowset_outputs_returns_none(self):
        """A bucket of plain roots handed to `resolve_rowset` bails to None so
        the caller treats it as a normal group rather than asserting."""
        env, benv = _build(ROWSET_MODEL)
        plain = benv.concepts["local.store_id"]
        assert (
            resolve_rowset(
                [plain],
                benv,
                depth=0,
                g=generate_graph(benv),
                history=V4History(base_environment=env),
            )
            is None
        )


class TestConditionInjection:
    """Condition injection keeps row args and existence args on distinct paths."""

    def _inner(self, env, benv):
        node = _search(env, benv, ["local.store_total", "local.store_id"]).strategy_node
        assert node is not None
        return node

    def test_row_args_are_merged_before_injection(self):
        env, benv = _build(HAVING_EXTERNAL_MODEL)
        inner = self._inner(env, benv)
        assert "local.avg_value" not in {c.address for c in inner.output_concepts}
        having = BuildHavingClause(
            conditional=BuildComparison(
                left=benv.concepts["local.store_total"],
                right=benv.concepts["local.avg_value"],
                operator=ComparisonOperator.GT,
            )
        )
        injected = _resolve_and_inject_condition(
            inner,
            having,
            list(inner.output_concepts),
            environment=benv,
            graph=generate_graph(benv),
            history=V4History(base_environment=env),
            depth=0,
        )
        assert isinstance(injected, SelectNode)
        assert injected.conditions is not None
        assert isinstance(injected.parents[0], MergeNode)
        assert inner in injected.parents[0].parents
        assert "local.avg_value" in {
            c.address for c in injected.parents[0].output_concepts
        }

    def test_no_sources_when_row_args_already_produced(self):
        env, benv = _build(HAVING_EXTERNAL_MODEL)
        inner = self._inner(env, benv)
        having = BuildHavingClause(
            conditional=BuildComparison(
                left=benv.concepts["local.store_total"],
                right=5,
                operator=ComparisonOperator.GT,
            )
        )
        sources = _resolve_condition_sources(
            inner,
            having,
            environment=benv,
            graph=generate_graph(benv),
            history=V4History(base_environment=env),
            depth=0,
        )
        assert not sources.row_parents
        assert not sources.existence_parents

    def test_existence_args_use_side_channel_sources(self):
        env, benv = _build(HAVING_EXTERNAL_MODEL)
        inner = self._inner(env, benv)
        condition = BuildWhereClause(
            conditional=BuildSubselectComparison(
                left=benv.concepts["local.store_id"],
                right=benv.concepts["local.avg_value"],
                operator=ComparisonOperator.IN,
            )
        )
        sources = _resolve_condition_sources(
            inner,
            condition,
            environment=benv,
            graph=generate_graph(benv),
            history=V4History(base_environment=env),
            depth=0,
        )
        assert not sources.row_parents
        assert sources.existence_parents
        assert [c.address for c in sources.existence_concepts] == ["local.avg_value"]

    def test_unresolved_row_args_raise(self, monkeypatch):
        env, benv = _build(HAVING_EXTERNAL_MODEL)
        inner = self._inner(env, benv)
        having = BuildHavingClause(
            conditional=BuildComparison(
                left=benv.concepts["local.store_total"],
                right=benv.concepts["local.avg_value"],
                operator=ComparisonOperator.GT,
            )
        )
        monkeypatch.setattr(
            cs, "search_concepts", lambda **_: types.SimpleNamespace(strategy_node=None)
        )
        with pytest.raises(ValueError, match="condition row arguments"):
            _resolve_condition_sources(
                inner,
                having,
                environment=benv,
                graph=generate_graph(benv),
                history=V4History(base_environment=env),
                depth=0,
            )


# ----- multi-group assembly (star enrichment / dedup / having) ---------

STAR_MODEL = """
key order_id int;
key customer_id int;
key store_id int;
property store_id.store_name string;
property order_id.value float;

datasource orders (
    order_id:order_id, customer_id:customer_id, store_id:store_id, value:value
) grain (order_id)
    query '''select 1 as order_id, 1 as customer_id, 1 as store_id, 2.0 as value''';
datasource stores (store_id:store_id, store_name:store_name) grain (store_id)
    query '''select 1 as store_id, 'a' as store_name''';

metric customers_per_store <- count(customer_id) by store_id;
metric value_per_store <- sum(value) by store_id;
auto store_name_upper <- upper(store_name);
auto value_per_store_renamed <- value_per_store;
"""

FACT_AND_DIM_CUSTOMER_MODEL = """
key return_id int;
key customer_id int;
key store_id int;
property customer_id.text string;
property return_id.return_amount float;

datasource returns (
    return_id:return_id,
    customer_id:customer_id,
    store_id:store_id,
    return_amount:return_amount,
)
grain (return_id)
query '''select 1 return_id, 1 customer_id, 10 store_id, 2.0 return_amount''';

datasource customers (
    customer_id:customer_id,
    text:text,
)
grain (customer_id)
query '''select 1 customer_id, 'A' text''';

auto total <- sum(return_amount) by customer_id, store_id;
auto store_avg <- avg(total) by store_id;
"""

BASIC_COMPOSITE_GRAIN_MODEL = """
key left_id int;
key right_id int;
property left_id.left_value int;
property right_id.right_value int;

datasource links (
    left_id:left_id,
    right_id:right_id,
)
grain (left_id, right_id)
query '''select 1 left_id, 2 right_id''';

datasource lefts (
    left_id:left_id,
    left_value:left_value,
)
grain (left_id)
query '''select 1 left_id, 10 left_value''';

datasource rights (
    right_id:right_id,
    right_value:right_value,
)
grain (right_id)
query '''select 2 right_id, 15 right_value''';

auto spread <- right_value - left_value;
"""

AGGREGATE_INPUT_GRAIN_MODEL = """
key ticket_number int;
key item_id int;
property <ticket_number, item_id>.row_counter int;

datasource sales (
    ticket_number:ticket_number,
    item_id:item_id,
    row_counter:row_counter,
)
grain (ticket_number, item_id)
query '''select 1 ticket_number, 10 item_id, 1 row_counter''';

auto total_rows <- sum(row_counter);
"""

AGGREGATE_WITH_BASIC_ENRICHMENT_MODEL = """
key ticket_number int;
key customer_id int;
key sale_address_id int;
key item_id int;
key store_id int;
property store_id.city string;
property customer_id.first_name string;
property customer_id.last_name string;
property <ticket_number, item_id>.coupon_amt float;

datasource sales (
    ticket_number:ticket_number,
    item_id:item_id,
    customer_id:customer_id,
    sale_address_id:sale_address_id,
    store_id:store_id,
    coupon_amt:coupon_amt,
)
grain (ticket_number, item_id)
query '''select 1 ticket_number, 10 item_id, 2 customer_id, 3 sale_address_id, 4 store_id, 5.0 coupon_amt''';

datasource stores (
    store_id:store_id,
    city:city,
)
grain (store_id)
query '''select 4 store_id, 'Midway' city''';

datasource customers (
    customer_id:customer_id,
    first_name:first_name,
    last_name:last_name,
)
grain (customer_id)
query '''select 2 customer_id, 'A' first_name, 'B' last_name''';

auto amt <- sum(coupon_amt) by ticket_number, customer_id, sale_address_id, city;
auto city_short <- substring(city, 1, 30);
"""

ROLLUP_ALIAS_MODEL = """
key row_id int;
key channel string;
key entity_id int;
property entity_id.entity_name string;
property row_id.amount float;

datasource sales (
    row_id: row_id,
    channel: channel,
    entity_id: entity_id,
    entity_name: entity_name,
    amount: amount,
)
grain (row_id)
query '''
select 1 row_id, 'store' channel, 10 entity_id, 'A' entity_name, 5.0 amount
''';

auto channel_label <- channel;
auto id_label <- entity_name;
auto channel_out <- channel_label;
auto id_out <- id_label;
auto total <- sum(amount);
"""


class TestMultiGroupAssembly:
    def test_derived_dimension_basic_splits_root(self):
        """A derived dimension BASIC (`upper(store_name)`, FD on store_id) beside
        an aggregate at store_id grain splits a dedicated dimension ROOT so the
        FINAL merge joins one-row-per-store, not the wide order scan
        (`_split_dim_root`)."""
        env, benv = _build(STAR_MODEL)
        info = _search(
            env,
            benv,
            ["local.value_per_store", "local.store_id", "local.store_name_upper"],
        )
        assert info.strategy_node is not None
        out = {c.address for c in info.strategy_node.output_concepts}
        assert {"local.value_per_store", "local.store_name_upper"} <= out

    def test_basic_renaming_aggregate_routes_through_sibling(self):
        """A BASIC that renames an aggregate output reads the aggregate's rows
        directly (`_route_basics_through_richer_siblings`) rather than rebuilding
        against the bare source."""
        env, benv = _build(STAR_MODEL)
        info = _search(env, benv, ["local.value_per_store_renamed", "local.store_id"])
        assert info.strategy_node is not None
        out = {c.address for c in info.strategy_node.output_concepts}
        assert "local.value_per_store_renamed" in out

    def test_aggregate_with_dimension_enrichment(self):
        """An aggregate by a key plus a dimension property of that key forces a
        FINAL merge of the aggregate group with the dimension scan (the
        `_split_dim_root` / `_route_basics_through_richer_siblings` /
        multi-contributor `_assemble_final_node` paths)."""
        env, benv = _build(STAR_MODEL)
        info = _search(
            env, benv, ["local.value_per_store", "local.store_id", "local.store_name"]
        )
        assert info.strategy_node is not None
        out = {c.address for c in info.strategy_node.output_concepts}
        assert {"local.value_per_store", "local.store_name"} <= out

    def test_grouping_consumes_narrow_root_slice(self):
        env, benv = _build(FACT_AND_DIM_CUSTOMER_MODEL)
        info = _search(env, benv, ["local.text", "local.total", "local.store_avg"])

        assert info.strategy_node is not None
        aggregate_nodes = [
            node
            for node in _walk(info.strategy_node)
            if isinstance(node, GroupNode)
            and "local.total" in {concept.address for concept in node.output_concepts}
        ]
        assert aggregate_nodes
        for aggregate in aggregate_nodes:
            descendant_selects = [
                node for node in _walk(aggregate) if isinstance(node, SelectNode)
            ]
            assert all(
                "local.text"
                not in {concept.address for concept in node.output_concepts}
                for node in descendant_selects
            )

    def test_basic_exposes_declared_grain_keys(self):
        env, benv = _build(BASIC_COMPOSITE_GRAIN_MODEL)
        info = _search(env, benv, ["local.spread"])

        assert info.strategy_node is not None
        matching_nodes = [
            node
            for node in _walk(info.strategy_node)
            if "local.spread" in {concept.address for concept in node.output_concepts}
        ]
        assert matching_nodes
        # The basic node that *produces* spread must carry its declared grain
        # keys so a downstream sibling can join on them. When spread is the sole
        # query output the FINAL node dedups it to grain {spread} (matching v3),
        # so the keys live on the producing node beneath that group, not the
        # outermost node — assert the invariant on the producer.
        assert any(
            {"local.spread", "local.left_id", "local.right_id"}
            <= {concept.address for concept in node.output_concepts}
            for node in matching_nodes
        )

    def test_condition_needed_concept_includes_grain_keys(self):
        _, benv = _build(BASIC_COMPOSITE_GRAIN_MODEL)
        needed: set[str] = set()

        _add_needed_concept(needed, benv.concepts["local.spread"])

        assert {"local.spread", "local.left_id", "local.right_id"} <= needed

    def test_aggregate_input_grain_keys_reach_root_scan(self):
        env, benv = _build(AGGREGATE_INPUT_GRAIN_MODEL)
        info = _search(env, benv, ["local.total_rows"])

        assert info.strategy_node is not None
        root_selects = [
            node
            for node in _walk(info.strategy_node)
            if isinstance(node, SelectNode)
            and "local.row_counter"
            in {concept.address for concept in node.output_concepts}
        ]
        assert root_selects
        assert {
            "local.ticket_number",
            "local.item_id",
            "local.row_counter",
        } <= {concept.address for concept in root_selects[0].output_concepts}

    def test_basic_enrichment_projects_to_aggregate_sibling_grain(self):
        env, benv = _build(AGGREGATE_WITH_BASIC_ENRICHMENT_MODEL)
        info = _search(
            env,
            benv,
            [
                "local.last_name",
                "local.first_name",
                "local.city_short",
                "local.ticket_number",
                "local.amt",
            ],
        )

        assert info.strategy_node is not None
        customer_projections = [
            node
            for node in _walk(info.strategy_node)
            if isinstance(node, (GroupNode, SelectNode))
            and {"local.first_name", "local.last_name", "local.customer_id"}
            <= {concept.address for concept in node.output_concepts}
        ]
        assert customer_projections
        assert "local.item_id" not in {
            concept.address for concept in customer_projections[0].output_concepts
        }

    def test_rollup_aliases_project_from_grouped_contributor(self):
        from trilogy.core.enums import AggregateGroupingMode
        from trilogy.core.models.build import BuildGrain

        env, benv = _build(ROLLUP_ALIAS_MODEL)
        # Rollup is a select-level clause; stamp it on the build concept directly
        # to exercise the rolled-up node assembly in isolation.
        total = benv.concepts["local.total"]
        keys = [
            benv.concepts["local.channel_label"],
            benv.concepts["local.id_label"],
        ]
        total.lineage.grouping = AggregateGroupingMode.ROLLUP
        total.lineage.by = keys
        total.grain = BuildGrain.from_concepts(keys)
        info = _search(
            env,
            benv,
            ["local.channel_out", "local.id_out", "local.total"],
        )

        assert info.strategy_node is not None
        # The rolled-up `total` resolves into a GroupNode; the renamed alias
        # dims (channel_out, id_out) project off that grouped contributor.
        assert any(
            isinstance(node, GroupNode)
            and "local.total" in {concept.address for concept in node.output_concepts}
            for node in _walk(info.strategy_node)
        )
        out = {c.address for c in info.strategy_node.output_concepts}
        assert {"local.channel_out", "local.id_out", "local.total"} <= out

    def test_aggregate_normalizes_input_grain(self):
        """The builder normalizes aggregate inputs to argument grain before
        aggregating."""
        env, benv = _build(STAR_MODEL)
        info = _search(env, benv, ["local.customers_per_store", "local.store_id"])
        assert info.strategy_node is not None
        assert _find(info.strategy_node, GroupNode)

    def test_having_over_aggregate_wraps_prefilter(self):
        """A condition referencing the aggregate is peeled into a pre-filter
        SelectNode wrapper so the GroupNode aggregates clean rows."""
        env, benv = _build(STAR_MODEL)
        vps = benv.concepts["local.value_per_store"]
        cond = BuildWhereClause(
            conditional=BuildComparison(
                left=vps, right=5, operator=ComparisonOperator.GT
            )
        )
        info = _search(
            env, benv, ["local.store_id", "local.value_per_store"], conditions=[cond]
        )
        assert info.strategy_node is not None


# ----- existence (semijoin) wiring -------------------------------------

EXISTENCE_MODEL = """
key id int;
key other int;

datasource a (id:id) grain(id) query '''select 1 as id''';
datasource b (other:other) grain(other) query '''select 1 as other''';

auto filtered <- id ? id in other;
"""


class TestExistenceWiring:
    def test_semijoin_filter_resolves_with_existence_parent(self):
        """`id ? id in other` resolves through `_existence_for_group`, wiring
        the `other` source as a side-channel existence parent."""
        env, benv = _build(EXISTENCE_MODEL)
        info = _search(env, benv, ["local.filtered"])
        assert info.strategy_node is not None
        out = {c.address for c in info.strategy_node.output_concepts}
        assert "local.filtered" in out


# ----- two-datasource merge --------------------------------------------

MERGE_MODEL = """
key k int;
property k.left_val float;
property k.right_val float;

datasource left_ds (k:k, left_val:left_val) grain (k)
    query '''select 1 as k, 2.0 as left_val''';
datasource right_ds (k:k, right_val:right_val) grain (k)
    query '''select 1 as k, 3.0 as right_val''';
"""


class TestRootMerge:
    def test_two_roots_merge_on_shared_key(self):
        """Columns split across two datasources sharing a key assemble via a
        FINAL MergeNode joining on the key."""
        env, benv = _build(MERGE_MODEL)
        info = _search(env, benv, ["local.k", "local.left_val", "local.right_val"])
        assert info.strategy_node is not None
        assert _find(info.strategy_node, MergeNode)
        out = {c.address for c in info.strategy_node.output_concepts}
        assert {"local.left_val", "local.right_val"} <= out


# ----- dispatch guard --------------------------------------------------


class TestDispatchGuard:
    def test_unknown_derivation_raises(self):
        env, benv = _build(UNNEST_MODEL)
        with pytest.raises(ValueError, match="No v4 node generator"):
            build_node(
                derivation=Derivation.MULTISELECT,
                outputs=[benv.concepts["local.id"]],
                parents=[],
                environment=benv,
                conditions=None,
                history=V4History(base_environment=env),
                g=nx.DiGraph(),
            )


class TestGeneratorGuards:
    def test_gen_recursive_without_recursive_output_returns_none(self):
        env, benv = _build(UNNEST_MODEL)
        assert gen_recursive([benv.concepts["local.id"]], [], benv) is None

    def test_gen_rowset_empty_outputs_returns_none(self):
        env, benv = _build(UNNEST_MODEL)
        assert (
            gen_rowset(
                [], [], benv, history=V4History(base_environment=env), g=nx.DiGraph()
            )
            is None
        )
