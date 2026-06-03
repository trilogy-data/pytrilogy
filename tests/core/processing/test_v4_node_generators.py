"""End-to-end coverage for the v4 per-derivation generators and the rowset
boundary builder.

Each test drives the real `search_concepts` planner against a tiny inline
environment so the dispatch table, the group-graph parent wiring, and the
generator bodies all run on a genuine plan — synthetic StrategyNodes wouldn't
exercise the lineage/grain branches the generators isinstance-check.
"""

import networkx as nx
import pytest

from trilogy import Environment
from trilogy.core.enums import ComparisonOperator, Derivation
from trilogy.core.env_processor import generate_graph
from trilogy.core.models.build import (
    BuildComparison,
    BuildHavingClause,
    BuildWhereClause,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.concept_strategies_v4 import (
    V4History,
    _merge_external_having_args,
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
from trilogy.core.processing.v4_node_generators.dispatch import build_node


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


def _find(node: StrategyNode, kind: type) -> bool:
    """True if any node in the parent tree is an instance of `kind`."""
    if isinstance(node, kind):
        return True
    return any(_find(p, kind) for p in node.parents)


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


class TestUnion:
    def test_union_resolves_to_union_node(self):
        env, benv = _build(UNION_MODEL)
        info = _search(env, benv, ["local.u"])
        assert info.strategy_node is not None
        assert _find(info.strategy_node, UnionNode)


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

    def test_rowset_order_by_protects_addresses(self):
        """An inner ORDER BY contributes its concepts to the protected set so
        `strip_tautological_not_null` doesn't drop a needed NOT NULL."""
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


class TestMergeExternalHavingArgs:
    """`_merge_external_having_args` cross-joins in any inner-HAVING argument the
    producer doesn't expose — a comparison against a SEPARATE scalar rowset
    (q14 `bucket_sum_l0 > avg_sales.average_sales`)."""

    def _inner(self, env, benv):
        node = _search(env, benv, ["local.store_total", "local.store_id"]).strategy_node
        assert node is not None
        return node

    def test_cross_joins_unproduced_scalar(self):
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
        merged = _merge_external_having_args(
            inner,
            having,
            benv,
            generate_graph(benv),
            V4History(base_environment=env),
            depth=0,
        )
        assert isinstance(merged, MergeNode)
        assert inner in merged.parents
        assert "local.avg_value" in {c.address for c in merged.output_concepts}

    def test_noop_when_every_having_arg_produced(self):
        env, benv = _build(HAVING_EXTERNAL_MODEL)
        inner = self._inner(env, benv)
        having = BuildHavingClause(
            conditional=BuildComparison(
                left=benv.concepts["local.store_total"],
                right=5,
                operator=ComparisonOperator.GT,
            )
        )
        result = _merge_external_having_args(
            inner,
            having,
            benv,
            generate_graph(benv),
            V4History(base_environment=env),
            depth=0,
        )
        assert result is inner

    def test_noop_when_external_args_do_not_resolve(self, monkeypatch):
        """If the external-arg sub-plan resolves to no node, fall back to the
        unmerged producer rather than dereferencing a missing strategy node."""
        import types

        import trilogy.core.processing.concept_strategies_v4 as cs

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
        result = _merge_external_having_args(
            inner,
            having,
            benv,
            generate_graph(benv),
            V4History(base_environment=env),
            depth=0,
        )
        assert result is inner


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

    def test_count_distinct_inserts_dedup_grain(self):
        """`count(customer_id) by store_id` counts DISTINCT customers, so the
        builder inserts a dedup GroupNode at {customer_id, store_id} below the
        count."""
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
                derivation="not_a_real_derivation",
                outputs=[benv.concepts["local.id"]],
                parents=[],
                environment=benv,
                conditions=None,
                history=V4History(base_environment=env),
                g=nx.DiGraph(),
            )


class TestGeneratorGuards:
    def test_gen_recursive_without_recursive_output_returns_none(self):
        from trilogy.core.processing.v4_node_generators.recursive import gen_recursive

        env, benv = _build(UNNEST_MODEL)
        assert gen_recursive([benv.concepts["local.id"]], [], benv) is None

    def test_gen_rowset_empty_outputs_returns_none(self):
        from trilogy.core.processing.v4_node_generators.rowset import gen_rowset

        env, benv = _build(UNNEST_MODEL)
        assert (
            gen_rowset(
                [], [], benv, history=V4History(base_environment=env), g=nx.DiGraph()
            )
            is None
        )
