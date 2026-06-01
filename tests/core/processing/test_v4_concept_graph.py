"""Tests for the v4 concept-graph helpers and the top-level multiselect
resolver.

The pure-graph helpers (`_filter_existence_only`, `_count_dedup_grain`) are
exercised on real build concepts pulled from a tiny inline environment — they
isinstance-check Build* lineage, so synthetic fakes don't reach the branches.
The end-to-end cases drive `search_concepts` (v4) so the build_concept_graph
existence loop, the count-dedup discriminator, and `_resolve_multiselect` all
run on a genuine plan.
"""

from trilogy import Environment
from trilogy.core.enums import ComparisonOperator, Derivation
from trilogy.core.env_processor import generate_graph
from trilogy.core.models.build import BuildComparison, BuildWhereClause
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.concept_strategies_v4 import V4History, search_concepts
from trilogy.core.processing.nodes import MergeNode
from trilogy.core.processing.v4_helper.concept_graph import (
    _count_dedup_grain,
    _filter_existence_only,
    build_concept_graph,
)


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


COUNT_MODEL = """
key order_id int;
key customer_id int;
key store_id int;

property order_id.value float;

datasource orders (
    order_id:order_id,
    customer_id:customer_id,
    store_id:store_id,
    value:value
)
grain (order_id)
query '''select 1 as order_id, 1 as customer_id, 1 as store_id, 2.0 as value''';

metric customers_per_store <- count(customer_id) by store_id;
metric customers_by_self <- count(customer_id) by customer_id, store_id;
metric value_per_store <- sum(value) by store_id;
"""

ROWSET_MODEL = """
key order_id int;
key store_id int;
property order_id.value float;

datasource orders (
    order_id:order_id,
    store_id:store_id,
    value:value
)
grain (order_id)
query '''select 1 as order_id, 1 as store_id, 2.0 as value''';

auto store_total <- sum(value) by store_id;

with high_value as
select store_id
where store_total > 5;
"""

EXISTENCE_MODEL = """
key id int;
key other int;

datasource a (id:id) grain(id) query '''select 1 as id''';
datasource b (other:other) grain(other) query '''select 1 as other''';

auto filtered <- id ? id in other;
"""


# ----- _count_dedup_grain ---------------------------------------------


class TestCountDedupGrain:
    def test_count_over_key_at_coarser_grain_needs_dedup(self):
        """count(customer_id) by store_id counts DISTINCT customers — the
        input has to dedup to {customer_id, store_id} first, not count the
        finer order rows."""
        _, benv = _build(COUNT_MODEL)
        m = benv.concepts["local.customers_per_store"]
        out_grain = frozenset(m.grain.components)
        assert _count_dedup_grain(m, out_grain) == frozenset(
            {"local.customer_id", "local.store_id"}
        )

    def test_count_with_key_grain_already_pinned_needs_no_dedup(self):
        """count(customer_id) by customer_id, store_id: the output grain
        already pins customer_id's grain, so there's nothing to dedup —
        returns empty."""
        _, benv = _build(COUNT_MODEL)
        m = benv.concepts["local.customers_by_self"]
        out_grain = frozenset(m.grain.components)
        assert _count_dedup_grain(m, out_grain) == frozenset()

    def test_non_aggregate_returns_empty(self):
        _, benv = _build(COUNT_MODEL)
        assert _count_dedup_grain(benv.concepts["local.store_id"], frozenset()) == (
            frozenset()
        )

    def test_non_count_aggregate_returns_empty(self):
        """A SUM is additive — it doesn't dedup its input, so no discriminator."""
        _, benv = _build(COUNT_MODEL)
        m = benv.concepts["local.value_per_store"]
        out_grain = frozenset(m.grain.components)
        assert _count_dedup_grain(m, out_grain) == frozenset()

    def test_count_metric_resolves_end_to_end(self):
        env, benv = _build(COUNT_MODEL)
        info = _search(env, benv, ["local.customers_per_store", "local.store_id"])
        assert info.strategy_node is not None
        _, cattrs = build_concept_graph(
            [
                benv.concepts["local.customers_per_store"],
                benv.concepts["local.store_id"],
            ],
            benv,
            [],
        )
        addr = "local.customers_per_store"
        assert cattrs[addr].agg_dedup_grain == frozenset(
            {"local.customer_id", "local.store_id"}
        )


# ----- _filter_existence_only -----------------------------------------


class TestFilterExistenceOnly:
    def test_semijoin_rhs_is_existence_only(self):
        """`id ? id in other` — `other` appears only as the existence RHS, so
        it's existence-only; `id` (a row arg) is not."""
        _, benv = _build(EXISTENCE_MODEL)
        f = benv.concepts["local.filtered"]
        assert f.derivation == Derivation.FILTER
        assert _filter_existence_only(f) == {"local.other"}

    def test_non_filter_returns_empty(self):
        _, benv = _build(EXISTENCE_MODEL)
        assert _filter_existence_only(benv.concepts["local.id"]) == set()

    def test_existence_edge_wired_into_concept_graph(self):
        _, benv = _build(EXISTENCE_MODEL)
        graph, _ = build_concept_graph([benv.concepts["local.filtered"]], benv, [])
        existence_edges = [
            (u, v) for u, v, d in graph.edges(data=True) if d.get("kind") == "existence"
        ]
        assert any(v == "local.filtered" for _, v in existence_edges)

    def test_semijoin_filter_resolves_end_to_end(self):
        env, benv = _build(EXISTENCE_MODEL)
        info = _search(env, benv, ["local.filtered"])
        assert info.strategy_node is not None


# ----- rowset identity tagging ----------------------------------------


class TestRowsetTagging:
    def test_plain_select_rowset_concept_tagged_with_name(self):
        """A PLAIN-select rowset's outputs carry `rowset_name` so the rowset
        grouping rule can bucket them together (q59)."""
        _, benv = _build(ROWSET_MODEL)
        rowset_concept = benv.concepts["high_value.store_id"]
        _, cattrs = build_concept_graph([rowset_concept], benv, [])
        nid = next(n for n, a in cattrs.items() if a.address == "high_value.store_id")
        assert cattrs[nid].rowset_name == "high_value"

    def test_non_rowset_concept_has_no_rowset_name(self):
        _, benv = _build(ROWSET_MODEL)
        _, cattrs = build_concept_graph([benv.concepts["local.store_id"]], benv, [])
        assert cattrs["local.store_id"].rowset_name is None


# ----- _resolve_multiselect -------------------------------------------

MULTISELECT_MODEL = """
key one int;
key other_one int;

datasource num1 (one:one) grain (one) query '''select 1 as one''';
datasource num_other (other_one:other_one) grain (other_one)
    query '''select 1 as other_one''';

SELECT
    one
MERGE
SELECT
    other_one
ALIGN
    one_key:one,other_one
;
"""


class TestResolveMultiselect:
    def test_merge_resolves_to_merge_node(self):
        env, benv = _build(MULTISELECT_MODEL)
        info = _search(env, benv, ["local.one_key"])
        assert isinstance(info.strategy_node, MergeNode)
        assert "local.one_key" in {
            c.address for c in info.strategy_node.output_concepts
        }

    def test_merge_caches_second_call(self):
        """Second search for the same list returns the cached BuildInfo."""
        env, benv = _build(MULTISELECT_MODEL)
        history = V4History(base_environment=env)
        g = generate_graph(benv)
        first = search_concepts(
            mandatory_list=[benv.concepts["local.one_key"]],
            history=history,
            environment=benv,
            depth=0,
            g=g,
        )
        second = search_concepts(
            mandatory_list=[benv.concepts["local.one_key"]],
            history=history,
            environment=benv,
            depth=0,
            g=g,
        )
        assert first.strategy_node is not None
        assert second.strategy_node is not None

    def test_unresolvable_arm_returns_empty(self):
        """If an arm references a concept with no datasource, the arm can't
        resolve and the whole multiselect returns an empty BuildInfo."""
        env, benv = _build("""
key one int;
key unsourced int;

datasource num1 (one:one) grain (one) query '''select 1 as one''';

SELECT
    one
MERGE
SELECT
    unsourced
ALIGN
    one_key:one,unsourced
;
""")
        info = _search(env, benv, ["local.one_key"])
        assert info.strategy_node is None

    def test_merge_with_outer_conditions(self):
        """An outer WHERE referencing the merge key is applied above the
        join via a wrapping SelectNode."""
        env, benv = _build(MULTISELECT_MODEL)
        key = benv.concepts["local.one_key"]
        cond_a = BuildWhereClause(
            conditional=BuildComparison(
                left=key, right=1, operator=ComparisonOperator.GT
            )
        )
        cond_b = BuildWhereClause(
            conditional=BuildComparison(
                left=key, right=100, operator=ComparisonOperator.LT
            )
        )
        info = _search(env, benv, ["local.one_key"], conditions=[cond_a, cond_b])
        assert info.strategy_node is not None
        assert "local.one_key" in {
            c.address for c in info.strategy_node.output_concepts
        }
