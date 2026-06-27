"""Tests for the v4 concept-graph helpers and the top-level multiselect
resolver.

The pure-graph helpers (`_filter_existence_only`, `_aggregate_input_grain`) are
exercised on real build concepts pulled from a tiny inline environment — they
isinstance-check Build* lineage, so synthetic fakes don't reach the branches.
The end-to-end cases drive `search_concepts` (v4) so the build_concept_graph
existence loop, the count-dedup discriminator, and `_resolve_multiselect` all
run on a genuine plan.
"""

import pytest

from trilogy import Environment
from trilogy.core.enums import ComparisonOperator, Derivation
from trilogy.core.env_processor import generate_graph
from trilogy.core.exceptions import NoDatasourceException
from trilogy.core.models.build import BuildComparison, BuildWhereClause
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing import concept_strategies_v4 as v4
from trilogy.core.processing.concept_strategies_v4 import V4History, search_concepts
from trilogy.core.processing.nodes import MergeNode, SelectNode
from trilogy.core.processing.v4_helper.concept_graph import (
    _aggregate_input_grain,
    _filter_existence_only,
    _upstream_window,
    build_concept_graph,
)
from trilogy.core.processing.v4_helper.constants import EdgeKind
from trilogy.core.processing.v4_helper.edges import edges_of_kind


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

WINDOW_OVER_AGG_MODEL = """
key store_id int;
key week_seq int;
key order_id int;
property order_id.sales float;

datasource sales (
    order_id:order_id,
    store_id:store_id,
    week_seq:week_seq,
    sales:sales
)
grain (order_id)
query '''select 1 as order_id, 1 as store_id, 1 as week_seq, 2.0 as sales''';

auto weekly_sales <- sum(sales) by store_id, week_seq;
# A BASIC sitting between the window and the aggregate (q36/q59 shape).
auto weekly_ratio <- weekly_sales::float / 2.0;
auto ranked <- rank(store_id, week_seq) over (partition by store_id order by weekly_ratio asc);
"""

EXISTENCE_MODEL = """
key id int;
key other int;

datasource a (id:id) grain(id) query '''select 1 as id''';
datasource b (other:other) grain(other) query '''select 1 as other''';

auto filtered <- id ? id in other;
"""

# Two aggregates at the SAME output grain (date_id) whose arguments are inline
# CASE expressions over DIFFERENT facts (web vs catalog). Their input grains
# must differ by the fact row key so partitioning keeps them in separate
# streams; collapsing both to the output grain co-sources them into one raw
# fact-to-fact join before aggregating (the q2.1/q2.2 fan-out).
INLINE_CASE_MULTIFACT_MODEL = """
key date_id int;
property date_id.dow int;
key web_line int;
property web_line.web_price float;
key cat_line int;
property cat_line.cat_price float;

datasource dates (date_id:date_id, dow:dow) grain (date_id)
query '''select 1 as date_id, 1 as dow''';
datasource web (web_line:web_line, web_price:web_price, date_id:date_id) grain (web_line)
query '''select 1 as web_line, 2.0 as web_price, 1 as date_id''';
datasource cat (cat_line:cat_line, cat_price:cat_price, date_id:date_id) grain (cat_line)
query '''select 1 as cat_line, 3.0 as cat_price, 1 as date_id''';

metric web_dow_sales <- sum(case when dow = 1 then web_price else 0.0 end) by date_id;
metric cat_dow_sales <- sum(case when dow = 1 then cat_price else 0.0 end) by date_id;
"""


# ----- _aggregate_input_grain -----------------------------------------


class TestAggregateInputGrain:
    def test_count_over_key_at_coarser_grain_uses_argument_grain(self):
        _, benv = _build(COUNT_MODEL)
        m = benv.concepts["local.customers_per_store"]
        out_grain = frozenset(m.grain.components)
        assert _aggregate_input_grain(m, benv, out_grain) == frozenset(
            {"local.customer_id", "local.store_id"}
        )

    def test_count_with_key_grain_already_pinned_matches_output_grain(self):
        _, benv = _build(COUNT_MODEL)
        m = benv.concepts["local.customers_by_self"]
        out_grain = frozenset(m.grain.components)
        assert _aggregate_input_grain(m, benv, out_grain) == out_grain

    def test_non_aggregate_returns_empty(self):
        _, benv = _build(COUNT_MODEL)
        assert _aggregate_input_grain(
            benv.concepts["local.store_id"], benv, frozenset()
        ) == (frozenset())

    def test_sum_uses_argument_grain(self):
        # The input grain is FD-minimized: `order_id` determines `store_id`, so the
        # minimal row identity is `{order_id}` alone (not `{order_id, store_id}`).
        _, benv = _build(COUNT_MODEL)
        m = benv.concepts["local.value_per_store"]
        out_grain = frozenset(m.grain.components)
        assert _aggregate_input_grain(m, benv, out_grain) == frozenset(
            {"local.order_id"}
        )

    def test_inline_case_argument_contributes_fact_grain(self):
        """An aggregate over an inline expression (`sum(case .. web_price ..)`)
        must derive its input grain from the concepts referenced *inside* the
        expression, not just the output grain. The arg is a Function, not a
        bare BuildConcept, so walking only top-level concept args would miss it
        and collapse the input grain to the output grain (the q2 fan-out).

        The result is FD-minimized: `web_line` (the fact key) determines `date_id`,
        so the minimal grain is `{web_line}` — still the fact grain (not collapsed
        to the output grain `{date_id}`), which is what prevents the fan-out."""
        _, benv = _build(INLINE_CASE_MULTIFACT_MODEL)
        m = benv.concepts["local.web_dow_sales"]
        out_grain = frozenset(m.grain.components)
        assert _aggregate_input_grain(m, benv, out_grain) == frozenset(
            {"local.web_line"}
        )

    def test_inline_case_aggregates_over_disjoint_facts_split_input_grain(self):
        """Two same-output-grain aggregates whose inline arguments read
        different facts must produce DIFFERENT input grains — the signal that
        keeps them in separate streams instead of one raw fact-to-fact join.

        After FD-minimization each grain is its own fact key (`{web_line}` /
        `{cat_line}`) rather than `out_grain ∪ {line}`; the anti-collapse signal is
        therefore "differs from out_grain", and the split signal is the distinct
        per-fact line keys."""
        _, benv = _build(INLINE_CASE_MULTIFACT_MODEL)
        web = benv.concepts["local.web_dow_sales"]
        cat = benv.concepts["local.cat_dow_sales"]
        out_grain = frozenset(web.grain.components)
        web_input = _aggregate_input_grain(web, benv, out_grain)
        cat_input = _aggregate_input_grain(cat, benv, out_grain)
        assert web_input != cat_input
        assert web_input != out_grain and cat_input != out_grain
        assert "local.web_line" in web_input and "local.web_line" not in cat_input
        assert "local.cat_line" in cat_input and "local.cat_line" not in web_input

    def test_count_metric_resolves_end_to_end(self):
        env, benv = _build(COUNT_MODEL)
        info = _search(env, benv, ["local.customers_per_store", "local.store_id"])
        assert info.strategy_node is not None
        _, cattrs, _ = build_concept_graph(
            [
                benv.concepts["local.customers_per_store"],
                benv.concepts["local.store_id"],
            ],
            benv,
            [],
        )
        addr = "local.customers_per_store"
        assert cattrs[addr].aggregate_input_grain == frozenset(
            {"local.customer_id", "local.store_id"}
        )


# ----- _upstream_window ------------------------------------------------


class TestUpstreamWindow:
    def test_carries_aggregate_grain_keys_through_basic(self):
        """A window whose argument is a BASIC over an aggregate (q36/q59)
        must pull the aggregate's grain keys through as window parents — a
        1-level lineage walk wouldn't reach them since the aggregate is a hop
        below the BASIC."""
        _, benv = _build(WINDOW_OVER_AGG_MODEL)
        ranked = benv.concepts["local.ranked"]
        assert ranked.derivation == Derivation.WINDOW
        parents = {p.address for p in _upstream_window(ranked, benv)}
        assert "local.store_id" in parents
        assert "local.week_seq" in parents

    def test_stops_at_aggregate_boundary(self):
        """The walk stops at the aggregate — it does not pull the aggregate's
        own source-row inputs (e.g. order_id) as window parents."""
        _, benv = _build(WINDOW_OVER_AGG_MODEL)
        ranked = benv.concepts["local.ranked"]
        parents = {p.address for p in _upstream_window(ranked, benv)}
        assert "local.order_id" not in parents


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
        _, _, edges = build_concept_graph([benv.concepts["local.filtered"]], benv, [])
        existence_edges = edges_of_kind(edges, EdgeKind.EXISTENCE)
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
        _, cattrs, _ = build_concept_graph([rowset_concept], benv, [])
        nid = next(n for n, a in cattrs.items() if a.address == "high_value.store_id")
        assert cattrs[nid].rowset_name == "high_value"

    def test_non_rowset_concept_has_no_rowset_name(self):
        _, benv = _build(ROWSET_MODEL)
        _, cattrs, _ = build_concept_graph([benv.concepts["local.store_id"]], benv, [])
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
    def test_merge_arms_use_arm_materialized_environments(self, monkeypatch):
        """Each multiselect arm is planned in its own materialized environment."""
        env, benv = _build(MULTISELECT_MODEL)
        seen_envs: list[BuildEnvironment] = []

        def fake_search_concepts(
            mandatory_list,
            history,
            environment,
            depth,
            g,
            accept_partial=False,
            conditions=None,
        ):
            seen_envs.append(environment)
            return v4.BuildInfo(
                strategy_node=SelectNode(
                    input_concepts=[],
                    output_concepts=list(mandatory_list),
                    environment=environment,
                )
            )

        monkeypatch.setattr(v4, "search_concepts", fake_search_concepts)
        monkeypatch.setattr(v4, "extra_align_joins", lambda *_args: [])
        monkeypatch.setattr(v4.StrategyNode, "rebuild_cache", lambda _self: None)

        info = v4._resolve_multiselect(
            benv.concepts["local.one_key"],
            [benv.concepts["local.one_key"]],
            benv,
            0,
            generate_graph(benv),
            V4History(base_environment=env),
            [],
        )

        assert info.strategy_node is not None
        assert len(seen_envs) == 2
        assert all(arm_env is not benv for arm_env in seen_envs)
        assert len({id(arm_env) for arm_env in seen_envs}) == 2

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

    def test_unresolvable_arm_raises(self):
        """An arm referencing a concept with no datasource is a genuine
        planning failure — it raises loudly rather than silently degrading
        (the v4 dispatch no longer falls back to v3 for implemented
        derivations)."""
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
        with pytest.raises(NoDatasourceException):
            _search(env, benv, ["local.one_key"])

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
