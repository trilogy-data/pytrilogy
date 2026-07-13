"""A cross-row computation (aggregate/window) referenced in both the WHERE and
the select list plays two roles with two scopes: the WHERE reference gates rows
using the population value at its own grain (ignoring its peers in the clause),
while the select output recomputes over the WHERE-filtered rows.

The inline spelling has always done this — an inline WHERE aggregate/window is
minted as its own virtual concept, distinct from the select output. The aliased
and named-concept spellings shared one address for both roles, so the
population value silently leaked into the projection (`sx` showed 12 where the
inline twin showed 2). Build-time normalization now rewrites the WHERE
reference to a minted twin, converging every spelling on inline semantics.
"""

from typing import Any

from trilogy import Dialects, Environment

AGG_SCHEMA = """key id int;
property id.x int;
property id.z int;
property id.f int;
datasource d ( id, x, z, f ) grain (id)
query '''select 1 as id, 1 as x, 2 as z, 1 as f
union all select 2, 1, 10, 0
union all select 3, 2, 100, 1''';
"""
# Group x=1 holds (z=2,f=1),(z=10,f=0). Population sum by x: {1: 12, 2: 100} —
# both pass `> 5`. Select-scope sum over f=1 rows: {1: 2, 2: 100}.
AGG_EXPECTED = [(1, 2), (2, 100)]

WINDOW_SCHEMA = """
key launch_id int;
property launch_id.vehicle_name string;
property launch_id.orb_pay float;

datasource launches (
    launch_id,
    vehicle_name,
    orb_pay
)
grain (launch_id)
query '''
select * from (values
    (1, 'A', 100.0),
    (2, 'A', 50.0),
    (3, 'B', 70.0),
    (4, 'B', 200.0),
    (5, 'C', 400.0),
    (6, 'C', 500.0)
) as t(launch_id, vehicle_name, orb_pay)
''';
"""
RANK = "rank vehicle_name order by sum(orb_pay) by vehicle_name desc"
# Population sums C=900, B=270, A=150 -> population ranks C1, B2, A3. The gate
# `>= 2` admits B and A rows; the select rank recomputes within them: B1, A2.
WINDOW_EXPECTED = [("A", 2), ("B", 1)]


def _rows(model: str, query: str) -> list[tuple[Any, ...]]:
    env = Environment()
    env.parse(model)
    executor = Dialects.DUCK_DB.default_executor(environment=env)
    return sorted(
        (tuple(r) for r in executor.execute_query(query).fetchall()),  # type: ignore[union-attr]
        key=str,
    )


def _sql(model: str, query: str) -> str:
    env = Environment()
    env.parse(model)
    executor = Dialects.DUCK_DB.default_executor(environment=env)
    return executor.generate_sql(query)[-1]


def test_aggregate_select_alias_in_where_matches_inline():
    rows = _rows(AGG_SCHEMA, "select x, sum(z) by x as sx where f = 1 and sx > 5;")
    assert rows == AGG_EXPECTED, rows


def test_aggregate_named_concept_in_where_matches_inline():
    rows = _rows(
        AGG_SCHEMA + "auto sx <- sum(z) by x;\n",
        "where f = 1 and sx > 5 select x, sx;",
    )
    assert rows == AGG_EXPECTED, rows


def test_aggregate_inline_control():
    rows = _rows(AGG_SCHEMA, "where f = 1 and sum(z) by x > 5 select x, sum(z) as v;")
    assert rows == AGG_EXPECTED, rows


def test_aggregate_where_only_reference_stays_population():
    # Not a select output: single role, population scope, no rewrite needed.
    rows = _rows(
        AGG_SCHEMA + "auto sx <- sum(z) by x;\n",
        "where f = 1 and sx > 5 select x, sum(z) as v;",
    )
    assert rows == AGG_EXPECTED, rows


def test_aggregate_having_binds_to_select_scope():
    # HAVING filters the projected (select-scope) value: x=1's filtered sum is
    # 2, so `having sx > 5` drops it even though its population sum is 12.
    rows = _rows(
        AGG_SCHEMA,
        "select x, sum(z) by x as sx where f = 1 and sx > 5 having sx > 5;",
    )
    assert rows == [(2, 100)], rows


def test_window_select_alias_in_where_recomputes_over_filtered():
    rows = _rows(WINDOW_SCHEMA, f"select vehicle_name, {RANK} as r where r >= 2;")
    assert rows == WINDOW_EXPECTED, rows


def test_window_named_concept_in_where_recomputes_over_filtered():
    rows = _rows(
        WINDOW_SCHEMA + f"auto r <- {RANK};\n",
        "where r >= 2 select vehicle_name, r;",
    )
    assert rows == WINDOW_EXPECTED, rows


def test_window_inline_control():
    rows = _rows(
        WINDOW_SCHEMA,
        f"where ({RANK}) >= 2 select vehicle_name, {RANK} as r;",
    )
    assert rows == WINDOW_EXPECTED, rows


def test_window_where_only_gate_stays_population():
    rows = _rows(
        WINDOW_SCHEMA + f"auto r <- {RANK};\n",
        "where r >= 2 select vehicle_name;",
    )
    assert rows == [("A",), ("B",)], rows


def test_aggregate_expression_wrapped_alias_matches_bare():
    # Wrapping the aggregate in any expression must not change its scope:
    # `sum(z) by x as sx` and `(sum(z) by x) + 1 as v` behave identically.
    rows = _rows(AGG_SCHEMA, "select x, (sum(z) by x) + 1 as v where f = 1 and v > 5;")
    assert rows == [(1, 3), (2, 101)], rows


def test_aggregate_expression_wrapped_inline():
    rows = _rows(
        AGG_SCHEMA,
        "where f = 1 and (sum(z) by x) + 1 > 5 select x, (sum(z) by x) + 1 as v;",
    )
    assert rows == [(1, 3), (2, 101)], rows


def test_aggregate_ref_chain_expression():
    rows = _rows(
        AGG_SCHEMA + "auto sx <- sum(z) by x;\nauto v <- sx + 1;\n",
        "where f = 1 and v > 5 select x, v;",
    )
    assert rows == [(1, 3), (2, 101)], rows


def test_window_expression_wrapped_alias():
    # Population ranks *10: C=10, B=20, A=30; gate >= 20 admits B and A rows;
    # the select value re-ranks within them: B=10, A=20.
    rows = _rows(
        WINDOW_SCHEMA, f"select vehicle_name, ({RANK}) * 10 as r where r >= 20;"
    )
    assert rows == [("A", 20), ("B", 10)], rows


def test_window_expression_wrapped_inline():
    rows = _rows(
        WINDOW_SCHEMA,
        f"where ({RANK}) * 10 >= 20 select vehicle_name, ({RANK}) * 10 as r;",
    )
    assert rows == [("A", 20), ("B", 10)], rows


def test_aggregate_where_ref_consumed_transitively_by_output():
    # sx is not itself an output, but v = sx + 1 is: the WHERE ref still needs
    # a population twin, or v inherits the population value of the shared sx.
    rows = _rows(
        AGG_SCHEMA + "auto sx <- sum(z) by x;\nauto v <- sx + 1;\n",
        "where f = 1 and sx > 5 select x, v;",
    )
    assert rows == [(1, 3), (2, 101)], rows


def test_aggregate_where_ref_chain_reaches_output():
    # The boundary crossed in the other direction: WHERE refs v (not an
    # output) whose chain reaches the output sx.
    rows = _rows(
        AGG_SCHEMA + "auto sx <- sum(z) by x;\nauto v <- sx + 1;\n",
        "where f = 1 and v > 5 select x, sx;",
    )
    assert rows == [(1, 2), (2, 100)], rows


def test_twin_does_not_collide_with_inline_select_virtual():
    # The select output embeds the identical inline expression the twin is
    # minted from; the twin's scope salt keeps their addresses distinct.
    rows = _rows(
        AGG_SCHEMA + "auto sx <- sum(z) by x;\n",
        "where f = 1 and sx > 5 select x, sx, (sum(z) by x) + 1 as v;",
    )
    assert rows == [(1, 2, 3), (2, 100, 101)], rows


def test_diamond_reference_inlines_on_every_path():
    # v reaches sx both directly and through m; both paths must inline the
    # population aggregate (population v = 3*sx = {36, 300} passes > 20;
    # select-scope v over f=1 rows = {6, 300}).
    rows = _rows(
        AGG_SCHEMA + "auto sx <- sum(z) by x;\nauto m <- sx * 2;\nauto v <- sx + m;\n",
        "where f = 1 and v > 20 select x, v, sx;",
    )
    assert rows == [(1, 6, 2), (2, 300, 100)], rows


def test_window_prefix_gate_keeps_single_instance():
    # `r <= k` admits a prefix of the window's own ordering: recomputing over
    # the admitted rows reproduces the population values (C1, B2), so no twin
    # is minted and the window is computed exactly once.
    model = WINDOW_SCHEMA
    query = f"select vehicle_name, {RANK} as r where r <= 2;"
    assert _rows(model, query) == [("B", 2), ("C", 1)]
    assert _sql(model, query).count("rank() over") == 1


def test_window_eq_one_gate_keeps_single_instance():
    model = WINDOW_SCHEMA + f"auto r <- {RANK};\n"
    query = "where r = 1 select vehicle_name, r;"
    assert _rows(model, query) == [("C", 1)]
    assert _sql(model, query).count("rank() over") == 1


def test_window_prefix_gate_with_row_level_peer_still_splits():
    # The peer predicate cuts within the ranked entities (orb_pay is
    # row-level), so admission is not a pure prefix and the twin is required:
    # gate on population ranks (C1, B2 pass), select recomputes over the
    # admitted rows (B keeps only 200.0 -> sums C=900, B=200 -> C1, B2).
    model = WINDOW_SCHEMA
    query = f"select vehicle_name, {RANK} as r where r <= 2 and orb_pay > 100;"
    assert _rows(model, query) == [("B", 2), ("C", 1)]
    assert _sql(model, query).count("rank() over") == 2


def test_window_non_prefix_gate_still_splits():
    # `>= k` selects a suffix — recomputation genuinely reshuffles, twin stays.
    model = WINDOW_SCHEMA
    query = f"select vehicle_name, {RANK} as r where r >= 2;"
    assert _rows(model, query) == WINDOW_EXPECTED
    assert _sql(model, query).count("rank() over") == 2


NAV_SCHEMA = """key sale_id int;
property sale_id.cat string;
property sale_id.wk int;
property sale_id.amt float;
datasource facts ( sale_id, cat, wk, amt ) grain (sale_id)
query '''select 1 as sale_id, 'A' as cat, 1 as wk, 10.0 as amt
union all select 2, 'A', 2, 40.0
union all select 3, 'A', 3, 80.0
union all select 4, 'B', 1, 5.0''';
auto nxt <- lead(amt, 1) over (partition by cat order by wk asc);
"""


def test_navigation_self_gate_keeps_single_instance():
    # A gate on a lead/lag output defers to after the window — recomputing
    # lead over rows admitted by its own value is never coherent (`is not
    # null` would emit rows whose displayed value is null). No twin, one lead.
    model = NAV_SCHEMA
    query = "where nxt is not null select cat, wk, nxt;"
    assert _rows(model, query) == [("A", 1, 40.0), ("A", 2, 80.0)]
    assert _sql(model, query).count("lead(") == 1


def test_navigation_gate_with_partition_peer_keeps_single_instance():
    # cat is the partition key: the peer only drops whole partitions, which
    # cannot reshuffle lead inside a surviving partition.
    model = NAV_SCHEMA
    query = "where nxt is not null and cat = 'A' select cat, wk, nxt;"
    assert _rows(model, query) == [("A", 1, 40.0), ("A", 2, 80.0)]
    assert _sql(model, query).count("lead(") == 1


def test_navigation_gate_with_row_level_peer_still_splits():
    # amt cuts within partitions, so the window's input population genuinely
    # changes: gate on population lead (admits A wk1/wk2), select recomputes
    # over the admitted rows (wk2's lead becomes null).
    model = NAV_SCHEMA
    query = "where nxt is not null and amt > 5 select cat, wk, nxt;"
    assert _sql(model, query).count("lead(") == 2
    assert _rows(model, query) == [("A", 1, 40.0), ("A", 2, None)]


def test_rowset_body_gets_dual_scope():
    # The normalization runs on every SelectLineage build, including rowset
    # bodies.
    rows = _rows(
        AGG_SCHEMA,
        "rowset gated <- select x, sum(z) by x as sx where f = 1 and sx > 5;\n"
        "select gated.x, gated.sx;",
    )
    assert rows == AGG_EXPECTED, rows


def test_single_row_global_aggregate_stays_shared():
    # `sum(z) by *` is a single-row scalar: its gating is owned by the
    # scalar-output filter path on the shared address, so no twin is minted
    # and the pinned global value (112, over all rows) is displayed.
    rows = _rows(
        AGG_SCHEMA + "auto tot <- sum(z) by *;\n",
        "where f = 1 and tot > 5 select x, tot;",
    )
    assert rows == [(1, 112), (2, 112)], rows


def test_alias_chain_in_where_follows_to_aggregate():
    # A pure-rename alias of the aggregate must behave exactly like the
    # aggregate itself: the chain is followed to the cross-row target.
    rows = _rows(
        AGG_SCHEMA + "auto sx <- sum(z) by x;\nauto sy <- sx;\n",
        "where f = 1 and sy > 5 select x, sy;",
    )
    assert rows == AGG_EXPECTED, rows


def test_expression_wrapped_bare_aggregate():
    # The nested aggregate has no explicit `by`, so group-atomicity is judged
    # against the select grain. f cuts within x groups -> twin: the gate sees
    # population sums (13, 102), the projection recomputes over f=1 rows.
    rows = _rows(
        AGG_SCHEMA + "auto v <- sum(z) + x;\n",
        "where f = 1 and v > 5 select x, v;",
    )
    assert rows == [(1, 3), (2, 102)], rows


def test_window_mirrored_literal_prefix_gate():
    # Literal-first spelling of the prefix gate: `2 >= r` mirrors to `r <= 2`.
    model = WINDOW_SCHEMA
    query = f"select vehicle_name, {RANK} as r where 2 >= r;"
    assert _rows(model, query) == [("B", 2), ("C", 1)]
    assert _sql(model, query).count("rank() over") == 1


def test_window_literal_first_eq_one_gate():
    # `1 = r` mirrors to `r = 1` (EQ is its own mirror): still a prefix gate.
    model = WINDOW_SCHEMA
    query = f"select vehicle_name, {RANK} as r where 1 = r;"
    assert _rows(model, query) == [("C", 1)]
    assert _sql(model, query).count("rank() over") == 1


def test_window_or_gate_splits():
    # An OR admits rows outside a prefix, so no prefix reasoning survives it;
    # here every population rank passes, and the recompute reproduces them.
    model = WINDOW_SCHEMA
    query = (
        f"select vehicle_name, {RANK} as r "
        "where (r <= 2 or r >= 3) and vehicle_name != 'D';"
    )
    assert _rows(model, query) == [("A", 3), ("B", 2), ("C", 1)]
    assert _sql(model, query).count("rank() over") == 2


def test_window_in_gate_splits():
    # Membership is not a prefix-selecting comparison, even when its literal
    # set happens to be one.
    model = WINDOW_SCHEMA
    query = f"select vehicle_name, {RANK} as r where r in (1, 2);"
    assert _rows(model, query) == [("B", 2), ("C", 1)]
    assert _sql(model, query).count("rank() over") == 2


def test_window_null_gate_splits():
    model = WINDOW_SCHEMA
    query = f"select vehicle_name, {RANK} as r where r is null;"
    assert _rows(model, query) == []
    assert _sql(model, query).count("rank() over") == 2


def test_window_wrapped_target_gate_splits():
    # The gate compares an expression OVER the rank, not the bare rank: not
    # provably prefix-selecting, so the twin stays.
    model = WINDOW_SCHEMA
    query = f"select vehicle_name, {RANK} as r where abs(r) <= 2;"
    assert _rows(model, query) == [("B", 2), ("C", 1)]
    assert _sql(model, query).count("rank() over") == 2


def test_navigation_or_gate_splits():
    # OR structure defeats the partition-atomicity analysis: gate on the
    # population lead, recompute over the admitted rows (B's lead goes null).
    model = NAV_SCHEMA
    query = "where nxt is not null or cat = 'B' select cat, wk, nxt;"
    assert _rows(model, query) == [("A", 1, 40.0), ("A", 2, None), ("B", 1, None)]
    assert _sql(model, query).count("lead(") == 2


def test_navigation_boolean_literal_conjunct_splits():
    # A bare boolean literal conjunct is neither a concept reference nor a
    # concept-bearing predicate; the conservative answer is to split.
    model = NAV_SCHEMA
    query = "where true and nxt is not null select cat, wk, nxt;"
    assert _rows(model, query) == [("A", 1, 40.0), ("A", 2, None)]
    assert _sql(model, query).count("lead(") == 2


def test_navigation_bare_boolean_concept_conjunct_splits():
    # A bare boolean concept conjunct keyed at row grain cuts within
    # partitions, so the twin stays.
    model = NAV_SCHEMA + "auto is_a <- cat = 'A';\n"
    query = "where is_a and nxt is not null select cat, wk, nxt;"
    assert _rows(model, query) == [("A", 1, 40.0), ("A", 2, None)]
    assert _sql(model, query).count("lead(") == 2


def test_navigation_expression_partition_splits():
    # Partitioned over an expression (materialized only at build time): the
    # partition keys are not bare references, so partition-atomicity cannot be
    # established and the twin stays.
    model = (
        NAV_SCHEMA
        + "auto nxt2 <- lead(amt, 1) over (partition by upper(cat) order by wk asc);\n"
    )
    query = "where nxt2 is not null select cat, wk, nxt2;"
    assert _rows(model, query) == [("A", 1, 40.0), ("A", 2, None)]
    assert _sql(model, query).count("lead(") == 2


def test_navigation_gate_referencing_peer_splits():
    # The target-bearing conjunct also references a row-level peer, so it is
    # not a bare deferred gate on the computed value: gate on population lead
    # (admits A wk1/wk2), recompute over the admitted rows.
    model = NAV_SCHEMA
    query = "where nxt > amt select cat, wk, nxt;"
    assert _rows(model, query) == [("A", 1, 40.0), ("A", 2, None)]
    assert _sql(model, query).count("lead(") == 2


GROUP_SCHEMA = """key id int;
property id.x int;
property id.z int;
property id.f int;
datasource d ( id, x, z, f ) grain (id)
query '''select 1 as id, 1 as x, 5 as z, 1 as f
union all select 2, 1, 5, 0
union all select 3, 2, 50, 1''';
"""


def test_group_to_always_splits():
    # Row admission reshuffles a group-to, so it always takes the twin: the
    # gate sees population g (x=1 -> 5 fails, x=2 -> 50 passes).
    rows = _rows(GROUP_SCHEMA, "select x, group(z) by x as g where f = 1 and g > 10;")
    assert rows == [(2, 50)], rows


def test_filter_item_output_in_where_stays_shared():
    # A FilterItem is row-invariant — its value doesn't depend on which rows
    # are visible — so the WHERE reference keeps the shared address.
    rows = _rows(AGG_SCHEMA + "auto fz <- z ? f = 1;\n", "where fz > 5 select x, fz;")
    assert rows == [(2, 100)], rows


def test_twin_keeps_scalar_refs_environment_resolved():
    # Only cross-row-bearing references are inlined into the twin; the scalar
    # w stays a plain reference. Population v (16, 300) gates; only x=2
    # survives and the projection recomputes there.
    rows = _rows(
        AGG_SCHEMA + "auto sx <- sum(z) by x;\nauto w <- z * 2;\nauto v <- sx + w;\n",
        "where f = 1 and v > 20 select x, v;",
    )
    assert rows == [(2, 300)], rows


def test_population_only_refs_share_sensitivity_cache():
    # Two population-only aggregate gates whose closures share only the
    # non-sensitive select key x: neither is an output, so no twin.
    rows = _rows(
        AGG_SCHEMA + "auto sx <- sum(z) by x;\nauto sf <- sum(f) by x;\n",
        "where f = 1 and sx > 5 and sf >= 0 select x;",
    )
    assert rows == [(1,), (2,)], rows


def test_duplicate_reference_inlines_once():
    # sx appears twice in v's lineage; both occurrences inline the population
    # aggregate (population v = 36/300 both pass; select-scope v = 6/300).
    rows = _rows(
        AGG_SCHEMA + "auto sx <- sum(z) by x;\nauto v <- sx * 2 + sx;\n",
        "where f = 1 and v > 30 select x, v;",
    )
    assert rows == [(1, 6), (2, 300)], rows


def test_constant_universe_stays_shared():
    # Every concept under the rank is constant/unnest-derived: row identity
    # for such universes is regenerated rather than carried, so a twin cannot
    # gate it (pre-existing hole shared with the inline spelling). No mint —
    # the shared instance keeps population ranks.
    model = (
        "auto nums <- unnest([1,2,3,4]);\n"
        "auto doubled <- nums * 2;\n"
        "auto cr <- rank doubled order by doubled desc;\n"
    )
    query = "where cr >= 2 select doubled, cr;"
    assert _rows(model, query) == [(2, 4), (4, 3), (6, 2)]
    assert _sql(model, query).count("rank() over") == 1


def test_macro_partition_key_in_where_built_twin():
    # The twin builds in the WHERE factory, whose scope salt inspects every
    # virtual it mints — including a def-macro call arriving as a partition
    # key. The macro wraps a scalar, so it stays unsalted and shared.
    model = (
        NAV_SCHEMA
        + "def par(c) -> upper(c);\n"
        + "auto nxt3 <- lead(amt, 1) over (partition by @par(cat) order by wk asc);\n"
    )
    query = "where nxt3 is not null and amt > 5 select cat, wk, nxt3;"
    assert _rows(model, query) == [("A", 1, 40.0), ("A", 2, None)]
    assert _sql(model, query).count("lead(") == 2


def test_macro_wrapped_inline_where_aggregate():
    # An inline WHERE aggregate arriving through a def-macro call still counts
    # as a cross-row instantiation for the WHERE factory's scope salt.
    rows = _rows(
        AGG_SCHEMA + "def bigsum(val) -> sum(val) by x;\n",
        "select x, sum(z) by x as sx where f = 1 and @bigsum(z) > 5;",
    )
    assert rows == AGG_EXPECTED, rows
