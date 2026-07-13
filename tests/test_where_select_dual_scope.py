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
