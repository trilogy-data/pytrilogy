"""Plan-time gate for the join fan-out contract.

A joined side that is not unique per its join keys multiplies the other side's
rows. The SQL stays valid and the failure is invisible unless you count rows, so
these models are checked at plan time as well as by execution.

The shapes here are the ones that broke: a union whose projection drops its
arms' grain keys, consumed as a dim. See ``tests/helpers/grain_contract.py`` for
why ``CTE.grain`` cannot be used directly. Curated models only — the checker is
deliberately conservative and flags legitimate joins whose key equivalence it
cannot see through.
"""

import pytest

from tests.helpers.grain_contract import cte_true_grain, find_fanout_joins
from trilogy import Dialects

# `species_upper` is a property of `species`; the facts are at `tree_id` grain
# and split across two partitions, so it can only be projected out of a union.
COARSE_PROPERTY_OVER_UNION = """
key tree_id string;
key species string;
key city enum<string>['A', 'B'];
property tree_id.dbh float;
property species.species_upper <- upper(species);

partial datasource a_trees (tree_id, city, species, dbh)
grain (tree_id)
complete where city = 'A'
query '''
select 'a1' as tree_id, 'A' as city, 'Quercus' as species, 10.0 as dbh
union all select 'a2', 'A', 'Quercus', 12.0
union all select 'a3', 'A', 'Tilia', 8.0
''';

partial datasource b_trees (tree_id, city, species, dbh)
grain (tree_id)
complete where city = 'B'
query '''
select 'b1' as tree_id, 'B' as city, 'Quercus' as species, 20.0 as dbh
union all select 'b2', 'B', 'Acer', 5.0
''';
"""

# `habitat` is a species-level property living only on two obs partitions that
# are themselves at obs_id grain, so the union serving it is a pure dim and
# cannot be widened by a fact path.
UNION_AS_PURE_DIM = """
key tree_id string;
key obs_id string;
key species string;
key region enum<string>['N', 'S'];
property tree_id.dbh float;
property species.habitat string;

partial datasource obs_n (obs_id, region, species, habitat)
grain (obs_id)
complete where region = 'N'
query '''
select 'n1' as obs_id, 'N' as region, 'Quercus' as species, 'upland' as habitat
union all select 'n2', 'N', 'Quercus', 'upland'
union all select 'n3', 'N', 'Tilia', 'riparian'
''';

partial datasource obs_s (obs_id, region, species, habitat)
grain (obs_id)
complete where region = 'S'
query '''
select 's1' as obs_id, 'S' as region, 'Quercus' as species, 'upland' as habitat
union all select 's2', 'S', 'Acer', 'mixed'
''';

datasource trees (tree_id, species, dbh)
grain (tree_id)
query '''
select 't1' as tree_id, 'Quercus' as species, 10.0 as dbh
union all select 't2', 'Tilia', 8.0
union all select 't3', 'Acer', 5.0
''';
"""

CASES = [
    pytest.param(
        COARSE_PROPERTY_OVER_UNION,
        "select tree_id, city, species, species_upper, dbh where species_upper != 'X';",
        5,
        id="coarse_property_projected_out_of_union",
    ),
    pytest.param(
        UNION_AS_PURE_DIM,
        "select tree_id, dbh, species, habitat;",
        3,
        id="union_as_pure_dim_joined_to_fact",
    ),
    pytest.param(
        UNION_AS_PURE_DIM,
        "select habitat, species, sum(dbh) -> total, count(tree_id) -> n;",
        3,
        id="union_as_pure_dim_under_aggregate",
    ),
]


@pytest.mark.parametrize("model,query,expected_rows", CASES)
def test_no_join_fans_out(model: str, query: str, expected_rows: int):
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(model)
    environment = executor.environment.materialize_for_select()

    processed = executor.parse_text(query)[-1]
    violations = find_fanout_joins(processed.ctes, environment)
    assert not violations, "\n".join(str(v) for v in violations)

    # The contract is about row counts, so also prove it against real rows —
    # a plan-shape assertion alone has never been sufficient here.
    rows = executor.execute_raw_sql(
        executor.generator.compile_statement(processed)
    ).fetchall()
    assert len(rows) == expected_rows


def test_union_all_true_grain_is_its_arms_not_its_projection():
    """The property that makes the contract non-trivial: a UNION ALL projecting
    fewer columns than its arms' grain still emits one row per arm row, so its
    declared grain is a claim rather than a fact."""
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(COARSE_PROPERTY_OVER_UNION)
    environment = executor.environment.materialize_for_select()

    processed = executor.parse_text("select species;")[-1]
    unions = [c for c in processed.ctes if type(c).__name__ == "UnionCTE"]
    assert unions, "expected the partitions to be unioned"
    union = unions[0]

    assert {c.address for c in union.output_columns} == {"local.species"}
    # ...yet it emits one row per tree, which is exactly why its consumer groups.
    assert cte_true_grain(union, environment) == {"local.tree_id"}
    consumer = next(
        c for c in processed.ctes if union.name in {p.name for p in c.parent_ctes}
    )
    assert consumer.group_to_grain


def test_checker_catches_the_fanout_when_the_gate_is_disabled(monkeypatch):
    """Positive control: a gate that cannot fail proves nothing.

    With `_grain_claim_needs_group` stubbed off, the coarse projection stops
    grouping, and the checker must report the join that then multiplies rows —
    the same defect that shipped 11 rows where 5 were correct."""
    from trilogy.core.processing.v4_node_generators import basic

    monkeypatch.setattr(
        basic, "_grain_claim_needs_group", lambda outputs, parent, environment: False
    )

    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(COARSE_PROPERTY_OVER_UNION)
    environment = executor.environment.materialize_for_select()
    processed = executor.parse_text(
        "select tree_id, city, species, species_upper, dbh where species_upper != 'X';"
    )[-1]

    violations = find_fanout_joins(processed.ctes, environment)
    assert violations, "checker failed to detect a known fan-out"
    assert any(v.join_keys == ("local.species",) for v in violations), violations

    rows = executor.execute_raw_sql(
        executor.generator.compile_statement(processed)
    ).fetchall()
    assert len(rows) == 11, "expected the un-gated plan to fan out to 11 rows"
