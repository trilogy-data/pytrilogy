"""Stage-matching rules shared by the two `then where` delivery sites.

These are unit-level on purpose: `hosting_stage_index` only misbehaves visibly
when ROOT happens to re-source a gate standalone, so an execution test cannot
be relied on to keep it honest.
"""

from trilogy import Environment, parse
from trilogy.core.models.build import BuildSelectLineage, Factory
from trilogy.core.processing.v4_helper.staged_where import (
    hosting_stage_index,
    stage_computes_cross_row,
    stage_lineage_addresses,
)

MODEL = """key id int;
property id.x int;
property id.z int;
property id.f int;
datasource d ( id, x, z, f ) grain (id)
query '''select 1 as id, 1 as x, 2 as z, 1 as f''';
"""


def _stages(select: str) -> BuildSelectLineage:
    env = Environment()
    penv, stmts = parse(MODEL + "\n" + select, env)
    return Factory(environment=penv).build(stmts[-1].as_lineage(penv))


def _cross_row_arg(built: BuildSelectLineage, stage: int):
    args = built.where_clauses[stage].conditional.concept_arguments
    return next(c for c in args if c.derivation.name == "AGGREGATE")


def test_flat_where_is_not_staged() -> None:
    assert _stages("where f = 1 select x;").where_clauses == []


def test_stage_computes_cross_row_splits_scalar_from_aggregate() -> None:
    built = _stages("where f = 1 then where sum(z) by x > 5 select x;")
    assert stage_computes_cross_row(built.where_clauses[0]) is False
    assert stage_computes_cross_row(built.where_clauses[1]) is True


def test_stage_lineage_addresses_sees_through_a_wrapper() -> None:
    built = _stages("where f = 1 then where 1.5 * sum(z) by x > 5 select x;")
    addresses = stage_lineage_addresses(built.where_clauses[1])
    assert any("_virt_agg" in address for address in addresses)


def test_hosting_stage_index_finds_the_computing_stage() -> None:
    built = _stages("where f = 1 then where sum(z) by x > 5 select x;")
    agg = _cross_row_arg(built, 1)
    assert hosting_stage_index(built.where_clauses, [agg]) == 1


def test_hosting_stage_ignores_a_later_stage_reading_an_input_column() -> None:
    # `z > 1` reads a column FEEDING the aggregate; treating it as the hosting
    # stage would inherit the aggregate's own gate into its re-source
    built = _stages("where f = 1 then where sum(z) by x > 5 then where z > 1 select x;")
    agg = _cross_row_arg(built, 1)
    assert hosting_stage_index(built.where_clauses, [agg]) == 1


def test_hosting_stage_ignores_an_earlier_stage_reading_an_input_column() -> None:
    # symmetric trap: stage 1 reads `z` too, but does not compute the aggregate
    built = _stages("where z > 0 then where sum(z) by x > 5 select x;")
    agg = _cross_row_arg(built, 1)
    assert hosting_stage_index(built.where_clauses, [agg]) == 1


def test_hosting_stage_index_is_none_for_a_plain_row_arg() -> None:
    built = _stages("where f = 1 then where sum(z) by x > 5 select x;")
    row_args = list(built.where_clauses[0].conditional.concept_arguments)
    assert hosting_stage_index(built.where_clauses, row_args) is None
