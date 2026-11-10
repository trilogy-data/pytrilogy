"""A statement re-expresses a coarser aggregate (`count(x) by *`) as a
re-aggregate of a finer demanded partial (`count(x) by g`) in its own
environment only; copies embedded in other lineages keep the authored form.
Planning passes that walk the demand's lineage must agree on which copy they
follow, or one reaches the partial through the rewrite and the other never
records it (#926)."""

import pytest

from trilogy import Dialects, Environment
from trilogy.core.exceptions import UnresolvableQueryException
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.aggregate_rollup import reaggregate_partials
from trilogy.core.processing.concept_strategies_v4 import _nested_row_grains
from trilogy.core.processing.utility import walk_lineage
from trilogy.execution.state.isolation import hidden_datasources

MODEL = """
key id int;
key g string;
property id.x float;
property g.region string;
auto part <- {agg}(x) by g;
auto whole <- {agg}(x) by {coarse};
auto level <- case when part > 8 then 'g' else 'all' end;
auto scaled <- whole * 2;
root datasource rows (id: id, g: g, x: x) grain (id) address rows;
root datasource groups (g: g, region: region) grain (g) address groups;
datasource levels (g: g, level: level) grain (g) address levels;
"""

SETUP = [
    (
        "CREATE TABLE rows AS SELECT i AS id, 'g' || (i % 7 % 4) AS g,"
        " ((i * 7) % 11)::DOUBLE AS x FROM range(40) t(i)"
    ),
    (
        "CREATE TABLE groups AS SELECT * FROM (VALUES ('g0', 'n'), ('g1', 'n'),"
        " ('g2', 's'), ('g3', 's')) t(g, region)"
    ),
]

QUERIES = [
    "select g, level, scaled order by g asc;",
    "select g, case when level = 'g' then whole else 0 end -> v order by g asc;",
    "select g, level, whole + 1 -> w order by g asc;",
    "select level, max(scaled) -> m order by level asc;",
]

AGGREGATES = ["count", "sum", "min", "max"]
COARSE = ["*", "region"]


def _executor(agg: str, coarse: str):
    ex = Dialects.DUCK_DB.default_executor(environment=Environment())
    for sql in SETUP:
        ex.execute_raw_sql(sql)
    ex.parse_text(MODEL.format(agg=agg, coarse=coarse))
    ex.update_datasource(ex.environment.datasources["levels"])
    return ex


def _rows(ex, query: str) -> list[tuple]:
    return [
        tuple(round(float(v), 6) if isinstance(v, (int, float)) else v for v in row)
        for row in ex.execute_text(query)[-1].fetchall()
    ]


# recomputing `level` beside a by-region aggregate plans a keyless join, with
# or without the stored table (#929)
KEYLESS_RECOMPUTE = pytest.mark.xfail(raises=UnresolvableQueryException, strict=True)


def _cases():
    for query in QUERIES:
        for agg in AGGREGATES:
            for coarse in COARSE:
                marks = (
                    [KEYLESS_RECOMPUTE]
                    if coarse == "region" and "select g, level," in query
                    else []
                )
                yield pytest.param(query, agg, coarse, marks=marks)


@pytest.mark.parametrize("query, agg, coarse", list(_cases()))
def test_stored_root_over_rolled_partial_matches_recompute(
    query: str, agg: str, coarse: str
):
    ex = _executor(agg, coarse)
    stored = _rows(ex, query)
    with hidden_datasources(ex.environment, ["levels"]):
        recomputed = _rows(ex, query)
    assert stored == recomputed
    assert stored


@pytest.mark.parametrize("coarse", COARSE)
@pytest.mark.parametrize("agg", AGGREGATES)
def test_row_grains_cover_every_resolved_lineage_concept(agg: str, coarse: str):
    ex = _executor(agg, coarse)
    env = ex.environment
    build_env: BuildEnvironment = env.materialize_for_select()
    whole = build_env.concepts["local.whole"]
    scaled = build_env.concepts["local.scaled"]
    part = build_env.concepts["local.part"]
    level = build_env.concepts["local.level"]
    rewritten = _rollup_rewrite(build_env, [level, scaled, part])
    assert rewritten, "the coarse aggregate should read the finer partial"
    mandatory = [build_env.concepts["local.g"], level, scaled]
    roots = {level.address}
    rows = _nested_row_grains(mandatory, roots, build_env)
    reached = walk_lineage(
        mandatory,
        descend=lambda c: c.address not in roots,
        resolve=lambda c: build_env.concepts.get(c.address) or c,
    )
    assert {c.address for c in reached} <= rows.keys()
    assert whole.address in rows


def _rollup_rewrite(build_env: BuildEnvironment, demanded) -> dict:
    rewritten = reaggregate_partials(
        demanded, build_env.concepts, build_env.datasources.values()
    )
    for address, concept in rewritten.items():
        build_env.concepts[address] = concept
    return rewritten


ROUNDS_MODEL = """
key id int;
key g string;
property id.x float;
auto r0_part <- count(x) by g;
auto r0_whole <- count(x) by *;
auto r0_level <- case when r0_part > 9 then 'g' else 'all' end;
auto r0_share <- r0_part / r0_whole;
auto kept <- x ? x * r0_whole < 300;
auto r1_part <- count(kept) by g;
auto r1_whole <- count(kept) by *;
auto r1_level <- case when r0_level = 'g' and r1_part > 5 then 'g' else 'all' end;
auto r1_share <- r1_part / r1_whole;
root datasource rows (id: id, g: g, x: x) grain (id) address rows;
datasource r0_round (g: g, level: r0_level, share: r0_share) grain (g) address r0_round;
datasource r1_round (g: g, level: r1_level, share: r1_share) grain (g) address r1_round;
datasource fits (g: g, r0_level: r0_level, r1_level: r1_level, r1_share: r1_share, r1_whole: r1_whole)
    grain (g) address fits;
"""


def test_build_chain_over_stored_rounds_plans_and_matches():
    ex = Dialects.DUCK_DB.default_executor(environment=Environment())
    ex.execute_raw_sql(SETUP[0])
    ex.parse_text(ROUNDS_MODEL)
    env = ex.environment
    order = ["r0_round", "r1_round", "fits"]
    for i, name in enumerate(order):
        with hidden_datasources(env, order[i + 1 :]):
            ex.update_datasource(env.datasources[name])
    query = "select g, r0_level, r1_level, r1_share, r1_whole order by g asc;"
    with hidden_datasources(env, ["fits"]):
        stored = _rows(ex, query)
    with hidden_datasources(env, order):
        recomputed = _rows(ex, query)
    assert stored == recomputed
    assert len(stored) == 4
