"""The reduced shapes of the expressions family's 0-60,000 sweep (0.3.512).

- `formal/Trilogy/SelectGrain.lean` (`bare_beside_by_is_select_grain`,
  `by_keys_alone_fan_out`): `count(tree_id) * min(height) by city` at
  city, species is one value per city, species; keyed by the `by` alone it
  paired every species' count with every other's. An expression of bare
  aggregates is at the select grain, never at their inputs' keys.
- `SelectGrain.unfiltered_bridge_keeps_rejected`,
  `SelectGrain.dimension_rescan_drops_gate`: a pairing bridged by the
  condition-phase scan, or a derived key looked up off a fresh scan of its
  dimension, read rows the WHERE dropped.
- `SelectGrain.resum_reads_partial_rows`,
  `SelectGrain.peel_of_joined_table_reads_rejected`: a re-sum of a filtered partial
  reads the partial's rows; a `by *` total's table joined to the WHERE's
  table is no constant of the query.

Rows checked by hand against `test_oracle_sweep_laws.ROWS` (heights 3, NULL,
5, NULL, 7, 2; forms broadleaf x4 with b1, NULL for Ulmus, linden for b2)."""

import pytest

from tests.discovery.test_oracle_sweep_laws import executor, normalize
from tests.discovery.test_rollup_resolution_laws import BASE
from trilogy import Dialects
from trilogy.core.enums import FunctionType
from trilogy.core.functions import FunctionFactory
from trilogy.core.models.author import _reads_bare_aggregate
from trilogy.core.models.build import Factory
from trilogy.parsing.parse_engine_v2 import parse_text

FORM0 = "coalesce(tree_form, 'none') as form0"

CASES = [
    # expressions 3102, reduced: a WHERE atom copied onto a re-sum of `v1`
    (
        (
            f"select {FORM0}, count(tree_id) by * as a, count(tree_id) as v1"
            " where sum(height) by tree_form > 3 and height > 3;"
        ),
        [("broadleaf", 2, 2)],
    ),
    (
        (
            f"select {FORM0}, count(tree_id) by * as a, count(tree_id) as v1"
            " where (sum(height) by tree_form > 3 and (min(height) by * = 0"
            " or height > 3));"
        ),
        [("broadleaf", 2, 2)],
    ),
    # expressions 2171, reduced: a bare count beside a value by city
    (
        (
            "select city, species, max(height) by * as v0, count(tree_id) *"
            " min(height) by city as v1, min(height) / count(tree_id) by city,"
            " species as v2;"
        ),
        [
            ("A", "Acer", 7, 6, 1.5),
            ("A", "Quercus", 7, 3, 5),
            ("A", "Ulmus", 7, 3, None),
            ("B", "Acer", 7, 2, 7),
            ("B", "Tilia", 7, 2, 2),
        ],
    ),
    # expressions 43598: two bare aggregates keyed by an input's key
    (
        "select city, species, sum(height + 1) as v0, count(tree_id) - count_distinct(tag) as v1;",
        [
            ("A", "Acer", 4, 0),
            ("A", "Quercus", 6, 0),
            ("A", "Ulmus", None, 0),
            ("B", "Acer", 8, 0),
            ("B", "Tilia", 3, 0),
        ],
    ),
    # expressions 33164, reduced: a row spine bridged by the unfiltered scan
    (
        (
            "select tree_id, count(tree_id) by city, tree_form - count(tree_id)"
            " as v0 where (tree_form = 'broadleaf' and max(height) by city,"
            " tree_form > 1);"
        ),
        [("a1", 2), ("a2", 2), ("a3", 2), ("b1", 0)],
    ),
    (
        (
            "select tree_id, count(tree_id) by city, tree_form - count(tree_id)"
            " as v0 where (tree_form = 'broadleaf' and max(height) by city,"
            " tree_form > 100);"
        ),
        [],
    ),
    # expressions 31752, reduced: a `by *` total of the trees under a WHERE on
    # the forms they join
    (
        "select tree_form, min(height) + count_distinct(tag) by * as v2 where tree_form = 'linden';",
        [("linden", 3)],
    ),
    # expressions 9445, reduced: a derived key's lookup regrafted off the
    # gated rows onto a fresh scan of the forms
    (
        f"select {FORM0} where max(height) by city, species < 4;",
        [("broadleaf",), ("linden",)],
    ),
    # expressions 48214, reduced: a re-sum's partial filtered in the same pass
    (
        (
            "select case when tree_form = 'broadleaf' then 'b' else 'o' end as fc,"
            " sum(height) by * as v0, sum(height) / count(tree_id) by * as v2"
            " where (max(height) by * != 1 or height between 3 and 7);"
        ),
        [("b", 17, 2.5), ("o", 17, 0.333333)],
    ),
    # expressions 30258
    (
        (
            "select tree_form, max(height) + min(height) as v0, sum(height) by *"
            " - count(tree_id) as v1, count_distinct(tag) by * / count(tree_id)"
            " by * as v2 where tree_form is null;"
        ),
        [(None, None, None, 1)],
    ),
]


@pytest.mark.parametrize("model", ["base", "summary"])
@pytest.mark.parametrize("query, expected", CASES)
def test_reduced_shape(query: str, expected: list[tuple], model: str):
    ex = executor(model)
    sql = ex.generate_sql(query)[-1]
    rows = ex.execute_raw_sql(sql).fetchall()
    assert normalize(rows) == normalize(expected), sql


@pytest.mark.parametrize(
    "expression, grain",
    [
        ("count(tree_id) * min(height) by city", {"local.city", "local.species"}),
        ("count(tree_id) - count_distinct(tag)", {"local.city", "local.species"}),
        ("sum(height) by city + 1", {"local.city"}),
    ],
)
def test_expression_of_aggregates_grain(expression: str, grain: set[str]):
    ex = Dialects.DUCK_DB.default_executor()
    ex.parse_text(BASE)
    env = ex.environment
    _, statements = parse_text(f"select city, species, {expression} as v;", env)
    select = statements[-1]
    built = Factory(environment=env, grain=select.grain).build(select.as_lineage(env))
    (v,) = [c for c in built.output_components if c.address == "local.v"]
    assert v.grain.components == grain


@pytest.mark.parametrize(
    "operator, bare", [(FunctionType.COUNT, True), (FunctionType.GROUPING, False)]
)
def test_unwrapped_aggregate_is_bare_unless_a_pass_key(
    operator: FunctionType, bare: bool
):
    ex = Dialects.DUCK_DB.default_executor()
    ex.parse_text(BASE)
    env = ex.environment
    function = FunctionFactory(env).create_function(
        [env.concepts["city"].reference], operator
    )
    assert _reads_bare_aggregate(function, env) is bare
