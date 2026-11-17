"""The reduced shapes of the 390,000-450,000 sweep and of the expressions
family's first seeds (0.3.511).

- `formal/Trilogy/SelectGrain.lean` (`value_over_group_holds_no_finer_key`,
  `value_over_group_bridged_exact`): `count(tree_id) + 1` is a value of its
  group's rows. Beside a value by city, tree_form the plan said it could be
  widened to `tree_form` and paired the two on it; the built node held no
  such column and they joined on city alone.
- `SelectGrain.blend_under_a_finer_reader_invents_pairs`: a value by species
  plus one by city, tree_form, read at city, species, pairs through the rows
  holding both keys.
- `SelectGrain.null_key_group_is_a_group`: `height > 2` is NULL where the
  height is, and the NULL group is one of the select's.
- `formal/Trilogy/Rollup.lean` (`value_off_a_partial_is_per_summary_row`,
  `filter_above_resum_keeps_rejected`, `resum_beside_finer_rows_multiplies`):
  a value over a summary's partial reads it re-summed; a WHERE atom finer
  than a re-sum is applied below it; a HAVING re-sums a partial once per
  summary row.
- `formal/Trilogy/Scalars.lean` (`bare_argument_reassociates`): an aggregate
  of one row is its argument, parenthesized where it is read.

Rows checked by hand against `test_oracle_sweep_laws.ROWS` (heights 3, NULL,
5, NULL, 7, 2)."""

import pytest

from tests.discovery.test_oracle_sweep_laws import executor, normalize
from tests.discovery.test_rollup_resolution_laws import BASE
from trilogy import Dialects
from trilogy.core.exceptions import UnresolvableQueryException
from trilogy.core.functions import propagates_argument_nulls

FC = "case when tree_form = 'broadleaf' then 'b' else 'o' end as fc"

CASES = [
    # expressions 100, reduced: a value over a group beside a mixed grain
    (
        (
            "select city, species, sum(height) by city, tree_form as v0,"
            " count(tree_id) + 1 as v2;"
        ),
        [
            ("A", "Acer", 8, 3),
            ("A", "Quercus", 8, 2),
            ("A", "Ulmus", None, 2),
            ("B", "Acer", 7, 2),
            ("B", "Tilia", 2, 2),
        ],
    ),
    # expressions 1275, reduced: a bridge elected to key a value's own inputs
    (
        "select tree_id, max(height) by city, tree_form * count(tree_id) as v1;",
        [("a1", 5), ("a2", 5), ("a3", 5), ("a4", None), ("b1", 7), ("b2", 2)],
    ),
    # expressions 100: the all-rows marker in a side's identity is no key
    (
        (
            "select city, species, sum(height) by city, tree_form as v0,"
            " count(tree_id) / count(tree_id) by * as v2;"
        ),
        [
            ("A", "Acer", 8, 0.333333),
            ("A", "Quercus", 8, 0.166667),
            ("A", "Ulmus", None, 0.166667),
            ("B", "Acer", 7, 0.166667),
            ("B", "Tilia", 2, 0.166667),
        ],
    ),
    # expressions 5, reduced
    (
        (
            "select tree_id, count_distinct(tag) by * * sum(height) by city, tree_form"
            " as v0, sum(height) as v1 where species = 'Acer';"
        ),
        [("a1", 6, 3), ("a2", 6, None), ("b1", 14, 7)],
    ),
    # expressions 2207: a side grained by a value another side supplies
    (
        (
            "select tree_id, sum(height + 1) / count(tree_id) by * as v0, max(height)"
            " by tree_form / count(tree_id) by city, tree_form as v1 where tree_form ="
            " 'broadleaf';"
        ),
        [
            ("a1", 1, 2.333333),
            ("a2", None, 2.333333),
            ("a3", 1.5, 2.333333),
            ("b1", 2, 7),
        ],
    ),
    # expressions 3224, reduced: a blend the reader's grain pins
    (
        (
            "select city, species, max(height) by species + sum(height) by city,"
            " tree_form as v0;"
        ),
        [
            ("A", "Acer", 15),
            ("A", "Quercus", 13),
            ("A", "Ulmus", None),
            ("B", "Acer", 14),
            ("B", "Tilia", 4),
        ],
    ),
    # expressions 563, reduced: the NULL group of a derived key
    (
        (
            "select height > 2 as big, count(tree_id) / count(tree_id) by * as v0"
            " where max(height) by species > 3;"
        ),
        [(None, 0.25), (True, 0.75)],
    ),
    # expressions 3913, reduced: an aggregate of one row is its argument
    (
        "select tree_id, sum(height + 1) * 10 as v0, 10 - sum(height - 1) as v1;",
        [
            ("a1", 40, 8),
            ("a2", None, None),
            ("a3", 60, 6),
            ("a4", None, None),
            ("b1", 80, 4),
            ("b2", 30, 9),
        ],
    ),
    # expressions 4340, reduced: a value over a per-species maximum
    (
        (
            "select coalesce(tree_form, 'none') as form0, count(tree_id) by * as v0,"
            " max(height) by * - max(height) as v1;"
        ),
        [("broadleaf", 6, 0), ("linden", 6, 5), ("none", 6, None)],
    ),
    # expressions 1371, reduced: a value over a count summed by tree_form
    (
        f"select {FC}, min(height) by * as v0, count(tree_id) * 10 as v1;",
        [("b", 2, 40), ("o", 2, 20)],
    ),
    # first 446174: `city = 'B'` applied above counts summed by species
    (
        (
            "select min(count(tree_id) by tree_form) as v0, max(height) as v1 where"
            " (city = 'B' and species = 'Acer');"
        ),
        [(1, 7)],
    ),
    # spellings 390302
    (
        (
            "select max(count(tree_id) by tree_form) as v0, count(min(height) by"
            " species) as v1 where city in ('B') having max(height) by * >= 3;"
        ),
        [(1, 2)],
    ),
    # spellings 430944: a stored sum re-summed once per tree of a finer side
    (
        (
            "select city, tree_form, count(tree_id ? count_distinct(tag) by * !="
            " max(height) by tree_form) as v0, avg(height) by * as v1, min(height ?"
            " avg(height) by city, species > 2) as v2 having sum(height) by city,"
            " tree_form < 10;"
        ),
        [
            ("A", "broadleaf", 3, 4.25, 3),
            ("B", "broadleaf", 1, 4.25, 7),
            ("B", "linden", 0, 4.25, None),
        ],
    ),
]


@pytest.mark.parametrize("model", ["base", "summary"])
@pytest.mark.parametrize("query, expected", CASES)
def test_reduced_shape(query: str, expected: list[tuple], model: str):
    ex = executor(model)
    sql = ex.generate_sql(query)[-1]
    rows = ex.execute_raw_sql(sql).fetchall()
    assert normalize(rows) == normalize(expected), sql


KEYLESS = (
    "select species, count_distinct(tag) + 1 as v1, sum(height) by tree_form as v2;"
)


@pytest.mark.parametrize(
    "model",
    [
        "base",
        # the summary's `sum(height) by tree_form` is a peel at tree_form, and
        # no side holds species beside it (handoff, area 1)
        pytest.param(
            "summary",
            marks=pytest.mark.xfail(strict=True, raises=UnresolvableQueryException),
        ),
    ],
)
def test_value_over_group_beside_its_property(model: str):
    """expressions 6, reduced: the same claim left the two sides keyless."""
    ex = executor(model)
    rows = ex.execute_raw_sql(ex.generate_sql(KEYLESS)[-1]).fetchall()
    assert normalize(rows) == [
        ("Acer", 3, 15),
        ("Quercus", 2, 15),
        ("Tilia", 2, 2),
        ("Ulmus", 2, None),
    ]


def test_collapsed_aggregate_argument_is_parenthesized():
    query = "select tree_id, sum(height + 1) * 10 as v0;"
    sql = executor("base").generate_sql(query)[-1]
    assert '("trees"."height" + 1) * 10' in sql, sql


@pytest.mark.parametrize(
    "expression, nullable",
    [
        ("height > 2", True),
        ("height between 2 and 5", True),
        ("height is null", False),
        ("coalesce(height, 0)", False),
    ],
)
def test_comparison_of_a_nullable_is_nullable(expression: str, nullable: bool):
    ex = Dialects.DUCK_DB.default_executor()
    ex.parse_text(BASE + f"auto probe <- {expression};")
    concept = ex.environment.materialize_for_select().concepts["local.probe"]
    assert propagates_argument_nulls(concept) is nullable
