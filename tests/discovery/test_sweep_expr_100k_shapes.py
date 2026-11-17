"""The reduced shapes of the expressions family's 60,000-100,000 sweep (0.3.513).

- `formal/Trilogy/SelectGrain.lean` (`sole_gate_paired_short_of_its_grain`):
  the one side computing a value at `(city, form)` met a FINAL gate at
  `(city, species)` on `city` alone, or on nothing at all.
- `SelectGrain.finished_value_resummed_per_row`: a finished bare aggregate
  joined beside a per-row stream was re-summed as a stored partial, once per
  row.
- A constant carried at a key (`select city, 2` beside a gate by `city,
  tree_form`) was built as a bare constant claiming `city`, and no plan
  sourced it; it takes the key's rows from its parent.
- `SelectGrain.gate_on_padding_passes_rejected`: a gate holding on NULL
  (`sum(x) by city is null`) re-applied above a join that padded its kept
  side passed every rejected key.

Rows checked by hand against `test_oracle_sweep_laws.ROWS` (heights 3, NULL,
5, NULL, 7, 2; forms broadleaf x4 with b1, NULL for Ulmus, linden for b2)."""

import pytest

from tests.discovery.test_oracle_sweep_laws import executor, normalize

CASES = [
    # expressions 38531
    (
        (
            "select city, tree_form, max(height) by tree_form * count(tree_id)"
            " by city as v0 where sum(height) by city, species < 6 and height"
            " is not null;"
        ),
        [("A", "broadleaf", 10), ("B", "linden", 2)],
    ),
    # expressions 47126, reduced: the gate's key held by no side
    (
        (
            "select city, tree_form, count(tree_id) by tree_form / count(tree_id)"
            " by city as v0 where max(height) by species != 5;"
        ),
        [("A", "broadleaf", 1.5), ("B", "broadleaf", 1.5), ("B", "linden", 0.5)],
    ),
    # expressions 99020, reduced
    (
        (
            "select city, species, tree_form, count(tree_id) by city, tree_form"
            " + count(tree_id) by species as v0, sum(height) as v1;"
        ),
        [
            ("A", "Acer", "broadleaf", 6, 3),
            ("A", "Quercus", "broadleaf", 4, 5),
            ("A", "Ulmus", None, 2, None),
            ("B", "Acer", "broadleaf", 4, 7),
            ("B", "Tilia", "linden", 2, 2),
        ],
    ),
    # expressions 60555
    (
        (
            "select city, tree_form, count_distinct(tag) by city + min(height)"
            " by * as v0 where sum(height) by city is null;"
        ),
        [],
    ),
    (
        (
            "select city, tree_form, count_distinct(tag) by city + min(height)"
            " by * as v0 where sum(height) by city is null or sum(height) by"
            " city > 8;"
        ),
        [("B", "broadleaf", 4), ("B", "linden", 4)],
    ),
    # expressions 62086, reduced
    (
        (
            "select city, tree_form, count(tree_id) by city - 1 as v0 where"
            " not (avg(height) by city != 10);"
        ),
        [],
    ),
    # first 22558, spellings 20968 (broken by a first version of the round):
    # a `by *` gate's all-rows marker is no key to widen a side to
    (
        (
            "select tree_form, min(height ? city = 'B') as v0 where (sum(height)"
            " by * > 0 and avg(height) by city > 4);"
        ),
        [("broadleaf", 7), ("linden", 2)],
    ),
    # expressions 89514, reduced: a constant keyed beside a gate's grain
    ("select city, 2 as v0 where max(height) by city, tree_form = 5;", [("A", 2)]),
    (
        "select city, 2 as v0 where max(height) by city, tree_form > 4;",
        [("A", 2), ("B", 2)],
    ),
]


@pytest.mark.parametrize("model", ["base", "summary"])
@pytest.mark.parametrize("query, expected", CASES)
def test_reduced_shape(query: str, expected: list[tuple], model: str):
    ex = executor(model)
    sql = ex.generate_sql(query)[-1]
    rows = ex.execute_raw_sql(sql).fetchall()
    assert normalize(rows) == normalize(expected), sql
