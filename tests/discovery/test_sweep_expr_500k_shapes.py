"""The reduced shapes of the expressions family's 350,000-500,000 sweep and the
leads it closed (0.3.517).

- A per-tree value over a gated count and a filtered `max(height)`, beside a
  renamed column and a HAVING at `tree_form` (spellings 404253): wrapping a
  join and joining a wrapped side were spelled one identifier, so the FINAL
  merge folded its two parents into one, kept one of their joins, and read
  `height` off a CTE it never joined (`Identity.naive_wrap_of_join_collides`).

Rows checked by hand against `test_oracle_sweep_laws.ROWS` (heights 3, NULL,
5, NULL, 7, 2; tags x, y, x, x, y, x; forms broadleaf x4 with b1, NULL for
Ulmus, linden for b2)."""

import pytest

from tests.discovery.test_oracle_sweep_laws import executor, normalize

CASES = [
    # spellings 404253, reduced
    (
        (
            "select tree_id, tall as v0, count(tree_id ? sum(height) by tree_form"
            " = 2) as v1, max(height ? height > 2) as v2 having count(tag) by"
            " tree_form = 1;"
        ),
        [("a4", None, 0, None), ("b2", False, 1, None)],
    ),
]


@pytest.mark.parametrize("model", ["base", "summary"])
@pytest.mark.parametrize("query, expected", CASES)
def test_reduced_shape(query: str, expected: list[tuple], model: str):
    ex = executor(model)
    sql = ex.generate_sql(query)[-1]
    rows = ex.execute_raw_sql(sql).fetchall()
    assert normalize(rows) == normalize(expected), sql
