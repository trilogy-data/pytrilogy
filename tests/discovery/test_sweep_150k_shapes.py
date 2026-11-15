"""The reduced shapes of the 90,000-150,000 sweep (0.3.504).

- `formal/Trilogy/SelectGrain.lean` (`total_off_row_stream_keeps_rejected`): a
  total is read off its own one-row group, never off a `?` filter's row
  stream, which would bring the keys the WHERE rejected.
- `formal/Trilogy/Rollup.lean` (`renamed_grain_same_groups`): a grain spelled
  through a rename (`by species, v0` with `city as v0`) is the grain it names,
  never a coarser partial of it.
- `formal/Trilogy/Rollup.lean` (`split_scan_reads_every_row`): a summary-rolled
  root the WHERE filters below its grain is sourced with the WHERE's columns.

Rows checked by hand against `test_oracle_sweep_laws.ROWS` (heights 3, NULL,
5, NULL, 7, 2)."""

import pytest

from tests.discovery.test_oracle_sweep_laws import executor, normalize

TREE_SCAN = 'as "trees"'

# (query, rows, trees scans with the summary in scope)
CASES = [
    # spellings 103208, reduced
    (
        (
            "select city, tree_form, sum(height) by * as v0, sum(height ? sum(height)"
            " by * >= 2) as v1 where max(height) by city, species is null;"
        ),
        [("A", None, None, None)],
        None,
    ),
    # spellings 103923, reduced
    (
        (
            "select tree_id, city as v0, max(height ? max(height) by city, species >= 10)"
            " as v2 having max(height) by city, species < 5;"
        ),
        [("a1", "A", None), ("a2", "A", None), ("b2", "B", None)],
        None,
    ),
    # spellings 131836, reduced: the summary plan cannot place its rows, so the
    # trees answer
    (
        (
            "select tree_form, max(max(height) by tree_form) as v1, min(height ?"
            " count(tree_id) by species > count(tree_id) by tree_form) as v2 having"
            " count(tree_id) by tree_form > 8;"
        ),
        [],
        None,
    ),
    # first family 93841: both nested aggregates roll up from one filtered scan
    (
        (
            "select max(min(height) by city) as v0, min(min(height) by tree_form) as v1"
            " where city = 'A';"
        ),
        [(3, 3)],
        0,
    ),
    (
        (
            "select max(max(height) by city) as v0, sum(sum(height) by tree_form) as v1"
            " where city = 'B';"
        ),
        [(7, 9)],
        0,
    ),
]


@pytest.mark.parametrize("query,expected,scans", CASES)
def test_sweep_150k_shape(query: str, expected: list, scans: int | None):
    for model in ("base", "summary"):
        sql = executor(model).generate_sql(query)[-1]
        rows = executor(model).execute_raw_sql(sql).fetchall()
        assert normalize(rows) == normalize(expected), f"{model}\n{sql}"
    if scans is not None:
        assert "tree_summary" in sql, sql
        assert sql.count(TREE_SCAN) == scans, sql
