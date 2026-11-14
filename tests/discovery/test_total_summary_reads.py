"""`formal/Trilogy/Rollup.lean` (`total_gate_from_summary`,
`total_gate_from_summary_min`): a total (`agg(x) by *`) a WHERE reads is over
every row, so a summary rolls it up whatever row atoms sit beside it; so is
the total of a select with no WHERE. Each case reads the summary for it and
scans the trees at most as often as the rows themselves need. Rows checked by
hand against `test_oracle_sweep_laws.ROWS` (heights 3, NULL, 5, NULL, 7, 2)."""

import pytest

from tests.discovery.test_oracle_sweep_laws import executor, normalize

TREE_SCAN = 'as "trees"'

# (query, rows, trees scans with the summary in scope)
CASES = [
    # the total beside a row atom the summary cannot apply
    (
        "select city, species where height > 2 and min(height) by * <= 2;",
        [("A", "Acer"), ("A", "Quercus"), ("B", "Acer")],
        1,
    ),
    (
        "select city where height > 2 and count(tree_id) by * != 6;",
        [],
        1,
    ),
    # parenthesized: the spelling 0.3.501's parser fix split into its atoms
    (
        "select city, species where (height > 2 and max(height) by * >= 7);",
        [("A", "Acer"), ("A", "Quercus"), ("B", "Acer")],
        1,
    ),
    # a total gate alone needs no tree at all
    (
        "select species where sum(height) by * > 1;",
        [("Acer",), ("Quercus",), ("Tilia",), ("Ulmus",)],
        0,
    ),
    # the total of an unfiltered select
    (
        "select species, count(tree_id) by * as v;",
        [("Acer", 6), ("Quercus", 6), ("Tilia", 6), ("Ulmus", 6)],
        0,
    ),
    # seed 70914, reduced: the one-row total cross-joined onto rows the gate
    # left empty pads no row (a FULL join on 1=1 returned one NULL row)
    (
        "select city, species, sum(height) as v1 where count(tree_id) by * != 5"
        " and count_distinct(tag) by city - count_distinct(tag) by city,"
        " tree_form < 0;",
        [],
        None,
    ),
    # a total compared to a value at another grain stays on the rows
    (
        "select city where max(height) by * < sum(height) by city;",
        [("A",), ("B",)],
        None,
    ),
]


@pytest.mark.parametrize("query,expected,scans", CASES)
def test_total_reads_the_summary(query: str, expected: list, scans: int | None):
    for model in ("base", "summary"):
        sql = executor(model).generate_sql(query)[-1]
        rows = executor(model).execute_raw_sql(sql).fetchall()
        assert normalize(rows) == normalize(expected), f"{model}\n{sql}"
    if scans is not None:
        assert "tree_summary" in sql, sql
        assert sql.count(TREE_SCAN) == scans, sql
