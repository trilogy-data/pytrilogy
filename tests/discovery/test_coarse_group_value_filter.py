"""`Rollup.coarse_group_value_filter_exact`: a WHERE over an aggregate at a
grain inside the summary's is the re-sum of the summary rows sharing that key,
joined back onto each summary row before the rows are re-summed."""

from tests.discovery.test_group_value_filter_joined import _run


def test_coarse_gate_reads_summary_not_partials():
    sql, rows = _run(
        "select species, sum(height) as v where sum(height) by city > 4"
        " order by species;"
    )
    assert "trees" not in sql, sql
    assert rows == [("Acer", 10), ("Quercus", 5), ("Tilia", 2)]


def test_coarse_gate_by_dimension_column():
    sql, rows = _run(
        "select form, sum(height) as v where sum(height) by city > 8 order by form;"
    )
    assert "trees" not in sql, sql
    assert rows == [("broad", 7), ("linden", 2)]


def test_coarse_gate_total():
    sql, rows = _run("select sum(height) as v where sum(height) by city > 8;")
    assert "trees" not in sql, sql
    assert rows == [(9,)]


def test_row_column_beside_gate_reads_base_rows():
    sql, rows = _run(
        "select form, sum(height) as v where sum(height) by city > 4"
        " and species != 'Tilia' order by form;"
    )
    assert "trees" in sql, sql
    assert rows == [("broad", 15)]
