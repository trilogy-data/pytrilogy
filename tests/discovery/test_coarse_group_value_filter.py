"""`Rollup.coarse_group_value_filter_exact`: a WHERE over an aggregate at a
grain inside the summary's is the re-sum of the summary rows sharing that key,
joined back onto each summary row before the rows are re-summed. A re-sum is
joined to the rows holding its partials on the group key alone
(`Rollup.partial_is_not_a_key`)."""

from tests.discovery.test_group_value_filter_joined import _run
from tests.discovery.test_rollup_resolution_laws import BASE, SUMMARY
from tests.discovery.test_rollup_resolution_laws import _run as run_rollup


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


def test_coarse_gate_at_summary_grain_reads_summary():
    sql, rows = _run(
        "select city, species, sum(height) as v where sum(height) by city > 8"
        " order by city, species;"
    )
    assert "trees" not in sql, sql
    assert rows == [("B", "Acer", 7), ("B", "Tilia", 2)]


def test_gate_over_resummed_stored_value_by_dimension_column():
    sql, rows = _run(
        "select form, sum(height) as v"
        " where sum(sum(height) by city, species) by city > 8 order by form;"
    )
    assert "trees" not in sql, sql
    assert rows == [("broad", 7), ("linden", 2)]


def test_resum_is_not_joined_on_its_partial():
    query = (
        "select tree_form, sum(height) as v, sum(sum(height) by city) by * as w"
        " where species = 'Acer';"
    )
    sql, rows = run_rollup(BASE + SUMMARY, query)
    assert "tree_summary" in sql, sql
    assert rows == [("broadleaf", 10, 10)]


def test_having_on_output_grain_aggregate_reads_summary():
    sql, rows = _run(
        "select city, sum(height) as v having sum(height) by city > 8 order by city;"
    )
    assert "trees" not in sql, sql
    assert rows == [("B", 9)]


def test_gate_by_dimension_column_reads_summary():
    sql, rows = _run(
        "select city, sum(height) as v where sum(height) by form > 2 order by city;"
    )
    assert "trees" not in sql, sql
    assert rows == [("A", 8), ("B", 7)]


def test_gate_by_key_and_dimension_column_joins_on_both():
    sql, rows = _run(
        "select city, sum(height) as v where sum(height) by city, form > 4"
        " order by city;"
    )
    assert "trees" not in sql, sql
    assert rows == [("A", 8), ("B", 7)]


def test_in_list_gate_reads_its_own_grain():
    sql, rows = _run(
        "select species, sum(height) as v where sum(height) by city in (8)"
        " order by species;"
    )
    assert "trees" not in sql, sql
    assert rows == [("Acer", 3), ("Quercus", 5)]
