import pytest

from trilogy import Dialects, Environment

MODEL = """
key tree_id int;
property tree_id.species string;
property tree_id.planted int;
property tree_id.dbh float;

datasource trees (
    tree_id: tree_id,
    species: species,
    planted: planted,
    dbh: dbh,
) grain (tree_id) address trees;

auto species_count <- count(tree_id) by species;
auto dominance_rank <- rank(species) over (order by count(tree_id) by species desc, species asc);
auto prev_species <- lag(species, 1) over (order by dominance_rank asc);
auto prev_species_by_agg <- lag(species, 1) over (order by species_count desc, species asc);
auto prev_count <- lag(species_count, 1) over (order by species asc);
auto prev_dbh <- lag(dbh, 1) over (order by planted asc);
auto prev_dbh_in_species <- lag(dbh, 1) over (partition by species order by planted asc);
"""

SETUP = """create table trees as select * from (values
 (1,'a',2001,1.0),(2,'a',2002,2.0),(3,'a',2003,3.0),
 (4,'b',2001,4.0),(5,'b',2004,5.0),(6,'c',2005,6.0)) t(tree_id, species, planted, dbh)"""


@pytest.fixture
def executor():
    env = Environment()
    env.parse(MODEL)
    ex = Dialects.DUCK_DB.default_executor(environment=env)
    ex.execute_raw_sql(SETUP)
    return ex


def test_navigation_window_grain_follows_frame(executor):
    env = executor.environment
    assert env.concepts["prev_species"].grain.components == {"local.species"}
    assert env.concepts["prev_species_by_agg"].grain.components == {"local.species"}
    assert env.concepts["prev_count"].grain.components == {"local.species"}


def test_navigation_window_grain_keeps_operand_grain(executor):
    env = executor.environment
    assert env.concepts["prev_dbh"].grain.components == {"local.tree_id"}
    assert env.concepts["prev_dbh_in_species"].grain.components == {"local.tree_id"}


def test_lag_over_coarser_window_does_not_fan_out(executor):
    sql = executor.generate_sql(
        "select species, dominance_rank, prev_species order by dominance_rank asc;"
    )[-1]
    rows = executor.execute_raw_sql(sql).fetchall()
    assert rows == [("a", 1, None), ("b", 2, "a"), ("c", 3, "b")], rows


def test_lag_over_aggregate_order_does_not_fan_out(executor):
    sql = executor.generate_sql(
        "select species, species_count, prev_species_by_agg order by species_count desc, species asc;"
    )[-1]
    rows = executor.execute_raw_sql(sql).fetchall()
    assert rows == [("a", 3, None), ("b", 2, "a"), ("c", 1, "b")], rows


def test_row_grain_lag_still_runs_over_rows(executor):
    sql = executor.generate_sql(
        "select tree_id, planted, dbh, prev_dbh order by planted asc, tree_id asc;"
    )[-1]
    rows = executor.execute_raw_sql(sql).fetchall()
    assert [r[0] for r in rows] == [1, 4, 2, 3, 5, 6], rows
    assert [r[3] for r in rows] == [None, 1.0, 4.0, 2.0, 3.0, 5.0], rows


def test_partitioned_row_grain_lag_still_runs_over_rows(executor):
    sql = executor.generate_sql(
        "select tree_id, species, prev_dbh_in_species order by species asc, tree_id asc;"
    )[-1]
    rows = executor.execute_raw_sql(sql).fetchall()
    assert rows == [
        (1, "a", None),
        (2, "a", 1.0),
        (3, "a", 2.0),
        (4, "b", None),
        (5, "b", 4.0),
        (6, "c", None),
    ], rows
