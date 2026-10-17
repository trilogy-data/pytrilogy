import pytest

from trilogy import Dialects, Environment
from trilogy.parsing.v2.lark_backend import parse_lark
from trilogy.parsing.v2.pest_backend import parse_pest

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
auto prev_species <- lag(distinct species, 1) over (order by dominance_rank asc);
auto prev_species_by_agg <- lag(distinct species, 1) over (order by species_count desc, species asc);
auto prev_species_per_tree <- lag(species, 1) over (order by dominance_rank asc);
auto prev_species_by_planted <- lag(distinct species, 1) over (order by planted asc);
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


def test_navigation_window_property_brings_its_key(executor):
    env = executor.environment
    assert env.concepts["prev_species_per_tree"].grain.components == {"local.tree_id"}
    assert env.concepts["prev_species_by_planted"].grain.components == {"local.tree_id"}


def test_per_tree_lag_runs_over_tree_rows(executor):
    sql = executor.generate_sql(
        "select tree_id, prev_species_per_tree order by tree_id asc;"
    )[-1]
    rows = executor.execute_raw_sql(sql).fetchall()
    assert [r[0] for r in rows] == [1, 2, 3, 4, 5, 6], rows
    assert sorted((r[1] for r in rows), key=str) == [None, "a", "a", "a", "b", "b"]


@pytest.mark.parametrize("parse", [parse_lark, parse_pest])
def test_distinct_operand_parses_on_both_backends(parse):
    parse("auto x <- lag(distinct species, 1) over (order by dominance_rank asc);")
    parse("auto y <- lag(distinct_flag) over (order by dominance_rank asc);")


def test_operand_named_like_keyword_is_not_distinct():
    env = Environment()
    env.parse(MODEL + """
property tree_id.distinct_tag string;
auto prev_tag <- lag(distinct_tag, 1) over (order by planted asc);
""")
    assert env.concepts["prev_tag"].grain.components == {"local.tree_id"}


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


TIE_MODEL = """
key route string;
key event_id int;
property event_id.event_time int;

datasource events (eid: event_id, route: route, t: event_time)
grain (event_id)
query '''
select 1 eid, 'A' route, 0 t union all
select 2 eid, 'A' route, 5 t union all
select 3 eid, 'A' route, 5 t union all
select 4 eid, 'B' route, 1 t
''';

auto prior_event_time <- lag(event_time, 1) over (partition by route order by event_time asc);
auto gap <- event_time - prior_event_time;
"""


def test_lag_over_tied_frame_values_keeps_one_row_per_event():
    env = Environment()
    env.parse(TIE_MODEL)
    assert env.concepts["prior_event_time"].grain.components == {
        "local.event_id",
        "local.route",
    }
    ex = Dialects.DUCK_DB.default_executor(environment=env)
    rows = ex.execute_text("select event_id, gap order by event_id asc;")[-1].fetchall()
    assert [r[0] for r in rows] == [1, 2, 3, 4], rows
    assert rows[0][1] is None and rows[3][1] is None, rows
    assert sorted([rows[1][1], rows[2][1]]) == [0, 5], rows
