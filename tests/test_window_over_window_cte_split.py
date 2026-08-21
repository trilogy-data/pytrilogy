from trilogy import Dialects, Environment

MODEL = """
key tree_id int;
property tree_id.species string;

datasource trees (
    tree_id: tree_id,
    species: species,
) grain (tree_id) address trees;

auto dominance_rank <- rank(species) over (order by count(tree_id) by species desc, species asc);
auto cumulative <- (sum count(tree_id) by species order by dominance_rank asc);
"""


def test_window_ordered_by_sibling_window_splits_ctes():
    env = Environment()
    env.parse(MODEL)
    executor = Dialects.DUCK_DB.default_executor(environment=env)
    executor.execute_raw_sql(
        "create table trees as select * from (values (1,'a'),(2,'a'),(3,'b')) t(tree_id, species)"
    )
    sql = executor.generate_sql("select species, dominance_rank, cumulative;")[-1]
    assert "over (order by rank()" not in sql, sql
    rows = executor.execute_raw_sql(sql).fetchall()
    assert rows == [("a", 1, 2), ("b", 2, 3)], rows
