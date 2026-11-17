import re

from trilogy import Dialects, Executor
from trilogy.core.models.environment import Environment
from trilogy.core.query_processor import process_query
from trilogy.dialect.bigquery import BigqueryDialect

MODEL = """
key tree_id int;
property tree_id.city string;
property tree_id.latitude float;
property tree_id.species string;
property tree_id.dbh float;
property <*>.watermark int;

datasource trees (tree_id, city, latitude, species, dbh)
grain (tree_id)
query '''select i tree_id, 'ams' city, (i % 100) * 0.01 latitude, 'sp' || (i % 5) species, i * 1.0 dbh from range(200) t(i)''';

datasource published (through: watermark)
query '''select 7 through where {published}''';

auto density_cell <- coalesce(cast(floor(latitude / 0.1) as bigint), -1);
auto cell_trees <- count(tree_id) by city, density_cell;
auto mean_dbh <- avg(dbh) by *;
auto class <- case when dbh > mean_dbh then 'big' else 'small' end;
auto class_avg <- avg(dbh) by class, species;
"""

SPLIT_KEYS = "select tree_id, city, species, class, class_avg, cell_trees;"
ONE_RELATION = "select tree_id, city, species, cell_trees;"


def executor(published: bool = True) -> Executor:
    engine = Dialects.DUCK_DB.default_executor()
    engine.execute_text(MODEL.replace("{published}", str(published).lower()))
    return engine


def materialized(sql: str) -> set[str]:
    return set(re.findall(r"^(\w+) as MATERIALIZED \(", sql, re.MULTILINE))


def joined_on(sql: str, *keys: str) -> set[str]:
    """The CTEs a join reads every one of `keys` off, on its left."""
    found: set[str] = set()
    for clause in re.findall(r"JOIN .* on (.*)", sql):
        sides = [set(re.findall(rf'"(\w+)"\."{key}" = ', clause)) for key in keys]
        found |= set.intersection(*sides)
    return found


def test_keys_assembled_by_a_join_are_read_off_a_barrier():
    engine = executor()
    sql = engine.generate_sql(SPLIT_KEYS)[-1]
    sides = joined_on(sql, "class", "species")
    assert sides and sides <= materialized(sql), sql
    assert len(engine.execute_text(SPLIT_KEYS)[-1].fetchall()) == 200


def test_keys_of_one_relation_need_no_barrier():
    sql = executor().generate_sql(ONE_RELATION)[-1]
    assert joined_on(sql, "city", "density_cell")
    assert not materialized(sql), sql


def test_other_dialects_state_no_barrier():
    env = Environment()
    env.parse(MODEL.replace("{published}", "true"))
    _, statements = env.parse(SPLIT_KEYS)
    sql = BigqueryDialect().compile_statement(process_query(env, statements[-1]))
    assert "MATERIALIZED" not in sql


def test_empty_stored_value_drops_no_row():
    rows = executor(published=False).execute_text("select tree_id, watermark;")[-1]
    assert sorted(rows.fetchall()) == [(i, None) for i in range(200)]


def test_stored_value_reaches_every_row():
    rows = executor().execute_text("select tree_id, watermark;")[-1].fetchall()
    assert sorted(rows) == [(i, 7) for i in range(200)]


def test_computed_total_still_cross_joins_inner():
    sql = executor().generate_sql("select tree_id, mean_dbh;")[-1]
    assert re.search(r"INNER JOIN .* on 1=1", sql), sql
