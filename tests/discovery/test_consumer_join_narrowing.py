"""A merge narrows its outer joins on what its consumers' join equalities
reject (handoff 2.1 step 4): a NULL never matches `=`, so a row padded on a
column a consumer joins on reaches nothing above it. The optimizer's join
upgrade made these after planning (`_inner_pair_rejections`)."""

from trilogy import Dialects
from trilogy.core.enums import JoinType
from trilogy.core.models.execute import Join, QueryDatasource
from trilogy.core.query_processor import get_query_datasources

MODEL = """
key model_code string;
property model_code.model_name string;
property model_code.seats int;
datasource aircraft_model (model_code: ~model_code, model_name, seats)
grain (model_code)
query '''SELECT * FROM (VALUES ('M0', 'A320', 3), ('M1', 'E175', 6)) t(model_code, model_name, seats)''';
key aircraft_id int;
unique property aircraft_id.tail_num string;
key carrier_code string;
property carrier_code.carrier_name string;
datasource carrier (carrier_code, carrier_name) grain (carrier_code)
query '''SELECT * FROM (VALUES ('C0', 'carrier0'), ('C1', 'carrier1')) t(carrier_code, carrier_name)''';
key id int;
datasource flight (id, tail_num: ~tail_num, carrier_code: ~carrier_code)
grain (id)
query '''SELECT * FROM (VALUES (0, 'N0', 'C0'), (1, 'N1', 'C0'), (2, 'N0', 'C0'), (3, 'N2', 'C0')) t(id, tail_num, carrier_code)''';
datasource aircraft (aircraft_id, tail_num, model_code: ~model_code)
grain ({grain})
query '''SELECT * FROM (VALUES (0, 'N0', 'M1'), (1, 'N1', 'M0'), (2, 'N2', 'M0'), (3, 'N3', 'M1')) t(aircraft_id, tail_num, model_code)''';
"""


def _executor(grain: str):
    executor = Dialects.DUCK_DB.default_executor()
    executor.parse_text(MODEL.replace("{grain}", grain))
    return executor


def _join_types(ds: QueryDatasource) -> list[tuple[str, JoinType]]:
    out = [
        (join.right.identifier, join.join_type)
        for join in ds.joins
        if isinstance(join, Join)
    ]
    for source in ds.datasources:
        if isinstance(source, QueryDatasource):
            out += _join_types(source)
    return out


def _planned(executor, query: str) -> list[tuple[str, JoinType]]:
    env = executor.environment
    _, statements = env.parse(query)
    return _join_types(get_query_datasources(env, statements[-1]))


def _rows(executor, query: str) -> list[tuple]:
    sql = executor.generate_sql(query)[-1]
    rows = executor.execute_raw_sql(sql).fetchall()
    return sorted((tuple(r) for r in rows), key=repr)


def test_a_consumer_join_on_a_partial_column_narrows_the_merge():
    """The carrier merge reads the flights as its padded side, keyed on
    `carrier_code`, which the flights alone supply: an aircraft with no
    flight is padded there and never joins a carrier."""
    executor = _executor("aircraft_id")
    query = "select model_name, carrier_name, count(id) as n;"
    flights = [t for name, t in _planned(executor, query) if name.startswith("flight")]
    assert flights == [JoinType.INNER], _planned(executor, query)
    assert _rows(executor, query) == [
        ("A320", "carrier0", 2),
        ("E175", "carrier0", 2),
        (None, "carrier1", 0),
    ]


REGROUP = """
key model_code string;
property model_code.seats int;
datasource aircraft_model (model_code: ~model_code, seats) grain (model_code)
query '''SELECT * FROM (VALUES ('M0', 7)) t(model_code, seats)''';
key aircraft_id int;
unique property aircraft_id.tail_num string;
key carrier_code string;
property carrier_code.carrier_name string;
datasource carrier (carrier_code, carrier_name) grain (carrier_code)
query '''SELECT * FROM (VALUES ('C0', 'carrier0')) t(carrier_code, carrier_name)''';
key id int;
datasource flight (id, tail_num, carrier_code: ~carrier_code) grain (id)
query '''SELECT * FROM (VALUES (0, 'N0', 'C0'), (1, 'N0', 'C0'), (2, 'N0', 'C0')) t(id, tail_num, carrier_code)''';
datasource aircraft (aircraft_id, tail_num, model_code: ~model_code) grain (tail_num)
query '''SELECT * FROM (VALUES (0, 'N0', 'M0')) t(aircraft_id, tail_num, model_code)''';
"""


def test_a_regroup_by_its_own_key_folds_into_the_narrowed_merge():
    """The flights' INNER on the regroup's `id` narrows the seats' FULL, so
    the per-flight regroup of seats is a projection: it renders in the merge
    it reads, not as a CTE of its own."""
    executor = Dialects.DUCK_DB.default_executor()
    executor.parse_text(REGROUP)
    query = "select carrier_name, sum(group(seats) by id) as s;"
    sql = executor.generate_sql(query)[-1]
    assert "FULL JOIN" not in sql, sql
    assert sql.count(" as (") == 6, sql
    assert _rows(executor, query) == [("carrier0", 21)]
