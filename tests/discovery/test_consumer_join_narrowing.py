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


KEYED = """
key model_code string;
property model_code.seats int;
datasource aircraft_model (model_code, seats) grain (model_code)
query '''SELECT * FROM (VALUES ('M0', 9), ('M1', 3), ('M2', 1)) t(model_code, seats)''';
key aircraft_id int;
unique property aircraft_id.tail_num string;
key carrier_code string;
property carrier_code.carrier_name string;
datasource carrier (carrier_code, carrier_name) grain (carrier_code)
query '''SELECT * FROM (VALUES ('C0', 'carrier0')) t(carrier_code, carrier_name)''';
key airport_id int;
property airport_id.airport_code string;
property airport_id.airport_name string;
datasource airport (airport_id, airport_code, airport_name) grain (airport_id)
query '''SELECT * FROM (VALUES (0, 'P0', 'airport0'), (1, 'P1', 'airport1')) t(airport_id, airport_code, airport_name)''';
key id int;
datasource flight (id, tail_num: ~tail_num, carrier_code, airport_code: ~airport_code)
grain (id)
query '''SELECT * FROM (VALUES (0, 'N0', 'C0', 'P0'), (1, 'N0', 'C0', 'P0'), (2, 'N2', 'C0', 'P0'), (3, 'N1', 'C0', 'P0')) t(id, tail_num, carrier_code, airport_code)''';
datasource aircraft (aircraft_id, tail_num, model_code: ~model_code) grain (tail_num)
query '''SELECT * FROM (VALUES (0, 'N0', 'M0'), (1, 'N1', 'M1'), (2, 'N2', 'M2'), (3, 'N3', 'M1')) t(aircraft_id, tail_num, model_code)''';
"""


def _keyed():
    executor = Dialects.DUCK_DB.default_executor()
    executor.parse_text(KEYED)
    return executor


def test_a_consumer_join_on_a_key_read_off_one_side_narrows_the_merge():
    """The airports' merge pads the aircraft on `tail_num`, a join key it
    reads off the aircraft alone (the flights bind it `~`): the aircraft
    lookup above joins on it, so a padded row reaches nothing."""
    executor = _keyed()
    query = "select tail_num, airport_name, count(id) as n;"
    planned = _planned(executor, query)
    assert JoinType.LEFT_OUTER not in [t for _, t in planned[1:]], planned
    assert _rows(executor, query) == [
        ("N0", "airport0", 2),
        ("N1", "airport0", 1),
        ("N2", "airport0", 1),
        ("N3", None, 0),
    ]


def test_a_consumer_inner_join_after_a_full_narrows_it_to_a_left():
    """The flights' INNER reads the aircraft's `tail_num`, so a model with no
    aircraft (padded on the aircraft) is dropped: the FULL keeps only the
    aircraft's padding, even after the merge has ordered its sources."""
    executor = _executor("aircraft_id")
    query = "select model_name, carrier_name, count(id) as n;"
    planned = [t for _, t in _planned(executor, query)]
    assert JoinType.FULL not in planned, planned
    assert _rows(executor, query) == [
        ("A320", "carrier0", 2),
        ("E175", "carrier0", 2),
        (None, "carrier1", 0),
    ]


CHAINED = """
key model_code string;
property model_code.model_name string;
property model_code.seats int;
datasource aircraft_model (model_code: ~model_code, model_name, seats)
grain (model_code)
query '''SELECT * FROM (VALUES ('M0', 'A320', 2)) t(model_code, model_name, seats)''';
key aircraft_id int;
unique property aircraft_id.tail_num string;
key carrier_code string;
property carrier_code.carrier_name string;
datasource carrier (carrier_code, carrier_name) grain (carrier_code)
query '''SELECT * FROM (VALUES ('C0', 'carrier0')) t(carrier_code, carrier_name)''';
key airport_id int;
property airport_id.airport_code string;
property airport_id.airport_name string;
datasource airport (airport_id, airport_code, airport_name) grain (airport_id)
query '''SELECT * FROM (VALUES (0, 'P0', 'airport0'), (1, 'P1', 'airport1')) t(airport_id, airport_code, airport_name)''';
key id int;
datasource flight (id, tail_num: ?tail_num, carrier_code: ~carrier_code, airport_code)
grain (id)
query '''SELECT * FROM (VALUES (0, 'N0', 'C0', 'P0'), (1, 'N0', 'C0', 'P1'), (2, null, 'C0', 'P0')) t(id, tail_num, carrier_code, airport_code)''';
datasource aircraft (aircraft_id, tail_num, model_code: ~model_code)
grain (aircraft_id)
query '''SELECT * FROM (VALUES (0, 'N0', 'M0')) t(aircraft_id, tail_num, model_code)''';
"""


def test_a_narrowed_merge_lets_its_consumer_narrow_the_merge_it_joins():
    """The airports' merge narrows on the carrier lookup, which leaves its
    `id` unpadded, so its INNER on the per-flight seats regroup pairs on
    plain `=`: that rejects the seats' FULL padding, which a second round
    narrows to a LEFT."""
    executor = Dialects.DUCK_DB.default_executor()
    executor.parse_text(CHAINED)
    query = (
        "select airport_name, model_name, carrier_name, sum(group(seats) by id) as s;"
    )
    planned = [t for _, t in _planned(executor, query)]
    assert JoinType.FULL not in planned, planned
    assert _rows(executor, query) == [
        ("airport0", "A320", "carrier0", 2),
        ("airport0", None, "carrier0", None),
        ("airport1", "A320", "carrier0", 2),
    ]


def test_a_where_through_a_computed_flag_narrows_the_merge_under_it():
    """`returned` keeps the orders whose `is_returned`, a flag computed over
    the returns' column, holds: forced true, it rejects every order the
    returns pad."""
    executor = Dialects.DUCK_DB.default_executor()
    executor.parse_text("""
key order_id int;
property order_id.returned_id int;
auto is_returned <- returned_id is not null;
datasource orders (id: order_id) grain (order_id)
query '''SELECT * FROM (VALUES (1), (2), (3)) t(id)''';
datasource returns (id: ~order_id, r: returned_id) grain (order_id)
query '''SELECT * FROM (VALUES (2, 20)) t(id, r)''';
auto returned <- order_id ? is_returned;
""")
    query = "where order_id not in returned select count(order_id) as n;"
    assert JoinType.LEFT_OUTER not in [t for _, t in _planned(executor, query)]
    assert _rows(executor, query) == [(2,)]


SUBSET = """
key cust_id int;
property cust_id.gender string;
datasource customers (id: cust_id, g: gender) grain (cust_id)
query '''SELECT * FROM (VALUES (1, 'F'), (2, 'F'), (3, 'M'), (4, 'M')) t(id, g)''';
key ws_id int;
property ws_id.ws_cust int;
datasource web_sales (id: ws_id, c: ws_cust) grain (ws_id)
query '''SELECT * FROM (VALUES (1, 1), (2, 2), (3, 3)) t(id, c)''';
key cs_id int;
property cs_id.cs_cust int;
datasource catalog_sales (id: cs_id, c: cs_cust) grain (cs_id)
query '''SELECT * FROM (VALUES (1, 1), (2, 3)) t(id, c)''';
"""


def test_a_where_through_a_passthrough_of_its_own_relation_narrows_the_merge():
    """The count's regroup is read through a joinless merge rendered as the
    same relation; the walk passes over that edge, so the WHERE on the
    rowset's key narrows the subset join under it."""
    executor = Dialects.DUCK_DB.default_executor()
    executor.parse_text(SUBSET)
    query = """with other as union((select ws_cust as csk), (select cs_cust as csk)) -> (csk);
where other.csk is not null
select gender, count(cust_id) as n
subset join other.csk = cust_id;"""
    assert JoinType.LEFT_OUTER not in [t for _, t in _planned(executor, query)]
    assert _rows(executor, query) == [("F", 2), ("M", 1)]


TAILS = """
key model_code string;
property model_code.model_name string;
property model_code.model_maker string;
property model_code.seats int;
datasource aircraft_model (model_code: model_code,
    model_name, model_maker, seats) grain (model_code)
query '''SELECT * FROM (VALUES ('M0', '737', 'BOEING', 9), ('M1', 'A320', 'BOEING', 3), ('M2', 'E175', 'BOEING', 1)) t(model_code, model_name, model_maker, seats)''';
key aircraft_id int;
unique property aircraft_id.tail_num string;
key carrier_code string;
property carrier_code.carrier_name string;
datasource carrier (carrier_code, carrier_name) grain (carrier_code)
query '''SELECT * FROM (VALUES ('C0', 'carrier0')) t(carrier_code, carrier_name)''';
key airport_id int;
property airport_id.airport_code string;
property airport_id.airport_name string;
datasource airport (airport_id, airport_code, airport_name) grain (airport_id)
query '''SELECT * FROM (VALUES (0, 'P0', 'airport0')) t(airport_id, airport_code, airport_name)''';
key id int;
datasource flight (id, tail_num: ~tail_num,
    carrier_code: carrier_code, airport_code: airport_code)
grain (id)
query '''SELECT * FROM (VALUES (0, 'N0', 'C0', 'P0'), (1, 'N0', 'C0', 'P0'), (2, 'N2', 'C0', 'P0'), (3, 'N1', 'C0', 'P0')) t(id, tail_num, carrier_code, airport_code)''';
datasource aircraft_by_tail_num (aircraft_id, tail_num, model_code: ~model_code)
grain (tail_num)
query '''SELECT * FROM (VALUES (0, 'N0', 'M0'), (1, 'N1', 'M1'), (2, 'N2', 'M2'), (3, 'N3', 'M2')) t(aircraft_id, tail_num, model_code)''';
datasource aircraft_by_aircraft_id (aircraft_id, tail_num, model_code: model_code)
grain (aircraft_id)
query '''SELECT * FROM (VALUES (0, 'N0', 'M0'), (1, 'N1', 'M1'), (2, 'N2', 'M2'), (3, 'N3', 'M2')) t(aircraft_id, tail_num, model_code)''';
datasource flight_count_by_carrier (carrier_code, n: count)
grain (carrier_code)
query '''SELECT * FROM (VALUES ('C0', 4)) t(carrier_code, n)''';
auto count <- count(id);
"""


def test_a_narrowing_that_widens_a_consumer_is_backed_out():
    """Narrowed on its consumer's INNER on `id`, the seats merge drops the
    aircraft no flight names, so the tail lookup above re-resolves into a
    LEFT from the aircraft that keeps them: N3 would come back."""
    executor = Dialects.DUCK_DB.default_executor()
    executor.parse_text(TAILS)
    query = "select tail_num, carrier_name, sum(group(seats) by id) as s;"
    assert _rows(executor, query) == [
        ("N0", "carrier0", 18),
        ("N1", "carrier0", 3),
        ("N2", "carrier0", 1),
    ]
