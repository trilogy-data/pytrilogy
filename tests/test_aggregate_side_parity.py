"""Stage 2 elects each merge's bridges over the sides the build will merge.

`sum(group(seats) by id)` beside `model_name` reads the flight rows (`id`,
`model_code`) only for `id`, which the `group_to` sibling already carries, so
the build drops that row feeder (`_may_cover`). Stage 2 counted it as a side
and so elected no bridge, leaving the group_to and the model extent with no
key in common. And when region domains join the solid rows, the solid side
must keep the domains' spans to pair on.
"""

from trilogy import Dialects

MODEL = """
key model_code string;
property model_code.model_name string;
property model_code.seats int;
datasource aircraft_model (model_code, model_name, seats) grain (model_code)
query '''SELECT 'M1' model_code, '737' model_name, 150 seats''';

key aircraft_id int;
unique property aircraft_id.tail_num string;
datasource aircraft (aircraft_id, tail_num, model_code: ~?model_code)
grain (tail_num)
query '''SELECT 1 aircraft_id, 'N1' tail_num, 'M1' model_code''';

key airport_id int;
property airport_id.airport_code string;
property airport_id.airport_name string;
datasource airport (airport_id, airport_code, airport_name) grain (airport_id)
query '''SELECT 1 airport_id, 'SFO' airport_code, 'San Fran' airport_name''';

key carrier_code string;
property carrier_code.carrier_name string;
datasource carrier (carrier_code, carrier_name) grain (carrier_code)
query '''SELECT 'UA' carrier_code, 'United' carrier_name''';

key id int;
auto flight_count <- count(id);
datasource flight (
    id,
    carrier_code: ~carrier_code,
    airport_code: ~airport_code,
    tail_num: ~tail_num,
)
grain (id)
query '''
SELECT 1 id, 'UA' carrier_code, 'SFO' airport_code, 'N1' tail_num UNION ALL
SELECT 2, 'UA', 'SFO', 'N1'
''';
"""


def _run(query: str) -> list[tuple]:
    executor = Dialects.DUCK_DB.default_executor()
    executor.parse_text(MODEL)
    return [tuple(row) for row in executor.execute_text(query)[-1].fetchall()]


def test_a_dropped_row_feeder_is_elected_as_the_bridge():
    assert _run("select model_name, sum(group(seats) by id) as s;") == [("737", 300)]


def test_the_solid_rows_keep_the_region_spans():
    assert _run(
        "select airport_name, flight_count, model_name, "
        "sum(group(seats) by id) as total_seats, carrier_name;"
    ) == [("San Fran", 2, "737", 300, "United")]
