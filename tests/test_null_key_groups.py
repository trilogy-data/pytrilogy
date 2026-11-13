"""NULL-key groups through the joins that assemble a select.

A flight with no `tail_num` is its own group: every model, carrier and
airport attribute read through the aircraft is NULL on it, and its count is
still 1. Each case once lost or doubled that group:

- two sides NULL a key both as a value and as padding for different spans:
  the value NULLs pair null-safely, and the padding guard's witness must be
  NULL only on padded rows (`test_both_value_and_span_padding_pair_their_values`,
  `test_a_value_null_key_beside_another_pairs_null_safely`);
- a value NULL group the other side has none of keeps its row
  (`test_a_value_null_group_with_no_partner_is_kept`);
- a side that NULLs a key through padding the other side lacks is not
  key-set equivalent to it (`test_unshared_padding_never_narrows_to_inner`).
"""

from tests.helpers.rows import fetch_rows
from trilogy import Dialects

MODEL_22 = """
key model_code string;
property model_code.model_name string;
property model_code.model_maker string;
property model_code.seats int;
datasource aircraft_model (model_code: model_code,
    model_name, model_maker, seats) grain (model_code)
query '''SELECT * FROM (VALUES ('M0', '737', 'BOEING', 8)) t(model_code, model_name, model_maker, seats) ''';
key aircraft_id int;
unique property aircraft_id.tail_num string;
key carrier_code string;
property carrier_code.carrier_name string;
datasource carrier (carrier_code, carrier_name) grain (carrier_code)
query '''SELECT * FROM (VALUES ('C0', 'carrier0'), ('C1', 'carrier1')) t(carrier_code, carrier_name) ''';
key airport_id int;
property airport_id.airport_code string;
property airport_id.airport_name string;
datasource airport (airport_id, airport_code, airport_name) grain (airport_id)
query '''SELECT * FROM (VALUES (0, 'P0', 'airport0')) t(airport_id, airport_code, airport_name) ''';
key id int;
datasource flight (id, tail_num: ?tail_num,
    carrier_code: ~carrier_code, airport_code: ~airport_code)
grain (id)
query '''SELECT * FROM (VALUES (0, 'N0', 'C1', 'P0'), (1, 'N1', 'C0', 'P0'), (2, null, 'C0', 'P0')) t(id, tail_num, carrier_code, airport_code) ''';

datasource aircraft_by_tail_num (aircraft_id, tail_num, model_code: ~?model_code)
grain (tail_num)
query '''SELECT * FROM (VALUES (0, 'N0', 'M0'), (1, 'N1', null)) t(aircraft_id, tail_num, model_code) ''';

datasource aircraft_by_aircraft_id (aircraft_id, tail_num, model_code: ~?model_code)
grain (aircraft_id)
query '''SELECT * FROM (VALUES (0, 'N0', 'M0'), (1, 'N1', null)) t(aircraft_id, tail_num, model_code) ''';

datasource flight_count_by_carrier (carrier_code, n: count)
grain (carrier_code)
query '''SELECT * FROM (VALUES ('C0', 2), ('C1', 1)) t(carrier_code, n) ''';
auto count <- count(id);
"""

MODEL_98 = """
key model_code string;
property model_code.model_name string;
property model_code.model_maker string;
property model_code.seats int;
datasource aircraft_model (model_code: model_code,
    model_name, model_maker, seats) grain (model_code)
query '''SELECT * FROM (VALUES ('M0', 'E175', 'BOEING', 6), ('M1', 'A320', 'BOEING', 8)) t(model_code, model_name, model_maker, seats) ''';
key aircraft_id int;
unique property aircraft_id.tail_num string;
key carrier_code string;
property carrier_code.carrier_name string;
datasource carrier (carrier_code, carrier_name) grain (carrier_code)
query '''SELECT * FROM (VALUES ('C0', 'carrier0')) t(carrier_code, carrier_name) ''';
key airport_id int;
property airport_id.airport_code string;
property airport_id.airport_name string;
datasource airport (airport_id, airport_code, airport_name) grain (airport_id)
query '''SELECT * FROM (VALUES (0, 'P0', 'airport0'), (1, 'P1', 'airport1')) t(airport_id, airport_code, airport_name) ''';
key id int;
datasource flight (id, tail_num: ~?tail_num,
    carrier_code: carrier_code, airport_code: airport_code)
grain (id)
query '''SELECT * FROM (VALUES (0, 'N0', 'C0', 'P0'), (1, 'N2', 'C0', 'P0'), (2, 'N1', 'C0', 'P1'), (3, 'N1', 'C0', 'P0'), (4, 'N0', 'C0', 'P1'), (5, null, 'C0', 'P0')) t(id, tail_num, carrier_code, airport_code) ''';

datasource aircraft_by_tail_num (aircraft_id, tail_num, model_code: ~?model_code)
grain (tail_num)
query '''SELECT * FROM (VALUES (0, 'N0', 'M0'), (1, 'N1', 'M1'), (2, 'N2', null)) t(aircraft_id, tail_num, model_code) ''';

datasource aircraft_by_aircraft_id (aircraft_id, tail_num, model_code: ~?model_code)
grain (aircraft_id)
query '''SELECT * FROM (VALUES (0, 'N0', 'M0'), (1, 'N1', 'M1'), (2, 'N2', null)) t(aircraft_id, tail_num, model_code) ''';
"""

MODEL_132 = """
key model_code string;
property model_code.model_name string;
property model_code.model_maker string;
property model_code.seats int;
datasource aircraft_model (model_code: model_code,
    model_name, model_maker, seats) grain (model_code)
query '''SELECT * FROM (VALUES ('M0', '737', 'AIRBUS', 1), ('M1', '737', 'BOEING', 5)) t(model_code, model_name, model_maker, seats) ''';
key aircraft_id int;
unique property aircraft_id.tail_num string;
key carrier_code string;
property carrier_code.carrier_name string;
datasource carrier (carrier_code, carrier_name) grain (carrier_code)
query '''SELECT * FROM (VALUES ('C0', 'carrier0'), ('C1', 'carrier1')) t(carrier_code, carrier_name) ''';
key airport_id int;
property airport_id.airport_code string;
property airport_id.airport_name string;
datasource airport (airport_id, airport_code, airport_name) grain (airport_id)
query '''SELECT * FROM (VALUES (0, 'P0', 'airport0'), (1, 'P1', 'airport1')) t(airport_id, airport_code, airport_name) ''';
key id int;
datasource flight (id, tail_num: ~?tail_num,
    carrier_code: carrier_code, airport_code: ~airport_code)
grain (id)
query '''SELECT * FROM (VALUES (0, 'N2', 'C0', 'P1'), (1, 'N2', 'C1', 'P1'), (2, 'N0', 'C1', 'P0'), (3, 'N0', 'C0', 'P0'), (4, 'N1', 'C1', 'P0'), (5, 'N1', 'C0', 'P1'), (6, null, 'C0', 'P0')) t(id, tail_num, carrier_code, airport_code) ''';

datasource aircraft_by_tail_num (aircraft_id, tail_num, model_code: ?model_code)
grain (tail_num)
query '''SELECT * FROM (VALUES (0, 'N0', 'M1'), (1, 'N1', 'M0'), (2, 'N2', null)) t(aircraft_id, tail_num, model_code) ''';

datasource aircraft_by_aircraft_id (aircraft_id, tail_num, model_code: ~?model_code)
grain (aircraft_id)
query '''SELECT * FROM (VALUES (0, 'N0', 'M1'), (1, 'N1', 'M0'), (2, 'N2', null)) t(aircraft_id, tail_num, model_code) ''';
"""

NARROWED = """
key model_code string;
property model_code.model_maker string;
property model_code.seats int;
datasource aircraft_model (model_code: ~model_code, model_maker, seats)
grain (model_code)
query '''SELECT * FROM (VALUES ('M0', 'AIRBUS', 9)) t(model_code, model_maker, seats)''';
key aircraft_id int;
unique property aircraft_id.tail_num string;
datasource aircraft (aircraft_id, tail_num, model_code) grain (tail_num)
query '''SELECT * FROM (VALUES (0, 'N0', 'M0')) t(aircraft_id, tail_num, model_code)''';
key id int;
datasource flight (id, tail_num: ~?tail_num) grain (id)
query '''SELECT * FROM (VALUES (0, 'N0'), (1, null)) t(id, tail_num)''';
"""


def _rows(model: str, query: str) -> list[tuple]:
    executor = Dialects.DUCK_DB.default_executor()
    executor.parse_text(model)
    rows = [tuple(row) for row in fetch_rows(executor, query)]
    return sorted(rows, key=lambda r: tuple((v is None, str(v)) for v in r))


def test_both_value_and_span_padding_pair_their_values():
    assert _rows(
        MODEL_22,
        "select tail_num, model_name, carrier_name, "
        "sum(group(seats) by id) as s, count(id) as n;",
    ) == [
        ("N0", "737", "carrier1", 8, 1),
        ("N1", None, "carrier0", None, 1),
        (None, None, "carrier0", None, 1),
    ]


def test_a_value_null_key_beside_another_pairs_null_safely():
    assert _rows(
        MODEL_98, "select model_name, model_maker, airport_name, count(id) as n;"
    ) == [
        ("A320", "BOEING", "airport0", 1),
        ("A320", "BOEING", "airport1", 1),
        ("E175", "BOEING", "airport0", 1),
        ("E175", "BOEING", "airport1", 1),
        (None, None, "airport0", 2),
    ]


def test_a_value_null_group_with_no_partner_is_kept():
    assert _rows(
        MODEL_132, "select model_name, airport_name, tail_num, count(id) as n;"
    ) == [
        ("737", "airport0", "N0", 2),
        ("737", "airport0", "N1", 1),
        ("737", "airport1", "N1", 1),
        (None, "airport0", None, 1),
        (None, "airport1", "N2", 2),
    ]


def test_unshared_padding_never_narrows_to_inner():
    assert _rows(NARROWED, "select model_maker, count(id) as n, max(seats) as mx;") == [
        ("AIRBUS", 1, 9),
        (None, 1, None),
    ]
