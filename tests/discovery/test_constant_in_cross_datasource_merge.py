"""A const referenced in a derived concept's CASE output branch must not
block cross-datasource resolution: constants have no datasource edges, so
they must be bucketed out of the merge-node graph search (like the root
merge already does) rather than fed to pathfinding as mandatory nodes."""

from trilogy import Dialects, Environment

MODEL = """
key passenger_id int;
property passenger_id.cabin string?;
property passenger_id.class_id int;

key class_id int;
property class_id.passenger_class int;

datasource fact_passengers (
    passenger_id,
    cabin,
    class_id: passenger_id.class_id
)
grain (passenger_id)
query '''
select * from (
    values
        (1, 'B96 B98', 10),
        (2, null,      30)
) as t(passenger_id, cabin, class_id)
''';

datasource dim_class (
    class_id,
    passenger_class
)
grain (class_id)
query '''
select * from (
    values
        (10, 1),
        (30, 3)
) as t(class_id, passenger_class)
''';

merge passenger_id.class_id into class_id;

const unknown_cabin <- 'Unknown';
"""


def fetch(query: str) -> list[tuple]:
    env = Environment()
    env.parse(MODEL)
    executor = Dialects.DUCK_DB.default_executor(environment=env)
    return [tuple(r) for r in executor.execute_query(query).fetchall()]


def test_constant_in_case_output_with_dim_join():
    rows = fetch("""auto cabin_type <- case
            when cabin is null then unknown_cabin
            else substring(cabin, 1, 1)
        end;
        select passenger_class, cabin_type
        order by passenger_class asc;""")
    assert rows == [(1, "B"), (3, "Unknown")]


def test_constant_in_case_condition_with_dim_join():
    rows = fetch("""auto cabin_type <- case
            when coalesce(cabin, unknown_cabin) = unknown_cabin then 'missing'
            else substring(cabin, 1, 1)
        end;
        select passenger_class, cabin_type
        order by passenger_class asc;""")
    assert rows == [(1, "B"), (3, "missing")]


def test_constant_through_def_macro_unnest_and_aggregate():
    rows = fetch("""def get_cabin_type(cabin: string) ->
            case
                when cabin = unknown_cabin then unknown_cabin
                else substring(cabin, 1, 1)
            end;
        auto cabin_value <- unnest(split(coalesce(cabin, unknown_cabin), ' '));
        auto cabin_type <- @get_cabin_type(coalesce(cabin, unknown_cabin));
        select passenger_class, cabin_type, count(cabin_value) as cabin_assignments
        order by passenger_class asc;""")
    assert rows == [(1, "B", 2), (3, "Unknown", 1)]
