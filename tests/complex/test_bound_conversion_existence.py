from tests.helpers.executor import mock_factory
from trilogy import Dialects
from trilogy.dialect import PrestoConfig


def test_bound_conversion_existence() -> None:
    executor = Dialects.DUCK_DB.default_executor()

    results = executor.execute_text("""

key date_string string;
auto date_converted <- date_string::date;
key id int;

auto latest_date <- max(date_converted) by *;
                          
datasource ds_info(
    date_string,
    id
)
grain (date_string, id)
query '''
select
    '2021-01-01' as date_string,
    1 as id
    union all
    select
    '2021-02-01' as date_string,
    2 as id
    union all
    select
    '2021-02-01' as date_string,
    3 as id
    union all
    select
    '2021-02-01' as date_string,
    4 as id
''';
                          
where date_converted=latest_date and 1=1
select
    date_converted,
    count(id) as id_count
;
""")[-1]

    rows = results.fetchall()[0]
    assert rows.date_converted is not None
    assert rows.date_converted.isoformat() == "2021-02-01"
    assert rows.id_count == 3


def test_bound_conversion_existence_presto() -> None:
    executor = Dialects.PRESTO.default_executor(
        conf=PrestoConfig(
            host="localhost",
            port=8080,
            username="user",
            password="password",
            catalog="default",
        ),
        _engine_factory=mock_factory,
    )

    results = executor.generate_sql("""

key date_string string;
key date_converted <- date_string::date;
key id int;

auto latest_date <- max(date_converted) by *;
                          
datasource ds_info(
    date_string,
    id
)
grain (date_string, id)
query '''
select
    '2021-01-01' as date_string,
    1 as id
    union all
    select
    '2021-02-01' as date_string,
    2 as id
    union all
    select
    '2021-02-01' as date_string,
    3 as id
    union all
    select
    '2021-02-01' as date_string,
    4 as id
''';
                          
where date_converted=latest_date and 1=1
select
    date_converted,
    count(id) as id_count
;
""")[-1]

    assert """WHERE
    "quizzical"."date_converted" = "cheerful"."latest_date" and 1 = 1
GROUP BY 
    2,
    1)""" in results
