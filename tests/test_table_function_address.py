from dataclasses import fields

import pytest

from trilogy import Dialects, Environment, Executor
from trilogy.constants import CONFIG, ParserBackend
from trilogy.core.models.datasource import AddressArgument
from trilogy.core.models.execute import CTE
from trilogy.dialect.table_function import MissingSourceArgumentException
from trilogy.parsing.v2.model import HydrationError

MODEL = """
key latitude float;
key longitude float;
key day int;
property <latitude, longitude, day>.temp float;

datasource forecast (latitude, longitude, day, temp)
grain (latitude, longitude, day)
address forecast_fn(latitude, longitude, unit := 'c');

key city string;
datasource cities (city, lat: latitude, lon: longitude)
grain (city)
address cities;
"""

SETUP = """
create macro forecast_fn(lat, lon, unit := 'f') as table
select range::int as day,
    lat * 10 + range + (case when unit = 'c' then 0 else 1000 end) as temp
from range(3);
create table cities as
select * from (values ('a', 1.5, 2.5), ('b', 3.0, 4.0)) t(city, lat, lon);
"""


@pytest.fixture
def executor() -> Executor:
    ex = Dialects.DUCK_DB.default_executor(environment=Environment())
    ex.execute_raw_sql(SETUP)
    ex.parse_text(MODEL)
    return ex


def rows(executor: Executor, query: str) -> list[tuple]:
    return [tuple(r) for r in executor.execute_text(query)[-1].fetchall()]


@pytest.mark.parametrize("backend", [ParserBackend.LARK, ParserBackend.PEST])
def test_parses_bound_and_fixed_arguments(monkeypatch, backend: ParserBackend):
    monkeypatch.setattr(CONFIG, "parser_backend", backend)
    env = Environment()
    env.parse("""
key lat float;
key note string;
datasource f (lat, note)
grain (lat)
address db.main.fn(lat, 'x', -1.5, true, tz := 'auto',);
""")
    assert env.datasources["f"].address.arguments == [
        AddressArgument(value="lat", bound=True),
        AddressArgument(value="x"),
        AddressArgument(value=-1.5),
        AddressArgument(value=True),
        AddressArgument(value="auto", name="tz"),
    ]


@pytest.mark.parametrize("backend", [ParserBackend.LARK, ParserBackend.PEST])
def test_bound_name_must_be_a_column(monkeypatch, backend: ParserBackend):
    monkeypatch.setattr(CONFIG, "parser_backend", backend)
    with pytest.raises(HydrationError, match="`lat` is not one of its columns"):
        Environment().parse(
            "key latitude float; datasource g (latitude) grain (latitude)"
            " address fn(lat);"
        )


def test_pins_render_as_arguments(executor: Executor):
    query = (
        "where latitude = 1.5 and longitude = 2.5 select day, temp order by day asc;"
    )
    sql = executor.generate_sql(query)[-1]
    assert "\"forecast_fn\"(1.5, 2.5, unit := 'c')" in sql
    assert rows(executor, query) == [(0, 15), (1, 16), (2, 17)]


def test_pins_either_side_and_nested(executor: Executor):
    assert rows(
        executor,
        "where 1.5 = latitude and (longitude = 2.5 and temp > 15)"
        " select day, temp order by day asc;",
    ) == [(1, 16), (2, 17)]


def test_bound_column_is_projected(executor: Executor):
    assert rows(
        executor,
        "where latitude = 1.5 and longitude = 2.5 select latitude, max(temp) as hi;",
    ) == [(1.5, 17)]


def test_joins_on_bound_columns(executor: Executor):
    assert rows(
        executor,
        "where latitude = 1.5 and longitude = 2.5"
        " select city, day, temp order by day asc;",
    ) == [("a", 0, 15), ("a", 1, 16), ("a", 2, 17)]


@pytest.mark.parametrize(
    "where",
    [
        "latitude = 1.5",
        "city = 'a' and latitude = 1.5",
        "(longitude = 1.5 or longitude = 3.0) and latitude = 1.5",
        "longitude > 1.0 and latitude = 1.5",
    ],
)
def test_missing_pin(executor: Executor, where: str):
    with pytest.raises(
        MissingSourceArgumentException,
        match=r"needs `longitude` as an argument\. Add a `where longitude = <literal>`",
    ):
        executor.generate_sql(f"where {where} select day, temp;")


def test_conflicting_pins(executor: Executor):
    with pytest.raises(MissingSourceArgumentException, match=r"to 2 values"):
        executor.generate_sql(
            "where latitude = 1.5 and latitude = 3.0 and longitude = 2.5"
            " select day, temp;"
        )


def test_render_source_without_query_names_argument(executor: Executor):
    datasource = executor.environment.datasources["forecast"]
    with pytest.raises(MissingSourceArgumentException, match="`latitude`"):
        executor.generator.render_source(datasource.address)


def test_no_where_names_first_argument(executor: Executor):
    with pytest.raises(MissingSourceArgumentException, match="`latitude`"):
        executor.generate_sql("select day, temp;")


def test_literal_only_arguments_render_bare_call(executor: Executor):
    executor.execute_raw_sql(
        "create macro days_fn(n) as table select range::int as day from range(n);"
    )
    executor.parse_text("datasource days (day) grain (day) address days_fn(2);")
    assert rows(executor, "select day order by day asc;") == [(0,), (1,)]


def test_namespaced_concept_in_message(executor: Executor):
    executor.environment.parse(MODEL, namespace="w")
    with pytest.raises(
        MissingSourceArgumentException, match="Add a `where w.longitude = <literal>`"
    ):
        executor.generate_sql("where w.latitude = 1.5 select w.day;")


def test_plain_cte_reads_its_base_datasource(executor: Executor, monkeypatch):
    seen: list[CTE] = []
    original = executor.generator.render_table_function

    def spy(address, cte=None):
        seen.append(cte)
        return original(address, cte)

    monkeypatch.setattr(executor.generator, "render_table_function", spy)
    executor.generate_sql("where latitude = 1.5 and longitude = 2.5 select day;")
    [cte] = seen
    plain = CTE(**{f.name: getattr(cte, f.name) for f in fields(CTE) if f.init})
    assert '"forecast_fn"(1.5, 2.5' in original(cte.source_address, plain)
