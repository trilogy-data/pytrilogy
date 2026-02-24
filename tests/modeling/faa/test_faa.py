from pathlib import Path

from pytest import raises

from trilogy import Dialects, Environment
from trilogy.core.exceptions import InvalidSyntaxException
from trilogy.hooks import DebuggingHook


def test_query_gen():
    """Make sure we inject another group by when conditions forced an evaluation with an early grain"""
    DebuggingHook()
    x = Environment(working_path=Path(__file__).parent)

    x = Dialects.DUCK_DB.default_executor(environment=x)

    sql = x.generate_sql(
        """import flight;

where local.dep_time.month_start between '2001-12-31'::date and '2002-03-31'::date  
select 
    count(carrier.name) as carrier_count;
    """
    )[-1]
    # if we don't have this group by, we will get the wrong result
    assert (
        '''GROUP BY 
    "carrier_carrier"."code",
    "carrier_carrier"."name"'''
        in sql
    )


def test_helpful_error():
    """Make sure we raise a helpful error when we have a join with no grain"""
    DebuggingHook()
    x = Environment(working_path=Path(__file__).parent)

    x = Dialects.DUCK_DB.default_executor(environment=x)
    with raises(InvalidSyntaxException) as e:
        x.generate_sql(
            """import flight;
        
select
    max(dep_time.year_start) as max_year,
    min(dep_time.year_start) min_year;
    """
        )
    assert "AS " in str(e.value)
    assert "\\n" not in e.value.args[0]


def test_hidden_field():
    """Make sure hidden fields are not included in select * expansions"""
    DebuggingHook()
    x = Environment(working_path=Path(__file__).parent)

    x = Dialects.DUCK_DB.default_executor(environment=x)

    sql = x.generate_sql(
        """import flight;
        import flight as flight;

where flight.carrier.name = 'Delta Air Lines'
select
    flight.origin.code,
    flight.destination.latitude,
    flight.destination.longitude,
    flight.destination.code,
    flight.total_distance,
    --flight.count as flight_count,
    avg(flight.aircraft.aircraft_model.seats) as avg_plane_size
order by flight_count desc
limit 100;
    """
    )[-1]
    assert '"juicy"."flight_count" desc' in sql, sql
