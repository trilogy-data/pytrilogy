from pathlib import Path

from trilogy import Dialects, Environment
from trilogy.core.models.author import Function


def test_datasource_func_namespace():
    env = Environment.from_file(Path(__file__).parent / "entrypoint.preql")
    year_assignment = env.datasources["trip.citibike_trips"].columns[-1]

    assert isinstance(year_assignment.alias, Function)
    assert year_assignment.alias.arguments[0].address.startswith("trip.")


def test_rendering():
    pass


def test_no_outer_join():
    env = Environment(working_path=Path(__file__).parent)
    dialect = Dialects.DUCK_DB.default_executor(environment=env)
    outer_join = """import trips;
  
select  
    start_time.date,
    count(start_time) as total_rides,  
    (sum(duration) / count(duration)) / 60 as avg_trip_duration_minutes  
order by  
    start_time.date asc
limit 100;  
"""
    sql = dialect.generate_sql(outer_join)
    assert len(env.datasources["citibike_trips"].nullable_concepts) == 0
    assert "FULL JOIN" not in sql[-1], sql[-1]
