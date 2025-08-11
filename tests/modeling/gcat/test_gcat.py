from pathlib import Path

from trilogy import Dialects, Environment
from trilogy.hooks import DebuggingHook


def test_join():
    DebuggingHook()
    env = Environment(
        working_path=Path(__file__).parent,
    )
    base = Dialects.DUCK_DB.default_executor(environment=env)

    queries = base.parse_text(
        """import launch;


where vehicle.name like '%Falcon%'
select 
platform.class,
# platform.name,
vehicle.name,
count(launch_tag) as launches;"""
    )

    sql = base.generate_sql(queries[-1])
    assert "RIGHT OUTER JOIN" in sql[0], sql[0]


def test_date_filter():

    DebuggingHook()
    env = Environment(
        working_path=Path(__file__).parent,
    )
    base = Dialects.DUCK_DB.default_executor(environment=env)

    queries = base.parse_text(
        """import launch;

where
  launch_date between date_add( current_date(), YEAR, -100) and current_date()
select
  date_trunc( launch_date, year) as year,
  case
    when date_diff(date_add(current_date(), YEAR, -100), current_date(), YEAR) <= 5
    then date_trunc(launch_date, month )
    else null
  end as month,
  count(launch.id) as launch_count,
  sum(payload.mass_kg) as total_payload_mass_kg,
  # avg(count(launch.id)) over order by year rows between 4 preceding and current row as launch_count_5yr_ma,
  # avg(sum(payload.mass_kg)) over order by year rows between 4 preceding and current row as mass_5yr_ma,
  # sum(count(launch.id)) over order by year asc as launch_count_cumulative,
  # sum(sum(payload.mass_kg)) over order by year asc as total_payload_mass_kg_cumulative
order by
  year asc
limit 2000;
"""
    )

    sql = base.generate_sql(queries[-1])
    assert "WHERE" in sql[0], sql[0]
    assert "BETWEEN" in sql[0], sql[0]
    assert "LIMIT 2000" in sql[0], sql[0]
    assert "ORDER BY year ASC" in sql[0], sql[0]
    assert "CASE WHEN" in sql[0], sql[0]