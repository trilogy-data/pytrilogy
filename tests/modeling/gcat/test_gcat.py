from pathlib import Path

from trilogy import Dialects, Environment
from trilogy.core.models.core import DataType
from trilogy.hooks import DebuggingHook

ROOT = Path(__file__).parent


def test_environment():
    DebuggingHook()
    env = Environment(
        working_path=Path(__file__).parent,
    )
    base = Dialects.DUCK_DB.default_executor(environment=env)
    base.execute_raw_sql(ROOT / "setup.sql")
    base.parse_text(
        """import launch;
"""
    )
    base.validate_environment()


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
    assert "FULL JOIN" in sql[0], sql[0]


def test_date_filter():

    DebuggingHook()
    env = Environment(
        working_path=Path(__file__).parent,
    )
    base = Dialects.DUCK_DB.default_executor(environment=env)

    queries = base.parse_text(
        """import launch;

where
  launch_date between date_sub( current_date(), YEAR, 100) and current_date()
select
  date_trunc( launch_date, year) as year,
  case
    when date_diff(date_add(current_date(), YEAR, -100), current_date(), YEAR) <= 5
    then date_trunc(launch_date, month )
    else null
  end as month,
  count( launch_tag ) as launch_count,
  sum(payload.mass::float) as total_payload_mass_kg,
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
    assert "date_add(current_date(), -100 * INTERVAL 1 year)" in sql[0]


def test_case_key():
    DebuggingHook()
    env = Environment(
        working_path=Path(__file__).parent,
    )
    base = Dialects.DUCK_DB.default_executor(environment=env)

    queries = base.parse_text(
        """import launch;

key launch_filter <- CASE WHEN launch_type_code = 'O' then "Orbital"
WHEN launch_type_code = 'D' then 'Deep Space'
ELSE 'Other' END;

        SELECT 
    vehicle.name,
     vehicle.variant,
       vehicle.class, vehicle.length, vehicle.leo_capacity,
launch_count,
vehicle.family,
vehicle.launch_mass, vehicle.to_thrust,
vehicle.diameter, round(sum(orb_pay),2) as total_mass,
array_to_string(array_agg(launch_filter), ', ') as launch_targets
order by total_mass desc limit 1;
"""
    )

    sql = base.generate_sql(queries[-1])
    assert "_launch_code" in sql[0], sql[0]


def test_nested_calc_failure():
    env = Environment(
        working_path=Path(__file__).parent,
    )
    base = Dialects.DUCK_DB.default_executor(environment=env)

    queries = base.parse_text(
        """import launch_dashboard;

auto filtered_launch <- launch_tag ? success_flag = 'E';

SELECT vehicle.name,
launch_count,
count(launch_tag ? was_complete_success) as successful_launches,
count(filtered_launch) as pad_aborts,



 limit 1;

"""
    )

    sql = base.generate_sql(queries[-1])
    assert "INVALID_REFERENCE_BUG" not in sql[0], sql[0]


def test_equals_comparison():

    env = Environment(
        working_path=Path(__file__).parent,
    )
    base = Dialects.DUCK_DB.default_executor(environment=env)

    queries = base.parse_text(
        """import launch_dashboard;
        where orb_pay is not null
select
  site.state_code,

  log(CASE WHEN sum(orb_pay)::int = 0 then 1 else sum(orb_pay)::int END, 10) as log_scale_orbital_tons,
  launch_count
order by
  log_scale_orbital_tons desc
limit 15;
"""
    )

    sql = base.generate_sql(queries[-1])
    assert """WHEN cast(sum("wakeful"."orb_pay") as int) = 0 THEN 1""" in sql[0], sql[0]


def test_environment_cleanup():
    """
    Test to ensure that the environment cleanup works correctly.
    """
    env = Environment(
        working_path=Path(__file__).parent,
    )
    base = Dialects.DUCK_DB.default_executor(environment=env)
    base.parse_text(
        """import launch_dashboard;

        """
    )
    pre_concepts = set(base.environment.concepts.keys())
    queries = base.parse_text(
        """
        
        SELECT launch_count, 
        count(site.state_code) as countries, 
        current_datetime() as datetime_function,
        current_timestamp() as timestamp_function,
        date_diff(min(launch_date), current_date(), year) as launch_days, 
        struct( first_launch -> min(launch_date), last_launch -> max(launch_date)) as launch_date_range,
        min(launch_date) as min_date;
        """
    )

    query = queries[-1]
    assert "local.datetime_function" in query.locally_derived
    assert (
        base.environment.concepts["local.datetime_function"].datatype
        == DataType.DATETIME
    )
    for c in query.locally_derived:
        base.environment.remove_concept(c)
    post_concepts = set(base.environment.concepts.keys())
    assert (
        pre_concepts == post_concepts
    ), f"Environment cleanup did not remove locally derived concepts: {post_concepts - pre_concepts}"

    queries = base.parse_text(
        """

select
launch_filter,
#launch_count
order by launch_filter asc
;"""
    )


def test_join_inclusion():
    """
    Test to ensure that the environment cleanup works correctly.
    """
    env = Environment(
        working_path=Path(__file__).parent,
    )
    base = Dialects.DUCK_DB.default_executor(environment=env)
    base.parse_text(
        """import launch_dashboard;

        """
    )
    queries = base.parse_text(
        """
import launch_dashboard;
where vehicle.name like '%Falcon%'

SELECT 
vehicle.full_name,
launch_count,
count(launch_tag ? was_complete_success) as successful_launches,
# count(launch_tag ? success_flag = 'E') as pad_aborts,
# count(vehicle.family) by * as all_vehicles,
# round(sum(orb_pay),2) as total_mass,
# array_to_string(array_distinct(array_agg(launch_filter)), ', ') as launch_targets
order by launch_count desc
limit 6;



        """
    )

    sql = base.generate_sql(queries[-1])
    assert (
        'LEFT OUTER JOIN "launch_info" as "launch_info" on "vehicle_lv_info"."LV_Name" = "launch_info"."LV_Type" AND "vehicle_lv_info"."LV_Variant" = "launch_info"."Variant"'
        in sql[0]
    ), sql[0]
