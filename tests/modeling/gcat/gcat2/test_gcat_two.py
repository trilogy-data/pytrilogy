from pathlib import Path

import pytest

from trilogy import Dialects, Environment
from trilogy.dialect import DuckDBConfig
from trilogy.hooks import DebuggingHook

BACKUP = """
import launch;

persist launch_info;
        """


def test_property_binding():
    exec = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=Path(__file__).parent),
        conf=DuckDBConfig(
            enable_gcs=True,
            enable_python_datasources=True,
        ),
    )
    DebuggingHook()
    exec.generate_sql("""
import launch;


        """)

    assert exec.environment.concepts["was_complete_success"].keys == {
        "local.launch_tag"
    }, exec.environment.concepts["was_complete_success"].keys


def test_extra_fields():
    from logging import INFO

    exec = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=Path(__file__).parent),
        conf=DuckDBConfig(
            enable_gcs=True,
            enable_python_datasources=True,
        ),
    )
    DebuggingHook(INFO)
    query = """
    import launch;
where date_part(launch_date, year) = 2010
select
  vehicle.stage.engine.fuel,
  count(launch_tag) as launches,
  avg(vehicle.stage.engine.thrust) as avg_engine_thrust_kn
order by
  launches desc
limit 50;"""
    sql = exec.generate_sql(query)[-1]

    assert "vehicle_data_updated_through_month_start" not in sql, sql


def test_extra_fields_two():
    from logging import INFO

    exec = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=Path(__file__).parent),
        conf=DuckDBConfig(
            enable_gcs=True,
            enable_python_datasources=True,
        ),
    )
    DebuggingHook(INFO)
    query = """
    import launch;
where date_part(launch_date, year) = 2010
select
  vehicle.stage.engine.fuel,
limit 50;"""
    sql = exec.generate_sql(query)[-1]

    assert "vehicle_data_updated_through_month_start" not in sql, sql


def test_refresh():
    exec = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=Path(__file__).parent),
        conf=DuckDBConfig(
            enable_gcs=True,
            enable_python_datasources=True,
        ),
    )
    DebuggingHook()
    sql = exec.generate_sql("""
import launch;

persist launch_info;
        """)[-1]

    assert "1=1" not in sql, sql


def test_copy():
    from logging import INFO

    exec = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=Path(__file__).parent),
        conf=DuckDBConfig(
            enable_gcs=True,
            enable_python_datasources=True,
        ),
    )
    DebuggingHook(INFO)
    sql = exec.generate_sql("""
import launch;

show  
select
    flight_id,
    launch_tag,

    org.e_name
;
        """)[-1]

    assert "1=1" not in sql, sql


@pytest.mark.skip(reason="Need to fix this in a followup")
def test_parquet_selection():
    exec = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=Path(__file__).parent),
        conf=DuckDBConfig(
            enable_gcs=True,
            enable_python_datasources=True,
        ),
    )
    DebuggingHook()
    sql = exec.generate_sql("""
import launch;

where launch_tag is not null
select
    launch_date,
    coalesce(CASE WHEN org.e_name = '-' then NULL else org.e_name end, org.u_name) as launch_org,
;
        """)[-1]

    assert "uv_run" not in sql, sql
