from pathlib import Path

import pytest

from trilogy import Dialects, Environment
from trilogy.dialect import DuckDBConfig
from trilogy.hooks import DebuggingHook

BACKUP = """
import launch;

persist launch_info;
        """


def test_refresh():
    exec = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=Path(__file__).parent),
        conf=DuckDBConfig(
            enable_gcs=True,
            enable_python_datasources=True,
        ),
    )
    DebuggingHook()
    sql = exec.generate_sql(
        """
import launch;

persist launch_info;
        """
    )[-1]

    assert "1=1" not in sql, sql


def test_copy():
    exec = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=Path(__file__).parent),
        conf=DuckDBConfig(
            enable_gcs=True,
            enable_python_datasources=True,
        ),
    )
    DebuggingHook()
    sql = exec.generate_sql(
        """
import launch;

show  where launch_date.year = 2025
and site.latitude is not null and site.longitude is not null
select
    flight_id,
    launch_tag,
    success_flag,
    launch_date,
    site.name,
    site.latitude,
    site.longitude,
    org.e_name as launch_org,
    orb_pay,
    vehicle.name
;
        """
    )[-1]

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
    sql = exec.generate_sql(
        """
import launch;

where launch_tag is not null
select
    launch_date,
    coalesce(CASE WHEN org.e_name = '-' then NULL else org.e_name end, org.u_name) as launch_org,
;
        """
    )[-1]

    assert "uv_run" not in sql, sql
