from datetime import date, timedelta
from pathlib import Path

import pytest

from trilogy import Dialects, Environment


@pytest.mark.skip(reason="Requires BigQuery credentials and setup")
def test_bigquery_etl():
    env = Environment(working_path=Path(__file__).parent)
    # load 10 days ago isoformat
    env.set_parameters(load_date=date.today() - timedelta(days=10))
    executor = Dialects.BIGQUERY.default_executor(environment=env)
    assert env is not None
    executor.execute_file("bq_funnel.preql")


def test_bigquery_etl_sql():
    env = Environment(working_path=Path(__file__).parent)
    # load 10 days ago isoformat
    env.set_parameters(load_date=date.today() - timedelta(days=10))
    executor = Dialects.BIGQUERY.default_executor(environment=env)
    assert env is not None
    cmds = executor.parse_file("bq_funnel.preql")
    for cmd in cmds:
        sql = executor.generate_sql(cmd)
        assert sql is not None
