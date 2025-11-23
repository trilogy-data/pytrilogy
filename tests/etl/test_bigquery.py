from datetime import date, timedelta
from pathlib import Path

from trilogy import Dialects, Environment


def test_bigquery_etl():
    env = Environment(working_path=Path(__file__).parent)
    # load 10 days ago isoformat
    env.set_parameters(load_date=date.today() - timedelta(days=10))
    executor = Dialects.BIGQUERY.default_executor(environment=env)
    assert env is not None
    executor.execute_file("bq_funnel.preql")
