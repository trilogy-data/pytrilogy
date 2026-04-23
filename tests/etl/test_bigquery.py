from datetime import date, timedelta
from logging import INFO
from pathlib import Path

import pytest

from trilogy import Dialects, Environment
from trilogy.hooks.query_debugger import DebuggingHook


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


def test_resolution_post_materialization():
    env = Environment(working_path=Path(__file__).parent)
    executor = Dialects.BIGQUERY.default_executor(environment=env)
    DebuggingHook(INFO)
    result = executor.generate_sql("""
 import sales_reporting;
 import order_product_items;

 create if not exists datasource order_product_items;

append order_product_items where order_item.created_at.date = '2021-11-01'::date;
    """)
    insert_query = result[-1]
    assert (
        "SELECT ARRAY_AGG(DISTINCT order_creation_date)" in insert_query
    ), insert_query
