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


def test_resolution_post_materialization():
    env = Environment(working_path=Path(__file__).parent)
    executor = Dialects.BIGQUERY.default_executor(environment=env)
    result = executor.generate_sql("""
 import sales_reporting;
 import order_product_items;

 create if not exists datasource order_product_items;

append order_product_items where order_item.created_at.date = '2021-11-01'::date;
    """)
    insert_query = result[-1]
    # A partitioned append replaces the slices its select covers: stage, clear
    # exactly those keys, insert. The partition column drives the delete and
    # nothing else may be cleared.
    assert "CREATE TEMP TABLE" in insert_query, insert_query
    assert "DELETE FROM" in insert_query and "WHERE EXISTS" in insert_query
    assert "`order_creation_date`" in insert_query.split("DELETE FROM")[1]
    assert "EXECUTE IMMEDIATE" not in insert_query
