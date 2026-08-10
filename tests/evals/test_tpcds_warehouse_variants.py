from __future__ import annotations

import sys
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "evals"))

from common import agent_runner
from common.categories import categories_for, funnel_order_for
from tpcds_agent.spec import SPEC
from tpcds_agent.warehouse_variants import (
    _append_aggregate_models,
    _run_sql,
)

from trilogy import Dialects
from trilogy.core.models.environment import Environment
from trilogy.scripts.explore import filter_hidden


def test_tpcds_registers_messy_warehouse_categories():
    categories = categories_for(SPEC)
    expected = {
        "sql_schema_aggregates",
        "enriched_aggregates",
        "sql_schema_noise",
        "enriched_noise",
    }
    assert expected <= categories.keys()
    assert expected <= set(funnel_order_for(SPEC))


def test_noise_variant_adds_unrelated_tables(tmp_path: Path):
    db_path = tmp_path / "noise.duckdb"
    _, table_count = _run_sql(db_path, "noise.sql")

    assert table_count == 12
    with duckdb.connect(str(db_path), read_only=True) as connection:
        assert connection.execute("select count(*) from hr_employee").fetchone() == (
            500,
        )
        assert connection.execute(
            "select count(*) from application_audit_event"
        ).fetchone() == (15000,)


def test_aggregate_models_are_private_and_materialized(tmp_path: Path):
    result = agent_runner.install_enriched_model(
        tmp_path, SPEC.default_enriched_dir, SPEC.enriched_skip_prefixes
    )
    assert result["exit_code"] == 0

    base = Environment(working_path=tmp_path)
    imports = (
        "import raw.store_sales as ss; "
        "import raw.catalog_sales as cs; "
        "import raw.web_sales as ws;"
    )
    base.parse(imports)
    base_public = {address for address, _ in filter_hidden(list(base.concepts.items()))}

    _append_aggregate_models(tmp_path)
    augmented = Environment(working_path=tmp_path)
    augmented.parse(imports)
    augmented_public = {
        address for address, _ in filter_hidden(list(augmented.concepts.items()))
    }

    assert augmented_public == base_public
    assert "ss._warehouse_store_sales_daily" in augmented.datasources

    executor = Dialects.DUCK_DB.default_executor(environment=augmented)
    sql = executor.generate_sql(
        "SELECT ss.sale_date.sk, ss.item.sk, ss.customer.sk, "
        "ss.pos_customer_demographic.sk, ss.pos_household_demographic.sk, "
        "ss.store.sk, ss.promotion.sk, ss.pos_address.sk, "
        "ss._warehouse_sum_ext_sales_price LIMIT 1;"
    )[-1]
    assert "agg_store_sales_daily" in sql
    assert '"store_sales"' not in sql
