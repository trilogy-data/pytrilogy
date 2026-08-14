from __future__ import annotations

import sys
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "evals"))

from common import agent_runner
from common.categories import categories_for, funnel_order_for
from tpcds_agent.spec import SPEC
from tpcds_agent.warehouse_variants import (
    CONFUSABLE_DIM_SNAPSHOTS,
    FACT_TABLES,
    PHYSICAL_TABLE_RENAMES,
    _append_aggregate_models,
    _confusable_ddl,
    _rename_benchmark_tables,
    _retarget_model_addresses,
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
        assert connection.execute(
            "select count(*) from dim_hr_employee"
        ).fetchone() == (500,)
        assert connection.execute(
            "select count(*) from fact_application_audit_event"
        ).fetchone() == (15000,)


def test_benchmark_tables_receive_dimension_and_fact_prefixes(tmp_path: Path):
    db_path = tmp_path / "warehouse.duckdb"
    with duckdb.connect(str(db_path)) as connection:
        for table in PHYSICAL_TABLE_RENAMES:
            connection.execute(f'create table "{table}" (id int)')

    _rename_benchmark_tables(db_path)

    with duckdb.connect(str(db_path), read_only=True) as connection:
        tables = {
            row[0]
            for row in connection.execute(
                "select table_name from information_schema.tables "
                "where table_schema = 'main' and table_type = 'BASE TABLE'"
            ).fetchall()
        }
    assert tables == set(PHYSICAL_TABLE_RENAMES.values())


def _seed_confusable_sources(connection: duckdb.DuckDBPyConnection) -> None:
    """Minimal warehouse shape `_confusable_ddl` needs: every fact (with a
    comment), one aggregate per daily-trap channel, and the snapshot dims."""
    for name in FACT_TABLES:
        connection.execute(
            f'create table "fact_{name}" (ss_item_sk int, ss_quantity int)'
        )
        connection.execute(f"insert into fact_{name} values (1, 2), (3, 4)")
        connection.execute(
            f"COMMENT ON TABLE \"fact_{name}\" IS 'Line-grain {name} rows.'"
        )
        connection.execute(
            f'COMMENT ON COLUMN "fact_{name}"."ss_quantity" IS ' "'Units on the line.'"
        )
    for name in (
        "store_sales",
        "store_returns",
        "catalog_sales",
        "catalog_returns",
        "web_sales",
        "web_returns",
    ):
        connection.execute(
            f'create table "fact_agg_{name}_daily" '
            "(date_sk int, sum_quantity int, count_rows int)"
        )
        connection.execute(f"insert into fact_agg_{name}_daily values (1, 6, 2)")
    for dim in CONFUSABLE_DIM_SNAPSHOTS:
        connection.execute(f'create table "{dim}" (sk int)')
        connection.execute(f"insert into {dim} values (1)")
        connection.execute(f"COMMENT ON TABLE \"{dim}\" IS 'Curated {dim}.'")


def test_confusable_traps_carry_source_comments(tmp_path: Path):
    """Traps must be documented like the tables they were derived from — a
    bare column list next to curated real tables leaks 'this one is fake'."""
    with duckdb.connect(str(tmp_path / "conf.duckdb")) as connection:
        _seed_confusable_sources(connection)
        _confusable_ddl(connection, level=3)

        def table_comment(table: str) -> str | None:
            row = connection.execute(
                "select comment from duckdb_tables() where table_name = ?",
                [table],
            ).fetchone()
            return row[0] if row else None

        assert table_comment("fact_store_sales_v2") == "Line-grain store_sales rows."
        assert table_comment("fact_web_sales_bak") == "Line-grain web_sales rows."
        assert table_comment("dim_item_snapshot") == "Curated dim_item."
        col = connection.execute(
            "select comment from duckdb_columns() "
            "where table_name = 'fact_store_sales_staging' "
            "and column_name = 'ss_quantity'"
        ).fetchone()
        assert col == ("Units on the line.",)
        # Grain trap: docs come from the LINE fact (the masquerade), never the
        # aggregate source, and land on the renamed measure column.
        assert table_comment("fact_store_sales_daily") == (
            "Line-grain store_sales rows."
        )
        daily_col = connection.execute(
            "select comment from duckdb_columns() "
            "where table_name = 'fact_store_sales_daily' "
            "and column_name = 'ss_quantity'"
        ).fetchone()
        assert daily_col == ("Units on the line.",)


def test_bare_confusable_categories_registered():
    categories = categories_for(SPEC)
    for level in (1, 2, 3):
        name = f"sql_bare_confusable_x{level}"
        assert name in categories
        assert name in funnel_order_for(SPEC)
        assert categories[name].harness == "sql"


def test_aggregate_models_are_private_and_materialized(tmp_path: Path):
    result = agent_runner.install_enriched_model(
        tmp_path, SPEC.default_enriched_dir, SPEC.enriched_skip_prefixes
    )
    assert result["exit_code"] == 0

    rewrites = _retarget_model_addresses(tmp_path)
    assert rewrites > 0
    assert "address fact_store_sales;" in (
        tmp_path / "raw" / "store_sales.preql"
    ).read_text(encoding="utf-8")

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
    assert "fact_agg_store_sales_daily" in sql
    assert '"store_sales"' not in sql
