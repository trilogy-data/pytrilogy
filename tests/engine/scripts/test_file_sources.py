"""Tests for various file source types (TSV, Parquet, SQL)."""

from pathlib import Path

from trilogy import Dialects, Environment


def test_tsv_source():
    script = """
key id int;
property id.name string;

datasource test_data(
    id,
    name
)
grain (id)
file `./test.tsv`;

select
    id.count as total_ids;
"""

    executor = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=Path(__file__).parent)
    )

    results = executor.execute_text(script)

    assert results[-1].fetchone()[0] == 4


def test_parquet_source():
    script = """
key id int;
property id.name string;

datasource test_data(
    id,
    name
)
grain (id)
file `./test.parquet`;

select
    id.count as total_ids;
"""

    executor = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=Path(__file__).parent)
    )

    results = executor.execute_text(script)

    assert results[-1].fetchone()[0] == 4


def test_sql_source():
    script = """
key id int;
property id.name string;

datasource test_data(
    id,
    name
)
grain (id)
file `./test.sql`;

select
    id.count as total_ids;
"""

    executor = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=Path(__file__).parent)
    )

    results = executor.execute_text(script)

    assert results[-1].fetchone()[0] == 4
