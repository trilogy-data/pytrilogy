"""Ingest and validation agree on DuckDB's column types.

`trilogy ingest` declares a column as `normalize_db_type` of its engine type.
That declaration must validate against the values the engine returns, or an
ingested model fails its own `validate`. A value of another type must not.
"""

import duckdb
import pytest

from trilogy import Dialects
from trilogy.core.enums import ValidationScope
from trilogy.core.models.core import DataType
from trilogy.core.validation.environment import validate_environment

LITERALS = [
    "true",
    "1::TINYINT",
    "1::SMALLINT",
    "1::INTEGER",
    "1::BIGINT",
    "1::HUGEINT",
    "1::UTINYINT",
    "1::USMALLINT",
    "1::UINTEGER",
    "1::UBIGINT",
    "1::UHUGEINT",
    "1.5::REAL",
    "1.5::DOUBLE",
    "1.5::DECIMAL(10,2)",
    "1::DECIMAL(38,0)",
    "'a'",
    "'a'::CHAR(3)",
    "DATE '2020-01-01'",
    "TIMESTAMP '2020-01-01 01:00:00'",
    "TIMESTAMP_S '2020-01-01 01:00:00'",
    "TIMESTAMP_MS '2020-01-01 01:00:00'",
    "TIMESTAMP_NS '2020-01-01 01:00:00'",
    "TIMESTAMPTZ '2020-01-01 01:00:00+00'",
    "uuid()",
    "'a'::BLOB",
    "'101'::BIT",
    "[1, 2]",
    "[1.5]",
    "['a']",
    "MAP {'a': 1}",
]

UNMAPPED = {"TIME", "INTERVAL", "STRUCT(a INTEGER)", "JSON"}


def _validate(declared: str, literal: str) -> list[str]:
    exec = Dialects.DUCK_DB.default_executor()
    exec.parse_text(
        f"key x {declared};\n"
        f"datasource t (x) grain (x) query '''select {literal} as x''';"
    )
    results = validate_environment(
        exec.environment, ValidationScope.DATASOURCES, exec=exec, generate_only=True
    )
    return [str(r.result) for r in results if r.result]


@pytest.mark.parametrize("literal", LITERALS)
def test_ingested_type_validates(literal: str):
    engine_type = duckdb.sql(f"select typeof({literal})").fetchone()[0]
    declared = Dialects.DUCK_DB.default_renderer().normalize_db_type(engine_type)
    assert declared != DataType.UNKNOWN, engine_type
    assert not _validate(declared.value, literal), (engine_type, declared)


@pytest.mark.parametrize("engine_type", sorted(UNMAPPED))
def test_unmapped_types_stay_unknown(engine_type: str):
    renderer = Dialects.DUCK_DB.default_renderer()
    assert renderer.normalize_db_type(engine_type) == DataType.UNKNOWN


@pytest.mark.parametrize(
    "declared", ["int", "bigint", "float", "double", "numeric", "numeric(10,2)"]
)
def test_boolean_is_not_a_number(declared: str):
    assert _validate(declared, "true")


def test_boolean_is_not_an_int_enum_member():
    assert _validate("enum<int>[1, 2]", "true")
