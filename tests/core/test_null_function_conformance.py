"""Each function's NULL behaviour, as the null analyses classify it
(formal/Trilogy/NullReject.lean), checked against its DuckDB rendering.

- ``NULL_STRICT_FUNCTIONS`` (`Fn.Strict`): a NULL argument gives NULL, so a
  non-null result proves every argument non-null.
- a strict function outside ``NULL_INTRODUCING_FUNCTIONS`` (`Fn.Total`) is
  non-null on non-null arguments, edge values included.
- a member of ``NULL_INTRODUCING_FUNCTIONS`` has non-null arguments giving NULL.
"""

import itertools
from typing import Any

import duckdb
import pytest

from trilogy.core.enums import (
    NULL_INTRODUCING_FUNCTIONS,
    NULL_STRICT_FUNCTIONS,
    FunctionType,
)
from trilogy.core.models.core import DataType
from trilogy.dialect.duckdb import DuckDBDialect

F = FunctionType

VALUES: dict[str, tuple[DataType, str, list[str]]] = {
    "int": (DataType.INTEGER, "(NULL::INT)", ["0", "(-1)", "3"]),
    "float": (DataType.FLOAT, "(NULL::DOUBLE)", ["0.0", "(-1.0)", "2.5"]),
    "str": (DataType.STRING, "(NULL::VARCHAR)", ["''", "'a'", "'ab,c'"]),
    "date": (DataType.DATE, "(NULL::DATE)", ["DATE '2020-02-29'"]),
    "ts": (DataType.DATETIME, "(NULL::TIMESTAMP)", ["TIMESTAMP '2020-02-29 10:00:00'"]),
    "list": (
        DataType.ARRAY,
        "(NULL::INT[])",
        ["([]::INT[])", "[1, 2]", "([NULL, 1]::INT[])"],
    ),
    "map": (DataType.MAP, "(NULL::MAP(VARCHAR, INT))", ["(MAP {'a': 1})"]),
    "struct": (DataType.STRUCT, "(NULL::STRUCT(f INT))", ["({'f': 1})"]),
}


def lit(*values: str) -> tuple[str, ...]:
    return values


# An argument is a VALUES kind, or a tuple of literal renderings for an option
# argument (a date part, a format) that is never NULL.
CASES: dict[FunctionType, list[str | tuple[str, ...]]] = {
    F.ADD: ["int", "int"],
    F.SUBTRACT: ["int", "int"],
    F.MULTIPLY: ["int", "int"],
    F.DIVIDE: ["int", "int"],
    F.MOD: ["int", "int"],
    F.POWER: ["float", "float"],
    F.ABS: ["int"],
    F.SQRT: ["float"],
    F.FLOOR: ["float"],
    F.CEIL: ["float"],
    F.ROUND: ["float", lit("1")],
    F.LOG: ["float", lit("10", "2")],
    F.CONCAT_STRICT: ["str", "str"],
    F.UPPER: ["str"],
    F.LOWER: ["str"],
    F.TRIM: ["str"],
    F.LTRIM: ["str"],
    F.RTRIM: ["str"],
    F.SUBSTRING: ["str", "int", "int"],
    F.STRPOS: ["str", "str"],
    F.CONTAINS: ["str", "str"],
    F.REPLACE: ["str", "str", "str"],
    F.HASH: ["str", lit("'md5'", "'sha256'")],
    F.HEX: ["str"],
    F.SPLIT: ["str", lit("','")],
    F.LENGTH: ["str"],
    F.REGEXP_CONTAINS: ["str", lit("'a'")],
    F.REGEXP_EXTRACT: ["str", lit("'(b)'"), lit("1")],
    F.REGEXP_REPLACE: ["str", lit("'a'"), "str"],
    F.CAST: ["str", lit("INTEGER")],
    F.DATE: ["ts"],
    F.DATETIME: ["date"],
    F.TIMESTAMP: ["date"],
    F.YEAR: ["date"],
    F.MONTH: ["date"],
    F.DAY: ["date"],
    F.QUARTER: ["date"],
    F.WEEK: ["date"],
    F.DAY_OF_WEEK: ["date"],
    F.HOUR: ["ts"],
    F.MINUTE: ["ts"],
    F.SECOND: ["ts"],
    F.MONTH_NAME: ["date"],
    F.DAY_NAME: ["date"],
    F.FORMAT_TIME: ["ts", lit("'%Y'")],
    F.PARSE_TIME: ["str", lit("'%Y'")],
    F.DATE_PART: ["date", lit("year")],
    F.DATE_TRUNCATE: ["date", lit("month")],
    F.DATE_ADD: ["date", lit("day"), "int"],
    F.DATE_SUB: ["date", lit("day"), "int"],
    F.DATE_DIFF: ["date", "date", lit("day")],
    F.INDEX_ACCESS: ["list", "int"],
    F.MAP_ACCESS: ["map", "str"],
    F.ATTR_ACCESS: ["struct", lit("'f'")],
    F.ARRAY_SUM: ["list"],
    F.ARRAY_DISTINCT: ["list"],
    F.ARRAY_SORT: ["list"],
    F.ARRAY_TO_STRING: ["list", lit("','")],
    F.ARRAY_TRANSFORM: ["list", lit("x"), lit("x + 1")],
    F.ARRAY_FILTER: ["list", lit("x"), lit("x > 1")],
    F.GENERATE_ARRAY: ["int", "int", lit("1")],
    F.MAP_KEYS: ["map"],
    F.MAP_VALUES: ["map"],
    F.NULLIF: ["int", "int"],
    F.STDDEV: ["int"],
    F.VARIANCE: ["int"],
}

# Strict by construction (they render their argument, or a value pinned to
# its anchors), or not renderable on DuckDB without an extension.
UNPROBED = {
    F.ALIAS,
    F.PARENTHETICAL,
    F.GROUP,
    F.GRAIN_PIN,
    F.UNIX_TO_TIMESTAMP,
    F.GEO_FROM_TEXT,
    F.GEO_POINT,
    F.GEO_DISTANCE,
    F.GEO_X,
    F.GEO_Y,
    F.GEO_CENTROID,
    F.GEO_TRANSFORM,
}

# NULL from non-null arguments on another dialect only: DuckDB's
# regexp_extract gives '' on no match (BigQuery gives NULL), and a struct
# field is NULL only when stored so.
NULL_ELSEWHERE = {F.REGEXP_EXTRACT, F.ATTR_ACCESS}


def _evaluate(op: FunctionType, args: list[str], kinds: list[Any]) -> Any:
    types = [VALUES[k][0] if isinstance(k, str) else DataType.STRING for k in kinds]
    sql = DuckDBDialect.FUNCTION_MAP[op](args, types)
    try:
        return duckdb.connect().execute(f"select {sql}").fetchall()[0][0]
    except duckdb.Error:
        return "error"


def _pools(kinds: list[Any]) -> list[list[str]]:
    return [VALUES[k][2] if isinstance(k, str) else list(k) for k in kinds]


def test_every_strict_function_is_probed() -> None:
    assert NULL_STRICT_FUNCTIONS - UNPROBED <= set(CASES)
    assert NULL_INTRODUCING_FUNCTIONS <= set(CASES)


@pytest.mark.parametrize(
    "op", sorted(NULL_STRICT_FUNCTIONS - UNPROBED, key=lambda f: f.value)
)
def test_null_argument_gives_null(op: FunctionType) -> None:
    kinds = CASES[op]
    base = [pool[-1] for pool in _pools(kinds)]
    for idx, kind in enumerate(kinds):
        if not isinstance(kind, str):
            continue
        args = list(base)
        args[idx] = VALUES[kind][1]
        assert _evaluate(op, args, kinds) is None, (op, args)


@pytest.mark.parametrize(
    "op",
    sorted(
        NULL_STRICT_FUNCTIONS - UNPROBED - NULL_INTRODUCING_FUNCTIONS,
        key=lambda f: f.value,
    ),
)
def test_total_function_is_non_null_on_values(op: FunctionType) -> None:
    kinds = CASES[op]
    for args in itertools.product(*_pools(kinds)):
        assert _evaluate(op, list(args), kinds) is not None, (op, args)


@pytest.mark.parametrize(
    "op", sorted(NULL_INTRODUCING_FUNCTIONS - NULL_ELSEWHERE, key=lambda f: f.value)
)
def test_introducing_function_is_null_on_some_values(op: FunctionType) -> None:
    kinds = CASES[op]
    assert any(
        _evaluate(op, list(args), kinds) is None
        for args in itertools.product(*_pools(kinds))
    ), op
