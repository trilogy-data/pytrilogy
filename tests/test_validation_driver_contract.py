"""Datasource validation against the row contract a native driver adapter keeps.

BigQuery and chdb build rows with ``trilogy.dialect.results``: positional
access, attribute access and tuple equality, but no SQLAlchemy ``_mapping``.
These run the DuckDB validation paths through that row type, so a
SQLAlchemy-only accessor fails here the way it does remotely.
"""

import pytest

from trilogy import Dialects
from trilogy.core.exceptions import (
    DatasourceColumnBindingError,
    DatasourceModelValidationError,
    ModelValidationError,
)
from trilogy.core.validation.datasource import describe_violation_row, row_to_dict
from trilogy.core.validation.environment import validate_environment
from trilogy.dialect.results import buffered_rows


class NativeDriverConnection:
    """Delegates to a real connection, re-wrapping results as driver rows."""

    def __init__(self, inner):
        self.inner = inner

    def execute(self, statement, parameters=None):
        result = self.inner.execute(statement, parameters)
        if not result.returns_rows:
            return buffered_rows([], [])
        columns = list(result.keys())
        return buffered_rows(
            columns, [tuple(row) for row in result.fetchall()], "NativeDriverRow"
        )

    def commit(self):
        return self.inner.commit()

    def begin(self):
        return self.inner.begin()

    def rollback(self):
        return self.inner.rollback()

    def in_transaction(self) -> bool:
        return self.inner.in_transaction()

    def get_transaction(self):
        return self.inner.get_transaction()

    def close(self) -> None:
        return self.inner.close()


def native_driver_executor(script: str):
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(script)
    executor.connection = NativeDriverConnection(executor.connection)
    return executor


def test_row_to_dict_reads_driver_rows():
    columns = ["genus_name", "genus_image"]
    row = buffered_rows(columns, [("acer", "maple.png")], "NativeDriverRow").fetchone()

    assert row_to_dict(row, columns) == {
        "genus_name": "acer",
        "genus_image": "maple.png",
    }


def test_row_to_dict_matches_sqlalchemy():
    from sqlalchemy import create_engine, text

    with create_engine("sqlite://").connect() as conn:
        result = conn.execute(text("SELECT 'acer' AS genus_name, 1 AS n"))
        columns = list(result.keys())
        row = result.fetchone()

    assert row_to_dict(row, columns) == dict(row._mapping)


def test_row_to_dict_keeps_names_a_namedtuple_would_renumber():
    """rename=True renumbers a field that is not a valid identifier, so the
    row's own field names are not the driver's."""
    columns = ["local.x", "count(*)"]
    row = buffered_rows(columns, [(1, 2)], "NativeDriverRow").fetchone()

    assert row._asdict() == {"_0": 1, "_1": 2}
    assert row_to_dict(row, columns) == {"local.x": 1, "count(*)": 2}


def test_describe_violation_row_reads_driver_rows():
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text("""
        key genus_name string;
        property genus_name.genus_image string;
        """)
    build_env = executor.environment.materialize_for_select()
    concept = build_env.concepts["local.genus_image"]
    key = build_env.concepts["local.genus_name"]
    columns = ["genus_image", "genus_name"]
    row = buffered_rows(columns, [("maple.png", "acer")], "NativeDriverRow").fetchone()

    assert (
        describe_violation_row(row, columns, concept, [key])
        == "local.genus_name='acer' -> local.genus_image='maple.png'"
    )


def test_type_mismatch_reports_against_driver_rows():
    executor = native_driver_executor("""
        key id int;
        property id.code int;

        datasource items (
            id: id,
            code: code,
        )
        grain (id)
        query '''
        SELECT 1 AS id, 'not_an_int' AS code
        ''';
        """)

    with pytest.raises(ModelValidationError) as exc_info:
        validate_environment(executor.environment, exec=executor)

    assert any(
        isinstance(child, DatasourceColumnBindingError)
        for child in exc_info.value.children or []
    )


def test_unique_property_violation_reports_against_driver_rows():
    executor = native_driver_executor("""
        key id int;
        unique property id.code string;

        datasource items (
            id: id,
            code: code,
        )
        grain (id)
        query '''
        SELECT 1 AS id, 'shared' AS code UNION ALL
        SELECT 2, 'shared'
        ''';
        """)

    with pytest.raises(ModelValidationError) as exc_info:
        validate_environment(executor.environment, exec=executor)

    messages = [
        child.message
        for child in exc_info.value.children or []
        if isinstance(child, DatasourceModelValidationError)
    ]
    assert any(
        "Unique property local.code maps to multiple local.id values" in message
        and "'shared'" in message
        for message in messages
    )


def test_declared_domain_violation_reports_against_driver_rows():
    executor = native_driver_executor("""
        key id int;
        property id.score int[0..100];

        datasource scores (
            id: id,
            score: score,
        )
        grain (id)
        query '''
        SELECT 1 AS id, 50 AS score UNION ALL
        SELECT 2, 250
        ''';
        """)

    with pytest.raises(ModelValidationError) as exc_info:
        validate_environment(executor.environment, exec=executor)

    messages = [
        child.message
        for child in exc_info.value.children or []
        if isinstance(child, DatasourceModelValidationError)
    ]
    assert any(
        "violate declared domain" in message
        and "local.id=2 -> local.score=250" in message
        for message in messages
    )


def test_grain_violation_reports_against_driver_rows():
    executor = native_driver_executor("""
        key id int;
        property id.code string;

        datasource items (
            id: id,
            code: code,
        )
        grain (id)
        query '''
        SELECT 1 AS id, 'a' AS code UNION ALL
        SELECT 1, 'b'
        ''';
        """)

    with pytest.raises(ModelValidationError) as exc_info:
        validate_environment(executor.environment, exec=executor)

    messages = [
        child.message
        for child in exc_info.value.children or []
        if isinstance(child, DatasourceModelValidationError)
    ]
    assert any("do not conform to grain" in message for message in messages)


def test_valid_model_passes_against_driver_rows():
    executor = native_driver_executor("""
        key id int;
        property id.code string;

        datasource items (
            id: id,
            code: code,
        )
        grain (id)
        query '''
        SELECT 1 AS id, 'a' AS code UNION ALL
        SELECT 2, 'b'
        ''';
        """)

    validate_environment(executor.environment, exec=executor)
