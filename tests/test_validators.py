from datetime import date, datetime
from decimal import Decimal

import pytest

from trilogy import Dialects
from trilogy.core.enums import ValidationScope
from trilogy.core.exceptions import (
    DatasourceColumnBindingError,
    DatasourceGrainValidationError,
    ModelValidationError,
)
from trilogy.core.models.core import (
    ArrayType,
    DataType,
    EnumType,
    MapType,
    NumericType,
    StructType,
    TraitDataType,
)
from trilogy.core.validation.datasource import type_check
from trilogy.core.validation.environment import validate_environment


def test_type_check():
    # Basic string tests
    assert type_check("hello", DataType.STRING)
    assert not type_check(123, DataType.STRING)
    assert type_check("", DataType.STRING)  # empty string

    # Integer tests
    assert type_check(123, DataType.INTEGER)
    assert type_check(0, DataType.INTEGER)
    assert type_check(-123, DataType.INTEGER)
    assert not type_check("123", DataType.INTEGER)
    assert not type_check(123.0, DataType.INTEGER)  # float should not match int

    # BIGINT tests (same as INTEGER in implementation)
    assert type_check(123, DataType.BIGINT)
    assert type_check(2**63 - 1, DataType.BIGINT)  # large integer
    assert not type_check("123", DataType.BIGINT)
    assert not type_check(123.0, DataType.BIGINT)

    # Float tests
    assert type_check(123.45, DataType.FLOAT)
    assert type_check(123, DataType.FLOAT)  # int should match float
    assert type_check(0.0, DataType.FLOAT)
    assert type_check(-123.45, DataType.FLOAT)
    assert not type_check("123.45", DataType.FLOAT)

    # Decimal support in float
    decimal_val = Decimal("123.45")
    assert type_check(decimal_val, DataType.FLOAT)

    # NumericType tests
    numeric_type = NumericType()  # Assuming NumericType can be instantiated
    assert type_check(123.45, numeric_type)
    assert type_check(123, numeric_type)
    assert type_check(decimal_val, numeric_type)
    assert not type_check("123", numeric_type)

    # NUMBER and NUMERIC tests (both handle int, float, Decimal)
    assert type_check(123, DataType.NUMBER)
    assert type_check(123.45, DataType.NUMBER)
    assert type_check(decimal_val, DataType.NUMBER)
    assert not type_check("123", DataType.NUMBER)

    assert type_check(123, DataType.NUMERIC)
    assert type_check(123.45, DataType.NUMERIC)
    assert type_check(decimal_val, DataType.NUMERIC)
    assert not type_check("123", DataType.NUMERIC)

    # Boolean tests
    assert type_check(True, DataType.BOOL)
    assert type_check(False, DataType.BOOL)
    assert not type_check(1, DataType.BOOL)  # int should not match bool
    assert not type_check(0, DataType.BOOL)
    assert not type_check("true", DataType.BOOL)

    # Date tests
    test_date = date(2023, 12, 25)
    assert type_check(test_date, DataType.DATE)
    assert not type_check("2023-12-25", DataType.DATE)
    assert not type_check(
        datetime.now(), DataType.DATE
    )  # datetime should not match date

    # DateTime and Timestamp tests
    test_datetime = datetime(2023, 12, 25, 15, 30, 45)
    assert type_check(test_datetime, DataType.DATETIME)
    assert type_check(test_datetime, DataType.TIMESTAMP)
    assert not type_check("2023-12-25 15:30:45", DataType.DATETIME)
    assert not type_check(
        test_date, DataType.DATETIME
    )  # date should not match datetime

    # Unix seconds tests
    assert type_check(1640995200, DataType.UNIX_SECONDS)  # int timestamp
    assert type_check(1640995200.123, DataType.UNIX_SECONDS)  # float timestamp
    assert not type_check("1640995200", DataType.UNIX_SECONDS)

    # Date part tests
    assert type_check("year", DataType.DATE_PART)
    assert type_check("month", DataType.DATE_PART)
    assert type_check("day", DataType.DATE_PART)
    assert not type_check(2023, DataType.DATE_PART)

    # Bytes tests
    assert type_check(b"\x00\x01", DataType.BYTES)
    assert type_check(bytearray(b"\x00\x01"), DataType.BYTES)
    assert not type_check("POINT(1 1)", DataType.BYTES)
    assert type_check(b"\x00\x01", DataType.GEOGRAPHY)

    # Array tests
    assert type_check([1, 2, 3], DataType.ARRAY)
    assert type_check([], DataType.ARRAY)  # empty array
    assert type_check(["a", "b"], DataType.ARRAY)
    assert not type_check("not a list", DataType.ARRAY)
    assert not type_check({"key": "value"}, DataType.ARRAY)

    # ArrayType tests
    array_type = ArrayType(type=DataType.INTEGER)
    assert type_check([1, 2, 3], array_type)
    assert not type_check("not a list", array_type)

    # Map tests
    assert type_check({"key": "value"}, DataType.MAP)
    assert type_check({}, DataType.MAP)  # empty dict
    assert type_check({"a": 1, "b": 2}, DataType.MAP)
    assert not type_check([1, 2, 3], DataType.MAP)
    assert not type_check("not a dict", DataType.MAP)

    # MapType tests
    map_type = MapType(key_type=DataType.STRING, value_type=DataType.STRING)
    assert type_check({"key": "value"}, map_type)
    assert not type_check([1, 2, 3], map_type)

    # Struct tests
    assert type_check({"field1": "value1"}, DataType.STRUCT)
    assert type_check({}, DataType.STRUCT)
    assert not type_check([1, 2, 3], DataType.STRUCT)

    # StructType tests
    struct_type = StructType(
        fields=[DataType.STRING, DataType.STRING],
        fields_map={"field1": DataType.STRING, "field2": DataType.STRING},
    )
    assert not type_check("not a dict", struct_type)

    # NULL tests
    assert type_check(None, DataType.NULL)
    assert not type_check("", DataType.NULL)
    assert not type_check(0, DataType.NULL)
    assert not type_check(False, DataType.NULL)

    # UNKNOWN tests (should accept anything)
    assert type_check("anything", DataType.UNKNOWN)
    assert type_check(123, DataType.UNKNOWN)
    assert type_check(None, DataType.UNKNOWN)
    assert type_check([1, 2, 3], DataType.UNKNOWN)
    assert type_check({"key": "value"}, DataType.UNKNOWN)

    # Nullable tests
    assert type_check(None, DataType.STRING)  # nullable by default
    assert type_check(None, DataType.INTEGER)
    assert type_check(None, DataType.FLOAT)
    assert type_check(None, DataType.BOOL)

    # Non-nullable tests
    assert not type_check(None, DataType.STRING, nullable=False)
    assert not type_check(None, DataType.INTEGER, nullable=False)
    assert not type_check(None, DataType.FLOAT, nullable=False)
    assert not type_check(None, DataType.BOOL, nullable=False)

    # EnumType tests
    enum_type = EnumType(
        type=DataType.STRING, values=["Full sun", "Partial shade", "Shade"]
    )
    assert type_check("Full sun", enum_type)
    assert type_check("Shade", enum_type)
    assert not type_check("full_sun", enum_type)  # original key, not display value
    assert not type_check(123, enum_type)  # wrong base type
    assert type_check(None, enum_type)  # nullable by default
    assert not type_check(None, enum_type, nullable=False)

    # TraitDataType tests (recursive handling)
    # Assuming TraitDataType wraps another DataType
    trait_string_type = TraitDataType(type=DataType.STRING, traits=[])
    assert type_check("hello", trait_string_type)
    assert not type_check(123, trait_string_type)
    assert type_check(None, trait_string_type)  # nullable by default
    assert not type_check(None, trait_string_type, nullable=False)

    # Nested TraitDataType
    nested_trait_type = TraitDataType(type=DataType.STRING, traits=[])
    assert type_check("hello", nested_trait_type)
    assert not type_check(123, nested_trait_type)

    # Edge cases and invalid types
    # Test with unsupported/custom types should return False
    class CustomType:
        pass

    custom_obj = CustomType()
    # These should all return False as they don't match any known type
    assert not type_check(custom_obj, DataType.STRING)
    assert not type_check(custom_obj, DataType.INTEGER)
    assert not type_check(custom_obj, DataType.ARRAY)


def test_type_check_edge_cases():
    """Additional edge case tests"""

    # Test very large numbers
    large_int = 2**100
    assert type_check(large_int, DataType.INTEGER)
    assert type_check(large_int, DataType.BIGINT)
    assert type_check(large_int, DataType.FLOAT)  # int should match float

    # Test special float values
    assert type_check(float("inf"), DataType.FLOAT)
    assert type_check(float("-inf"), DataType.FLOAT)
    # Note: NaN might need special handling depending on requirements
    # assert type_check(float('nan'), DataType.FLOAT)

    # Test empty collections
    assert type_check([], DataType.ARRAY)
    assert type_check({}, DataType.MAP)
    assert type_check({}, DataType.STRUCT)

    # Test nested collections
    nested_list = [[1, 2], [3, 4]]
    nested_dict = {"outer": {"inner": "value"}}
    assert type_check(nested_list, DataType.ARRAY)
    assert type_check(nested_dict, DataType.MAP)
    assert type_check(nested_dict, DataType.STRUCT)


def test_validate_datasource_fails_early_for_missing_grain_column():
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text("""
        key city string;
        property city.data_updated_through datetime;

        datasource city_update_time (
            data_updated_through: data_updated_through
        )
        grain (city)
        query '''
        SELECT 'USNYC' as city, TIMESTAMP '2024-01-01 00:00:00' as data_updated_through
        ''';
        """)

    with pytest.raises(ModelValidationError) as exc_info:
        validate_environment(
            executor.environment,
            scope=ValidationScope.DATASOURCES,
            exec=executor,
        )

    assert any(
        isinstance(child, DatasourceGrainValidationError)
        and "not present in datasource output: local.city" in child.message
        for child in exc_info.value.children or []
    )


_MULTI_DATASOURCE_PREAMBLE = """
    key aircraft_id int;
    property aircraft_id.tail_num string;
    key flight_id int;
"""

_AIRCRAFT_DATASOURCE = """
    datasource aircraft (
        aircraft_id: aircraft_id,
        tail_num: tail_num,
    )
    grain (aircraft_id)
    query '''
    SELECT 1 AS aircraft_id, 'N1' AS tail_num UNION ALL
    SELECT 2, 'N2' UNION ALL
    SELECT 3, 'N3'
    ''';
"""


def test_multi_datasource_complete_binding_cardinality_mismatch():
    """A concept bound complete in two datasources must have matching
    distinct cardinality — if one has fewer values, it should be marked partial."""
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(_MULTI_DATASOURCE_PREAMBLE + _AIRCRAFT_DATASOURCE + """
        datasource flight (
            flight_id: flight_id,
            tail_num: tail_num,
        )
        grain (flight_id)
        query '''
        SELECT 10 AS flight_id, 'N1' AS tail_num UNION ALL
        SELECT 20, 'N1' UNION ALL
        SELECT 30, 'N2'
        ''';
        """)

    with pytest.raises(ModelValidationError) as exc_info:
        validate_environment(executor.environment, exec=executor)

    assert any(
        isinstance(child, DatasourceColumnBindingError)
        and "tail_num" in child.message
        and "missing values in datasource flight" in child.message
        and "not marked as partial" in child.message
        for child in exc_info.value.children or []
    )


def test_multi_datasource_partial_binding_is_not_flagged():
    """Marking the smaller binding as partial should suppress the mismatch error."""
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(_MULTI_DATASOURCE_PREAMBLE + _AIRCRAFT_DATASOURCE + """
        datasource flight (
            flight_id: flight_id,
            tail_num: ~tail_num,
        )
        grain (flight_id)
        query '''
        SELECT 10 AS flight_id, 'N1' AS tail_num UNION ALL
        SELECT 20, 'N1' UNION ALL
        SELECT 30, 'N2'
        ''';
        """)

    validate_environment(executor.environment, exec=executor)


def test_inferred_type_check():
    from trilogy.core.validation.datasource import inferred_type_check

    # Simple match
    assert inferred_type_check(DataType.STRING, DataType.STRING)
    assert not inferred_type_check(DataType.STRING, DataType.INTEGER)

    # TraitDataType unwrapping
    trait = TraitDataType(type=DataType.INTEGER, traits=[])
    assert inferred_type_check(trait, DataType.INTEGER)
    assert inferred_type_check(DataType.INTEGER, trait)
    assert not inferred_type_check(trait, DataType.STRING)

    # EnumType matching
    enum_a = EnumType(type=DataType.STRING, values=["a", "b"])
    enum_b = EnumType(type=DataType.STRING, values=["a", "b"])
    enum_c = EnumType(type=DataType.STRING, values=["x", "y"])
    assert inferred_type_check(enum_a, enum_b)
    assert not inferred_type_check(enum_a, enum_c)
    # EnumType vs non-EnumType
    assert not inferred_type_check(enum_a, DataType.STRING)
    assert not inferred_type_check(DataType.STRING, enum_a)


def test_validate_environment_without_executor():
    """validate_environment with exec=None uses nullcontext — no crash."""
    from trilogy import Environment
    from trilogy.core.enums import ValidationScope
    from trilogy.core.validation.environment import validate_environment

    env = Environment()
    env.parse("key id int;")
    # Should not raise — exercises the nullcontext() branch
    validate_environment(env, scope=ValidationScope.CONCEPTS)


def test_duckdb_refine_runtime_value_type():
    from trilogy.dialect.duckdb import DuckDBDialect

    dialect = DuckDBDialect()

    # bytes → geography when result_type is GEOGRAPHY
    assert (
        dialect.refine_runtime_value_type_for_validation(
            None, b"\x00", DataType.BYTES, DataType.GEOGRAPHY, DataType.GEOGRAPHY
        )
        == DataType.GEOGRAPHY
    )

    # No refinement when conditions don't match
    assert (
        dialect.refine_runtime_value_type_for_validation(
            None, b"\x00", DataType.BYTES, DataType.GEOGRAPHY, DataType.STRING
        )
        == DataType.BYTES
    )
    assert (
        dialect.refine_runtime_value_type_for_validation(
            None, "hello", DataType.STRING, DataType.STRING, None
        )
        == DataType.STRING
    )


def test_duckdb_get_result_column_types():
    from unittest.mock import Mock

    from trilogy.dialect.duckdb import DuckDBDialect

    dialect = DuckDBDialect()

    # No cursor → None
    assert dialect.get_result_column_types_for_validation(object()) is None

    # With cursor description
    cursor = Mock()
    cursor.description = [("Name", "VARCHAR"), ("Age", "INTEGER"), ("X",)]
    result = Mock()
    result.cursor = cursor

    types = dialect.get_result_column_types_for_validation(result)
    assert types is not None
    assert types["name"] == DataType.STRING
    assert types["age"] == DataType.INTEGER
    assert "x" not in types  # skipped due to len < 2


def test_base_dialect_validation_defaults():
    from trilogy.dialect.base import BaseDialect

    dialect = BaseDialect()
    assert (
        dialect.refine_runtime_value_type_for_validation(
            None, "v", DataType.STRING, DataType.INTEGER
        )
        == DataType.STRING
    )
    assert dialect.get_result_column_types_for_validation(object()) is None


def test_validation_scope_reentrant():
    """Nested validation_scope calls should not reset the cache."""
    from trilogy import Dialects

    executor = Dialects.DUCK_DB.default_executor()
    with executor.validation_scope():
        executor._validation_datasource_cache["test"] = "value"
        # Re-entering should not reset
        with executor.validation_scope():
            assert executor._validation_datasource_cache.get("test") == "value"
        # Still intact after inner exit
        assert executor._validation_datasource_cache.get("test") == "value"
    # Cleaned up after outer exit
    assert executor._validation_datasource_cache is None


def test_validation_scope_cleans_up_temp_tables():
    """validation_scope should drop temp tables on exit."""
    from trilogy import Dialects

    executor = Dialects.DUCK_DB.default_executor()
    with executor.validation_scope():
        executor.execute_raw_sql(
            'CREATE TEMP TABLE "__trilogy_validation_cache_test" AS SELECT 1 AS x'
        )
        executor._validation_temp_tables.append("__trilogy_validation_cache_test")
    # Table should be dropped
    import pytest as _pytest

    with _pytest.raises(Exception):
        executor.execute_raw_sql('SELECT * FROM "__trilogy_validation_cache_test"')


def test_get_validation_cached_datasource_skips_non_python_script():
    """Non-PYTHON_SCRIPT datasources should be returned as-is."""
    from trilogy import Dialects
    from trilogy.core.enums import AddressType
    from trilogy.core.models.datasource import Address, Datasource

    executor = Dialects.DUCK_DB.default_executor()
    ds = Datasource(
        name="test_ds",
        columns=[],
        address=Address(location="test_table", type=AddressType.TABLE),
    )
    with executor.validation_scope():
        result = executor.get_validation_cached_datasource(ds)
        assert result is ds  # unchanged

    # Without validation scope (cache is None)
    result = executor.get_validation_cached_datasource(ds)
    assert result is ds
