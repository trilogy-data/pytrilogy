from datetime import date, datetime
from decimal import Decimal

from trilogy.core.models.core import (
    ArrayType,
    DataType,
    MapType,
    NumericType,
    StructType,
    TraitDataType,
)
from trilogy.core.validation.datasource import type_check


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
