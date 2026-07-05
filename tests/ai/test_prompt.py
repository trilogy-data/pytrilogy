from trilogy.ai.prompts import (
    TRILOGY_CREATE_QUERY_TOOL,
    TRILOGY_QUERY_TOOL,
    create_query_request_options,
    datatype_to_field_prompt,
)
from trilogy.authoring import (
    ArrayType,
    DataType,
    MapType,
    NumericType,
    StructType,
    TraitDataType,
)
from trilogy.core.models.core import StructComponent


def test_datatype_to_field_prompt():
    """Test datatype_to_field_prompt function with all supported datatype variants"""

    # Test basic DataType enum values
    assert datatype_to_field_prompt(DataType.STRING) == "string"
    assert datatype_to_field_prompt(DataType.BYTES) == "bytes"
    assert datatype_to_field_prompt(DataType.BOOL) == "bool"
    assert datatype_to_field_prompt(DataType.INTEGER) == "int"
    assert datatype_to_field_prompt(DataType.FLOAT) == "float"
    assert datatype_to_field_prompt(DataType.NUMERIC) == "numeric"
    assert datatype_to_field_prompt(DataType.DATE) == "date"
    assert datatype_to_field_prompt(DataType.DATETIME) == "datetime"
    assert datatype_to_field_prompt(DataType.TIMESTAMP) == "timestamp"
    assert datatype_to_field_prompt(DataType.NULL) == "null"
    assert datatype_to_field_prompt(DataType.UNKNOWN) == "unknown"
    assert datatype_to_field_prompt(DataType.ANY) == "any"

    # Test NumericType
    numeric_type = NumericType(precision=10, scale=2)
    assert datatype_to_field_prompt(numeric_type) == "NUMERIC(10, 2)>"

    # Test default NumericType
    default_numeric = NumericType()
    assert datatype_to_field_prompt(default_numeric) == "NUMERIC(20, 5)>"

    # Test ArrayType
    array_string = ArrayType(type=DataType.STRING)
    assert datatype_to_field_prompt(array_string) == "ARRAY<string>"

    array_int = ArrayType(type=DataType.INTEGER)
    assert datatype_to_field_prompt(array_int) == "ARRAY<int>"

    # Test nested ArrayType
    nested_array = ArrayType(type=ArrayType(type=DataType.FLOAT))
    assert datatype_to_field_prompt(nested_array) == "ARRAY<ARRAY<float>>"

    # Test MapType
    map_string_int = MapType(key_type=DataType.STRING, value_type=DataType.INTEGER)
    assert datatype_to_field_prompt(map_string_int) == "MAP<string, int>"

    map_int_float = MapType(key_type=DataType.INTEGER, value_type=DataType.FLOAT)
    assert datatype_to_field_prompt(map_int_float) == "MAP<int, float>"

    # Test nested MapType
    nested_map = MapType(
        key_type=DataType.STRING, value_type=ArrayType(type=DataType.INTEGER)
    )
    assert datatype_to_field_prompt(nested_map) == "MAP<string, ARRAY<int>>"

    # Test StructComponent
    struct_comp = StructComponent(name="field1", type=DataType.STRING)
    assert datatype_to_field_prompt(struct_comp) == "field1: string"

    struct_comp_numeric = StructComponent(
        name="amount", type=NumericType(precision=15, scale=3)
    )
    assert datatype_to_field_prompt(struct_comp_numeric) == "amount: NUMERIC(15, 3)>"

    # Test StructType
    struct_fields = [
        StructComponent(name="id", type=DataType.INTEGER),
        StructComponent(name="name", type=DataType.STRING),
        StructComponent(name="balance", type=NumericType(precision=10, scale=2)),
    ]
    fields_map = {
        "id": struct_fields[0],
        "name": struct_fields[1],
        "balance": struct_fields[2],
    }
    struct_type = StructType(fields=struct_fields, fields_map=fields_map)
    expected_struct = "STRUCT<id: int, name: string, balance: NUMERIC(10, 2)>>"
    assert datatype_to_field_prompt(struct_type) == expected_struct

    # Test TraitDataType
    trait_string = TraitDataType(type=DataType.STRING, traits=["indexed", "unique"])
    assert datatype_to_field_prompt(trait_string) == "string(indexed,unique)"

    trait_numeric = TraitDataType(
        type=NumericType(precision=8, scale=4), traits=["primary"]
    )
    assert datatype_to_field_prompt(trait_numeric) == "NUMERIC(8, 4)>(primary)"

    trait_array = TraitDataType(
        type=ArrayType(type=DataType.INTEGER), traits=["sorted"]
    )
    assert datatype_to_field_prompt(trait_array) == "ARRAY<int>(sorted)"

    # Test primitive types (int, float, str)
    assert datatype_to_field_prompt(42) == "42"
    assert datatype_to_field_prompt(3.14) == "3.14"
    assert datatype_to_field_prompt("hello") == "hello"

    # Test complex nested structures
    complex_struct_fields = [
        StructComponent(
            name="metadata",
            type=MapType(key_type=DataType.STRING, value_type=DataType.STRING),
        ),
        StructComponent(name="items", type=ArrayType(type=DataType.INTEGER)),
        StructComponent(
            name="config",
            type=TraitDataType(type=DataType.STRING, traits=["encrypted"]),
        ),
    ]
    complex_fields_map = {
        "metadata": complex_struct_fields[0],
        "items": complex_struct_fields[1],
        "config": complex_struct_fields[2],
    }
    complex_struct = StructType(
        fields=complex_struct_fields, fields_map=complex_fields_map
    )
    expected_complex = "STRUCT<metadata: MAP<string, string>, items: ARRAY<int>, config: string(encrypted)>"
    assert datatype_to_field_prompt(complex_struct) == expected_complex


def test_query_request_options():
    options = create_query_request_options()

    assert options.require_tool is True
    assert options.tool_choice is None
    assert len(options.tools) == 2
    assert options.tools[0] == TRILOGY_CREATE_QUERY_TOOL
    assert options.tools[1] == TRILOGY_QUERY_TOOL


def test_aggregate_functions_renders_known_aggregates():
    """The agent prompt's `Aggregate Functions:` section must list the actual
    aggregate signatures. The previous filter used the enum name instead of
    the member, leaving the list empty and rendering as ``[]`` — useless to
    the model. Regression test for that exact bug. Same-arity aggregates are
    now consolidated onto one signature group (``sum|max|...(<arg1>)``)."""
    from trilogy.ai.constants import AGGREGATE_FUNCTIONS

    assert isinstance(AGGREGATE_FUNCTIONS, str)
    assert AGGREGATE_FUNCTIONS  # not empty
    # Canonical aggregates must appear, consolidated under their `(<arg1>)` signature.
    assert "(<arg1>)" in AGGREGATE_FUNCTIONS
    for name in ("sum", "count", "avg", "max", "min"):
        assert name in AGGREGATE_FUNCTIONS


def test_trilogy_syntax_reference_embeds_aggregate_functions():
    """End-to-end check that the syntax reference the agent system prompt is
    built from now contains real aggregate signatures, not the literal `[]`
    that the broken filter used to produce."""
    from trilogy.ai.prompts import get_trilogy_syntax_reference

    ref = get_trilogy_syntax_reference()
    assert "Aggregate Functions:" in ref
    aggregates_block = ref.split("Aggregate Functions:", 1)[1].split("Functions:", 1)[0]
    assert "sum" in aggregates_block and "(<arg1>)" in aggregates_block
    assert aggregates_block.strip() != "[]"
