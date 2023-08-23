from preql.core.enums import DataType, Purpose

from preql.parsing.parse_engine import arg_to_datatype
from preql.parsing.parse_engine import (
    argument_to_purpose,
    function_args_to_output_purpose,
)


def test_arg_to_datatype():
    assert arg_to_datatype(1.00) == DataType.FLOAT
    assert arg_to_datatype("test") == DataType.STRING


def test_argument_to_purpose(test_environment):
    assert argument_to_purpose(1.00) == Purpose.CONSTANT
    assert argument_to_purpose("test") == Purpose.CONSTANT
    assert argument_to_purpose(test_environment.concepts["order_id"]) == Purpose.KEY
    assert (
        function_args_to_output_purpose(
            [
                "test",
                1.00,
            ]
        )
        == Purpose.CONSTANT
    )
    assert (
        function_args_to_output_purpose(
            ["test", 1.00, test_environment.concepts["order_id"]]
        )
        == Purpose.PROPERTY
    )
