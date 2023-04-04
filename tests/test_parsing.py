from preql.parsing.parse_engine import arg_to_datatype
from preql.core.enums import DataType


def test_arg_to_datatype():
    assert arg_to_datatype(1.00) == DataType.FLOAT
    assert arg_to_datatype("test") == DataType.STRING
