import pyarrow as pa
import pytest

from trilogy.io.describe import datasource_stub, payload, trilogy_type

TYPES = [
    (pa.bool_(), "bool"),
    (pa.int8(), "int"),
    (pa.int32(), "int"),
    (pa.uint32(), "int"),
    (pa.int64(), "bigint"),
    (pa.uint64(), "bigint"),
    (pa.float32(), "float"),
    (pa.float64(), "float"),
    (pa.decimal128(10, 2), "numeric"),
    (pa.date32(), "date"),
    (pa.date64(), "date"),
    (pa.timestamp("us"), "datetime"),
    (pa.timestamp("us", tz="UTC"), "timestamp"),
    (pa.binary(), "bytes"),
    (pa.large_binary(), "bytes"),
    (pa.list_(pa.int64()), "array"),
    (pa.large_list(pa.int64()), "array"),
    (pa.struct([("a", pa.int64())]), "struct"),
    (pa.map_(pa.string(), pa.int64()), "map"),
    (pa.null(), "null"),
    (pa.string(), "string"),
    (pa.large_string(), "string"),
    (pa.time64("us"), "string"),
]


@pytest.mark.parametrize("arrow_type,expected", TYPES, ids=lambda v: str(v))
def test_every_arrow_type_maps_to_a_trilogy_type(arrow_type, expected):
    assert trilogy_type(arrow_type) == expected


SCHEMA = pa.schema([("id", pa.int64()), ("state", pa.string())])


def test_datasource_stub_names_itself_after_the_script():
    stub = datasource_stub(SCHEMA, "/tmp/my-landmarks.py")
    assert stub.startswith("datasource my_landmarks(")
    assert "    id: id,\n    state: state" in stub
    assert "grain (id)" in stub
    assert stub.endswith("file `/tmp/my-landmarks.py`;")


def test_datasource_stub_takes_an_explicit_name():
    assert datasource_stub(SCHEMA, "x.py", name="chosen").startswith(
        "datasource chosen("
    )


def test_datasource_stub_of_a_columnless_schema_has_an_empty_grain():
    assert "grain ()" in datasource_stub(pa.schema([]), "x.py")


def test_payload_reports_schema_pushdown_and_a_stub():
    result = payload(SCHEMA, ("limit", "filters"), "x.py")
    assert result["contract"] == 1
    assert result["schema"] == [
        {"name": "id", "type": "bigint", "nullable": True},
        {"name": "state", "type": "string", "nullable": True},
    ]
    assert result["pushdown"] == ["limit", "filters"]
    assert result["datasource"].startswith("datasource x(")


def test_payload_reports_a_non_nullable_field():
    schema = pa.schema([pa.field("id", pa.int64(), nullable=False)])
    assert payload(schema, (), "x.py")["schema"][0]["nullable"] is False
