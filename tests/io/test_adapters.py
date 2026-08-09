import pyarrow as pa
import pytest

from trilogy.io.adapters import register_adapter, to_reader
from trilogy.io.errors import TrilogyIOError

ROWS = [{"i": i, "state": "CA" if i % 2 else "NY"} for i in range(6)]
SCHEMA = pa.schema([("i", pa.int64()), ("state", pa.string())])


def rows_of(obj, schema=None) -> int:
    return to_reader(obj, schema).read_all().num_rows


def test_pyarrow_types():
    table = pa.Table.from_pylist(ROWS)
    assert rows_of(table) == 6
    assert rows_of(table.to_batches()[0]) == 6
    assert rows_of(table.to_reader()) == 6


def test_to_reader_passes_a_reader_through_untouched():
    reader = pa.Table.from_pylist(ROWS).to_reader()
    assert to_reader(reader) is reader


def test_pylist_and_pydict():
    assert rows_of(ROWS) == 6
    assert rows_of({"a": [1, 2, 3]}) == 3


def test_list_of_dicts_beats_the_generic_iterable_probe():
    assert to_reader(ROWS).schema.names == ["i", "state"]


def test_iterators_of_every_supported_element():
    assert rows_of(iter(pa.Table.from_pylist(ROWS).to_batches())) == 6
    assert rows_of(pa.Table.from_pylist(ROWS) for _ in range(3)) == 18
    assert rows_of(ROWS for _ in range(2)) == 12
    assert rows_of([pa.Table.from_pylist(ROWS)] * 2) == 12


def test_generators_are_not_materialized():
    pulled: list[int] = []

    def batches():
        for i in range(3):
            pulled.append(i)
            yield pa.Table.from_pylist(ROWS)

    reader = to_reader(batches())
    assert pulled == [0]
    next(reader)
    assert pulled == [0]


def test_empty_input_needs_a_declared_schema():
    assert rows_of([], SCHEMA) == 0
    assert to_reader([], SCHEMA).schema.names == ["i", "state"]
    with pytest.raises(TrilogyIOError, match="empty result"):
        to_reader([])


def test_none_is_an_empty_result():
    assert rows_of(None, SCHEMA) == 0


def test_arrow_capsule_interface():
    duckdb = pytest.importorskip("duckdb")
    assert rows_of(duckdb.sql("select 1 as a union all select 2")) == 2


def test_pandas():
    pd = pytest.importorskip("pandas")
    reader = to_reader(pd.DataFrame(ROWS))
    assert reader.read_all().num_rows == 6


def test_unsupported_type_names_the_type():
    with pytest.raises(TrilogyIOError, match="Cannot convert"):
        to_reader(object())


def test_arrow_c_array_capsule_without_a_stream():
    struct = pa.StructArray.from_arrays([pa.array([1, 2, 3])], names=["i"])
    assert not hasattr(struct, "__arrow_c_stream__")
    assert to_reader(struct).read_all().column_names == ["i"]


def test_an_exhausted_iterator_still_needs_a_declared_schema():
    assert rows_of(iter(()), SCHEMA) == 0
    with pytest.raises(TrilogyIOError, match="empty result"):
        to_reader(iter(()))


def test_later_elements_are_cast_onto_the_schema_the_first_one_set():
    narrow = pa.Table.from_pylist([{"i": 9}], schema=pa.schema([("i", pa.int32())]))
    reader = to_reader(iter([pa.Table.from_pylist([{"i": 1}]), narrow]))
    assert reader.schema.field("i").type == pa.int64()
    assert reader.read_all().column("i").to_pylist() == [1, 9]


@pytest.fixture
def restore_adapters():
    from trilogy.io import adapters

    original = list(adapters._ADAPTERS)
    yield
    adapters._ADAPTERS[:] = original


def test_register_adapter_overrides_a_builtin(restore_adapters):
    marker = pa.Table.from_pylist([{"x": 1}])
    register_adapter(lambda o: isinstance(o, str), lambda o, schema: marker.to_reader())
    assert to_reader("anything").read_all().column_names == ["x"]


def test_register_adapter_works_as_a_decorator(restore_adapters):
    @register_adapter(lambda o: isinstance(o, complex))
    def from_complex(obj, schema):
        return pa.Table.from_pylist([{"re": obj.real}]).to_reader()

    assert to_reader(1 + 2j).read_all().column("re").to_pylist() == [1.0]
    assert from_complex(3 + 0j, None).read_all().num_rows == 1
