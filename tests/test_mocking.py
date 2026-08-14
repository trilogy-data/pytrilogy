from decimal import Decimal

import pytest
from click.exceptions import Exit

from trilogy import Dialects
from trilogy.core.exceptions import ModelValidationError
from trilogy.core.models.core import (
    ArrayType,
    DataType,
    EnumType,
    MapType,
    NumericType,
    StructComponent,
    StructType,
    TraitDataType,
)
from trilogy.core.validation.environment import validate_environment
from trilogy.dialect.mock import ARRAY_MOCK_SIZE, mock_datatype
from trilogy.scripts.common import validate_environment as cli_validate_environment


def test_mock_datatype_enum_int_random():
    enum = EnumType(type=DataType.INTEGER, values=[0, 1, 2, 3])
    rows = mock_datatype(enum, DataType.INTEGER, scale_factor=50, is_key=False)
    assert len(rows) == 50
    assert set(rows).issubset({0, 1, 2, 3})


def test_mock_datatype_enum_string_random():
    enum = EnumType(type=DataType.STRING, values=["A", "B", "C"])
    rows = mock_datatype(enum, DataType.STRING, scale_factor=20, is_key=False)
    assert len(rows) == 20
    assert set(rows).issubset({"A", "B", "C"})


def test_mock_datatype_enum_key_cycles_deterministically():
    enum = EnumType(type=DataType.INTEGER, values=[0, 1, 2])
    rows = mock_datatype(enum, DataType.INTEGER, scale_factor=7, is_key=True)
    assert rows == [0, 1, 2, 0, 1, 2, 0]


def test_mock_datatype_enum_empty_raises():
    enum = EnumType(type=DataType.INTEGER, values=[])
    with pytest.raises(ValueError):
        mock_datatype(enum, DataType.INTEGER, scale_factor=5)


def test_mock_datatype_numeric_type_returns_decimals():
    numeric = NumericType(15, 2)
    rows = mock_datatype(numeric, numeric, scale_factor=20)
    assert len(rows) == 20
    assert all(isinstance(r, Decimal) for r in rows)
    assert all(r == r.quantize(Decimal("0.01")) for r in rows)


def test_mock_datatype_numeric_type_key_unique():
    numeric = NumericType(15, 2)
    rows = mock_datatype(numeric, numeric, scale_factor=50, is_key=True)
    assert len(set(rows)) == 50


def test_mock_datatype_numeric_trait_unwraps():
    traited = TraitDataType(type=NumericType(15, 2), traits=["usd"])
    rows = mock_datatype(traited, traited, scale_factor=10)
    assert all(isinstance(r, Decimal) for r in rows)


def test_mock_datatype_numeric_respects_precision_and_scale():
    numeric = NumericType(4, 2)
    rows = mock_datatype(numeric, numeric, scale_factor=100)
    assert all(abs(r) < 100 for r in rows)
    assert all(r == r.quantize(Decimal("0.01")) for r in rows)


def test_mock_datatype_bare_numeric_still_supported():
    rows = mock_datatype(DataType.NUMERIC, DataType.NUMERIC, scale_factor=10)
    assert len(rows) == 10
    assert all(isinstance(r, Decimal) for r in rows)


def test_mock_datatype_bigint_and_number():
    assert mock_datatype(DataType.BIGINT, DataType.BIGINT, 5, is_key=True) == [
        1,
        2,
        3,
        4,
        5,
    ]
    rows = mock_datatype(DataType.NUMBER, DataType.NUMBER, scale_factor=5)
    assert all(isinstance(r, float) for r in rows)


def test_mock_datatype_bytes():
    rows = mock_datatype(DataType.BYTES, DataType.BYTES, scale_factor=5)
    assert all(isinstance(r, bytes) for r in rows)
    keys = mock_datatype(DataType.BYTES, DataType.BYTES, scale_factor=5, is_key=True)
    assert len(set(keys)) == 5


def test_mock_datatype_array_shape_matches_declared_element_type():
    arr = ArrayType(type=DataType.STRING)
    rows = mock_datatype(arr, arr, scale_factor=10)
    assert all(isinstance(r, list) and len(r) == ARRAY_MOCK_SIZE for r in rows)
    assert all(isinstance(v, str) for r in rows for v in r)


def test_mock_datatype_array_key_unique_and_homogeneous():
    arr = ArrayType(type=DataType.STRING)
    rows = mock_datatype(arr, arr, scale_factor=10, is_key=True)
    assert len({tuple(r) for r in rows}) == 10
    assert all(isinstance(v, str) for r in rows for v in r)


def test_mock_datatype_array_of_precision_numeric():
    arr = ArrayType(type=NumericType(15, 2))
    rows = mock_datatype(arr, arr, scale_factor=5)
    assert all(isinstance(v, Decimal) for r in rows for v in r)


def test_mock_datatype_map():
    mp = MapType(key_type=DataType.STRING, value_type=DataType.INTEGER)
    rows = mock_datatype(mp, mp, scale_factor=10)
    assert all(isinstance(r, dict) for r in rows)
    assert all(
        isinstance(k, str) and isinstance(v, int) for r in rows for k, v in r.items()
    )
    keyed = mock_datatype(mp, mp, scale_factor=10, is_key=True)
    assert len({tuple(sorted(r.items())) for r in keyed}) == 10


def test_mock_datatype_struct():
    struct = StructType(
        fields=[
            StructComponent(name="a", type=DataType.INTEGER),
            StructComponent(name="b", type=DataType.STRING),
        ],
        fields_map={"a": DataType.INTEGER, "b": DataType.STRING},
    )
    rows = mock_datatype(struct, struct, scale_factor=10, is_key=True)
    assert len({r["a"] for r in rows}) == 10
    assert all(isinstance(r["b"], str) for r in rows)


def test_mock_datatype_unsupported_remains_loud():
    with pytest.raises(NotImplementedError):
        mock_datatype(DataType.GEOGRAPHY, DataType.GEOGRAPHY, 5)


def test_mock_validate_passes_with_precision_numeric_column():
    """q89 regression: unit-mode validation must survive a model whose
    datasource has a numeric(p,s) column, referenced by nothing."""
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text("""
        type usd numeric;

        key id int;
        property id.ext_sales_price numeric(15,2)::usd;
        property id.quantity int;

        datasource store_sales (
            id: id,
            ext_sales_price: ext_sales_price,
            quantity: quantity,
        )
        grain (id)
        address store_sales_tbl;
    """)

    cli_validate_environment(executor, mock=True, quiet=True)


def test_mock_validate_passes_with_composite_columns():
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text("""
        key id int;
        property id.big bigint;
        property id.blob bytes;
        property id.tags array<string>;
        property id.bare_list list;
        property id.counts map<string, int>;
        property id.nested struct<a: int, b: string>;

        datasource spicy (
            id: id,
            big: big,
            blob: blob,
            tags: tags,
            bare_list: bare_list,
            counts: counts,
            nested: nested,
        )
        grain (id)
        address spicy_tbl;

        mock datasource spicy;
    """)

    validate_environment(executor.environment, exec=executor)
    assert executor.execute_raw_sql(
        "select map_keys(counts) from spicy_tbl limit 1"
    ).fetchall()
    tags_type = {
        row[0]: row[1]
        for row in executor.execute_raw_sql("describe spicy_tbl").fetchall()
    }
    assert tags_type["tags"] == "VARCHAR[]"
    assert tags_type["counts"] == "MAP(VARCHAR, BIGINT)"


def test_mock_castable_values_for_datasource_cast_bindings():
    """A string column feeding a datasource-declared cast (`concept::type:
    other`) must mock as strings that survive the cast, or validation of the
    derived concept aborts the whole connection's transaction."""
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text("""
        key sk int;
        property sk._date_string string;
        property sk.date_val date;

        datasource dates (
            D_DATE_SK: sk,
            D_DATE: _date_string,
            _date_string::date: date_val,
        )
        grain (sk)
        address date_tbl;
    """)

    cli_validate_environment(executor, mock=True, quiet=True)


def test_mock_unsupported_column_error_names_concept():
    executor = Dialects.DUCK_DB.default_executor()
    with pytest.raises(NotImplementedError, match="local.geo"):
        executor.execute_text("""
            key id int;
            property id.geo geography;
            datasource t (id: id, geo: geo) grain (id) address t_tbl;
            mock datasource t;
        """)


def test_mock_validate_passes_with_enum_datasource():
    """Mocked enum-typed columns must produce values from the enum's allowed
    set so datasource type-binding validation succeeds."""
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text("""
        key id int;
        property id.category enum<int>[0, 1, 2];
        property id.color enum<string>['RED', 'BLUE', 'GREEN'];

        datasource thing (
            id: id,
            category: category,
            color: color,
        )
        grain (id)
        address my_thing;

        mock datasource thing;
    """)

    validate_environment(executor.environment, exec=executor)


def test_mock_validate_passes_with_non_key_grain_component():
    """A datasource grained on a non-KEY concept (e.g. a property date) must
    still satisfy grain validation after mocking — grain components have to
    be unique-per-row even when their purpose isn't KEY."""
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text("""
        key id int;
        property id.flight_date date;
        auto flight_count <- count(id);

        datasource flight (
            id: id,
            flight_date: flight_date,
        )
        grain (id)
        address flight_tbl;

        datasource flight_count_by_date (
            flight_date: flight_date,
            flight_count: flight_count,
        )
        grain (flight_date)
        address flight_count_by_date_tbl;

        mock datasource flight, flight_count_by_date;
    """)

    validate_environment(executor.environment, exec=executor)


def test_mock_validate_passes_with_enum_key_property():
    """An enum used as a property on a KEY concept should mock + validate
    cleanly through the unit (mock) path."""
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text("""
        key id int;
        property id.status enum<string>['active', 'inactive', 'pending'];

        datasource records (
            id: id,
            status: status,
        )
        grain (id)
        address records_tbl;
    """)

    cli_validate_environment(executor, mock=True, quiet=True)


def test_cli_validate_quiet_collects_target_failures():
    """The quiet validation path collects per-target failures via the
    on_target_complete callback and surfaces them as a single
    ModelValidationError summary."""
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text("""
        key id int;
        property id.tail_num string;

        datasource flight (
            id: id,
            tail_num: tail_num,
        )
        grain (id)
        query '''
        SELECT 1 AS id, 123 AS tail_num
        ''';
    """)

    with pytest.raises(ModelValidationError) as exc_info:
        cli_validate_environment(executor, mock=False, quiet=True)

    assert "tail_num" in str(exc_info.value)


def test_cli_validate_quiet_records_synthesis_error(monkeypatch):
    """If core validation raises ModelValidationError before any per-target
    callback fires (e.g. a synthesis error setting up grain_check concepts),
    the quiet path should still surface it as an environment-level failure."""
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text("""
        key id int;
        datasource thing (id: id) grain (id) address thing_tbl;
    """)

    def fake_validate(*args, **kwargs):
        raise ModelValidationError("synthesis failed")

    monkeypatch.setattr(
        "trilogy.core.validation.environment.validate_environment", fake_validate
    )

    with pytest.raises(ModelValidationError) as exc_info:
        cli_validate_environment(executor, mock=False, quiet=True)

    assert "synthesis failed" in str(exc_info.value)


def test_cli_validate_rich_collects_target_failures():
    """The rich (non-quiet) validation path renders failures and exits with
    click.Exit(1)."""
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text("""
        key id int;
        property id.tail_num string;

        datasource flight (
            id: id,
            tail_num: tail_num,
        )
        grain (id)
        query '''
        SELECT 1 AS id, 123 AS tail_num
        ''';
    """)

    with pytest.raises(Exit):
        cli_validate_environment(executor, mock=False, quiet=False)


def test_cli_validate_rich_records_synthesis_error(monkeypatch):
    """The rich path's synthesis fallback should record an environment-level
    failure when core validation raises before any per-target callback fires."""
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text("""
        key id int;
        datasource thing (id: id) grain (id) address thing_tbl;
    """)

    def fake_validate(*args, **kwargs):
        raise ModelValidationError("synthesis failed")

    monkeypatch.setattr(
        "trilogy.core.validation.environment.validate_environment", fake_validate
    )

    with pytest.raises(Exit):
        cli_validate_environment(executor, mock=False, quiet=False)


def test_cli_validate_quiet_success():
    """A clean environment under the quiet path should return without raising."""
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text("""
        key id int;
        property id.name string;
        datasource thing (
            id: id,
            name: name,
        )
        grain (id)
        query '''
        SELECT 1 AS id, 'a' AS name UNION ALL SELECT 2, 'b'
        ''';
    """)

    cli_validate_environment(executor, mock=False, quiet=True)
