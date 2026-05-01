import pytest
from click.exceptions import Exit

from trilogy import Dialects
from trilogy.core.exceptions import ModelValidationError
from trilogy.core.models.core import DataType, EnumType
from trilogy.core.validation.environment import validate_environment
from trilogy.dialect.mock import mock_datatype
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
