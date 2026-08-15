import random
from collections.abc import Callable, Iterable
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from trilogy.core.enums import FunctionType, Purpose
from trilogy.core.models.author import Concept, ConceptRef, Function
from trilogy.core.models.core import (
    CONCRETE_TYPES,
    ArrayType,
    DataType,
    EnumType,
    MapType,
    NumericType,
    StructType,
    TraitDataType,
    ValidatedType,
)
from trilogy.core.models.datasource import Address, Datasource
from trilogy.core.models.environment import Environment
from trilogy.core.statements.execute import ProcessedMockStatement
from trilogy.dialect.results import MockResult

if TYPE_CHECKING:
    import pyarrow
    from pyarrow import Table

DEFAULT_SCALE_FACTOR = 100
ARRAY_MOCK_SIZE = 5
MAP_MOCK_SIZE = 3
# element types assumed for parameterless `array`/`map` declarations
BARE_ARRAY_DEFAULT = ArrayType(type=DataType.INTEGER)
BARE_MAP_DEFAULT = MapType(key_type=DataType.STRING, value_type=DataType.INTEGER)


def safe_name(name: str) -> str:
    return "".join(c if c.isalnum() or c == "_" else "_" for c in name)


def mock_email(scale_factor: int, is_key: bool = False) -> list[str]:
    providers = ["example.com", "test.com", "mock.com", "sample.org"]
    if is_key:
        return [
            f"user{i}@{providers[i % len(providers)]}"
            for i in range(1, scale_factor + 1)
        ]
    return [
        f"user{random.randint(1, 999999)}@{random.choice(providers)}"
        for _ in range(scale_factor)
    ]


def mock_hex_code(scale_factor: int, is_key: bool = False) -> list[str]:
    if is_key:
        return [f"#{i:06x}" for i in range(1, scale_factor + 1)]
    return [f"#{random.randint(0, 0xFFFFFF):06x}" for _ in range(scale_factor)]


def mock_strings(scale_factor: int, is_key: bool) -> list[Any]:
    if is_key:
        return [f"key_{i}" for i in range(1, scale_factor + 1)]
    return [f"mock_string_{random.randint(0, 999_999)}" for _ in range(scale_factor)]


def mock_integers(scale_factor: int, is_key: bool) -> list[Any]:
    if is_key:
        return list(range(1, scale_factor + 1))
    return [random.randint(0, 999_999) for _ in range(scale_factor)]


def mock_floats(scale_factor: int, is_key: bool) -> list[Any]:
    if is_key:
        return [float(i) for i in range(1, scale_factor + 1)]
    return [random.uniform(0, 999_999) for _ in range(scale_factor)]


def mock_bytes(scale_factor: int, is_key: bool) -> list[Any]:
    if is_key:
        return [f"key_{i}".encode() for i in range(1, scale_factor + 1)]
    return [random.randbytes(12) for _ in range(scale_factor)]


def mock_bools(scale_factor: int, is_key: bool) -> list[Any]:
    # booleans can only have 2 unique values, so keys don't make sense here
    return [random.choice([True, False]) for _ in range(scale_factor)]


def mock_dates(scale_factor: int, is_key: bool) -> list[Any]:
    if is_key:
        base_date = date(2023, 1, 1)
        return [
            date.fromordinal(base_date.toordinal() + i) for i in range(scale_factor)
        ]
    return [date(2023, 1, random.randint(1, 28)) for _ in range(scale_factor)]


def mock_datetimes(scale_factor: int, is_key: bool) -> list[Any]:
    if is_key:
        base_dt = datetime(2023, 1, 1, 0, 0, 0)
        return [
            datetime.fromtimestamp(base_dt.timestamp() + i) for i in range(scale_factor)
        ]
    return [
        datetime(
            2023,
            1,
            1,
            random.randint(0, 23),
            random.randint(0, 59),
            random.randint(0, 59),
        )
        for _ in range(scale_factor)
    ]


def mock_decimals(
    precision: int, scale: int, scale_factor: int, is_key: bool
) -> list[Any]:
    # Decimal, not float: exact-numeric columns must not pick up binary-float
    # artifacts. Values are quantized to the declared scale.
    unit = Decimal(1).scaleb(-scale)
    if is_key:
        return [Decimal(i) * unit for i in range(1, scale_factor + 1)]
    hi = min(10**precision - 1, 999_999 * 10**scale)
    return [Decimal(random.randint(0, hi)) * unit for _ in range(scale_factor)]


def mock_arrays(datatype: ArrayType, scale_factor: int, is_key: bool) -> list[Any]:
    element = datatype.value_data_type
    if is_key:
        # rows are distinguished by a unique, same-typed leading element
        leads = mock_datatype(element, element, scale_factor, True)
        tail = mock_datatype(element, element, ARRAY_MOCK_SIZE - 1, False)
        return [[lead, *tail] for lead in leads]
    pool = mock_datatype(element, element, scale_factor * ARRAY_MOCK_SIZE, False)
    return [
        pool[i * ARRAY_MOCK_SIZE : (i + 1) * ARRAY_MOCK_SIZE]
        for i in range(scale_factor)
    ]


def mock_maps(datatype: MapType, scale_factor: int, is_key: bool) -> list[Any]:
    key_type = datatype.key_data_type
    value_type = datatype.value_data_type
    keys = mock_datatype(key_type, key_type, MAP_MOCK_SIZE, True)
    if is_key:
        # rows are distinguished by a unique value under the first key
        firsts = mock_datatype(value_type, value_type, scale_factor, True)
        rest = mock_datatype(value_type, value_type, MAP_MOCK_SIZE - 1, False)
        return [dict(zip(keys, [first, *rest])) for first in firsts]
    pool = mock_datatype(value_type, value_type, scale_factor * MAP_MOCK_SIZE, False)
    return [
        dict(zip(keys, pool[i * MAP_MOCK_SIZE : (i + 1) * MAP_MOCK_SIZE]))
        for i in range(scale_factor)
    ]


def mock_structs(datatype: StructType, scale_factor: int, is_key: bool) -> list[Any]:
    columns = {
        name: mock_datatype(ftype, ftype, scale_factor, is_key and idx == 0)
        for idx, (name, ftype) in enumerate(datatype.field_types.items())
    }
    return [
        {name: values[i] for name, values in columns.items()}
        for i in range(scale_factor)
    ]


BASE_GENERATORS: dict[DataType, Callable[[int, bool], list[Any]]] = {
    DataType.STRING: mock_strings,
    DataType.INTEGER: mock_integers,
    DataType.BIGINT: mock_integers,
    DataType.FLOAT: mock_floats,
    DataType.DOUBLE: mock_floats,
    DataType.NUMBER: mock_floats,
    DataType.BYTES: mock_bytes,
    DataType.BOOL: mock_bools,
    DataType.DATE: mock_dates,
    DataType.DATETIME: mock_datetimes,
    DataType.TIMESTAMP: mock_datetimes,
}


def mock_validated(
    full_type: ValidatedType, scale_factor: int, is_key: bool
) -> list[Any]:
    if full_type.pattern is not None:
        raise NotImplementedError(
            f"Mocking is not implemented for regex-validated type {full_type}"
        )
    base = full_type.data_type
    ranges = full_type.ranges
    if base in (DataType.INTEGER, DataType.BIGINT):
        pool: list[int] = []
        for r in ranges:
            lo = int(r.min) if r.min is not None else int(r.max) - 999_999  # type: ignore[arg-type]
            hi = int(r.max) if r.max is not None else int(r.min) + 999_999  # type: ignore[arg-type]
            pool.extend(range(lo, min(hi, lo + scale_factor - 1) + 1))
        if is_key:
            # A key must not repeat: a domain smaller than scale_factor caps
            # the row count (the mock table truncates to its shortest column).
            return pool[:scale_factor]
        return [random.choice(pool) for _ in range(scale_factor)]
    if base in (DataType.FLOAT, DataType.DOUBLE, DataType.NUMBER, DataType.NUMERIC):
        bounds: list[tuple[float, float]] = []
        for r in ranges:
            flo = float(r.min) if r.min is not None else float(r.max) - 999_999.0  # type: ignore[arg-type]
            fhi = float(r.max) if r.max is not None else float(r.min) + 999_999.0  # type: ignore[arg-type]
            bounds.append((flo, fhi))
        if is_key:
            flo, fhi = bounds[0]
            step = (fhi - flo) / max(scale_factor, 1)
            return [flo + step * i for i in range(scale_factor)]
        return [random.uniform(*random.choice(bounds)) for _ in range(scale_factor)]
    if base in (DataType.DATE, DataType.DATETIME, DataType.TIMESTAMP):
        default_span = (
            timedelta(days=999) if base == DataType.DATE else timedelta(seconds=999_999)
        )
        spans: list[tuple[Any, int]] = []
        for r in ranges:
            rmin: Any = r.min
            rmax: Any = r.max
            tlo = rmin if rmin is not None else rmax - default_span
            thi = rmax if rmax is not None else rmin + default_span
            delta = thi - tlo
            units = delta.days if base == DataType.DATE else int(delta.total_seconds())
            spans.append((tlo, units))
        unit = "days" if base == DataType.DATE else "seconds"
        if is_key:
            tlo, units = spans[0]
            return [
                tlo + timedelta(**{unit: i})
                for i in range(min(scale_factor, units + 1))
            ]
        out: list[Any] = []
        for _ in range(scale_factor):
            tlo, units = random.choice(spans)
            out.append(tlo + timedelta(**{unit: random.randint(0, units)}))
        return out
    raise NotImplementedError(f"Mocking not implemented for validated type {full_type}")


def mock_datatype(
    full_type: Any, datatype: CONCRETE_TYPES, scale_factor: int, is_key: bool = False
) -> list[Any]:
    if isinstance(full_type, ValidatedType):
        return mock_validated(full_type, scale_factor, is_key)
    if isinstance(full_type, EnumType):
        values = list(full_type.values)
        if not values:
            raise ValueError(f"Enum {full_type} has no values to mock")
        if is_key:
            # A key must not repeat: each enum value at most once. A domain
            # smaller than scale_factor caps the row count instead — the mock
            # table is truncated to its shortest column.
            return values[:scale_factor]
        return [random.choice(values) for _ in range(scale_factor)]
    if isinstance(full_type, TraitDataType):
        # An enum under a trait keeps its finite domain — the trait generator
        # would step outside it (e.g. enum<string>[...]::email_address). A
        # regex-validated string under a trait is the opposite case: the
        # generator is the only way to satisfy the pattern.
        if (
            not isinstance(full_type.type, EnumType)
            and full_type.type == DataType.STRING
        ):
            # TODO: get stdlib inventory some other way?
            if full_type.traits == ["email_address"]:
                # email mock function
                return mock_email(scale_factor, is_key)
            elif full_type.traits == ["hex"]:
                return mock_hex_code(scale_factor, is_key)
        return mock_datatype(full_type.type, full_type.type, scale_factor, is_key)

    concrete: Any = (
        full_type
        if isinstance(full_type, (NumericType, ArrayType, MapType, StructType))
        else datatype
    )
    # parameterless spellings of parameterized types get canonical defaults
    if concrete == DataType.NUMERIC:
        concrete = NumericType()
    elif concrete == DataType.ARRAY:
        concrete = BARE_ARRAY_DEFAULT
    elif concrete == DataType.MAP:
        concrete = BARE_MAP_DEFAULT

    if isinstance(concrete, NumericType):
        return mock_decimals(concrete.precision, concrete.scale, scale_factor, is_key)
    if isinstance(concrete, ArrayType):
        return mock_arrays(concrete, scale_factor, is_key)
    if isinstance(concrete, MapType):
        return mock_maps(concrete, scale_factor, is_key)
    if isinstance(concrete, StructType):
        return mock_structs(concrete, scale_factor, is_key)

    base = concrete if isinstance(concrete, DataType) else concrete.data_type
    generator = BASE_GENERATORS.get(base)
    if generator is None:
        raise NotImplementedError(f"Mocking is not implemented for datatype {datatype}")
    return generator(scale_factor, is_key)


def arrow_column_type(datatype: CONCRETE_TYPES) -> "pyarrow.DataType | None":
    """Explicit arrow type where inference would pick the wrong container:
    python dicts infer as struct, so declared maps need pa.map_. Everything
    else infers correctly from the generated values; None means infer."""
    import pyarrow as pa

    while isinstance(datatype, TraitDataType):
        datatype = datatype.type
    if datatype == DataType.MAP:
        datatype = BARE_MAP_DEFAULT
    if not isinstance(datatype, MapType):
        return None
    key = arrow_scalar_type(datatype.key_data_type)
    value = arrow_scalar_type(datatype.value_data_type)
    if key is None or value is None:
        return None
    return pa.map_(key, value)


def arrow_scalar_type(datatype: CONCRETE_TYPES) -> "pyarrow.DataType | None":
    import pyarrow as pa

    while isinstance(datatype, TraitDataType):
        datatype = datatype.type
    if isinstance(datatype, NumericType):
        return pa.decimal128(datatype.precision, datatype.scale)
    if not isinstance(datatype, DataType):
        return None
    scalar_map: dict[DataType, pyarrow.DataType] = {
        DataType.STRING: pa.string(),
        DataType.INTEGER: pa.int64(),
        DataType.BIGINT: pa.int64(),
        DataType.FLOAT: pa.float64(),
        DataType.DOUBLE: pa.float64(),
        DataType.NUMBER: pa.float64(),
        DataType.NUMERIC: pa.decimal128(NumericType().precision, NumericType().scale),
        DataType.BOOL: pa.bool_(),
        DataType.DATE: pa.date32(),
        DataType.DATETIME: pa.timestamp("us"),
        DataType.TIMESTAMP: pa.timestamp("us"),
        DataType.BYTES: pa.binary(),
    }
    return scalar_map.get(datatype)


def cast_target_map(environment: Environment) -> dict[str, CONCRETE_TYPES]:
    """Concepts whose physical column feeds a datasource-declared cast
    (`concept::type: other` column bindings) must mock as values that survive
    that cast; a random string in a column cast to date aborts validation."""
    out: dict[str, CONCRETE_TYPES] = {}
    for ds in environment.datasources.values():
        for col in ds.columns:
            if (
                isinstance(col.alias, Function)
                and col.alias.operator == FunctionType.CAST
                and len(col.alias.arguments) == 2
                and isinstance(col.alias.arguments[0], ConceptRef)
                and isinstance(
                    col.alias.arguments[1],
                    (
                        DataType,
                        MapType,
                        ArrayType,
                        NumericType,
                        StructType,
                        TraitDataType,
                        EnumType,
                        ValidatedType,
                    ),
                )
            ):
                out[col.alias.arguments[0].address] = col.alias.arguments[1]
    return out


class MockManager:

    def __init__(
        self, environment: Environment, scale_factor: int = DEFAULT_SCALE_FACTOR
    ):
        self.environment = environment
        self.concept_mocks: dict[str, Any] = {}
        self.scale_factor = scale_factor
        # Concepts that must be unique-per-row to satisfy any datasource grain.
        # Without this, an aggregate datasource grained on a non-KEY concept
        # (e.g. a date) gets duplicate rows and fails grain validation.
        self.key_addresses: set[str] = {
            addr for addr, c in environment.concepts.items() if c.purpose == Purpose.KEY
        }
        for ds in environment.datasources.values():
            self.key_addresses.update(ds.grain.components)
        self.cast_targets = cast_target_map(environment)

    def mock_concept(self, concept: Concept | ConceptRef):
        if concept.address in self.concept_mocks:
            return False
        is_key = concept.address in self.key_addresses
        cast_target = self.cast_targets.get(concept.address)
        try:
            if (
                cast_target is not None
                and concept.datatype.data_type == DataType.STRING
            ):
                self.concept_mocks[concept.address] = [
                    str(v)
                    for v in mock_datatype(
                        cast_target, cast_target, self.scale_factor, is_key
                    )
                ]
            else:
                self.concept_mocks[concept.address] = mock_datatype(
                    concept.datatype,
                    concept.output_datatype,
                    self.scale_factor,
                    is_key,
                )
        except NotImplementedError as e:
            raise NotImplementedError(
                f"Cannot mock column bound to {concept.address}: {e}"
            ) from e
        return True

    def create_mock_table(
        self, concepts: Iterable[Concept | ConceptRef], headers: list[str]
    ) -> "Table":
        from pyarrow import array, table

        concepts = list(concepts)
        # A key with a finite domain (enum, validated range) yields fewer than
        # scale_factor values; the table caps at its shortest column so keys
        # stay unique.
        n = min(len(self.concept_mocks[c.address]) for c in concepts)
        data: dict[str, Any] = {}
        for h, c in zip(headers, concepts):
            values = self.concept_mocks[c.address][:n]
            explicit = arrow_column_type(c.datatype)
            data[h] = array(values, type=explicit) if explicit is not None else values
        return table(data)


def handle_processed_mock_statement(
    query: ProcessedMockStatement, environment: Environment, executor
) -> MockResult:
    """Handle processed mock statements."""
    # For mock statements, we can simulate some output based on targets
    mock_manager = MockManager(environment)
    output = []
    for target in query.targets:
        datasource = environment.datasources.get(target)
        if not datasource:
            raise ValueError(f"Datasource {target} not found in environment")
        mock_datasource(datasource, mock_manager, executor)
        output.append(
            {
                "target": target,
                "status": "mocked",
            }
        )
    return MockResult(output, ["target", "status"])


def mock_datasource(datasource: Datasource, manager: MockManager, executor):
    concrete: list[ConceptRef] = []
    headers: list[str] = []
    for k, col in datasource.concrete_columns.items():
        manager.mock_concept(col.concept)
        concrete.append(col.concept)
        headers.append(k)

    table = manager.create_mock_table(concrete, headers)

    # duckdb load the pyarrow table
    executor.execute_raw_sql(
        "register(:name, :tbl)", {"name": "mock_tbl", "tbl": table}
    )
    address = safe_name(datasource.safe_address)
    executor.execute_write_sql(
        f"""CREATE OR REPLACE TABLE {address} AS SELECT * FROM mock_tbl"""
    )
    # overwrite the address since we've mangled the name
    datasource.address = Address(location=address)
