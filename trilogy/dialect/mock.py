import random
from datetime import date, datetime
from typing import TYPE_CHECKING, Any, Iterable

from trilogy.core.enums import Purpose
from trilogy.core.models.author import Concept, ConceptRef
from trilogy.core.models.core import CONCRETE_TYPES, ArrayType, DataType, TraitDataType
from trilogy.core.models.datasource import Address, Datasource
from trilogy.core.models.environment import Environment
from trilogy.core.statements.execute import ProcessedMockStatement
from trilogy.dialect.results import MockResult

if TYPE_CHECKING:
    from pyarrow import Table

DEFAULT_SCALE_FACTOR = 100


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


def mock_datatype(
    full_type: Any, datatype: CONCRETE_TYPES, scale_factor: int, is_key: bool = False
) -> list[Any]:
    if isinstance(full_type, TraitDataType):
        if full_type.type == DataType.STRING:
            # TODO: get stdlib inventory some other way?
            if full_type.traits == ["email_address"]:
                # email mock function
                return mock_email(scale_factor, is_key)
            elif full_type.traits == ["hex"]:
                return mock_hex_code(scale_factor, is_key)
        return mock_datatype(full_type.type, full_type.type, scale_factor, is_key)
    elif datatype == DataType.INTEGER:
        if is_key:
            # unique integers for keys
            return list(range(1, scale_factor + 1))
        return [random.randint(0, 999_999) for _ in range(scale_factor)]
    elif datatype == DataType.STRING:
        if is_key:
            # unique strings for keys
            return [f"key_{i}" for i in range(1, scale_factor + 1)]
        return [
            f"mock_string_{random.randint(0, 999_999)}" for _ in range(scale_factor)
        ]
    elif datatype == DataType.FLOAT:
        if is_key:
            # unique floats for keys
            return [float(i) for i in range(1, scale_factor + 1)]
        return [random.uniform(0, 999_999) for _ in range(scale_factor)]
    elif datatype == DataType.NUMERIC:
        if is_key:
            # unique numerics for keys
            return [float(i) for i in range(1, scale_factor + 1)]
        return [round(random.uniform(0, 999_999), 2) for _ in range(scale_factor)]
    elif datatype == DataType.BOOL:
        # booleans can only have 2 unique values, so keys don't make sense here
        return [random.choice([True, False]) for _ in range(scale_factor)]
    elif datatype == DataType.DATE:
        if is_key:
            # unique dates for keys - spread across multiple months/years if needed
            base_date = date(2023, 1, 1)
            return [
                date.fromordinal(base_date.toordinal() + i) for i in range(scale_factor)
            ]
        return [date(2023, 1, random.randint(1, 28)) for _ in range(scale_factor)]
    elif datatype in (DataType.DATETIME, DataType.TIMESTAMP):
        if is_key:
            # unique datetimes for keys - increment by seconds
            base_dt = datetime(2023, 1, 1, 0, 0, 0)
            return [
                datetime.fromtimestamp(base_dt.timestamp() + i)
                for i in range(scale_factor)
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
    elif isinstance(datatype, ArrayType):
        # arrays as keys don't typically make sense, but generate unique if requested
        if is_key:
            return [
                [mock_datatype(datatype.type, datatype.value_data_type, 5, False)[0], i]
                for i in range(scale_factor)
            ]
        return [
            [mock_datatype(datatype.type, datatype.value_data_type, 5, False)]
            for _ in range(scale_factor)
        ]
    raise NotImplementedError(f"Mocking not implemented for datatype {datatype}")


class MockManager:

    def __init__(
        self, environment: Environment, scale_factor: int = DEFAULT_SCALE_FACTOR
    ):
        self.environment = environment
        self.concept_mocks: dict[str, Any] = {}
        self.scale_factor = scale_factor

    def mock_concept(self, concept: Concept | ConceptRef):
        if concept.address in self.concept_mocks:
            return False
        concrete = self.environment.concepts[concept.address]
        self.concept_mocks[concept.address] = mock_datatype(
            concept.datatype,
            concept.output_datatype,
            self.scale_factor,
            True if concrete.purpose == Purpose.KEY else False,
        )
        return True

    def create_mock_table(
        self, concepts: Iterable[Concept | ConceptRef], headers: list[str]
    ) -> "Table":
        from pyarrow import table

        data = {h: self.concept_mocks[c.address] for h, c in zip(headers, concepts)}
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
    executor.execute_raw_sql(
        f"""CREATE OR REPLACE TABLE {address} AS SELECT * FROM mock_tbl"""
    )
    # overwrite the address since we've mangled the name
    datasource.address = Address(location=address)
