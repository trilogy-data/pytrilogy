import random
from datetime import date, datetime
from typing import TYPE_CHECKING, Any, Iterable

from trilogy.core.models.author import Concept, ConceptRef
from trilogy.core.models.core import CONCRETE_TYPES, ArrayType, DataType
from trilogy.core.models.datasource import Datasource
from trilogy.core.models.environment import Environment
from trilogy.core.statements.execute import ProcessedMockStatement
from trilogy.dialect.results import MockResult

if TYPE_CHECKING:
    from pyarrow import Table

DEFAULT_SCALE_FACTOR = 100


def mock_datatype(
    full_type: Any, datatype: CONCRETE_TYPES, scale_factor: int
) -> list[Any]:
    if datatype == DataType.INTEGER:
        # random integer in array of scale factor size
        return [random.randint(0, 999_999) for _ in range(scale_factor)]
    elif datatype == DataType.STRING:
        # random string in array of scale factor size
        return [
            f"mock_string_{random.randint(0, 999_999)}" for _ in range(scale_factor)
        ]
    elif datatype == DataType.FLOAT:
        # random float in array of scale factor size
        return [random.uniform(0, 999_999) for _ in range(scale_factor)]
    elif datatype == DataType.BOOL:
        # random boolean in array of scale factor size
        return [random.choice([True, False]) for _ in range(scale_factor)]
    elif datatype == DataType.DATE:
        # random date in array of scale factor size
        return [date(2023, 1, random.randint(1, 28)) for _ in range(scale_factor)]
    elif datatype in (DataType.DATETIME, DataType.TIMESTAMP):
        # random datetime in array of scale factor size
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
        return [
            [mock_datatype(datatype.type, datatype.value_data_type, 5)]
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
        self.concept_mocks[concept.address] = mock_datatype(
            concept.datatype, concept.output_datatype, self.scale_factor
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
    executor.execute_raw_sql(
        f"""CREATE OR REPLACE TABLE {datasource.safe_address} AS SELECT * FROM mock_tbl"""
    )
