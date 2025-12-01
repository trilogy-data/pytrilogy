from trilogy.core.statements.execute import ProcessedMockStatement
from trilogy.core.models.environment import Environment
from trilogy.core.models.datasource import Datasource
from trilogy.core.models.author import Concept, ConceptRef
from trilogy.core.models.core import DataType, CONCRETE_TYPES, ArrayType
from trilogy.dialect.results import MockResult 
import random
import pandas as pd

def mock_datatype(full_type:CONCRETE_TYPES, datatype: CONCRETE_TYPES, scale_factor:int = 100):
    if datatype == DataType.INTEGER:
        # random integer in array of scale factor size
        return [
            random.randint(0, 999_999) for _ in range(scale_factor)
        ]
    elif datatype == DataType.STRING:
        # random string in array of scale factor size
       return [
            f"mock_string_{random.randint(0, 999_999)}" for _ in range(scale_factor)
        ]
    elif datatype == DataType.FLOAT:
        # random float in array of scale factor size
        return  [
            random.uniform(0, 999_999) for _ in range(scale_factor)
        ]
    elif datatype == DataType.BOOL:
        # random boolean in array of scale factor size
        return [
            random.choice([True, False]) for _ in range(scale_factor)
        ]
    elif datatype== DataType.DATE:
        # random date in array of scale factor size
        return [
            f"2023-01-{random.randint(1,28):02d}" for _ in range(scale_factor)
        ]
    elif datatype == DataType.DATETIME:
        # random datetime in array of scale factor size
        return [
            f"2023-01-01T{random.randint(0,23):02d}:{random.randint(0,59):02d}:{random.randint(0,59):02d}Z" for _ in range(scale_factor)
        ]
    elif isinstance(datatype, ArrayType):
        return [
            [mock_datatype(datatype.value, datatype.value_data_type, 5)] for _ in range(scale_factor)
            ]
    raise NotImplementedError(f"Mocking not implemented for datatype {datatype}")

class MockManager():

    def __init__(self, environment:Environment, scale_factor:int = 100):
        self.environment = environment
        self.concept_mocks = {}
        self.scale_factor = scale_factor    

    def mock_concept(self, concept:Concept  | ConceptRef):
        if concept.address in self.concept_mocks:
            return False
        self.concept_mocks[concept.address] = mock_datatype(
            concept.datatype,
            concept.output_datatype,
            self.scale_factor
        )
        return True

    def create_mock_dataframe(self, concepts: list[Concept | ConceptRef], headers: list[str]) -> pd.DataFrame:  
        data = [self.concept_mocks[x.address] for x in concepts]
        rows = zip(*data)
        return pd.DataFrame(rows, columns=headers)
        
        


def handle_processed_mock_statement(
    query: ProcessedMockStatement,  environment: Environment, executor
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


def mock_datasource(datasource:Datasource, manager:MockManager, executor):
    concrete = []
    headers:list[str] = []
    for x in datasource.columns:
        if not x.is_concrete:
            continue
        manager.mock_concept(x.concept)
        concrete.append(x.concept)
        headers.append(x.alias)
    
    df = manager.create_mock_dataframe(concrete, headers)

    # duckdb load the dataframe to a table
    executor.execute_raw_sql("register(:name, :df)", {"name": 'df', "df": df})
    executor.execute_raw_sql(f'''CREATE OR REPLACE TABLE {datasource.safe_address} AS SELECT * FROM df''')




