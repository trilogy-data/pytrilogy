from pathlib import Path

from trilogy import Dialects, Environment, Executor
from trilogy.core.enums import ComparisonOperator
from trilogy.authoring import Concept, Datasource, ConceptRef, Function, DataType
from trilogy.core.enums import Purpose, FunctionType
from trilogy.core.models.build import (
    BuildConcept,
    BuildDatasource,
    BuildConditional,
    BuildComparison,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.models.execute import (
    CTE,
    QueryDatasource,
)
from trilogy.core.statements.execute import CTE, ProcessedQuery
from trilogy.hooks import DebuggingHook
from trilogy.parsing.common import function_to_concept
from trilogy.core.validation.datasource import validate_datasource
from trilogy.core.validation.environment import validate_environment
from trilogy.core.exceptions import DatasourceModelValidationError
from pytest import raises


def test_dataset_validation_iris():
    DebuggingHook()
    env = Environment(
        working_path=Path(__file__).parent,
    )
    exec = Dialects.DUCK_DB.default_executor(environment=env)
    with open(Path(__file__).parent / "setup.sql", "r") as f:
        exec.execute_raw_sql(f.read())

    exec.parse_text("""import iris;""")

    validate_environment(env, exec)

    env = Environment(
        working_path=Path(__file__).parent,
    )
    exec = Dialects.DUCK_DB.default_executor(environment=env)
    with open(Path(__file__).parent / "setup.sql", "r") as f:
        exec.execute_raw_sql(f.read())

    exec.parse_text("""import iris_bad;""")

    with raises(DatasourceModelValidationError):
        validate_environment(env, exec)


def test_join_validation_iris():
    DebuggingHook()
    env = Environment(
        working_path=Path(__file__).parent,
    )

    exec = Dialects.DUCK_DB.default_executor(environment=env)
    with open(Path(__file__).parent / "setup.sql", "r") as f:
        exec.execute_raw_sql(f.read())

    exec.parse_text("""import iris_joins;""")

    with raises(DatasourceModelValidationError):
        validate_environment(env, exec)
