from pathlib import Path

from pytest import raises

from trilogy import Dialects, Environment
from trilogy.core.exceptions import DatasourceModelValidationError, ModelValidationError
from trilogy.core.validation.environment import validate_environment
from trilogy.hooks import DebuggingHook


def test_dataset_validation_iris():
    DebuggingHook()
    env = Environment(
        working_path=Path(__file__).parent,
    )
    exec = Dialects.DUCK_DB.default_executor(environment=env)
    with open(Path(__file__).parent / "setup.sql", "r") as f:
        exec.execute_raw_sql(f.read())

    exec.parse_text("""import iris;""")

    validate_environment(env, exec=exec)


def test_bad_column_validation_iris():
    env = Environment(
        working_path=Path(__file__).parent,
    )
    exec = Dialects.DUCK_DB.default_executor(environment=env)
    with open(Path(__file__).parent / "setup.sql", "r") as f:
        exec.execute_raw_sql(f.read())

    exec.parse_text("""import iris_bad;""")

    with raises(ModelValidationError) as e:
        validate_environment(env, exec=exec)
        assert any(
            isinstance(c, DatasourceModelValidationError) for c in e.value.children
        )


def test_join_validation_iris():

    env = Environment(
        working_path=Path(__file__).parent,
    )

    exec = Dialects.DUCK_DB.default_executor(environment=env)
    with open(Path(__file__).parent / "setup.sql", "r") as f:
        exec.execute_raw_sql(f.read())

    exec.parse_text("""import iris_joins;""")

    with raises(ModelValidationError):
        validate_environment(env, exec=exec)


def test_grain_iris():
    DebuggingHook()
    env = Environment(
        working_path=Path(__file__).parent,
    )

    exec = Dialects.DUCK_DB.default_executor(environment=env)
    with open(Path(__file__).parent / "setup.sql", "r") as f:
        exec.execute_raw_sql(f.read())

    exec.parse_text("""import iris_grain;""")

    with raises(ModelValidationError):
        validate_environment(env, exec=exec)
