from pathlib import Path

from pytest import fixture

from trilogy import Dialects, Environment, Executor

ROOT = Path(__file__).parent


@fixture(scope="session")
def gcat_env_base():
    env = Environment(
        working_path=Path(__file__).parent,
    )
    base = Dialects.DUCK_DB.default_executor(environment=env)
    base.execute_raw_sql(ROOT / "setup.sql")
    yield base


@fixture(scope="function")
def gcat_env(gcat_env_base: Executor):
    gcat_env_base.environment = Environment(
        working_path=Path(__file__).parent,
    )

    yield gcat_env_base
