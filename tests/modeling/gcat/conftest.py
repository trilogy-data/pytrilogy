from pathlib import Path

from pytest import fixture

from trilogy import Dialects, Environment

ROOT = Path(__file__).parent


@fixture(scope="session")
def gcat_env():
    env = Environment(
        working_path=Path(__file__).parent,
    )
    base = Dialects.DUCK_DB.default_executor(environment=env)
    base.execute_raw_sql(ROOT / "setup.sql")
    yield base
