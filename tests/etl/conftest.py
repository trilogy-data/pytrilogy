import pytest
from trilogy import Dialects, Environment
from pathlib import Path


@pytest.fixture(scope="module")
def executor():
    base = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=Path(__file__).parent)
    )
    base.execute_file(Path(__file__).parent / "generate.sql")
    yield base
