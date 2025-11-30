from pathlib import Path

import pytest

from trilogy import Dialects, Environment


@pytest.fixture(scope="function")
def executor():
    base = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=Path(__file__).parent)
    )
    base.execute_file(Path(__file__).parent / "generate.sql")
    yield base
