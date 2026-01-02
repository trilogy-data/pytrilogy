from pathlib import Path

from trilogy import Dialects, Environment
from trilogy.dialect import DuckDBConfig
from trilogy.hooks import DebuggingHook

BACKUP = """
import launch;

persist launch_info;
        """


def test_refresh():
    exec = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=Path(__file__).parent),
        conf=DuckDBConfig(
            enable_gcs=True,
            enable_python_datasources=True,
        ),
    )
    DebuggingHook()
    sql = exec.generate_sql(
        """
import launch;

persist launch_info;
        """
    )[-1]

    assert "1=1" not in sql, sql
