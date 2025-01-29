from typing import TYPE_CHECKING, Any

from sqlalchemy import text

from trilogy.core.models.environment import Environment
from trilogy.dialect.duckdb import DuckDBDialect
from trilogy.engine import ExecutionEngine

if TYPE_CHECKING:
    try:
        from pandas import DataFrame
    except ImportError:
        DataFrame = Any


class DataframeDialect(DuckDBDialect):
    pass


class DataframeConnectionWrapper(ExecutionEngine):
    def __init__(self, engine: ExecutionEngine, dataframes: dict[str, "DataFrame"]):
        self.engine = engine
        self.dataframes = dataframes
        self.connection = None

    def setup(self, env: Environment, connection):
        self._register_dataframes(env, connection)

    def _register_dataframes(self, env: Environment, connection):
        for ds in env.datasources.values():
            if ds.safe_address in self.dataframes:
                connection.execute(
                    text("register(:name, :df)"),
                    {"name": ds.safe_address, "df": self.dataframes[ds.safe_address]},
                )
            else:
                raise ValueError(
                    f"Dataframe {ds.safe_address} not found in dataframes on connection config, have {self.dataframes.keys()}"
                )
        pass

    def add_dataframe(self, name: str, df: "DataFrame", connection, env: Environment):
        self.dataframes[name] = df
        self._register_dataframes(env, connection)

    def connect(self) -> Any:
        return self.engine.connect()
