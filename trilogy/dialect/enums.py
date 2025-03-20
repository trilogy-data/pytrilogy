from enum import Enum
from typing import TYPE_CHECKING, Callable, List, Optional

from trilogy.core.models.environment import Environment

if TYPE_CHECKING:
    from trilogy import Executor
    from trilogy.hooks.base_hook import BaseHook

from trilogy.constants import Rendering, logger
from trilogy.dialect.config import DialectConfig


def default_factory(conf: DialectConfig, config_type):
    from sqlalchemy import create_engine

    if not isinstance(conf, config_type):
        raise TypeError(
            f"Invalid dialect configuration for type {type(config_type).__name__}, is {type(conf)}"
        )
    if conf.connect_args:
        return create_engine(
            conf.connection_string(), future=True, connect_args=conf.connect_args
        )
    return create_engine(conf.connection_string(), future=True)


class Dialects(Enum):
    BIGQUERY = "bigquery"
    SQL_SERVER = "sql_server"
    DUCK_DB = "duck_db"
    PRESTO = "presto"
    TRINO = "trino"
    POSTGRES = "postgres"
    SNOWFLAKE = "snowflake"
    DATAFRAME = "dataframe"

    @classmethod
    def _missing_(cls, value):
        if value == "duckdb":
            return cls.DUCK_DB
        return super()._missing_(value)

    def default_renderer(self, conf=None, _engine_factory: Callable = default_factory):
        from trilogy.render import get_dialect_generator

        return get_dialect_generator(self)

    def default_engine(self, conf=None, _engine_factory: Callable = default_factory):
        if self == Dialects.BIGQUERY:
            from google.auth import default
            from google.cloud import bigquery

            from trilogy.dialect.config import BigQueryConfig

            credentials, project = default()
            client = bigquery.Client(credentials=credentials, project=project)
            conf = conf or BigQueryConfig(project=project, client=client)
            return _engine_factory(
                conf,
                BigQueryConfig,
            )
        elif self == Dialects.SQL_SERVER:
            raise NotImplementedError()
        elif self == Dialects.DUCK_DB:
            from trilogy.dialect.config import DuckDBConfig

            if not conf:
                conf = DuckDBConfig()
            return _engine_factory(conf, DuckDBConfig)
        elif self == Dialects.SNOWFLAKE:
            from trilogy.dialect.config import SnowflakeConfig

            return _engine_factory(conf, SnowflakeConfig)
        elif self == Dialects.POSTGRES:
            logger.warn(
                "WARN: Using experimental postgres dialect. Most functionality will not work."
            )
            import importlib

            spec = importlib.util.find_spec("psycopg2")
            if spec is None:
                raise ImportError(
                    "postgres driver not installed. python -m pip install pypreql[postgres]"
                )
            from trilogy.dialect.config import PostgresConfig

            return _engine_factory(conf, PostgresConfig)
        elif self == Dialects.PRESTO:
            from trilogy.dialect.config import PrestoConfig

            return _engine_factory(conf, PrestoConfig)
        elif self == Dialects.TRINO:
            from trilogy.dialect.config import TrinoConfig

            return _engine_factory(conf, TrinoConfig)
        elif self == Dialects.DATAFRAME:
            from trilogy.dialect.config import DataFrameConfig
            from trilogy.dialect.dataframe import DataframeConnectionWrapper

            if not conf:
                conf = DataFrameConfig(dataframes={})

            base = _engine_factory(conf, DataFrameConfig)

            return DataframeConnectionWrapper(base, dataframes=conf.dataframes)
        else:
            raise ValueError(
                f"Unsupported dialect {self} for default engine creation; create one explicitly."
            )

    def default_executor(
        self,
        environment: Optional["Environment"] = None,
        hooks: List["BaseHook"] | None = None,
        conf: DialectConfig | None = None,
        rendering: Rendering | None = None,
        _engine_factory: Callable | None = None,
    ) -> "Executor":
        from trilogy import Executor

        if _engine_factory is not None:
            return Executor(
                engine=self.default_engine(conf=conf, _engine_factory=_engine_factory),
                environment=environment or Environment(),
                dialect=self,
                rendering=rendering,
                hooks=hooks,
            )

        return Executor(
            engine=self.default_engine(conf=conf),
            environment=environment or Environment(),
            dialect=self,
            rendering=rendering,
            hooks=hooks,
        )
