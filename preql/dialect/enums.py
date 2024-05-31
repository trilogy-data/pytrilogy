from enum import Enum
from typing import List, TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from preql.hooks.base_hook import BaseHook
    from preql import Executor, Environment

from preql.dialect.config import DialectConfig
from preql.constants import logger


class Dialects(Enum):
    BIGQUERY = "bigquery"
    SQL_SERVER = "sql_server"
    DUCK_DB = "duck_db"
    PRESTO = "presto"
    TRINO = "trino"
    POSTGRES = "postgres"
    SNOWFLAKE = "snowflake"

    @classmethod
    def _missing_(cls, value):
        if value == "duckdb":
            return cls.DUCK_DB
        return super()._missing_(value)

    def default_engine(self, conf=None):
        if self == Dialects.BIGQUERY:
            from sqlalchemy import create_engine
            from google.auth import default
            from google.cloud import bigquery

            credentials, project = default()
            client = bigquery.Client(credentials=credentials, project=project)
            return create_engine(
                f"bigquery://{project}?user_supplied_client=True",
                connect_args={"client": client},
            )
        elif self == Dialects.SQL_SERVER:
            from sqlalchemy import create_engine

            raise NotImplementedError()
        elif self == Dialects.DUCK_DB:
            from sqlalchemy import create_engine
            from preql.dialect.config import DuckDBConfig

            if not conf:
                conf = DuckDBConfig()
            if not isinstance(conf, DuckDBConfig):
                raise TypeError("Invalid dialect configuration for type duck_db")
            return create_engine(conf.connection_string(), future=True)
        elif self == Dialects.SNOWFLAKE:
            from sqlalchemy import create_engine
            from preql.dialect.config import SnowflakeConfig

            if not isinstance(conf, SnowflakeConfig):
                raise TypeError("Invalid dialect configuration for type snowflake")
            return create_engine(conf.connection_string(), future=True)
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
            from sqlalchemy import create_engine
            from preql.dialect.config import PostgresConfig

            if not isinstance(conf, PostgresConfig):
                raise TypeError("Invalid dialect configuration for type postgres")

            return create_engine(conf.connection_string(), future=True)
        else:
            raise ValueError(
                f"Unsupported dialect {self} for default engine creation; create one explicitly."
            )

    def default_executor(
        self,
        environment: Optional["Environment"] = None,
        hooks: List["BaseHook"] | None = None,
        conf: DialectConfig | None = None,
    ) -> "Executor":
        from preql import Executor, Environment

        return Executor(
            engine=self.default_engine(conf=conf),
            environment=environment or Environment(),
            dialect=self,
            hooks=hooks,
        )
