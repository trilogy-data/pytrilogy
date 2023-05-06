from enum import Enum
from typing import List, TYPE_CHECKING

if TYPE_CHECKING:
    from preql.hooks.base_hook import BaseHook
    from preql import Executor, Environment


class Dialects(Enum):
    BIGQUERY = "bigquery"
    SQL_SERVER = "sql_server"
    DUCK_DB = "duck_db"

    def default_engine(self):
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

            return create_engine(r"duckdb:///:memory:", future=True)
        else:
            raise ValueError(f"Unsupported dialect {self}")

    def default_executor(
        self, environment: "Environment", hooks: List["BaseHook"] | None = None
    ) -> "Executor":
        from preql import Executor

        return Executor(
            engine=self.default_engine(),
            environment=environment,
            dialect=self,
            hooks=hooks,
        )
