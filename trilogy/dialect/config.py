from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    try:
        from pandas import DataFrame
    except ImportError:
        DataFrame = Any


@dataclass
class RetryPolicy:
    """Defines retry behavior for matching errors."""

    max_attempts: int = 3
    base_delay_seconds: float = 1.0
    max_delay_seconds: float = 60.0
    exponential_base: float = 2.0

    def get_delay(self, attempt: int) -> float:
        """Calculate delay for a given attempt using exponential backoff."""
        delay = self.base_delay_seconds * (self.exponential_base ** (attempt - 1))
        return min(delay, self.max_delay_seconds)


@dataclass
class RetryConfig:
    """Configuration for retry behavior with regex pattern matching."""

    patterns: dict[str, RetryPolicy] = field(default_factory=dict)

    def get_policy_for_error(self, error_message: str) -> RetryPolicy | None:
        """Find matching retry policy for an error message."""
        import re

        for pattern, policy in self.patterns.items():
            if re.search(pattern, error_message, re.IGNORECASE):
                return policy
        return None


class DialectConfig:
    def __init__(self, retry_config: RetryConfig | None = None):
        self.retry_config = retry_config

    def connection_string(self) -> str:
        raise NotImplementedError

    def create_connect_args(self) -> dict:
        return {}

    def merge_config(self, other: "DialectConfig") -> "DialectConfig":
        for key, value in other.__dict__.items():
            if value is not None:
                setattr(self, key, value)
        return self


class BigQueryConfig(DialectConfig):
    def __init__(
        self,
        project: str | None = None,
        client: Any | None = None,
        retry_config: RetryConfig | None = None,
    ):
        super().__init__(retry_config=retry_config)
        self.project = project
        self.client = client

    def connection_string(self) -> str:
        return f"bigquery://{self.project}?user_supplied_client=True"

    def create_connect_args(self) -> dict:
        if not self.client:
            from google.auth import default
            from google.cloud import bigquery

            credentials, project = default()
            self.client = bigquery.Client(credentials=credentials, project=project)
            self.project = project

        return {"client": self.client}


class DuckDBConfig(DialectConfig):
    def __init__(
        self,
        path: str | None = None,
        enable_python_datasources: bool | None = None,
        enable_gcs: bool | None = None,
        retry_config: RetryConfig | None = None,
    ):
        super().__init__(retry_config=retry_config)
        self.path = path
        self._enable_python_datasources = enable_python_datasources
        self._enable_gcs = enable_gcs
        self.guid = id(self)

    @property
    def enable_python_datasources(self) -> bool:
        return self._enable_python_datasources or False

    @property
    def enable_gcs(self) -> bool:
        return self._enable_gcs or False

    def connection_string(self) -> str:
        if not self.path:
            return "duckdb:///:memory:"
        return f"duckdb:///{self.path}"


class PostgresConfig(DialectConfig):
    def __init__(
        self,
        host: str,
        port: int,
        username: str,
        password: str,
        database: str,
        retry_config: RetryConfig | None = None,
    ):
        super().__init__(retry_config=retry_config)
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.database = database

    def connection_string(self) -> str:
        return f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}"


class SQLServerConfig(DialectConfig):
    def __init__(
        self,
        host: str,
        port: int,
        username: str,
        password: str,
        database: str,
        retry_config: RetryConfig | None = None,
    ):
        super().__init__(retry_config=retry_config)
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.database = database

    def connection_string(self) -> str:
        return f"sqlserver//{self.username}:{self.password}@{self.host}:{self.port}"


class SnowflakeConfig(DialectConfig):
    def __init__(
        self,
        account: str,
        username: str,
        password: str,
        database: str | None = None,
        schema: str | None = None,
        retry_config: RetryConfig | None = None,
    ):
        super().__init__(retry_config=retry_config)
        self.account = account
        self.username = username
        self.password = password
        self.database = database
        self.schema = schema
        if self.schema and not self.database:
            raise ValueError("Setting snowflake schema also requires setting database")

    def connection_string(self) -> str:
        if self.schema:
            return f"snowflake://{self.username}:{self.password}@{self.account}/{self.database}/{self.schema}"
        if self.database:
            return f"snowflake://{self.username}:{self.password}@{self.account}/{self.database}"
        return f"snowflake://{self.username}:{self.password}@{self.account}"


class PrestoConfig(DialectConfig):
    def __init__(
        self,
        host: str,
        port: int,
        username: str,
        password: str,
        catalog: str,
        schema: str | None = None,
        retry_config: RetryConfig | None = None,
    ):
        super().__init__(retry_config=retry_config)
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.catalog = catalog
        self.schema = schema

    def connection_string(self) -> str:
        if self.schema:
            return f"presto://{self.username}:{self.password}@{self.host}:{self.port}/{self.catalog}/{self.schema}"
        return f"presto://{self.username}:{self.password}@{self.host}:{self.port}/{self.catalog}"


class TrinoConfig(PrestoConfig):
    def connection_string(self) -> str:
        if self.schema:
            return f"trino://{self.username}:{self.password}@{self.host}:{self.port}/{self.catalog}/{self.schema}"
        return f"trino://{self.username}:{self.password}@{self.host}:{self.port}/{self.catalog}"


class DataFrameConfig(DuckDBConfig):
    def __init__(self, dataframes: dict[str, "DataFrame"]):
        super().__init__()
        self.dataframes = dataframes
