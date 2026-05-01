import tempfile
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

from trilogy.constants import REMOTE_PREFIXES

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

    def create_engine_args(self) -> dict:
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
        enable_spatial: bool | None = None,
        gcs_cache_bust: bool | None = None,
        retry_config: RetryConfig | None = None,
    ):
        super().__init__(retry_config=retry_config)
        self.path = path
        self._enable_python_datasources = enable_python_datasources
        self._enable_gcs = enable_gcs
        self._enable_spatial = enable_spatial
        self._gcs_cache_bust = gcs_cache_bust
        self.guid = id(self)

    @property
    def enable_python_datasources(self) -> bool:
        return self._enable_python_datasources or False

    @property
    def enable_gcs(self) -> bool:
        return self._enable_gcs or False

    @property
    def enable_spatial(self) -> bool:
        return self._enable_spatial or False

    @property
    def gcs_cache_bust(self) -> bool:
        return self._gcs_cache_bust or False

    def connection_string(self) -> str:
        if not self.path:
            return "duckdb:///:memory:"
        return f"duckdb:///{self.path}"


class SQLiteConfig(DialectConfig):
    def __init__(
        self,
        path: str | None = None,
        retry_config: RetryConfig | None = None,
        staging_path: str | None = None,
    ):
        super().__init__(retry_config=retry_config)
        self._remote = bool(path and path.startswith(REMOTE_PREFIXES))
        self.path: str | None
        if self._remote:
            self.path = self._download_remote(path, staging_path)  # type: ignore
        else:
            self.path = path

    @staticmethod
    def _download_remote(url: str, staging_path: str | None = None) -> str:
        tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False, dir=staging_path)
        urllib.request.urlretrieve(url, tmp.name)
        return tmp.name

    def connection_string(self) -> str:
        if not self.path:
            return "sqlite:///:memory:"
        if self._remote:
            return "sqlite://"
        return f"sqlite:///{self.path}"

    def create_engine_args(self) -> dict:
        if self._remote:
            import sqlite3

            assert self.path is not None
            posix_path = Path(self.path).as_posix()
            return {
                "creator": lambda: sqlite3.connect(
                    f"file:{posix_path}?mode=ro", uri=True
                )
            }
        return {}


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


class ClickhouseConfig(DialectConfig):
    """Config for ClickHouse.

    mode="chdb": embedded in-process via chdb (good for unit tests / local dev).
    mode="server": remote/local ClickHouse server via clickhouse-sqlalchemy.
    """

    def __init__(
        self,
        host: str | None = None,
        port: int | None = None,
        username: str | None = None,
        password: str | None = None,
        database: str | None = None,
        secure: bool = False,
        mode: str = "chdb",
        chdb_path: str | None = None,
        retry_config: RetryConfig | None = None,
    ):
        super().__init__(retry_config=retry_config)
        if mode not in ("chdb", "server"):
            raise ValueError(
                f"ClickhouseConfig mode must be 'chdb' or 'server', got {mode!r}"
            )
        self.mode = mode
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.database = database
        self.secure = secure
        self.chdb_path = chdb_path

    def connection_string(self) -> str:
        if self.mode == "chdb":
            return f"chdb:///{self.chdb_path or ':memory:'}"
        from urllib.parse import quote, urlsplit

        # Accept host as bare hostname or full URL like
        # "https://foo.clickhouse.cloud:8443". URL form selects the HTTP driver
        # and contributes secure/port; explicit constructor args still win.
        raw_host = self.host or "localhost"
        url_port: int | None = None
        url_secure = False
        use_http = False
        if "://" in raw_host:
            parsed = urlsplit(raw_host)
            host = parsed.hostname or "localhost"
            url_port = parsed.port
            url_scheme = parsed.scheme.lower()
            if url_scheme in ("http", "https"):
                use_http = True
                url_secure = url_scheme == "https"
        else:
            host = raw_host

        secure = self.secure or url_secure
        scheme = "clickhouse+http" if use_http else "clickhouse+native"
        if self.port is not None:
            port = self.port
        elif url_port is not None:
            port = url_port
        elif use_http:
            port = 8443 if secure else 8123
        else:
            port = 9440 if secure else 9000

        user = self.username or "default"
        auth = f"{user}:{quote(self.password, safe='')}" if self.password else user
        suffix = f"/{self.database or 'default'}"
        if use_http and secure:
            query = "?protocol=https"
        elif not use_http and secure:
            query = "?secure=true"
        else:
            query = ""
        return f"{scheme}://{auth}@{host}:{port}{suffix}{query}"
