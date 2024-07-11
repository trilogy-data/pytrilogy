class DialectConfig:

    def __init__(self):
        pass

    def connection_string(self) -> str:
        raise NotImplementedError

    @property
    def connect_args(self) -> dict:
        return {}


class BigQueryConfig(DialectConfig):
    def __init__(self, project: str, client):
        self.project = project
        self.client = client

    def connection_string(self) -> str:
        return f"bigquery://{self.project}?user_supplied_client=True"

    @property
    def connect_args(self) -> dict:
        return {"client": self.client}


class DuckDBConfig(DialectConfig):
    def __init__(self, path: str | None = None):
        self.path = path

    def connection_string(self) -> str:
        if not self.path:
            return "duckdb:///:memory:"
        return f"duckdb:///{self.path}"


class PostgresConfig(DialectConfig):
    def __init__(
        self, host: str, port: int, username: str, password: str, database: str
    ):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.database = database

    def connection_string(self) -> str:
        return f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}"


class SQLServerConfig(DialectConfig):
    def __init__(
        self, host: str, port: int, username: str, password: str, database: str
    ):
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
    ):
        self.account = account
        self.username = username
        self.password = password

    def connection_string(self) -> str:
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
    ):
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
