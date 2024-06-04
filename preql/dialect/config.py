class DialectConfig:
    pass


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
