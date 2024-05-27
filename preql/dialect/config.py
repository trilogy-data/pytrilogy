class DialectConfig:
    pass


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
