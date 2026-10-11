from sqlalchemy.engine import make_url

from trilogy.dialect.config import PostgresConfig, SQLServerConfig


def test_postgres_connection_string_includes_database_and_escapes_password():
    url = make_url(PostgresConfig("h", 5432, "u", "p@ss:/", "db").connection_string())
    assert url.drivername == "postgresql"
    assert (url.host, url.port, url.username) == ("h", 5432, "u")
    assert url.password == "p@ss:/"
    assert url.database == "db"


def test_sql_server_connection_string_is_a_valid_pyodbc_url():
    url = make_url(SQLServerConfig("h", 1433, "u", "p@ss", "db").connection_string())
    assert url.drivername == "mssql+pyodbc"
    assert (url.host, url.port, url.database) == ("h", 1433, "db")
    assert url.password == "p@ss"
    assert url.query["driver"] == "ODBC Driver 18 for SQL Server"
