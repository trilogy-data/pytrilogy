from unittest.mock import Mock

import pytest

from trilogy import Dialects, parse
from trilogy.core.enums import CreateMode, FunctionType, Ordering
from trilogy.core.models.core import DataType
from trilogy.core.statements.execute import ColumnInfo, CreateTableInfo
from trilogy.dialect.config import MySQLConfig
from trilogy.dialect.mysql import (
    MySQLDialect,
    date_diff,
    date_truncate,
    render_ordering,
)


def test_mysql_config_connection_string():
    config = MySQLConfig(
        host="db.example.com",
        port=3307,
        username="user@example.com",
        password="p@ss/word",
        database="analytics",
    )

    assert config.connection_string() == (
        "mysql+pymysql://user%40example.com:p%40ss%2Fword@"
        "db.example.com:3307/analytics?charset=utf8mb4"
    )


def test_mysql_default_engine(monkeypatch):
    config = MySQLConfig("localhost", "user", "password", "database")
    factory = Mock(return_value="engine")
    monkeypatch.setattr("importlib.util.find_spec", lambda name: object())

    engine = Dialects.MYSQL.default_engine(config, _engine_factory=factory)

    assert engine == "engine"
    factory.assert_called_once_with(config, MySQLConfig)


def test_mysql_default_engine_requires_config():
    with pytest.raises(ValueError, match="MySQLConfig"):
        Dialects.MYSQL.default_engine()


def test_mysql_renderer_is_registered():
    assert isinstance(Dialects.MYSQL.default_renderer(), MySQLDialect)


def test_mysql_query_uses_limit_and_backtick_quoting():
    env, statements = parse("""
        key id int;
        datasource items (id: id) grain (id) address items;
        select id limit 5;
        """)
    dialect = MySQLDialect()

    sql = dialect.compile_statement(dialect.generate_queries(env, [statements[-1]])[0])

    assert "LIMIT 5" in sql
    assert "TOP 5" not in sql
    assert "`items`.`id`" in sql


def test_mysql_ordering_avoids_nulls_syntax():
    assert render_ordering("`x`", Ordering.ASCENDING) == "`x` asc"
    assert render_ordering("`x`", Ordering.ASC_NULLS_FIRST) == "`x` asc"
    assert render_ordering("`x`", Ordering.DESC_NULLS_LAST) == "`x` desc"
    assert render_ordering("`x`", Ordering.DESC_NULLS_AUTO) == "`x` desc"
    assert render_ordering("`x`", Ordering.ASC_NULLS_LAST) == (
        "(`x`) IS NULL asc, `x` asc"
    )
    assert render_ordering("`x`", Ordering.DESC_NULLS_FIRST) == (
        "(`x`) IS NULL desc, `x` desc"
    )


def test_mysql_query_order_by_has_no_nulls_clause():
    env, statements = parse("""
        key id int;
        property id.score int;
        datasource items (id: id, score: score) grain (id) address items;
        select id, score order by score desc nulls first;
        """)
    dialect = MySQLDialect()

    sql = dialect.compile_statement(dialect.generate_queries(env, [statements[-1]])[0])

    assert "nulls" not in sql.lower()
    assert "IS NULL desc" in sql


def test_mysql_window_order_by_has_no_nulls_clause():
    env, statements = parse("""
        key id int;
        property id.score int;
        datasource items (id: id, score: score) grain (id) address items;
        auto ranked <- rank id by score asc nulls last;
        select id, ranked;
        """)
    dialect = MySQLDialect()

    sql = dialect.compile_statement(dialect.generate_queries(env, [statements[-1]])[0])

    assert "nulls" not in sql.lower()
    assert "IS NULL asc" in sql


def test_mysql_date_functions():
    assert date_diff("started_at", "ended_at", "day") == (
        "TIMESTAMPDIFF(DAY, started_at, ended_at)"
    )
    assert date_truncate("created_at", "month") == (
        "TIMESTAMP(DATE_FORMAT(created_at, '%Y-%m-01 00:00:00'))"
    )
    assert date_truncate("created_at", "week") == (
        "TIMESTAMP(DATE_SUB(DATE(created_at), INTERVAL WEEKDAY(created_at) DAY))"
    )
    assert (
        MySQLDialect.FUNCTION_MAP[FunctionType.DATE_ADD](["created_at", "day", "3"], [])
        == "DATE_ADD(created_at, INTERVAL 3 DAY)"
    )


def test_mysql_function_overrides():
    functions = MySQLDialect.FUNCTION_MAP

    assert functions[FunctionType.RANDOM]([], []) == "RAND()"
    assert functions[FunctionType.STRPOS](["body", "'needle'"], []) == (
        "LOCATE('needle', body)"
    )
    assert functions[FunctionType.IS_NOT_DISTINCT](["left", "right"], []) == (
        "(left <=> right)"
    )
    assert functions[FunctionType.CAST](["item_id", "TEXT"], []) == (
        "CAST(item_id AS CHAR)"
    )


def test_mysql_string_literal_escaping():
    dialect = MySQLDialect()

    assert dialect.render_string_literal("it's") == "'it\\'s'"
    assert dialect.render_string_literal("a\\.b") == "'a\\\\.b'"


def test_mysql_create_or_replace_table():
    dialect = MySQLDialect()
    target = CreateTableInfo(
        name="target",
        columns=[
            ColumnInfo(name="id", type=DataType.INTEGER),
            ColumnInfo(name="name", type=DataType.STRING),
        ],
    )

    sql = dialect.compile_create_table_statement(target, CreateMode.CREATE_OR_REPLACE)

    assert sql.startswith("DROP TABLE IF EXISTS `target`;")
    assert "CREATE TABLE `target`" in sql
    assert "CREATE OR REPLACE TABLE" not in sql
    assert "name TEXT" in sql


def test_mysql_schema_introspection_uses_current_database():
    result = Mock()
    result.fetchall.return_value = [
        ("id", "bigint", "NO", "primary identifier"),
        ("name", "varchar", "YES", ""),
    ]
    executor = Mock()
    executor.execute_raw_sql.return_value = result

    columns = MySQLDialect().get_table_schema(executor, "items")

    assert [column.trilogy_type for column in columns] == [
        DataType.BIGINT,
        DataType.STRING,
    ]
    query = executor.execute_raw_sql.call_args.args[0]
    assert "table_schema = DATABASE()" in query
    assert "column_comment" in query
