from pathlib import Path

import pytest

from trilogy import Dialects, Environment
from trilogy.core.enums import CreateMode
from trilogy.core.models.core import DataType
from trilogy.core.statements.execute import ColumnInfo, CreateTableInfo
from trilogy.dialect.config import SQLiteConfig
from trilogy.dialect.sqlite import SQLiteDialect, date_part, date_truncate


def test_sqlite_default_engine_in_memory():
    executor = Dialects.SQLITE.default_executor()
    results = executor.execute_text("select 1 as value;")[0].fetchall()
    assert results[0].value == 1


def test_sqlite_default_engine_file_support(tmp_path: Path):
    db_path = tmp_path / "sqlite_engine_test.db"
    conf = SQLiteConfig(path=str(db_path))

    writer = Dialects.SQLITE.default_executor(conf=conf)
    writer.execute_raw_sql("CREATE TABLE items(id INTEGER)")
    writer.execute_raw_sql("INSERT INTO items VALUES (1), (2)")
    writer.connection.commit()
    writer.close()

    reader = Dialects.SQLITE.default_executor(conf=conf)
    results = reader.execute_raw_sql(
        "SELECT COUNT(*) as item_count FROM items"
    ).fetchall()
    assert results[0].item_count == 2
    reader.close()


def test_date_diff_rendering():
    environment = Environment()
    _, queries = environment.parse(
        """
    const today <- current_date();

    select today
    where date_add(current_date(), day, -30) < today;
    """
    )
    executor = Dialects.SQLITE.default_executor(environment=environment)
    sql = executor.generate_sql(queries[-1])[0]

    assert "datetime(" in sql
    assert "day" in sql


def test_string_functions():
    environment = Environment()
    _, queries = environment.parse(
        """
    const greeting <- '  Hello, World!  ';
    select
        greeting,
        lower(greeting) -> greeting_lower,
        upper(greeting) -> greeting_upper,
        len(greeting) -> greeting_length,
        trim(greeting) -> greeting_trimmed,
        ltrim(greeting) -> greeting_ltrimmed,
        rtrim(greeting) -> greeting_rtrimmed,
        substring(greeting, 3, 5) -> greeting_substring,
        replace(greeting, 'World', 'Trilogy') -> greeting_replaced,
        concat(greeting, ' Welcome to Trilogy.') -> greeting_concatenated,
        greeting like '%world%' -> contains_world,
        greeting ilike '%WORLD%' -> contains_world_case_insensitive,
        contains(greeting, 'world') -> contains_function
    ;
        """
    )

    executor = Dialects.SQLITE.default_executor(environment=environment)
    row = executor.execute_query(queries[-1]).fetchall()[0]

    assert row.greeting == "  Hello, World!  "
    assert row.greeting_lower == "  hello, world!  "
    assert row.greeting_upper == "  HELLO, WORLD!  "
    assert row.greeting_length == 17
    assert row.greeting_trimmed == "Hello, World!"
    assert row.greeting_ltrimmed == "Hello, World!  "
    assert row.greeting_rtrimmed == "  Hello, World!"
    assert row.greeting_substring == "Hello"
    assert row.greeting_replaced == "  Hello, Trilogy!  "
    assert row.greeting_concatenated == "  Hello, World!   Welcome to Trilogy."
    assert row.contains_world == 1
    assert row.contains_world_case_insensitive == 1
    assert row.contains_function == 1


def test_math_functions():
    environment = Environment()
    _, queries = environment.parse(
        """
    const revenue <- 100.50;
    const order_id <- 1;

    auto inflated_order_value <- multiply(revenue, 2);
    auto fixed_order_value <- inflated_order_value / 2;
    auto order_add <- revenue + 2;
    auto order_sub <- revenue - 2;
    auto order_nested <- revenue * 2/2;
    auto rounded <- round(revenue + 2.01,2);

    select
        order_id,
        inflated_order_value,
        order_nested,
        fixed_order_value,
        order_sub,
        order_add,
        rounded
    ;
        """
    )

    executor = Dialects.SQLITE.default_executor(environment=environment)
    row = executor.execute_query(queries[-1]).fetchall()[0]

    assert row.order_id == 1
    assert row.inflated_order_value == 201.0
    assert row.fixed_order_value == 100.5
    assert row.order_sub == 98.5
    assert row.order_add == 102.5
    assert row.order_nested == 100.5
    assert row.rounded == 102.51


def test_date_functions():
    environment = Environment()
    _, queries = environment.parse(
        """
    const order_id <- 1;
    const order_timestamp <- current_datetime();
    select
        order_id,
        order_timestamp,
        date(order_timestamp) -> order_date,
        datetime(order_timestamp) -> order_timestamp_datetime,
        timestamp(order_timestamp) -> order_timestamp_cast,
        day(order_timestamp) -> order_day,
        week(order_timestamp) -> order_week,
        month(order_timestamp) -> order_month,
        quarter(order_timestamp) -> order_quarter,
        year(order_timestamp) -> order_year,
        date_trunc(order_timestamp, month) -> order_month_trunc,
        date_add(order_timestamp, month, 1) -> one_month_post_order,
        date_sub(order_timestamp, month, 1) -> one_month_pre_order,
        date_sub(order_timestamp, day, 30) -> thirty_days_ago,
        date_part(order_timestamp, day_of_week) -> order_day_of_week_part,
        month_name(order_timestamp) -> order_month_name,
        day_name(order_timestamp) -> order_day_name,
        format_time(order_timestamp, '%Y-%m-%d %H:%M:%S') -> order_timestamp_strftime,
        parse_time(format_time(order_timestamp, '%Y-%m-%d %H:%M:%S'), '%Y-%m-%d %H:%M:%S') -> order_timestamp_parse,
        date_diff(thirty_days_ago, order_timestamp, day) -> date_diff_days
    ;
        """
    )

    executor = Dialects.SQLITE.default_executor(environment=environment)
    row = executor.execute_query(queries[-1]).fetchall()[0]

    assert row.order_id == 1
    assert row.one_month_post_order > row.order_timestamp
    assert row.one_month_pre_order < row.order_timestamp
    assert row.thirty_days_ago < row.order_timestamp
    assert row.date_diff_days == 30
    assert 1 <= row.order_day_of_week_part <= 7
    assert isinstance(row.order_month_name, str) and len(row.order_month_name) > 0
    assert isinstance(row.order_day_name, str) and len(row.order_day_name) > 0
    assert row.order_timestamp_parse is not None


def test_date_part_quarter():
    environment = Environment()
    _, queries = environment.parse(
        """
    const ts <- current_datetime();
    select date_part(ts, quarter) -> q;
    """
    )
    executor = Dialects.SQLITE.default_executor(environment=environment)
    row = executor.execute_query(queries[-1]).fetchall()[0]
    assert 1 <= row.q <= 4


def test_date_part_unsupported():
    with pytest.raises(ValueError, match="Unsupported date part for sqlite"):
        date_part("ts", "invalid_part")


def test_date_truncate_parts():
    environment = Environment()
    _, queries = environment.parse(
        """
    const ts <- current_datetime();
    select
        date_trunc(ts, day) -> trunc_day,
        date_trunc(ts, year) -> trunc_year,
        date_trunc(ts, hour) -> trunc_hour,
        date_trunc(ts, minute) -> trunc_minute,
        date_trunc(ts, second) -> trunc_second,
        date_trunc(ts, week) -> trunc_week,
        date_trunc(ts, quarter) -> trunc_quarter
    ;
    """
    )
    executor = Dialects.SQLITE.default_executor(environment=environment)
    row = executor.execute_query(queries[-1]).fetchall()[0]
    assert row.trunc_day is not None
    assert row.trunc_year is not None
    assert row.trunc_hour is not None
    assert row.trunc_minute is not None
    assert row.trunc_second is not None
    assert row.trunc_week is not None
    assert row.trunc_quarter is not None


def test_date_truncate_unsupported():
    with pytest.raises(ValueError, match="Unsupported date truncation for sqlite"):
        date_truncate("ts", "invalid_part")


def test_compile_create_table_create_or_replace():
    dialect = SQLiteDialect()
    table_info = CreateTableInfo(
        name="test_table",
        columns=[
            ColumnInfo(name="id", type=DataType.INTEGER),
            ColumnInfo(name="name", type=DataType.STRING),
        ],
    )
    sql = dialect.compile_create_table_statement(
        table_info, CreateMode.CREATE_OR_REPLACE
    )
    assert sql.startswith("DROP TABLE IF EXISTS")
    assert "CREATE TABLE" in sql


def test_compile_create_table_normal():
    dialect = SQLiteDialect()
    table_info = CreateTableInfo(
        name="test_table",
        columns=[ColumnInfo(name="id", type=DataType.INTEGER)],
    )
    sql = dialect.compile_create_table_statement(table_info, CreateMode.CREATE)
    assert sql.startswith("CREATE TABLE")
    assert "DROP TABLE" not in sql


def test_get_table_schema():
    executor = Dialects.SQLITE.default_executor()
    executor.execute_raw_sql(
        "CREATE TABLE schema_test(id INTEGER NOT NULL, name TEXT, value REAL)"
    )
    schema = executor.generator.get_table_schema(executor, "schema_test")
    assert len(schema) == 3
    names = [row[0] for row in schema]
    assert "id" in names
    assert "name" in names
    assert "value" in names
    id_row = next(r for r in schema if r[0] == "id")
    assert id_row[2] == "NO"
    name_row = next(r for r in schema if r[0] == "name")
    assert name_row[2] == "YES"


def test_get_table_primary_keys():
    executor = Dialects.SQLITE.default_executor()
    executor.execute_raw_sql("CREATE TABLE pk_test(id INTEGER PRIMARY KEY, name TEXT)")
    pks = executor.generator.get_table_primary_keys(executor, "pk_test")
    assert pks == ["id"]


def test_get_table_primary_keys_none():
    executor = Dialects.SQLITE.default_executor()
    executor.execute_raw_sql("CREATE TABLE nopk_test(id INTEGER, name TEXT)")
    pks = executor.generator.get_table_primary_keys(executor, "nopk_test")
    assert pks == []


def test_aggregate_functions():
    executor = Dialects.SQLITE.default_executor()
    results = executor.execute_text(
        """
key order_id int;
property order_id.revenue float;
property order_id.quantity int;
property order_id.is_premium bool;
key category string;

datasource orders (
    order_id,
    revenue,
    quantity,
    is_premium,
    category
)
grain (order_id)
query '''
select 1 as order_id, 100.50 as revenue, 2 as quantity, 1 as is_premium, 'A' as category
union all
select 2, 250.75, 1, 0, 'B'
union all
select 3, 75.25, 3, 1, 'A'
union all
select 4, 300.00, 1, 0, 'C'
union all
select 5, 150.80, 4, 1, 'B'
''';

auto total_revenue <- sum(revenue);
auto max_revenue <- max(revenue);
auto min_revenue <- min(revenue);
auto avg_revenue <- avg(revenue);
auto order_count <- count(order_id);
auto total_quantity <- sum(quantity);
auto any_premium <- bool_or(is_premium);
auto all_premium <- bool_and(is_premium);

select
    total_revenue,
    max_revenue,
    min_revenue,
    avg_revenue,
    order_count,
    total_quantity,
    any_premium,
    all_premium
;
"""
    )

    row = list(results[-1].fetchall())[0]
    assert row.total_revenue == 877.30
    assert row.max_revenue == 300.00
    assert row.min_revenue == 75.25
    assert abs(row.avg_revenue - 175.46) < 0.01
    assert row.order_count == 5
    assert row.total_quantity == 11
    assert row.any_premium == 1
    assert row.all_premium == 0
