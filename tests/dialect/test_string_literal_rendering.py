"""String literals must render dialect-correctly: BigQuery and Snowflake treat
backslash as an escape character, so a trilogy literal `\\.` (common in regexes)
must be emitted as `\\\\.` there — verbatim emission is an illegal escape
sequence. Standard-SQL dialects keep backslashes literal and only need the
quote doubled."""

from trilogy import parse
from trilogy.dialect.base import BaseDialect
from trilogy.dialect.bigquery import BigqueryDialect
from trilogy.dialect.duckdb import DuckDBDialect
from trilogy.dialect.snowflake import SnowflakeDialect


def test_base_dialect_doubles_quotes():
    assert BaseDialect().render_string_literal("it's") == "'it''s'"
    assert BaseDialect().render_string_literal("a\\.b") == "'a\\.b'"


def test_bigquery_escapes_backslashes_and_quotes():
    assert BigqueryDialect().render_string_literal("a\\.b") == "'a\\\\.b'"
    assert BigqueryDialect().render_string_literal("it's") == "'it\\'s'"


def test_snowflake_escapes_backslashes():
    assert SnowflakeDialect().render_string_literal("a\\.b") == "'a\\\\.b'"
    assert SnowflakeDialect().render_string_literal("it's") == "'it''s'"


def test_regex_literal_compiles_per_dialect():
    src = r"""key url string;
datasource urls (url: url) grain (url) address urls;
auto d <- regexp_extract(url, 'a\.b', 0);
select d;
"""
    env, statements = parse(src)
    sel = statements[-1]

    bq = BigqueryDialect()
    bq_sql = bq.compile_statement(bq.generate_queries(env.duplicate(), [sel])[0])
    assert "'a\\\\.b'" in bq_sql

    duck = DuckDBDialect()
    duck_sql = duck.compile_statement(duck.generate_queries(env.duplicate(), [sel])[0])
    assert "'a\\.b'" in duck_sql
