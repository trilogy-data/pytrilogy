import pytest

from tests.engine.test_dialects import _MockEngine
from trilogy import Dialects, Executor
from trilogy.constants import Rendering
from trilogy.core.models.environment import Environment
from trilogy.dialect.config import DatabricksConfig
from trilogy.dialect.databricks import DatabricksDialect, java_format

SCHEMA = """
key id int;
property id.name string;
property id.ts datetime;
property id.d date;
property id.tags list<string>;
datasource t (id: id, name: name, ts: ts, d: d, tags: tags)
grain (id)
address main.sales.t;
"""


def _sql(query: str) -> str:
    executor = Executor(
        dialect=Dialects.DATABRICKS,
        engine=_MockEngine(),
        environment=Environment(),
        rendering=Rendering(parameters=False),
    )
    executor.parse_text(SCHEMA)
    return executor.generate_sql(query)[-1]


def test_string_literal_escapes_backslash_and_quote():
    # `'it''s'` is two adjacent literals that concatenate to `its`
    assert DatabricksDialect().render_string_literal(r"it's \d") == r"'it\'s \\d'"


def test_three_part_name_is_backtick_quoted():
    assert "FROM\n    `main`.`sales`.`t` as `t`" in _sql("select id;")


def test_calendar_truncation_is_a_date():
    assert "cast(date_trunc('month', `t`.`ts`) as DATE)" in _sql(
        "select date_trunc(ts, month) as m;"
    )


def test_date_offset_of_a_date_stays_a_date():
    assert "cast(timestampadd(day, (3), `t`.`d`) as DATE)" in _sql(
        "select date_add(d, day, 3) as d3;"
    )


def test_date_diff_counts_boundaries():
    sql = _sql("select date_diff(d, d, month) as n;")
    assert "timestampdiff(month, date_trunc('month', `t`.`d`)," in sql


def test_unnest_is_an_explode_in_the_select_list():
    assert "explode(`t`.`tags`) as `tag`" in _sql("select id, unnest(tags) as tag;")


def test_index_access_is_one_based_and_null_safe():
    assert "try_element_at(`t`.`tags`, 1)" in _sql("select tags[1] as first;")


@pytest.mark.parametrize(
    "strftime, java",
    [
        ("'%Y-%m-%d'", "'yyyy-MM-dd'"),
        ("'%H:%M:%S'", "'HH:mm:ss'"),
        ("'%Y at %H'", r"'yyyy\' at \'HH'"),
        ("'100%%'", "'100%'"),
    ],
)
def test_strftime_formats_translate_to_java(strftime: str, java: str):
    assert java_format(strftime) == java


def test_config_url_and_env_fallback(monkeypatch):
    monkeypatch.setenv("DATABRICKS_TOKEN", "dapi-x")
    conf = DatabricksConfig(
        host="https://adb-1.azuredatabricks.net/",
        http_path="/sql/1.0/warehouses/abc",
        catalog="main",
    )
    assert conf.connection_string() == (
        "databricks://token:dapi-x@adb-1.azuredatabricks.net"
        "?catalog=main&http_path=%2Fsql%2F1.0%2Fwarehouses%2Fabc"
    )


def test_config_names_what_is_missing(monkeypatch):
    for name in ("DATABRICKS_HOST", "DATABRICKS_HTTP_PATH", "DATABRICKS_TOKEN"):
        monkeypatch.delenv(name, raising=False)
    with pytest.raises(ValueError, match="host, http_path, access_token"):
        DatabricksConfig()


def test_complex_constants_render_inline():
    executor = Executor(
        dialect=Dialects.DATABRICKS,
        engine=_MockEngine(),
        environment=Environment(),
    )
    sql = executor.generate_sql(
        "const arr <- [1, 2]; const m <- {'a': 1}; select len(arr) as n, m['a'] as a;"
    )[-1]
    assert "size(array(1, 2))" in sql
    assert "try_element_at(map('a', 1), 'a')" in sql


def test_staging_table_is_dropped_as_temporary():
    assert (
        DatabricksDialect().render_staging_drop("`stage`")
        == "DROP TEMPORARY TABLE `stage`"
    )
