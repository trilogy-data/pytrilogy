import pytest

from trilogy import Dialects, Environment, parse
from trilogy.core.enums import Purpose
from trilogy.execution.state.isolation import hidden_datasources

RAW = (
    "CREATE TABLE raw AS SELECT i AS id, CASE WHEN i % 3 = 0 THEN 'a'"
    " WHEN i % 3 = 1 THEN 'b' ELSE 'c' END AS species FROM range(30) t(i)"
)

MODEL = """
key id int;
property id.species string;
key cls <- case when species = 'a' then 'x' else 'y' end;
root datasource raw (id: id, species: species) grain (id) address raw;
auto n_by_cls <- count(id) by cls;
datasource fits (fit_class: cls, n: n_by_cls) grain (cls) address fits;
"""


def test_derived_key_is_its_own_grain():
    env, _ = parse(MODEL)
    cls = env.concepts["cls"]
    assert cls.purpose == Purpose.KEY
    assert cls.grain.components == {"local.cls"}
    assert cls.keys is None
    built = env.materialize_for_select().concepts["local.cls"]
    assert built.grain.components == {"local.cls"}
    assert not built.keys


@pytest.mark.parametrize(
    "query", ["select count(cls) as v;", "select count_distinct(cls) as v;"]
)
@pytest.mark.parametrize("hide_raw", [False, True])
def test_count_derived_key_counts_values(query: str, hide_raw: bool):
    ex = Dialects.DUCK_DB.default_executor(environment=Environment())
    ex.execute_raw_sql(RAW)
    ex.parse_text(MODEL)
    ex.update_datasource(ex.environment.datasources["fits"])
    hidden = ["raw"] if hide_raw else []
    with hidden_datasources(ex.environment, hidden):
        assert ex.execute_text(query)[-1].fetchall() == [(2,)]


def test_derived_key_count_beside_parent_count():
    ex = Dialects.DUCK_DB.default_executor(environment=Environment())
    ex.execute_raw_sql(RAW)
    ex.parse_text(MODEL)
    ex.update_datasource(ex.environment.datasources["fits"])
    query = "select cls, count(id) as ids order by cls asc;"
    expected = [("x", 10), ("y", 20)]
    with hidden_datasources(ex.environment, ["fits"]):
        assert ex.execute_text(query)[-1].fetchall() == expected
    assert ex.execute_text(query)[-1].fetchall() == expected


@pytest.mark.parametrize(
    "query", ["select count(cls) as v;", "select count_distinct(cls) as v;"]
)
def test_count_derived_key_reads_key_table(query: str):
    ex = Dialects.DUCK_DB.default_executor(environment=Environment())
    ex.execute_raw_sql(RAW)
    ex.parse_text(MODEL)
    ex.update_datasource(ex.environment.datasources["fits"])
    sql = ex.generate_sql(query)[-1]
    assert '"fits"' in sql and '"raw"' not in sql
    assert ex.execute_text(query)[-1].fetchall() == [(2,)]


RENAMED_MODEL = """
key id int;
property id.species string;
key sp <- species;
key id2 <- id;
root datasource raw (id: id, species: species) grain (id) address raw;
auto n_by_sp <- count(id) by sp;
datasource fits (sp: sp, n: n_by_sp) grain (sp) address fits;
"""


def test_renamed_value_declared_key_is_its_own_grain():
    env, _ = parse(RENAMED_MODEL)
    sp = env.concepts["sp"]
    assert sp.purpose == Purpose.KEY
    assert sp.grain.components == {"local.sp"}
    assert sp.keys is None
    id2 = env.concepts["id2"]
    assert id2.purpose == Purpose.KEY
    assert id2.grain.components == {"local.id"}


SP_QUERIES = ["select count(sp) as v;", "select count_distinct(sp) as v;"]


@pytest.mark.parametrize("query", SP_QUERIES)
@pytest.mark.parametrize("hidden", [[], ["fits"], ["raw"]])
def test_renamed_key_counts_values(query: str, hidden: list[str]):
    ex = Dialects.DUCK_DB.default_executor(environment=Environment())
    ex.execute_raw_sql(RAW)
    ex.parse_text(RENAMED_MODEL)
    ex.update_datasource(ex.environment.datasources["fits"])
    if not hidden:
        assert '"raw"' not in ex.generate_sql(query)[-1]
    with hidden_datasources(ex.environment, hidden):
        assert ex.execute_text(query)[-1].fetchall() == [(3,)]


def test_renamed_key_beside_parent_count():
    ex = Dialects.DUCK_DB.default_executor(environment=Environment())
    ex.execute_raw_sql(RAW)
    ex.parse_text(RENAMED_MODEL)
    ex.update_datasource(ex.environment.datasources["fits"])
    assert ex.execute_text("select count(id2) as v;")[-1].fetchall() == [(30,)]
    query = "select sp, count(id) as n order by sp asc;"
    expected = [("a", 10), ("b", 10), ("c", 10)]
    with hidden_datasources(ex.environment, ["fits"]):
        assert ex.execute_text(query)[-1].fetchall() == expected
    assert ex.execute_text(query)[-1].fetchall() == expected
