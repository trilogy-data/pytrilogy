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
