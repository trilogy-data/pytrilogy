import pytest

from trilogy import Dialects, Environment, Executor
from trilogy.core.exceptions import UnresolvableQueryException
from trilogy.execution.state.isolation import hidden_datasources

PARTITION = """
key id int;
property id.city string;
property id.species string;
property id.height float;

partial datasource trees (id: id, city: city, species: species, height: height)
grain (id)
complete where city = 'X'
query '''select 1 as id, 'X' as city, 'oak' as species, 2.0 as height
union all select 2, 'X', 'elm', 3.0''';
"""

FK = """
key cust_id int;
key order_id int;
property order_id.amount int;

datasource custs (cid: cust_id) grain (cust_id)
query '''select range as cid from range(3)''';
datasource orders (oid: order_id, cid: ~cust_id, amt: amount) grain (order_id)
query '''select range as oid, 0 as cid, 1 as amt from range(2)''';
"""

MERGED = """
key id int;
key city string;
property id.source string;
property id.raw_height float;
property id.height float;
property city.scale float;

root datasource cells (city: city, scale: scale) grain (city)
query '''select 'X' as city, 2.0 as scale''';
root datasource raw (id: id, city: city, source: source, raw_height: raw_height)
grain (id)
query '''select 1 as id, 'X' as city, 'A' as source, 1.0 as raw_height''';
auto merged_height <- max(raw_height * scale) by id;
merge merged_height into height;
partial datasource built (id: id, city: city, source: source, height: height)
grain (id)
complete where city = 'X'
query '''select 1 as id, 'X' as city, 'A' as source, 2.0 as height''';
"""


def _executor(model: str) -> Executor:
    executor = Dialects.DUCK_DB.default_executor(environment=Environment())
    executor.parse_text(model)
    return executor


@pytest.mark.parametrize(
    "query",
    [
        "select id;",
        "select count(id) as n;",
        "select max(height) as m;",
        "select upper(species) as u;",
        "select count(id ? species = 'oak') as n;",
        "select count(id) by species as n;",
        "rowset s <- select id, species; select count(s.id) as n;",
    ],
)
def test_partial_only_source_needs_its_scope(query: str) -> None:
    with pytest.raises(UnresolvableQueryException, match="no complete sources"):
        _executor(PARTITION).generate_sql(query)


@pytest.mark.parametrize(
    "query, expected",
    [
        ("select count(id) as n where city = 'X';", [(2,)]),
        ("select max(height) as m where city = 'X';", [(3.0,)]),
        (
            "rowset s <- select id where city = 'X'; select count(s.id) as n;",
            [(2,)],
        ),
    ],
)
def test_scoped_partial_source_resolves(query: str, expected: list) -> None:
    assert _executor(PARTITION).execute_text(query)[-1].fetchall() == expected


def test_where_outside_the_scope_names_it() -> None:
    with pytest.raises(UnresolvableQueryException, match="falls outside every source"):
        _executor(PARTITION).generate_sql("select count(id) as n where city = 'Y';")


def test_partial_foreign_key_still_aggregates() -> None:
    rows = _executor(FK).execute_text(
        "select cust_id, sum(amount) as total order by cust_id asc;"
    )[-1]
    assert rows.fetchall() == [(0, 2), (1, None), (2, None)]


@pytest.mark.parametrize(
    "query", ["select id, height;", "select count(id ? source = 'A') as n;"]
)
def test_merge_with_hidden_inputs_is_not_a_source(query: str) -> None:
    executor = _executor(MERGED)
    with hidden_datasources(executor.environment, ["raw", "cells"]):
        with pytest.raises(UnresolvableQueryException, match="no complete sources"):
            executor.generate_sql(query)
        scoped = query.replace(";", " where city = 'X';")
        assert executor.execute_text(scoped)[-1].fetchall()
