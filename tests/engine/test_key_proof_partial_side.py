"""A WHERE proving a join key non-null forces a side only where that side
holds every key value. An unnest drops the parent rows whose array is empty,
so a species with no ecoregion has no row there: `species != 'oak'` keeps its
trees, and a merge joining the unnested rows on `species` must keep them."""

import pytest

from tests.discovery.test_relation_rollup_laws import BASE, EQUAL_MERGES
from trilogy import Dialects, Environment
from trilogy.executor import Executor

TREES = "(1, 'A', 'oak', 3), (2, 'A', 'pine', 4), (3, 'B', 'ash', 5)"
SPECIES = "('oak', [1, 2]), ('pine', [2]), ('ash', []::int[])"
TABLES = [
    f"create table trees as select * from (values {TREES}) t(tree_id, city, species, height)",
    f"create table enrichment as select * from (values {SPECIES}) t(species, native_ecoregions)",
    "create table eco as select * from (values (1, 'wet'), (2, 'dry')) t(ecoregion_id, biome)",
]


@pytest.fixture(params=sorted(EQUAL_MERGES))
def executor(request) -> Executor:
    executor = Dialects.DUCK_DB.default_executor(environment=Environment())
    for table in TABLES:
        executor.execute_raw_sql(table)
    executor.parse_text(BASE + EQUAL_MERGES[request.param])
    return executor


def _rows(executor: Executor, query: str) -> list[tuple]:
    return [tuple(r) for r in executor.execute_text(query)[-1].fetchall()]


def test_a_key_where_keeps_the_rows_an_unnest_lacks(executor: Executor) -> None:
    query = (
        "select city, ecoregion_id, count(tree_id) as n{} order by city, ecoregion_id;"
    )
    assert _rows(executor, query.format("")) == [
        ("A", 1, 1),
        ("A", 2, 2),
        ("B", None, 1),
    ]
    assert _rows(executor, query.format(" where species != 'oak'")) == [
        ("A", 2, 1),
        ("B", None, 1),
    ]
