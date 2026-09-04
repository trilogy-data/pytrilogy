"""A partition family answers a whole-population request only when its
`complete where` claims jointly exhaust every discriminator they name.

A claim on `(city, source)` covers city A only together with the arms for
the other `source` values; on its own it is one cell of the cross product.
Electing such an arm as the cover for `city = 'A'` silently answered the
query from a strict subset of the rows, and which subset depended on which
files happened to exist.
"""

import pytest

from trilogy import Dialects, Environment
from trilogy.core.exceptions import UnresolvableQueryException

CONCEPTS = """
key city enum<string>['A', 'B'];
key source enum<string>['MUN', 'OSM'];
key id string;
property id.x float;
"""


def _rows(city: str, source: str) -> str:
    return (
        f"select '{city}-{source}-1' as id, '{city}' as city, '{source}' as source, 1.0 as x"
        f" union all select '{city}-{source}-2', '{city}', '{source}', 2.0"
    )


def _raw(city: str, source: str, complete_where: str) -> str:
    return f"""
root partial datasource raw_{city}_{source} (id: id, city: city, source: source, x: x)
grain (id) complete where {complete_where}
query '''{_rows(city, source)}''';
"""


def _pub(city: str) -> str:
    return f"""
partial datasource pub_{city} (id: id, city: city, source: source, x: x)
grain (id) complete where city = '{city}'
query '''{_rows(city, "MUN")} union all {_rows(city, "OSM")}''';
"""


def _run(model: str, query: str) -> tuple[list[tuple], str]:
    env = Environment()
    env.parse(model)
    executor = Dialects.DUCK_DB.default_executor(environment=env)
    sql = executor.generate_sql(query)[-1]
    return executor.execute_text(query)[-1].fetchall(), sql


CELLS = [(c, s) for c in ("A", "B") for s in ("MUN", "OSM")]
FULL_GRID = CONCEPTS + "".join(
    _raw(c, s, f"city = '{c}' and source = '{s}'") for c, s in CELLS
)


def test_full_cross_product_of_cells_is_a_cover():
    rows, sql = _run(FULL_GRID, "select id order by id asc;")
    assert len(rows) == 8
    assert all(f"raw_{c}_{s}" in sql for c, s in CELLS)


def test_a_missing_cell_is_not_covered_by_the_other_arm_for_its_city():
    model = CONCEPTS + "".join(
        _raw(c, s, f"city = '{c}' and source = '{s}'")
        for c, s in CELLS
        if (c, s) != ("A", "OSM")
    )
    with pytest.raises(UnresolvableQueryException):
        _run(model, "select id order by id asc;")


def test_published_partitions_win_over_a_partial_grid():
    model = (
        CONCEPTS
        + "".join(
            _raw(c, s, f"city = '{c}' and source = '{s}'")
            for c, s in CELLS
            if (c, s) != ("B", "OSM")
        )
        + _pub("A")
        + _pub("B")
    )
    for query in ("select id order by id asc;", "select city, count(id) -> n;"):
        rows, sql = _run(model, query)
        assert "pub_A" in sql and "pub_B" in sql
        assert not any(f"raw_{c}_{s}" in sql for c, s in CELLS)
    rows, _ = _run(model, "select id order by id asc;")
    assert len(rows) == 8


def test_city_filter_still_reads_that_city_alone():
    rows, sql = _run(FULL_GRID, "select id where city = 'A' order by id asc;")
    assert [r[0] for r in rows] == ["A-MUN-1", "A-MUN-2", "A-OSM-1", "A-OSM-2"]
    assert "raw_B_MUN" not in sql and "raw_B_OSM" not in sql
