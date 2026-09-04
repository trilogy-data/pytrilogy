"""An aggregate grouped by a DERIVED axis (`min(x) by cell`, cell computed from
row properties) joined back to the rows it came from.

The FINAL merge re-sources its row contributor to the keys it projects, and the
join-key carry synthesizes the sibling's axis onto it. That carry widened only
plain projections and merges: a row-preserving FilterNode (the inline filter's
CASE projection) and a UnionNode (partitioned `complete where` sources) were
skipped, so the axis was never rendered on the row side and the merge raised
the keyless-join guard. Which spelling failed depended on which node kind
happened to sit at the row side of the merge.
"""

import pytest

from trilogy import Dialects, Environment

SINGLE = """
key id string;
property id.src string;
property id.lat float;
property id.lon float;

datasource rows (id: id, src: src, lat: lat, lon: lon)
grain (id)
query '''
select * from (values ('a', 'm', 1.0, 1.0), ('b', 'o', 1.0, 1.0), ('c', 'o', 2.0, 2.0))
as t(id, src, lat, lon)
''';

auto cell <- concat(cast(floor(lat) as string), ':', cast(floor(lon) as string));
auto anchor <- min(id ? src != 'o') by cell;
auto anchor_any <- min(id) by cell;
"""

PARTITIONED = """
key city enum<string>['A'];
key id string;
key source enum<string>['municipal', 'community', 'osm'];
property id.cell string;

root partial datasource municipal (id: id, city: city, source: source, cell: cell)
grain (id)
complete where city = 'A' and source = 'municipal'
query '''select * from (values ('tem-1', 'A', 'municipal', 'c1'), ('tem-2', 'A', 'municipal', 'c2')) as t(id, city, source, cell)''';

root partial datasource community (id: id, city: city, source: source, cell: cell)
grain (id)
complete where city = 'A' and source = 'community'
query '''select * from (values ('community-1', 'A', 'community', 'c2')) as t(id, city, source, cell)''';

root partial datasource osm (id: id, city: city, source: source, cell: cell)
grain (id)
complete where city = 'A' and source = 'osm'
query '''select * from (values ('osm-1', 'A', 'osm', 'c1'), ('osm-2', 'A', 'osm', 'c3')) as t(id, city, source, cell)''';

auto anchor <- min(id ? source != 'osm') by cell;
auto row_key <- concat(id, '#');
auto self_by_key <- min(id ? source = 'municipal') by id;
auto self_by_alias <- min(id ? source = 'municipal') by row_key;
auto c_by_key <- coalesce(self_by_key, anchor, id);
auto c_by_alias <- coalesce(self_by_alias, anchor, id);
auto is_primary <- id = c_by_alias;
"""


def _run(model: str, query: str) -> list[tuple]:
    env = Environment()
    env.parse(model)
    executor = Dialects.DUCK_DB.default_executor(environment=env)
    return executor.execute_text(query)[-1].fetchall()


@pytest.mark.parametrize(
    "query,expected",
    [
        (
            "select id, anchor order by id asc;",
            [("a", "a"), ("b", "a"), ("c", None)],
        ),
        (
            "select id, coalesce(anchor, id) as c order by id asc;",
            [("a", "a"), ("b", "a"), ("c", "c")],
        ),
        (
            "select id, anchor where id != 'zz' order by id asc;",
            [("a", "a"), ("b", "a"), ("c", None)],
        ),
        (
            "select id, anchor_any order by id asc;",
            [("a", "a"), ("b", "a"), ("c", "c")],
        ),
    ],
)
def test_filtered_aggregate_over_derived_axis_rejoins_rows(query, expected):
    assert _run(SINGLE, query) == expected


PARTITIONED_CLUSTERS = [
    ("community-1", "community-1"),
    ("osm-1", "tem-1"),
    ("osm-2", "osm-2"),
    ("tem-1", "tem-1"),
    ("tem-2", "tem-2"),
]


@pytest.mark.parametrize("concept", ["c_by_key", "c_by_alias"])
@pytest.mark.parametrize("where", ["", "where city = 'A'"])
def test_partitioned_sources_rejoin_on_aliased_axis(concept, where):
    rows = _run(PARTITIONED, f"select id, {concept} {where} order by id asc;")
    assert rows == PARTITIONED_CLUSTERS


def test_partitioned_sources_rejoin_survivor_flag():
    rows = _run(PARTITIONED, "select id, is_primary order by id asc;")
    assert rows == [(i, i == c) for i, c in PARTITIONED_CLUSTERS]


def test_partitioned_sources_prune_to_cluster_anchors():
    rows = _run(
        PARTITIONED,
        "select id, c_by_alias where id = c_by_alias order by id asc;",
    )
    assert rows == [(i, c) for i, c in PARTITIONED_CLUSTERS if i == c]
