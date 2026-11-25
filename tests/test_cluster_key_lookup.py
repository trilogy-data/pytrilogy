"""A per-row side paired with aggregates keyed by a derived cluster (#1008).

Trees are clustered by a grid cell (`cluster_id`: the first municipal tree of
the cell, else the tree itself), every cluster value is an aggregate by
`cluster_id`, and each tree publishes its cluster's values beside its own
reported correction. The per-tree side holds `tree_id`, not the cluster:
`cluster_id` is computed off aggregates, so it is looked up from the node that
computes it, never re-derived on the tree's own rows (where each
`min(...) by cell` would read one row). The cell is per city (`cell_deg`):
the `where city = ...` every partitioned query carries pins the city, so the
tree determines its cell and the lookup joins on the two.

0.3.505 refused to re-derive the cluster on the tree's rows (#1004) and the
merge cross-joined; 0.3.504 re-derived it, published each tree as its own
cluster and dropped the trees it absorbed. Every case is checked against rows
computed in Python."""

import random
from collections import defaultdict
from decimal import Decimal
from itertools import combinations

import pytest

from tests.helpers.rows import executor_for, fetch_rows, normalize_numbers
from trilogy.core.enums import JoinType
from trilogy.core.models.execute import Join, QueryDatasource
from trilogy.core.query_processor import get_query_datasources
from trilogy.executor import Executor

TEMPLATE = """
key city enum<string>['X', 'Y'];
property city.cell_deg float;
datasource cities (city, cell_deg) grain (city)
query '''{cities}''';

key tree_id string;
key src enum<string>['MUNI', 'OSM_A'];
property tree_id.raw_lat float;
property tree_id.source_label string;
property tree_id.latitude float;
property tree_id.source_latitude float;
property tree_id.position_adjustment string;
{partitions}

auto src_label <- concat(src, '');
merge src_label into source_label;
auto source_class <- case when substring(source_label, 1, 4) = 'OSM_'
    then 'osm' else 'municipal' end;

auto cell <- cast(floor(raw_lat / cell_deg) as int);
auto anchor <- min(tree_id ? source_class = 'municipal') by cell;
auto cluster_id <- coalesce(anchor, tree_id);

key modified_tree_id string;
property modified_tree_id.modified_lat float;
datasource mods (modified_tree_id, modified_lat) grain (modified_tree_id)
query '''{mods}''';
merge modified_tree_id into ~tree_id;

auto snap_cell <- cast(floor(raw_lat * 10) as int);
property snap_cell.snap_lat float;
property snap_cell.snap_reason string;
datasource snaps (snap_cell, snap_lat, snap_reason) grain (snap_cell)
query '''{snaps}''';
auto placed_lat <- coalesce(snap_lat, raw_lat);

def by_source(value) -> coalesce(
    (max(value ? source_class = 'municipal') by cluster_id),
    (max(value ? source_class = 'osm') by cluster_id)
);
auto merged_lat <- coalesce(modified_lat, @by_source(placed_lat));
auto merged_adjustment <- case when modified_lat is not null then 'reported'
    else @by_source(snap_reason) end;
auto merged_source_lat <- @by_source(raw_lat);
auto merged_sources <- array_to_string(
    array_sort(array_distinct((array_agg(source_label) by cluster_id))), ',');
merge merged_lat into latitude;
merge merged_adjustment into position_adjustment;
merge merged_source_lat into source_latitude;
"""

PARTITION = """
partial datasource trees_{city}_{src} (tree_id, city, src, raw_lat)
grain (tree_id)
complete where city = '{city}' and src = '{src}'
query '''{rows}''';"""

CELL_DEG = {"X": 1.0, "Y": 2.0}
SOURCES = ["MUNI", "OSM_A"]
COLUMNS = [
    "cluster_id",
    "latitude",
    "source_latitude",
    "position_adjustment",
    "merged_sources",
]
SUBSETS = [
    list(combo)
    for size in range(1, len(COLUMNS) + 1)
    for combo in combinations(COLUMNS, size)
]

Tree = tuple[str, str, str, float]
Snap = tuple[int, float | None, str | None]

# X cells are [1, 4), Y cells [5, 10): a cell never spans both cities
TREES: list[Tree] = [
    ("a", "X", "MUNI", 1.21),
    ("b", "X", "OSM_A", 1.75),
    ("c", "X", "OSM_A", 3.33),
    ("f", "X", "MUNI", 1.5),
    ("d", "Y", "MUNI", 15.05),
    ("e", "Y", "OSM_A", 14.5),
    ("g", "Y", "OSM_A", 19.0),
]
MODS = {"c": 9.9, "e": 14.2}


def snaps_for(rng: random.Random | None = None) -> list[Snap]:
    """Every snap cell, with a value on some: a missing cell would be a row
    the declared-complete `snaps` says cannot exist."""
    pick = rng.random if rng else (lambda: 0.0)
    return [
        (
            (s, round(s / 10 + 0.01, 2), "road" if s % 2 else "building")
            if (s % 3 == 0 if rng is None else pick() < 0.4)
            else (s, None, None)
        )
        for s in range(200)
    ]


SNAPS = snaps_for()


def _values(rows: list[tuple], columns: str, typed: tuple = ()) -> str:
    """Inline rows; none at all are a `typed` row filtered away."""

    def lit(v) -> str:
        if v is None:
            return "NULL"
        return f"'{v}'" if isinstance(v, str) else repr(v)

    body = ", ".join(
        "(" + ", ".join(lit(v) for v in row) + ")" for row in rows or [typed]
    )
    return f"select * from (values {body}) t({columns})" + (
        "" if rows else " where false"
    )


def model(trees: list[Tree], mods: dict[str, float], snaps: list[Snap]) -> str:
    partitions = []
    for city in CELL_DEG:
        for src in SOURCES:
            rows = [t for t in trees if t[1] == city and t[2] == src]
            values = _values(rows, "tree_id, city, src, raw_lat", ("", city, src, 0.0))
            partitions.append(PARTITION.format(city=city, src=src, rows=values))
    return TEMPLATE.format(
        cities=_values(list(CELL_DEG.items()), "city, cell_deg"),
        partitions="\n".join(partitions),
        mods=_values(list(mods.items()), "modified_tree_id, modified_lat", ("", 0.0)),
        snaps=_values(snaps, "snap_cell, snap_lat, snap_reason"),
    )


def query(columns: list[str], city: str) -> str:
    return f"select tree_id, {', '.join(columns)} where city = '{city}';"


def expected(
    trees: list[Tree],
    mods: dict[str, float],
    snaps: list[Snap],
    columns: list[str],
    city: str,
) -> list[tuple]:
    rows = [t for t in trees if t[1] == city]
    snap = {s: (lat, reason) for s, lat, reason in snaps}

    def cell(t: Tree) -> int:
        return int(Decimal(str(t[3])) // Decimal(str(CELL_DEG[t[1]])))

    def snapped(t: Tree) -> tuple[float | None, str | None]:
        return snap[int(Decimal(str(t[3])) * 10 // 1)]

    def klass(t: Tree) -> str:
        return "osm" if t[2].startswith("OSM_") else "municipal"

    anchors: dict[int, str] = {}
    for t in rows:
        if klass(t) == "municipal":
            anchors[cell(t)] = min(anchors.get(cell(t), t[0]), t[0])
    cluster = {t[0]: anchors.get(cell(t), t[0]) for t in rows}
    members: dict[str, list[Tree]] = defaultdict(list)
    for t in rows:
        members[cluster[t[0]]].append(t)

    def by_source(cid: str, value) -> object:
        for source_class in ("municipal", "osm"):
            found = [
                v
                for t in members[cid]
                if klass(t) == source_class and (v := value(t)) is not None
            ]
            if found:
                return max(found)
        return None

    def placed(t: Tree) -> float:
        lat = snapped(t)[0]
        return t[3] if lat is None else lat

    out = []
    for t in rows:
        cid = cluster[t[0]]
        modified = mods.get(t[0])
        values = {
            "cluster_id": cid,
            "latitude": modified if modified is not None else by_source(cid, placed),
            "source_latitude": by_source(cid, lambda m: m[3]),
            "position_adjustment": (
                "reported"
                if modified is not None
                else by_source(cid, lambda m: snapped(m)[1])
            ),
            "merged_sources": ",".join(sorted({m[2] for m in members[cid]})),
        }
        out.append((t[0], *(values[c] for c in columns)))
    return normalize_numbers(out)


def draw(seed: int) -> tuple[list[Tree], dict[str, float], list[Snap]]:
    rng = random.Random(seed)
    trees = [
        (
            f"t{i}",
            city := rng.choice(["X", "Y"]),
            rng.choice(SOURCES),
            round(rng.uniform(1, 4) if city == "X" else rng.uniform(10, 19.9), 2),
        )
        for i in range(rng.randint(2, 12))
    ]
    mods = {
        t[0]: round(rng.uniform(1, 20), 2) for t in rng.sample(trees, rng.randint(0, 2))
    }
    return trees, mods, snaps_for(rng)


@pytest.fixture(scope="module")
def fixed() -> Executor:
    executor = executor_for(model(TREES, MODS, SNAPS))
    yield executor
    executor.close()


def test_issue_query_publishes_each_tree_with_its_cluster(fixed: Executor):
    rows = fetch_rows(fixed, query(["latitude"], "X"))
    assert normalize_numbers(rows) == normalize_numbers(
        [("a", 1.51), ("b", 1.51), ("c", 9.9), ("f", 1.51)]
    )


@pytest.mark.parametrize("city", list(CELL_DEG))
@pytest.mark.parametrize("columns", SUBSETS, ids="+".join)
def test_cluster_values_match_rows(fixed: Executor, columns: list[str], city):
    rows = fetch_rows(fixed, query(columns, city))
    assert normalize_numbers(rows) == expected(TREES, MODS, SNAPS, columns, city)


def _directional_joins(ds: QueryDatasource) -> list[str]:
    out = [
        join.right.identifier
        for join in ds.joins
        if isinstance(join, Join)
        and join.join_type in (JoinType.LEFT_OUTER, JoinType.RIGHT_OUTER)
    ]
    for source in ds.datasources:
        if isinstance(source, QueryDatasource):
            out += _directional_joins(source)
    return out


def test_the_cluster_lookup_is_planned_inner(fixed: Executor):
    """The snap-reason merge FULL joins two tree branches, the city filter
    forcing the stream, and LEFT joins the cluster lookup its consumers join
    on `cluster_id`: both sides are forced, so the planner joins it INNER
    rather than leaving the LEFT to the optimizer."""
    env = fixed.environment
    _, statements = env.parse(query(COLUMNS[:2] + COLUMNS[3:], "X"))
    outer = _directional_joins(get_query_datasources(env, statements[-1]))
    assert all(name.startswith("mods") for name in outer), outer


@pytest.mark.parametrize("columns", [["merged_sources"], ["latitude"]])
def test_a_cluster_value_by_its_cluster(fixed: Executor, columns: list[str]):
    rows = fetch_rows(
        fixed, f"select cluster_id, {', '.join(columns)} where city = 'X';"
    )
    # a reported correction is per tree, so a cluster carries each one
    per_tree = expected(TREES, MODS, SNAPS, ["cluster_id", *columns], "X")
    assert normalize_numbers(rows) == normalize_numbers({r[1:] for r in per_tree})


@pytest.mark.parametrize("seed", range(10))
def test_cluster_values_random_rows(seed: int):
    trees, mods, snaps = draw(seed)
    executor = executor_for(model(trees, mods, snaps))
    try:
        for city in CELL_DEG:
            for columns in (COLUMNS, ["latitude"], ["position_adjustment"]):
                rows = fetch_rows(executor, query(columns, city))
                assert normalize_numbers(rows) == expected(
                    trees, mods, snaps, columns, city
                ), (seed, city, columns)
    finally:
        executor.close()


FLAT = """
key tree_id string;
property tree_id.raw_lat float;
property tree_id.source_label string;
key src string;
datasource raw (tree_id, src, raw_lat) grain (tree_id)
query '''{rows}''';
{label}
auto cell <- floor(raw_lat);
{cluster}
auto agg <- {agg};
"""
LABELS = {
    "stored": "merge src into source_label;",
    "merged": "auto src_label <- concat(src, '');\nmerge src_label into source_label;",
}
CLUSTERS = {
    "anchor_or_self": "auto anchor <- min(tree_id ? substring(source_label, 1, 1) = 'M')"
    " by cell;\nauto cluster_id <- coalesce(anchor, tree_id);",
    "first_or_self": "auto anchor <- min(tree_id) by cell;\n"
    "auto cluster_id <- coalesce(anchor, tree_id);",
    "first": "auto cluster_id <- min(tree_id) by cell;",
    "cell": "auto cluster_id <- cell;",
}
AGGS = {
    "count": "count(source_label) by cluster_id",
    "max": "max(source_label) by cluster_id",
    "sources": "array_to_string(array_sort(array_distinct("
    "(array_agg(source_label) by cluster_id))), ',')",
}
FLAT_ROWS = [
    ("a", "M_A", 1.5),
    ("b", "O_B", 1.7),
    ("c", "O_B", 3.2),
    ("d", "O_C", 3.9),
    ("e", "M_A", 5.1),
    ("f", "M_B", 5.4),
]


def flat_expected(cluster: str, agg: str, by: str) -> list[tuple]:
    cells: dict[int, list[tuple]] = defaultdict(list)
    for row in FLAT_ROWS:
        cells[int(row[2])].append(row)

    def cluster_of(row: tuple) -> object:
        members = cells[int(row[2])]
        municipal = [m[0] for m in members if m[1].startswith("M")]
        return {
            "anchor_or_self": min(municipal) if municipal else row[0],
            "first_or_self": min(m[0] for m in members),
            "first": min(m[0] for m in members),
            "cell": int(row[2]),
        }[cluster]

    labels: dict[object, list[str]] = defaultdict(list)
    for row in FLAT_ROWS:
        labels[cluster_of(row)].append(row[1])
    value = {
        "count": len,
        "max": max,
        "sources": lambda ls: ",".join(sorted(set(ls))),
    }[agg]
    return normalize_numbers(
        {
            (
                row[0] if by == "tree_id" else cluster_of(row),
                value(labels[cluster_of(row)]),
            )
            for row in FLAT_ROWS
        }
    )


@pytest.mark.parametrize("by", ["tree_id", "cluster_id"])
@pytest.mark.parametrize("agg", list(AGGS))
@pytest.mark.parametrize("cluster", list(CLUSTERS))
@pytest.mark.parametrize("label", list(LABELS))
def test_an_aggregate_by_a_derived_cluster(label: str, cluster: str, agg: str, by: str):
    """A cluster computed off an aggregate by a per-row cell, and a value
    aggregated by the cluster over a label stored or merged from a
    derivation: read per tree, the tree side looks its cluster up by the cell
    it determines; read per cluster, the label's producer is the aggregate's
    input even though it names the label by its merged spelling."""
    executor = executor_for(
        FLAT.format(
            rows=_values(FLAT_ROWS, "tree_id, src, raw_lat"),
            label=LABELS[label],
            cluster=CLUSTERS[cluster],
            agg=AGGS[agg],
        )
    )
    try:
        rows = fetch_rows(executor, f"select {by}, agg;")
        assert normalize_numbers(rows) == flat_expected(cluster, agg, by)
    finally:
        executor.close()
