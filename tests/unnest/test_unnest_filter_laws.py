"""`formal/Trilogy/Unnest.lean`: an `unnest` repeats its parent row once per
element, and a row with an empty or NULL list reads its element as NULL. A
WHERE on an element keeps a parent row iff one of those elements passes
(`Unnest.filter_exists`), so a disjunct over the row alone keeps it whatever
its list holds (`Unnest.or_keeps_disjunct`). An aggregate over the kept rows
reads each once, however many of its elements pass
(`Unnest.agg_reads_each_row_once`, `Unnest.expansion_sum_overcounts`). The
elements pair with the rows they expand on the rows' identity; on the
aggregate's grouping key or on nothing, a row meets elements of other rows
(`Unnest.coarse_pair_overcounts`, `Unnest.keyless_pair_keeps_every_row`).

Each case builds random trees, a species-keyed region list (with duplicates,
empty and NULL lists, and species with no list) and holds each query to the
reference computed here. `subset` and `equal` spell the element as the key it
is merged into, `ecoregion_id`. A plain merge claims the two domains are one,
so under `equal` the `eco` table holds exactly the regions some list names; a
region no list names would be padded in with no tree."""

import random
from dataclasses import dataclass

import pytest

from trilogy import Dialects

SEEDS = range(12)
CITIES = ["A", "B", "C"]
SPECIES = ["oak", "pine", "elm", "ash", "fir"]
REGIONS = range(5)

BASE = """
key tree_id int;
key species string;
key city string;
property tree_id.height int;
key ecoregion_id int;
property ecoregion_id.biome string;
property species.native_ecoregions list<int>;
auto tree_eco_region <- unnest(native_ecoregions);

datasource trees (tree_id: tree_id, city: city, species: species, height: height)
grain (tree_id)
address trees;

root datasource enrichment (species: species, native_ecoregions: ?native_ecoregions)
grain (species)
address enrichment;

root datasource eco (ecoregion_id: ecoregion_id, biome: ?biome)
grain (ecoregion_id)
address eco;
"""

SUBSET = BASE + "merge tree_eco_region into ~ecoregion_id;\n"

# a second table binding `~species` licenses padding species, which re-sourced
# the unnest's parent as the list alone
SIGHTINGS = """
key sighting_id int;
datasource sightings (sighting_id: sighting_id, species: ~species)
grain (sighting_id)
query '''select 1 as sighting_id, 'oak' as species''';
"""

SPELLINGS = {
    "plain": (BASE, "tree_eco_region"),
    "subset": (SUBSET, "ecoregion_id"),
    "equal": (BASE + "merge tree_eco_region into ecoregion_id;\n", "ecoregion_id"),
    "licensed": (SUBSET + SIGHTINGS, "ecoregion_id"),
}


@dataclass
class Data:
    trees: list[tuple[int, str, str, int]]
    lists: dict[str, list[int] | None]
    biomes: dict[int, str]

    def elements(self, species: str) -> list[int | None]:
        return self.lists.get(species) or [None]

    def listed(self) -> set[int]:
        return {r for values in self.lists.values() for r in values or []}


def random_data(seed: int) -> Data:
    rng = random.Random(seed)
    trees = [
        (i, rng.choice(CITIES), rng.choice(SPECIES), rng.randint(1, 20))
        for i in range(rng.randint(4, 12))
    ]
    lists: dict[str, list[int] | None] = {}
    for species in SPECIES:
        roll = rng.random()
        if roll < 0.1:
            continue
        if roll < 0.2:
            lists[species] = None
        else:
            lists[species] = [rng.choice(REGIONS) for _ in range(rng.randint(0, 4))]
    biomes = {r: rng.choice(["wet", "dry"]) for r in REGIONS}
    return Data(trees, lists, biomes)


def _sql_list(values: list[int] | None) -> str:
    if values is None:
        return "NULL::int[]"
    return f"[{', '.join(map(str, values))}]::int[]"


def eco_regions(data: Data, spelling: str) -> set[int]:
    return data.listed() if spelling == "equal" else set(REGIONS)


def load(executor, data: Data, spelling: str) -> None:
    regions = eco_regions(data, spelling)
    trees = ", ".join(f"({t}, '{c}', '{s}', {h})" for t, c, s, h in data.trees)
    lists = ", ".join(f"('{s}', {_sql_list(v)})" for s, v in data.lists.items())
    biomes = ", ".join(f"({r}, '{b}')" for r, b in data.biomes.items() if r in regions)
    executor.execute_raw_sql(
        "create table trees as select * from (values "
        f"{trees}) as t(tree_id, city, species, height)"
    )
    executor.execute_raw_sql(
        "create table enrichment as select * from (values "
        f"{lists}) as t(species, native_ecoregions)"
    )
    executor.execute_raw_sql("create table eco (ecoregion_id int, biome varchar)")
    if biomes:
        executor.execute_raw_sql(f"insert into eco values {biomes}")


def predicates(rng: random.Random) -> list[tuple[str, object]]:
    k, j = rng.choice(REGIONS), rng.choice(REGIONS)
    return [
        (f"{{e}} = {k}", lambda e, t: e == k),
        (f"{{e}} in ({k}, {j})", lambda e, t: e in (k, j)),
        (f"{{e}} = {k} and city = 'A'", lambda e, t: e == k and t[1] == "A"),
        (
            f"{{e}} > {k} or city = 'B'",
            lambda e, t: (e is not None and e > k) or t[1] == "B",
        ),
    ]


def _group(rows, key, value, agg):
    out: dict = {}
    for t in rows:
        out.setdefault(key(t), []).append(value(t))
    return sorted(
        (*((k,) if not isinstance(k, tuple) else k), agg(v)) for k, v in out.items()
    )


SHAPES = {
    "rows": (
        "select tree_id where {p};",
        lambda kept: sorted((t[0],) for t in kept),
    ),
    "city_rows": (
        "select city, tree_id where {p};",
        lambda kept: sorted((t[1], t[0]) for t in kept),
    ),
    "count_by_city": (
        "select city, count(tree_id) as n where {p};",
        lambda kept: _group(kept, lambda t: t[1], lambda t: t[0], len),
    ),
    "count_by_species": (
        "select species, count(tree_id) as n where {p};",
        lambda kept: _group(kept, lambda t: t[2], lambda t: t[0], len),
    ),
    "sum_by_city": (
        "select city, sum(height) as s where {p};",
        lambda kept: _group(kept, lambda t: t[1], lambda t: t[3], sum),
    ),
    "count": (
        "select count(tree_id) as n where {p};",
        lambda kept: [(len(kept),)] if kept else [],
    ),
}


def kept_trees(data: Data, test) -> list[tuple[int, str, str, int]]:
    return [t for t in data.trees if any(test(e, t) for e in data.elements(t[2]))]


def _cases():
    for spelling in SPELLINGS:
        for shape in SHAPES:
            for seed in SEEDS:
                yield pytest.param(
                    spelling, shape, seed, id=f"{spelling}-{shape}-{seed}"
                )


def _run(executor, query: str) -> list[tuple]:
    sql = executor.generate_sql(query)[-1]
    assert "on 1=1" not in sql.lower(), (query, sql)
    return sorted(tuple(r) for r in executor.execute_raw_sql(sql).fetchall())


def _executor(spelling: str, data: Data):
    executor = Dialects.DUCK_DB.default_executor()
    load(executor, data, spelling)
    executor.parse_text(SPELLINGS[spelling][0])
    return executor


@pytest.mark.parametrize("spelling, shape, seed", list(_cases()))
def test_element_filter_keeps_rows_with_a_passing_element(spelling, shape, seed):
    template, reference = SHAPES[shape]
    data = random_data(seed)
    executor = _executor(spelling, data)
    for text, test in predicates(random.Random(seed)):
        query = template.format(p=text.format(e=SPELLINGS[spelling][1]))
        assert _run(executor, query) == reference(kept_trees(data, test)), query


@pytest.mark.parametrize("spelling", ["plain", "equal", "licensed"])
@pytest.mark.parametrize("seed", SEEDS)
def test_element_pairs_with_the_row_it_expands(spelling, seed):
    """Selected beside the row, each element pairs with its own row only, once
    per position: an element is keyed by its ordinal, so a filter keeps a
    repeated value as often as the list repeats it. A selected merged key pads
    a member of its table no tree lists."""
    data = random_data(seed)
    executor = _executor(spelling, data)
    element = SPELLINGS[spelling][1]
    for k in REGIONS:
        expected = sorted(
            (t[0], k) for t in data.trees for e in data.lists.get(t[2]) or [] if e == k
        )
        if not expected and spelling != "plain" and k in eco_regions(data, spelling):
            expected = [(None, k)]
        query = f"select tree_id, {element} where {element} = {k};"
        assert _run(executor, query) == expected, query
