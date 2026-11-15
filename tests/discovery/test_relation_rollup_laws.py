"""`formal/Trilogy/Cover.lean` rule 6: an aggregate by a value the summary key
relates to through a list (an ecoregion of a species' range, its biome) is the
summary joined to the DISTINCT (species, value) pairs and re-summed
(`Cover.keyed_relation_count`, `Cover.distinct_pairs_resum_exact`). A pair
listed twice counts once (`Cover.repeated_element_counts_once`); a species with
an empty or NULL list keeps its rows under a NULL value, as the tree rows do.

Random trees and lists (repeats, empty, NULL, unlisted species) are read with
and without a `(city, species)` summary storing the count, sum and max; both
must answer alike, and the summary must be read wherever no WHERE applies."""

from functools import partial

import pytest

from tests.helpers.planned_sides import every_side_planned, record_planned_sides
from tests.unnest.test_unnest_filter_laws import (
    REGIONS,
    Data,
    declared_unique,
    load,
    random_data,
    without_repeats,
)
from trilogy import Dialects
from trilogy.core.processing import join_resolution

SEEDS = range(10)

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

SUMMARY = """
auto n_trees <- count(tree_id);
auto total_height <- sum(height);
auto top_height <- max(height);
datasource summary (city, species, n_trees, total_height, top_height)
grain (city, species)
query '''select city, species, count(*) as n_trees, sum(height) as total_height,
max(height) as top_height from trees group by 1, 2''';
"""

EQUAL = "merge tree_eco_region into ecoregion_id;\n"

# (model, the related value, the `load` spelling that sizes `eco`, a literal)
SPELLINGS = {
    "element": (BASE, "tree_eco_region", "plain", "1"),
    "merged": (BASE + EQUAL, "ecoregion_id", "equal", "1"),
    "property": (BASE + EQUAL, "biome", "equal", "'wet'"),
}

SHAPES = {
    "count": "{v}, count(tree_id) as n",
    "sum": "{v}, sum(height) as s",
    "max": "{v}, max(height) as m",
    "beside_city": "city, {v}, count(tree_id) as n",
    "beside_key": "species, {v}, sum(height) as s",
    "two": "{v}, count(tree_id) as n, sum(height) as s",
}

FILTERS = {
    "none": "",
    "city": "city = 'A'",
    "species": "species != 'oak'",
    "value": "{v} = {k}",
}


# served with no WHERE once planned: a key beside a merged related value reads
# the trees (right, a miss)
OPEN = {("merged", "beside_key")}


def _executor(model: str, data: Data, spelling: str):
    executor = Dialects.DUCK_DB.default_executor()
    load(executor, data, spelling)
    executor.parse_text(model)
    return executor


def _rows(executor, query: str) -> tuple[str, list[tuple]]:
    sql = executor.generate_sql(query)[-1]
    return sql, sorted(
        (tuple(r) for r in executor.execute_raw_sql(sql).fetchall()), key=repr
    )


@pytest.mark.parametrize("where", list(FILTERS))
@pytest.mark.parametrize("shape", list(SHAPES))
@pytest.mark.parametrize("spelling", list(SPELLINGS))
@pytest.mark.parametrize("seed", SEEDS)
def test_relation_reads_the_summary(seed, spelling, shape, where):
    model, value, sizing, literal = SPELLINGS[spelling]
    data = random_data(seed)
    condition = FILTERS[where].format(v=value, k=literal)
    query = (
        f"select {SHAPES[shape].format(v=value)}"
        f"{' where ' + condition if condition else ''};"
    )
    _, expected = _rows(_executor(model, data, sizing), query)
    sql, got = _rows(_executor(model + SUMMARY, data, sizing), query)
    assert got == expected, (query, sql)
    served = where == "none" and (spelling, shape) not in OPEN
    assert ('"summary"' in sql and '"trees"' not in sql) == served, sql


def test_relation_data_repeats_an_element():
    """The seeds exercise the shapes the rule is about."""
    lists = [random_data(seed).lists for seed in SEEDS]
    flat = [v for ls in lists for v in ls.values()]
    assert any(v and len(set(v)) < len(v) for v in flat)
    assert any(v == [] for v in flat) and any(v is None for v in flat)
    assert any(len(ls) < 5 for ls in lists)
    assert set(REGIONS) == {r for v in flat for r in v or []}


ECO_SUMMARY = """
datasource summary_eco (city, ecoregion_id, n_trees) grain (city, ecoregion_id)
query '''select city, e as ecoregion_id, count(*) as n_trees from trees
join (select distinct species, unnest(native_ecoregions) as e from enrichment) using (species)
group by 1, 2''';
"""


@pytest.mark.parametrize("seed", SEEDS)
def test_a_summary_binding_the_value_is_read_as_stored(seed):
    """A table that stores the count by the related value needs no relation
    join: the search reads it, not the species summary."""
    data = random_data(seed)
    model = BASE + EQUAL + SUMMARY + ECO_SUMMARY
    query = (
        "select ecoregion_id, count(tree_id) as n"
        " where ecoregion_id is not null having n > 0;"
    )
    _, expected = _rows(_executor(BASE + EQUAL, data, "equal"), query)
    sql, got = _rows(_executor(model, data, "equal"), query)
    assert got == expected, sql
    assert '"summary_eco"' in sql and '"summary"' not in sql, sql


@pytest.mark.parametrize("seed", SEEDS)
def test_a_partial_summary_is_not_read(seed):
    """A summary missing rows cannot answer for them: the trees do."""
    data = random_data(seed)
    query = "select tree_eco_region, count(tree_id) as n;"
    _, expected = _rows(_executor(BASE, data, "plain"), query)
    partial = SUMMARY.replace("datasource summary", "partial datasource summary")
    sql, got = _rows(_executor(BASE + partial, data, "plain"), query)
    assert got == expected, sql
    assert '"summary"' not in sql, sql


@pytest.mark.parametrize("shape", list(SHAPES))
@pytest.mark.parametrize("spelling", list(SPELLINGS))
@pytest.mark.parametrize("seed", SEEDS)
def test_unique_list_pairs_need_no_distinct(seed, spelling, shape):
    """Over a list declared `::unique`, the summary's pairs are distinct as
    listed: the relation join reads them without a DISTINCT, and a property of
    the element (two ecoregions share a biome) still dedups."""
    model, value, sizing, _ = SPELLINGS[spelling]
    data = without_repeats(random_data(seed))
    query = f"select {SHAPES[shape].format(v=value)};"
    _, expected = _rows(_executor(model, data, sizing), query)
    plain_sql, _ = _rows(_executor(model + SUMMARY, data, sizing), query)
    sql, got = _rows(_executor(declared_unique(model) + SUMMARY, data, sizing), query)
    assert got == expected, (query, sql)
    if (spelling, shape) in OPEN:
        return
    assert '"summary"' in sql and '"trees"' not in sql, sql
    fewer = sql.upper().count("GROUP BY") < plain_sql.upper().count("GROUP BY")
    assert fewer == (spelling != "property"), sql


def test_elided_passthrough_keeps_its_merge_planned(monkeypatch):
    stated = record_planned_sides(monkeypatch)
    model, value, sizing, _ = SPELLINGS["element"]
    _rows(
        _executor(model, random_data(0), sizing),
        f"select {SHAPES['count'].format(v=value)} where city = 'A';",
    )
    assert every_side_planned(stated)


@pytest.mark.parametrize("spelling", list(SPELLINGS))
def test_the_relation_join_is_planned(monkeypatch, spelling):
    stated = record_planned_sides(monkeypatch)
    model, value, sizing, _ = SPELLINGS[spelling]
    sql, _ = _rows(
        _executor(model + SUMMARY, random_data(0), sizing),
        f"select {SHAPES['beside_city'].format(v=value)};",
    )
    assert '"summary"' in sql, sql
    assert every_side_planned(stated)


# trees and lists over the same species, as the model's complete bindings say
CONFORMING = [
    seed
    for seed in range(40)
    if {t[2] for t in random_data(seed).trees} == set(random_data(seed).lists)
][:3]


def _blind_to_partials(score, x, **kwargs):
    return score(x, **{**kwargs, "root": "", "multi_partial": False})


@pytest.mark.parametrize("where", list(FILTERS))
@pytest.mark.parametrize("spelling", list(SPELLINGS))
def test_the_walk_order_decides_no_rows(monkeypatch, spelling, where):
    """The walk's partial term picks plan shape only: a `~` side joined
    first (an unnest of the lists beside the trees) answers alike."""
    model, value, sizing, literal = SPELLINGS[spelling]
    condition = FILTERS[where].format(v=value, k=literal)
    blind = partial(_blind_to_partials, join_resolution._score_join_candidate)
    for select in SHAPES.values():
        query = (
            f"select {select.format(v=value)}"
            f"{' where ' + condition if condition else ''};"
        )
        for seed in CONFORMING:
            data = random_data(seed)
            _, walked = _rows(_executor(model, data, sizing), query)
            with monkeypatch.context() as m:
                m.setattr(join_resolution, "_score_join_candidate", blind)
                sql, blind_rows = _rows(_executor(model, data, sizing), query)
            assert blind_rows == walked, (query, seed, sql)


ELEMENT_ORACLE = """select e, count(distinct tree_id) from trees t
left join enrichment using (species)
left join lateral (select unnest(native_ecoregions) as e) on true
where {where} group by 1"""


@pytest.mark.parametrize("where", ["city = 'A'", "species != 'oak'"])
@pytest.mark.parametrize("seed", CONFORMING)
def test_a_filtered_relation_keeps_the_empty_list_group(seed, where):
    """A tree whose species lists no ecoregion counts under a NULL one, with
    a WHERE as without."""
    model, value, sizing, _ = SPELLINGS["element"]
    executor = _executor(model, random_data(seed), sizing)
    sql, got = _rows(executor, f"select {value}, count(tree_id) as n where {where};")
    expected = executor.execute_raw_sql(ELEMENT_ORACLE.format(where=where))
    assert got == sorted((tuple(r) for r in expected.fetchall()), key=repr), sql
