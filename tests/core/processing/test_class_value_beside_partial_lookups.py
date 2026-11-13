"""A class value at species grain, derived from the species and a species
property reached through partial lookups, read beside the trees (#972). Each
plan reads the class once per species and joins it back to the trees on
species; no CTE repeats a tree, and a WHERE on per-tree columns neither
multiplies the class totals' table by the kept trees nor keeps a species whose
trees it dropped."""

import re
from dataclasses import replace

import pytest

from tests.helpers.planning import plan
from tests.helpers.rows import (
    CTE_NAME,
    as_float,
    cte_rows,
    executor_for,
    fetch_rows,
    widest_cte,
)
from trilogy.core.enums import Derivation, JoinType
from trilogy.core.processing.join_resolution import (
    JoinFacts,
    SideFacts,
    _dimension_of,
    get_join_type,
)
from trilogy.core.processing.v4_helper.group_behaviors import behavior_for
from trilogy.core.processing.v4_helper.group_graph import GroupFacts, _offered_at_grain

MODEL = """
key tree_id string;
key species string;
key genus string;
property tree_id.city string;
property tree_id.dbh float;
property tree_id.height_m float;
property tree_id.is_imagery bool;
property species.tree_form string;
property genus.height_a float;
property genus.crown_b float;
property genus.crown_level string;

datasource trees (tree_id, city, species, dbh, height_m: ?height_m,
    is_imagery: ?is_imagery) grain (tree_id)
query '''select * from (values
('t1','bos','acer rubrum',10.0, 5.0, false),
('t2','bos','pinus strobus',20.0, null, true),
('t3','bos','acer rubrum',30.0, null, false),
('t4','bos','acer rubrum',40.0, 7.0, true),
('t5','sfo','acer rubrum',50.0, null, null),
('t6','bos','acer saccharum',60.0, null, null),
('t7','bos','novus',70.0, null, null)
) t(tree_id, city, species, dbh, height_m, is_imagery)''';

datasource enrichment (species: ~species, genus: ?genus, tree_form: ?tree_form)
grain (species)
query '''select * from (values ('acer rubrum','acer','broadleaf'),
('pinus strobus','pinus','conifer'), ('acer saccharum','acer','broadleaf'))
t(species, genus, tree_form)''';

datasource height_fits (taxon: ~genus, a: height_a) grain (genus)
query '''select * from (values ('acer', 2.0)) t(taxon, a)''';

datasource crown_fits (level: crown_level, taxon: ~genus, b: crown_b) grain (genus)
query '''select * from (values ('genus', 'pinus', 3.0)) t(level, taxon, b)'''
where crown_level = 'genus';

key fit_class string;
property fit_class.fit_a float;
property fit_class.fit_b float;
datasource class_fits (taxon: fit_class, a: fit_a, b: fit_b) grain (fit_class)
query '''select * from (values ('Angiosperm', 1.0, 10.0), ('Gymnosperm', 4.0, 40.0),
('all', 5.0, 50.0)) t(taxon, a, b)''';

auto fallback_class <- case
    when species = 'novus' then 'all'
    when tree_form = 'conifer' then 'Gymnosperm'
    else 'Angiosperm'
end;
def class_row(value, cls) -> max(value ? fit_class = cls) by *;
def for_class(value) -> case
    when fallback_class = 'Angiosperm' then @class_row(value, 'Angiosperm')
    when fallback_class = 'Gymnosperm' then @class_row(value, 'Gymnosperm')
    when fallback_class = 'all' then @class_row(value, 'all')
end;
auto class_a <- @for_class(fit_a);
auto class_b <- @for_class(fit_b);
auto tree_a <- coalesce(height_a, class_a);
auto tree_b <- coalesce(crown_b, class_b);
auto pred_h <- dbh * tree_a;
auto pred_c <- dbh * tree_b;
auto species_city_n <- count(tree_id) by city, species;
auto rare <- species_city_n < 2;
"""

TREES = 7
WHERE = " where coalesce(is_imagery, false) = false or height_m is not null"
# tree_id, city, species, genus, pred_h, pred_c, rare (over every tree)
ROWS = {
    "t1": ("bos", "acer rubrum", "acer", 20.0, 100.0, False),
    "t2": ("bos", "pinus strobus", "pinus", 80.0, 60.0, True),
    "t3": ("bos", "acer rubrum", "acer", 60.0, 300.0, False),
    "t4": ("bos", "acer rubrum", "acer", 80.0, 400.0, False),
    "t5": ("sfo", "acer rubrum", "acer", 100.0, 500.0, True),
    "t6": ("bos", "acer saccharum", "acer", 120.0, 600.0, True),
    "t7": ("bos", "novus", None, 350.0, 3500.0, True),
}
# under WHERE: t2 is dropped, so pinus strobus has no tree; the bos acer
# rubrum trees t1, t3, t4 all stay
KEPT = [t for t in ROWS if t != "t2"]


def _expected(trees: list[str], columns: list[str]) -> list[tuple]:
    names = ["city", "species", "genus", "pred_h", "pred_c", "rare"]
    return sorted(
        tuple(t if c == "tree_id" else ROWS[t][names.index(c)] for c in columns)
        for t in trees
    )


CASES = [
    (["tree_id", "pred_h"], ""),
    (["tree_id", "city", "species", "pred_h", "pred_c"], ""),
    (["tree_id", "city", "species", "genus", "pred_h", "pred_c", "rare"], ""),
    (["tree_id", "pred_h"], WHERE),
    (["tree_id", "city", "species", "pred_h", "pred_c"], WHERE),
    (["tree_id", "city", "species", "genus", "pred_h", "pred_c"], WHERE),
]


@pytest.fixture(scope="module")
def executor():
    executor = executor_for(MODEL)
    yield executor
    executor.close()


@pytest.mark.parametrize("columns,where", CASES)
def test_each_tree_once(executor, columns: list[str], where: str) -> None:
    query = f"select {', '.join(columns)}{where};"
    sql = executor.generate_sql(query)[-1]
    rows = sorted(tuple(as_float(v) for v in r) for r in fetch_rows(executor, query))
    trees = KEPT if where else list(ROWS)
    assert rows == _expected(trees, columns), sql
    assert widest_cte(executor, sql)[0] <= TREES, sql


def test_rare_reads_the_kept_trees(executor) -> None:
    query = f"select tree_id, rare{WHERE};"
    rows = dict(fetch_rows(executor, query))
    assert set(rows) == set(KEPT)
    assert rows["t1"] is False and rows["t6"] is True


@pytest.mark.parametrize(
    "query,expected",
    [
        ("select sum(pred_h) as v;", [(810.0,)]),
        (f"select count(pred_c) as v{WHERE};", [(6,)]),
        ("select max(pred_c) as v where dbh > 100;", []),
    ],
)
def test_a_total_over_the_predictions(executor, query: str, expected) -> None:
    sql = executor.generate_sql(query)[-1]
    assert [tuple(as_float(v) for v in r) for r in fetch_rows(executor, query)] == (
        expected
    ), sql
    assert widest_cte(executor, sql)[0] <= TREES, sql


def test_the_class_is_computed_once_per_species(executor) -> None:
    sql = executor.generate_sql("select tree_id, pred_h;")[-1]
    computing = [
        name
        for name in CTE_NAME.findall(sql)
        if re.search(
            rf"^{name} as \((?:(?!\n\w+ as \().)*END as \"fallback_class\"",
            sql,
            re.MULTILINE | re.DOTALL,
        )
    ]
    assert computing, sql
    for name in computing:
        assert cte_rows(executor, sql, name) == 4, sql


GATE = """
key tree_id string;
key fit_class string;
property tree_id.dbh float;
property fit_class.fit_a float;
datasource trees (tree_id, dbh) grain (tree_id)
query '''select * from (values ('t1',10.0), ('t2',20.0), ('t3',30.0)) t(tree_id, dbh)''';
datasource class_fits (taxon: fit_class, a: fit_a) grain (fit_class)
query '''select * from (values ('broadleaf', 1.0), ('conifer', 4.0)) t(taxon, a)''';
auto conifer_a <- max(fit_a ? fit_class = 'conifer') by *;
auto pred <- dbh * conifer_a;
"""


@pytest.mark.parametrize(
    "query,expected,peeled",
    [
        # a value of the total beside the kept rows: the total reads its table
        ("select tree_id, pred where dbh > 15;", [("t2", 80.0), ("t3", 120.0)], True),
        ("select sum(pred) as v where dbh > 15;", [(200.0,)], True),
        # the total alone: the WHERE gates it on whether it kept a row
        ("select conifer_a where dbh > 100;", [], False),
        ("select conifer_a where dbh > 15;", [(4.0,)], False),
    ],
)
def test_a_where_gates_a_total(query: str, expected, peeled: bool) -> None:
    executor = executor_for(GATE)
    try:
        sql = executor.generate_sql(query)[-1]
        rows = [tuple(as_float(v) for v in r) for r in fetch_rows(executor, query)]
        assert sorted(rows) == expected, sql
        if peeled:
            assert widest_cte(executor, sql)[0] <= 3, sql
    finally:
        executor.close()


def _side(grain: set[str], determined: frozenset[str] = frozenset(), **kw) -> SideFacts:
    return SideFacts(grain=frozenset(grain), determined=determined, **kw)


def test_a_dimension_is_determined_by_the_fact_rows() -> None:
    trees = _side({"tree_id"}, frozenset({"species", "genus", "city"}))
    species = _side({"species", "genus"}, frozenset({"tree_form"}))
    assert _dimension_of(species, trees)
    assert not _dimension_of(trees, species)
    assert not _dimension_of(_side(set()), trees)
    assert not _dimension_of(species, _side({"species", "genus"}))


def test_a_padded_lookup_keeps_the_fact_rows() -> None:
    lookup = _side({"species"}, nullables=frozenset({"species"}))
    fact = _side({"tree_id"}, frozenset({"species"}))
    facts = JoinFacts(sides={"lookup": lookup, "fact": fact})
    assert get_join_type("lookup", "fact", {"species"}, facts) == JoinType.RIGHT_OUTER
    assert get_join_type("fact", "lookup", {"species"}, facts) == JoinType.LEFT_OUTER
    # a NULL species the lookup holds as a value is a real row: it is kept
    valued = replace(lookup, value_nullables=frozenset({"species"}))
    facts = JoinFacts(sides={"lookup": valued, "fact": fact})
    assert get_join_type("lookup", "fact", {"species"}, facts) == JoinType.LEFT_OUTER


def _facts(primary: set[str], grain: set[str], derivation=Derivation.BASIC):
    return GroupFacts(
        primary=primary,
        grain=frozenset(grain),
        derivation=derivation,
        behavior=behavior_for(derivation),
    )


def test_a_class_step_offers_final_only_what_its_grain_holds() -> None:
    _, environment = plan(MODEL, "select tree_id, fallback_class;")
    offered = {"local.fallback_class", "local.species", "local.tree_id", "local.city"}
    fact = _facts({"local.fallback_class"}, {"local.species"})
    assert _offered_at_grain(offered, fact, environment) == {
        "local.fallback_class",
        "local.species",
    }
    # rows that expand (an unnest) or carry no grain keep every column
    unnest = _facts({"local.fallback_class"}, {"local.species"}, Derivation.UNNEST)
    assert _offered_at_grain(offered, unnest, environment) == offered
    assert _offered_at_grain(offered, _facts({"local.pred_h"}, set()), environment) == (
        offered
    )
